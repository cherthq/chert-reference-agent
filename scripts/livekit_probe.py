"""Opt-in LiveKit probe for a customer-owned receiver and project.

Uses synthetic media only; never outputs credentials or retains caller media.
Receiver-side return audio and resource cleanup require separate observation.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import hmac
import json
import logging
import os
import time
from urllib.parse import urlsplit
import uuid

import aiohttp
import jwt
from livekit import api, rtc

from chert_reference_agent.config import Config
from chert_reference_agent.media import audio_chunk


def envelope(kind='call.incoming', call=None):
    now = datetime.now(timezone.utc).isoformat()
    return {'event_id': 'synthetic-event-' + uuid.uuid4().hex, 'type': kind,
            'created_at': now, 'data': {'call_id': call or 'synthetic-call-' + uuid.uuid4().hex,
            'line_id': 'synthetic-line', 'media': {'audio': True, 'video': True},
            'occurred_at': now}}


def signed(obj, secret, version):
    body = json.dumps(obj, separators=(',', ':')).encode()
    return body, {'X-Chert-Signature-Version': str(version),
                  'X-Chert-Signature': 'sha256=' + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest(),
                  'Content-Type': 'application/json'}


def request_payload(cfg, path, obj):
    if cfg.signing_enabled:
        keys = cfg.webhook_routes[path]
        version = max(keys)
        return signed(obj, keys[version], version)
    return json.dumps(obj, separators=(',', ':')).encode(), {'Content-Type': 'application/json'}


async def post(session, endpoint, body, headers):
    started = time.monotonic()
    async with session.post(endpoint, data=body, headers=headers, allow_redirects=False) as response:
        result = await response.json()
        return response.status, result, (time.monotonic() - started) * 1000


async def worker(url, token, identity, duration=5):
    """Independently decode remote tracks and publish only generated return PCM."""
    room = rtc.Room()
    streams, tasks = [], []
    counts = dict(audio_frames=0, non_silent_frames=0, tone_frames=0,
                  video_frames=0, video_changes=0, return_frames=0)
    source = None

    async def audio(stream):
        async for event in stream:
            samples = event.frame.data
            peak = max((abs(x) for x in samples), default=0)
            counts['audio_frames'] += 1
            counts['non_silent_frames'] += int(peak > 100)
            # Lossy codecs preclude exact PCM hashing; count approximate 440 Hz frames.
            crossings = sum(a <= 0 < b for a, b in zip(samples, samples[1:]))
            hz = crossings * event.frame.sample_rate / max(1, len(samples))
            counts['tone_frames'] += int(peak > 100 and 350 <= hz <= 550)

    async def video(stream):
        previous = None
        async for event in stream:
            digest = hashlib.sha256(event.frame.data).digest()
            counts['video_frames'] += 1
            counts['video_changes'] += int(previous is not None and previous != digest)
            previous = digest

    @room.on('track_subscribed')
    def subscribed(track, publication, participant):
        if participant.identity != identity:
            return
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            stream = rtc.AudioStream(track, capacity=8, sample_rate=48000, num_channels=1)
            consume = audio
        elif track.kind == rtc.TrackKind.KIND_VIDEO:
            stream = rtc.VideoStream(track, capacity=2, format=rtc.VideoBufferType.RGB24)
            consume = video
        else:
            return
        streams.append(stream)
        tasks.append(asyncio.create_task(consume(stream)))

    try:
        await asyncio.wait_for(room.connect(url, token), 10)
        source = rtc.AudioSource(48000, 1, queue_size_ms=100)
        track = rtc.LocalAudioTrack.create_audio_track('synthetic-return-tone', source)
        await room.local_participant.publish_track(
            track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE))
        for sequence in range(duration * 50):
            await source.capture_frame(rtc.AudioFrame(audio_chunk(sequence), 48000, 1, 960))
            counts['return_frames'] += 1
            await asyncio.sleep(0.02)
        if any(task.done() and task.exception() for task in tasks):
            raise RuntimeError('decode_failed')
        return counts
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # Attempt every release; report any failures without exposing SDK exceptions.
        closes = [stream.aclose() for stream in streams]
        if source is not None:
            closes.append(source.aclose())
        closes.append(room.disconnect())
        results = await asyncio.gather(*(asyncio.wait_for(c, 3) for c in closes), return_exceptions=True)
        if any(isinstance(r, BaseException) for r in results):
            raise RuntimeError('worker_cleanup_failed') from None


async def live_probe(cfg, endpoint):
    parsed = urlsplit(endpoint)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.port not in (None, 443)):
        raise ValueError('invalid_endpoint')
    if parsed.path not in cfg.webhook_routes:
        raise ValueError('unknown_route')
    obj = envelope()
    body, headers = request_payload(cfg, parsed.path, obj)
    record = {'evidence_class': 'local_livekit', 'started_at': datetime.now(timezone.utc).isoformat(),
              'source_sha': cfg.source_sha, 'image_digest': cfg.image_digest,
              'config_revision': cfg.config_revision, 'outcome': 'fail',
              'agent_return_audio': 'unobserved', 'hosted_logs': 'unobserved',
              'stopped_service_tasks': 'unobserved', 'cleanup_receipts': None}
    record['correlation'] = hmac.new(cfg.livekit_api_secret.encode(), obj['data']['call_id'].encode(),
                                     hashlib.sha256).hexdigest()[:24]
    provider = api.LiveKitAPI(url=cfg.livekit_url, api_key=cfg.livekit_api_key,
                             api_secret=cfg.livekit_api_secret)
    room_name = None
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15), trust_env=False) as session:
        try:
            record['signing_enabled'] = cfg.signing_enabled
            record['invalid_signature'] = None
            if cfg.signing_enabled:
                bad = dict(headers, **{'X-Chert-Signature': 'sha256=' + '0' * 64})
                status, _, _ = await post(session, endpoint, body, bad)
                record['invalid_signature'] = status == 401
            results = await asyncio.gather(*(post(session, endpoint, body, headers) for _ in range(4)))
            status, accepted, elapsed = results[0]
            record['concurrent_identical'] = all(s == 200 and r == accepted for s, r, _ in results)
            record['response_ms'] = max(r[2] for r in results)
            age_ms = (datetime.now(timezone.utc) - datetime.fromisoformat(obj['created_at'])).total_seconds() * 1000
            record['within_budget'] = age_ms < cfg.decision_budget_ms
            if status != 200 or accepted.get('action') != 'accept':
                raise RuntimeError('not_accepted')
            claims = jwt.decode(accepted['participant_token'], cfg.livekit_api_secret,
                                algorithms=['HS256'], issuer=cfg.livekit_api_key)
            room_name = claims['video']['room']
            if not room_name.startswith(cfg.namespace + '-') or accepted['livekit_url'] != cfg.livekit_url:
                raise RuntimeError('binding_failed')
            record['distinct_identity'] = claims['sub'] != accepted['remote_participant_identity']
            roster = await provider.room.list_participants(api.ListParticipantsRequest(room=room_name))
            agents = [p for p in roster.participants if p.identity == accepted['remote_participant_identity']]
            record['exact_room_agent_tracks'] = len(agents) == 1 and len(agents[0].tracks) == 2
            record['decoded'] = await worker(cfg.livekit_url, accepted['participant_token'],
                                             accepted['remote_participant_identity'])
            c = record['decoded']
            record['probe_pass'] = (not cfg.signing_enabled or record['invalid_signature']) and all(record[k] for k in ('concurrent_identical',
                'distinct_identity', 'exact_room_agent_tracks', 'within_budget')) and (
                c['tone_frames'] >= 10 and c['video_changes'] >= 5 and c['return_frames'] >= 100)
        except Exception:
            record['probe_pass'] = False
        finally:
            terminal, terminal_headers = request_payload(cfg, parsed.path, envelope('call.ended', obj['data']['call_id']))
            try:
                status, _, _ = await post(session, endpoint, terminal, terminal_headers)
                record['terminal_ack'] = status == 200
            except Exception:
                record['terminal_ack'] = False
            if room_name:
                try:
                    remaining = await provider.room.list_rooms(api.ListRoomsRequest(names=[room_name]))
                    record['exact_room_absent'] = not any(r.name == room_name for r in remaining.rooms)
                except Exception:
                    record['exact_room_absent'] = None
            try:
                await provider.aclose()
                record['probe_api_closed'] = True
            except Exception:
                record['probe_api_closed'] = False
    record['outcome'] = 'partial' if record.get('probe_pass') and record.get('exact_room_absent') else 'fail'
    record['ended_at'] = datetime.now(timezone.utc).isoformat()
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute-approved-live', action='store_true')
    args = parser.parse_args()
    if not args.execute_approved_live:
        parser.exit(2, 'No connection made. Pass --execute-approved-live only for your configured test resources.\n')
    logging.disable(logging.CRITICAL)
    try:
        record = asyncio.run(live_probe(Config.from_env(), os.environ['CHERT_WEBHOOK_URL']))
    except Exception:
        raise SystemExit('livekit_probe_failed') from None
    print(json.dumps(record, sort_keys=True))
    raise SystemExit(0 if record['outcome'] == 'partial' else 1)


if __name__ == '__main__':
    main()
