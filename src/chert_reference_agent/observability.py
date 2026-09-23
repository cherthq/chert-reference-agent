"""Strict output allowlist: never serialize payloads or exception messages."""
from datetime import datetime, timezone
import hashlib
import hmac
import json

_EVENTS = {'startup', 'decision', 'cleanup', 'watch', 'shutdown', 'rejected', 'lifecycle'}
_REASONS = {'ready', 'recovery_failed', 'accept', 'decline', 'terminal', 'timeout', 'prepare_failed', 'success', 'failed', 'ttl', 'missing_worker', 'worker_departed', 'producer_stall', 'agent_failed', 'missing_media', 'summary', 'shutdown', 'journal_failed', 'capacity', 'body_too_large', 'invalid_signature', 'invalid_envelope', 'event_conflict', 'started', 'received', 'incoming_stall'}
_COUNTERS = {'published_audio', 'published_video', 'outgoing_audio_frames', 'outgoing_video_frames', 'incoming_audio_frames', 'incoming_audio_samples', 'incoming_audio_peak', 'incoming_non_silent_frames', 'incoming_video_frames', 'incoming_video_changes', 'worker_present', 'unexpected_publishers', 'resources', 'receipts', 'elapsed_ms'}


class SafeLogger:
    def __init__(self, config, sink=None):
        self.config = config
        self.sink = sink or print
        self.key = config.livekit_api_secret.encode()

    def emit(self, event, reason, correlation=None, event_ref=None, **fields):
        if event not in _EVENTS or reason not in _REASONS:
            raise ValueError('unknown log enum')
        record = dict(timestamp=datetime.now(timezone.utc).isoformat(), event=event, reason=reason, source_sha=self.config.source_sha, image_digest=self.config.image_digest, config_revision=self.config.config_revision)
        if correlation:
            record['correlation'] = hmac.new(self.key, correlation.encode(), hashlib.sha256).hexdigest()[:24]
        if event_ref:
            record['event_ref'] = hmac.new(self.key, event_ref.encode(), hashlib.sha256).hexdigest()[:24]
        for key, value in fields.items():
            if key in _COUNTERS and isinstance(value, (int, float)) and -1e15 < value < 1e15:
                record[key] = value
        self.sink(json.dumps(record, allow_nan=False, sort_keys=True))
