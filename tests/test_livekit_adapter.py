import asyncio
import base64
import json
import time
from types import SimpleNamespace

import pytest
from livekit import rtc

from chert_reference_agent.livekit_adapter import LiveKitAdapter


class FakeParticipant:
    def __init__(self):
        self.tracks = []

    async def publish_track(self, track, options):
        self.tracks.append((track, options))
        return SimpleNamespace(sid=f"track-{len(self.tracks)}")


class FakeRoom:
    def __init__(self):
        self.handlers = {}
        self.local_participant = FakeParticipant()
        self.remote_participants = {}
        self.connected = False
        self.connect_error = False
        self.disconnect_error = False

    def on(self, name, callback=None):
        def register(fn):
            self.handlers[name] = fn
            return fn

        return register(callback) if callback else register

    async def connect(self, url, token):
        self.url, self.token = url, token
        if self.connect_error:
            raise RuntimeError("private connection data")
        self.connected = True

    async def disconnect(self):
        self.connected = False
        if self.disconnect_error:
            raise RuntimeError("private disconnect data")


class FakeAPI:
    def __init__(self):
        self.room = self
        self.created = []
        self.deleted = []
        self.closed = False

    async def create_room(self, request):
        self.created.append(request.name)

    async def delete_room(self, request):
        self.deleted.append(request.room)

    async def list_rooms(self, request):
        return SimpleNamespace(rooms=[])

    async def aclose(self):
        self.closed = True


def spec():
    return dict(
        room="reference-synthetic",
        agent_identity="agent-synthetic",
        worker_identity="worker-synthetic",
        expires_at=int(time.time()) + 120,
    )


def claims(token):
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


def adapter(room=None, api=None):
    room = room or FakeRoom()
    api = api or FakeAPI()
    return (
        LiveKitAdapter(
            "wss://synthetic.invalid",
            "synthetic-key",
            "synthetic-secret-at-least-32-bytes-long",
            room_factory=lambda: room,
            api_factory=lambda **kwargs: api,
        ),
        room,
        api,
    )


@pytest.mark.asyncio
async def test_real_sdk_grants_frames_and_publication_transport_boundary():
    subject, room, api = adapter()
    value = spec()
    handle = await subject.prepare(value)
    worker, agent = claims(subject.mint_worker_token(value)), claims(room.token)
    assert room.url == "wss://synthetic.invalid"
    assert worker["sub"] == value["worker_identity"]
    assert agent["sub"] == value["agent_identity"]
    assert worker["sub"] != agent["sub"]
    for token in (worker, agent):
        assert token["video"]["room"] == value["room"]
        assert token["video"]["roomJoin"] and token["video"]["canPublish"]
        assert token["video"]["canSubscribe"]
        assert isinstance(token["nbf"], int) and isinstance(token["exp"], int)
        assert 0 < token["exp"] - token["nbf"] <= 7200
    assert len(room.local_participant.tracks) == 2
    assert {options.source for _, options in room.local_participant.tracks} == {
        rtc.TrackSource.SOURCE_MICROPHONE,
        rtc.TrackSource.SOURCE_CAMERA,
    }
    await asyncio.sleep(0.06)
    snapshot = handle.snapshot()
    assert snapshot["published_audio"] and snapshot["published_video"]
    assert snapshot["outgoing_audio_frames"] > 0
    assert snapshot["outgoing_video_frames"] > 0
    assert all(isinstance(v, (int, float, bool)) for v in snapshot.values())
    await subject.close_room(value["room"])
    assert not room.connected and api.deleted == [value["room"]]
    await subject.aclose()


@pytest.mark.asyncio
async def test_partial_connect_failure_remains_closeable():
    room = FakeRoom()
    room.connect_error = True
    subject, room, api = adapter(room)
    with pytest.raises(RuntimeError):
        await subject.prepare(spec())
    await subject.close_room(spec()["room"])
    assert api.deleted == [spec()["room"]]
    await subject.aclose()


@pytest.mark.asyncio
async def test_delete_attempted_even_when_disconnect_fails():
    subject, room, api = adapter()
    await subject.prepare(spec())
    room.disconnect_error = True
    with pytest.raises(Exception):
        await subject.close_room(spec()["room"])
    assert api.deleted == [spec()["room"]]
    room.disconnect_error = False
    await subject.close_room(spec()["room"])
    await subject.aclose()


@pytest.mark.asyncio
async def test_worker_presence_disconnect_and_unexpected_publisher():
    subject, room, _ = adapter()
    handle = await subject.prepare(spec())
    stranger = SimpleNamespace(identity="other", track_publications={})
    room.handlers["participant_connected"](stranger)
    assert not handle.snapshot()["worker_present"]
    worker = SimpleNamespace(identity=spec()["worker_identity"], track_publications={})
    room.handlers["participant_connected"](worker)
    assert handle.snapshot()["worker_present"]
    room.handlers["participant_disconnected"](worker)
    assert handle.failed.is_set()
    assert not handle.snapshot()["worker_present"]
    await subject.close_room(spec()["room"])
    await subject.aclose()


@pytest.mark.asyncio
async def test_consumers_count_real_sdk_frames_without_retaining_media():
    from chert_reference_agent.media import audio_chunk, video_frame, WIDTH, HEIGHT

    subject, _, _ = adapter()
    handle = await subject.prepare(spec())

    async def audio_events():
        yield SimpleNamespace(frame=rtc.AudioFrame(audio_chunk(0), 48000, 1, 960))
        yield SimpleNamespace(frame=rtc.AudioFrame(bytes(1920), 48000, 1, 960))

    async def video_events():
        for sequence in (0, 0, 1):
            yield SimpleNamespace(
                frame=rtc.VideoFrame(WIDTH, HEIGHT, rtc.VideoBufferType.RGB24, video_frame(sequence))
            )

    await handle.consume_audio(audio_events())
    await handle.consume_video(video_events())
    value = handle.snapshot()
    assert value["incoming_audio_frames"] == 2
    assert value["incoming_audio_samples"] == 1920
    assert value["incoming_audio_peak"] == 4096
    assert value["incoming_non_silent_frames"] == 1
    assert value["incoming_video_frames"] == 3
    assert value["incoming_video_changes"] == 1
    await subject.close_room(spec()["room"])
    await subject.aclose()


@pytest.mark.asyncio
async def test_cancelled_close_remains_retryable_and_attempts_delete():
    subject, room, api = adapter()
    await subject.prepare(spec())
    original = room.disconnect
    entered = asyncio.Event()

    async def blocked_disconnect():
        entered.set()
        await asyncio.Event().wait()

    room.disconnect = blocked_disconnect
    closing = asyncio.create_task(subject.close_room(spec()["room"]))
    await entered.wait()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert api.deleted == [spec()["room"]]
    room.disconnect = original
    await subject.close_room(spec()["room"])
    assert not room.connected
    await subject.aclose()


@pytest.mark.asyncio
async def test_producer_failure_signals_handle_and_all_tasks_stop():
    subject, _, _ = adapter()
    handle = await subject.prepare(spec())

    async def failing():
        raise RuntimeError("private producer error")

    handle.spawn(failing())
    await asyncio.wait_for(handle.failed.wait(), 1)
    await subject.close_room(spec()["room"])
    assert not handle.tasks
    await subject.aclose()


@pytest.mark.asyncio
async def test_unknown_publisher_does_not_create_consumer():
    subject, room, _ = adapter()
    handle = await subject.prepare(spec())
    room.handlers["track_subscribed"](
        SimpleNamespace(kind=rtc.TrackKind.KIND_AUDIO),
        SimpleNamespace(sid="private-sid"),
        SimpleNamespace(identity="unexpected-private-id"),
    )
    assert handle.snapshot()["unexpected_publishers"] == 1
    assert not handle.streams
    await subject.close_room(spec()["room"])
    await subject.aclose()


@pytest.mark.asyncio
async def test_failed_local_resource_close_is_retried():
    subject, _, _ = adapter()
    handle = await subject.prepare(spec())

    class Resource:
        attempts = 0

        async def aclose(self):
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("temporary close failure")

    resource = Resource()
    handle.sources.append(resource)
    with pytest.raises(RuntimeError):
        await subject.close_room(spec()["room"])
    await subject.close_room(spec()["room"])
    assert resource.attempts == 2
    await subject.aclose()


@pytest.mark.asyncio
async def test_delete_ack_requires_absence_and_retry():
    subject, _, api = adapter()
    await subject.prepare(spec())

    async def retained(request):
        assert list(request.names) == [spec()["room"]]
        return SimpleNamespace(rooms=[SimpleNamespace(name=spec()["room"])])

    original = api.list_rooms
    api.list_rooms = retained
    with pytest.raises(RuntimeError, match="room_delete_unverified"):
        await subject.close_room(spec()["room"])
    api.list_rooms = original
    await subject.close_room(spec()["room"])
    await subject.aclose()


@pytest.mark.asyncio
async def test_actual_api_constructor_receives_config_and_builds_https_client():
    from livekit import api as sdk_api

    clients = []

    def make_api(**kwargs):
        client = sdk_api.LiveKitAPI(**kwargs)
        clients.append(client)
        return client

    subject = LiveKitAdapter(
        "wss://synthetic.invalid",
        "synthetic-key",
        "synthetic-secret-at-least-32-bytes-long",
        api_factory=make_api,
    )
    assert clients[0].room._client.host == "https://synthetic.invalid"
    import jwt

    token = jwt.decode(
        subject.mint_worker_token(spec()), "synthetic-secret-at-least-32-bytes-long", algorithms=["HS256"]
    )
    assert token["iss"] == "synthetic-key"
    await subject.aclose()  # no request; constructor/close only


@pytest.mark.asyncio
async def test_partial_second_publication_failure_still_cleans_sources_and_room():
    subject, room, api = adapter()
    original = room.local_participant.publish_track

    async def fail_second(track, options):
        if options.source == rtc.TrackSource.SOURCE_CAMERA:
            raise RuntimeError("synthetic publication failure")
        return await original(track, options)

    room.local_participant.publish_track = fail_second
    with pytest.raises(RuntimeError):
        await subject.prepare(spec())
    handle = subject._handles[spec()["room"]]
    assert len(handle.sources) == 2
    await subject.close_room(spec()["room"])
    assert not handle.sources and not handle.tasks and not room.connected
    assert api.deleted == [spec()["room"]]
    await subject.aclose()


@pytest.mark.asyncio
async def test_outer_cancel_then_hung_delete_is_bounded():
    subject, room, api = adapter()
    await subject.prepare(spec())
    entered = asyncio.Event()

    async def blocked_disconnect():
        entered.set()
        await asyncio.Event().wait()

    async def blocked_delete(request):
        await asyncio.Event().wait()

    room.disconnect = blocked_disconnect
    api.delete_room = blocked_delete
    closing = asyncio.create_task(subject.close_room(spec()["room"]))
    await entered.wait()
    closing.cancel()
    # asyncio.wait does not cancel again: a second cancellation would hide the bug.
    done, pending = await asyncio.wait({closing}, timeout=1.5)
    if pending:
        closing.cancel()
        await asyncio.gather(closing, return_exceptions=True)
    assert done, "delete remained unbounded after original cancellation"
    assert isinstance(closing.exception(), RuntimeError)
    subject._handles.clear()  # stalled transport is intentional; no real connection
    await subject.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", [rtc.TrackKind.KIND_AUDIO, rtc.TrackKind.KIND_VIDEO])
async def test_same_sid_resubscription_consumes_new_track_and_retires_old(monkeypatch, kind):
    from chert_reference_agent.livekit_adapter import MediaHandle
    from chert_reference_agent.media import audio_chunk, video_frame

    room = FakeRoom()
    handle = MediaHandle(room, "expected-worker", cleanup_step_timeout=0.05)
    handle.bind_events()
    streams = []

    class Stream:
        def __init__(self, track, **kwargs):
            self.track = track
            self.queue = asyncio.Queue()
            self.closed = 0
            streams.append(self)

        def __aiter__(self):
            return self

        async def __anext__(self):
            return await self.queue.get()

        async def aclose(self):
            self.closed += 1

    monkeypatch.setattr(rtc, "AudioStream" if kind == rtc.TrackKind.KIND_AUDIO else "VideoStream", Stream)
    old, new = SimpleNamespace(kind=kind), SimpleNamespace(kind=kind)
    publication = SimpleNamespace(sid="same-sid")
    worker = SimpleNamespace(identity="expected-worker")
    room.handlers["track_subscribed"](old, publication, worker)
    room.handlers.get("track_unsubscribed", lambda *_: None)(old, publication, worker)
    room.handlers["track_subscribed"](new, publication, worker)
    try:
        assert len(streams) == 2
        assert handle.streams["same-sid"] is streams[1]
        # A delayed duplicate unsubscribe for the prior track must not retire the new one.
        room.handlers["track_unsubscribed"](old, publication, worker)
        assert handle.streams["same-sid"] is streams[1]
        frame = (
            rtc.AudioFrame(audio_chunk(0), 48000, 1, 960)
            if kind == rtc.TrackKind.KIND_AUDIO
            else rtc.VideoFrame(320, 180, rtc.VideoBufferType.RGB24, video_frame(0))
        )
        await streams[1].queue.put(SimpleNamespace(frame=frame))
        for _ in range(20):
            await asyncio.sleep(0)
        metric = "incoming_audio_frames" if kind == rtc.TrackKind.KIND_AUDIO else "incoming_video_frames"
        assert handle.snapshot()[metric] == 1
        assert streams[0].closed == 1
        assert not handle.failed.is_set()
    finally:
        await handle.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["error", "timeout", "cancel"])
async def test_retired_stream_survives_failed_close_for_cleanup_retry(monkeypatch, failure):
    from chert_reference_agent.livekit_adapter import MediaHandle

    room = FakeRoom()
    handle = MediaHandle(room, "expected-worker", cleanup_step_timeout=0.02)
    handle.bind_events()
    entered = asyncio.Event()

    class Stream:
        attempts = 0
        retry = False

        def __init__(self, track, **kwargs):
            pass

        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.Event().wait()

        async def aclose(self):
            self.attempts += 1
            entered.set()
            if not self.retry:
                if failure == "error":
                    raise RuntimeError("synthetic close failure")
                await asyncio.Event().wait()

    monkeypatch.setattr(rtc, "AudioStream", Stream)
    track = SimpleNamespace(kind=rtc.TrackKind.KIND_AUDIO)
    publication = SimpleNamespace(sid="same-sid")
    worker = SimpleNamespace(identity="expected-worker")
    room.handlers["track_subscribed"](track, publication, worker)
    stream = handle.streams["same-sid"]
    room.handlers["track_unsubscribed"](track, publication, worker)
    await entered.wait()
    if failure == "cancel":
        for task in list(handle.tasks):
            task.cancel()
        await asyncio.gather(*handle.tasks, return_exceptions=True)
    else:
        await asyncio.wait_for(handle.failed.wait(), 0.2)
    assert handle.retired_streams == [stream]
    stream.retry = True
    assert await handle.close()
    assert stream.attempts == 2
    assert not handle.retired_streams and not handle.tasks


@pytest.mark.asyncio
async def test_unknown_none_track_unsubscribe_is_idempotent():
    from chert_reference_agent.livekit_adapter import MediaHandle

    room = FakeRoom()
    handle = MediaHandle(room, "expected-worker")
    handle.bind_events()
    publication = SimpleNamespace(sid="already-unsubscribed")
    worker = SimpleNamespace(identity="expected-worker")
    for _ in range(2):
        room.handlers["track_unsubscribed"](None, publication, worker)
    assert not handle.streams and not handle.retired_streams
    assert not handle.failed.is_set()
    assert await handle.close()
