"""Direct LiveKit media boundary. Construction is real; tests replace transports only."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from livekit import api, rtc

from .media import FPS, HEIGHT, SAMPLE_RATE, SAMPLES_PER_CHUNK, WIDTH, audio_chunk, video_frame


class MediaHandle:
    def __init__(self, room: Any, worker_identity: str, cleanup_step_timeout: float = 1.0) -> None:
        self.room = room
        self.cleanup_step_timeout = cleanup_step_timeout
        self.worker_identity = worker_identity
        self.failed = asyncio.Event()
        self.audio_submitted = asyncio.Event()
        self.video_submitted = asyncio.Event()
        self.closing = False
        self.tasks: set[asyncio.Task] = set()
        self.streams: dict[str, Any] = {}
        self._stream_tracks: dict[str, Any] = {}
        self._stream_tasks: dict[str, asyncio.Task] = {}
        self.retired_streams: list[Any] = []
        self.sources: list[Any] = []
        self.counts: dict[str, int | bool] = {
            "published_audio": False,
            "published_video": False,
            "outgoing_audio_frames": 0,
            "outgoing_video_frames": 0,
            "incoming_audio_frames": 0,
            "incoming_audio_samples": 0,
            "incoming_audio_peak": 0,
            "incoming_non_silent_frames": 0,
            "incoming_video_frames": 0,
            "incoming_video_changes": 0,
            "worker_present": False,
            "unexpected_publishers": 0,
        }

    def snapshot(self) -> dict[str, int | bool]:
        return {
            key: min(value, 2**53 - 1) if type(value) is int else value for key, value in self.counts.items()
        }

    def spawn(self, coroutine: Any) -> asyncio.Task:
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)

        def finished(done: asyncio.Task) -> None:
            self.tasks.discard(done)
            if not done.cancelled():
                error = done.exception()  # retrieve without emitting arbitrary SDK error text
                if error is not None and not self.closing:
                    self.failed.set()

        task.add_done_callback(finished)
        return task

    def bind_events(self) -> None:
        @self.room.on("participant_connected")
        def connected(participant: Any) -> None:
            if participant.identity == self.worker_identity:
                self.counts["worker_present"] = True

        @self.room.on("participant_disconnected")
        def disconnected(participant: Any) -> None:
            if participant.identity == self.worker_identity:
                self.counts["worker_present"] = False
                if not self.closing:
                    self.failed.set()

        @self.room.on("disconnected")
        def room_disconnected(*args: Any) -> None:
            if not self.closing:
                self.failed.set()

        @self.room.on("track_subscribed")
        def subscribed(track: Any, publication: Any, participant: Any) -> None:
            if participant.identity != self.worker_identity:
                self.counts["unexpected_publishers"] += 1
                return
            self.counts["worker_present"] = True
            key = publication.sid
            if key in self.streams or self.closing:
                return
            try:
                if track.kind == rtc.TrackKind.KIND_AUDIO:
                    stream = rtc.AudioStream(track, capacity=8, sample_rate=SAMPLE_RATE, num_channels=1)
                    self.streams[key] = stream
                    self._stream_tracks[key] = track
                    self._stream_tasks[key] = self.spawn(self.consume_audio(stream))
                elif track.kind == rtc.TrackKind.KIND_VIDEO:
                    stream = rtc.VideoStream(track, capacity=2, format=rtc.VideoBufferType.RGB24)
                    self.streams[key] = stream
                    self._stream_tracks[key] = track
                    self._stream_tasks[key] = self.spawn(self.consume_video(stream))
            except Exception:
                self.failed.set()

        @self.room.on("track_unsubscribed")
        def unsubscribed(track: Any, publication: Any, participant: Any) -> None:
            key = publication.sid
            if participant.identity != self.worker_identity or self.closing:
                return
            # SID remains constant across SDK resubscriptions, but the Track changes.
            # Ignore late duplicate events referring to the previous Track instance.
            if key not in self.streams or self._stream_tracks.get(key) is not track:
                return
            stream = self.streams.pop(key)
            self._stream_tracks.pop(key)
            consumer = self._stream_tasks.pop(key)
            self.retired_streams.append(stream)  # retained before any cancellation/await
            consumer.cancel()
            self.spawn(self.retire_stream(stream, consumer))

    async def retire_stream(self, stream: Any, consumer: asyncio.Task) -> None:
        await asyncio.wait_for(asyncio.gather(consumer, return_exceptions=True), self.cleanup_step_timeout)
        await asyncio.wait_for(stream.aclose(), self.cleanup_step_timeout)
        self.retired_streams.remove(stream)

    async def consume_audio(self, stream: Any) -> None:
        async for event in stream:
            samples = event.frame.data
            peak = max((abs(sample) for sample in samples), default=0)
            self.counts["incoming_audio_frames"] += 1
            self.counts["incoming_audio_samples"] += len(samples)
            self.counts["incoming_audio_peak"] = max(self.counts["incoming_audio_peak"], peak)
            self.counts["incoming_non_silent_frames"] += int(peak > 0)

    async def consume_video(self, stream: Any) -> None:
        previous = None
        async for event in stream:
            digest = hashlib.sha256(event.frame.data).digest()
            self.counts["incoming_video_frames"] += 1
            self.counts["incoming_video_changes"] += int(previous is not None and digest != previous)
            previous = digest

    async def audio(self, source: rtc.AudioSource) -> None:
        sequence = 0
        while True:
            started = time.monotonic()
            frame = rtc.AudioFrame(audio_chunk(sequence), SAMPLE_RATE, 1, SAMPLES_PER_CHUNK)
            await source.capture_frame(frame)
            self.counts["outgoing_audio_frames"] += 1
            self.audio_submitted.set()
            sequence += 1
            await asyncio.sleep(max(0, 0.020 - (time.monotonic() - started)))

    async def video(self, source: rtc.VideoSource) -> None:
        sequence = 0
        while True:
            started = time.monotonic()
            frame = rtc.VideoFrame(WIDTH, HEIGHT, rtc.VideoBufferType.RGB24, video_frame(sequence))
            source.capture_frame(frame)
            self.counts["outgoing_video_frames"] += 1
            self.video_submitted.set()
            sequence += 1
            await asyncio.sleep(max(0, 1 / FPS - (time.monotonic() - started)))

    async def close(self) -> bool:
        """Attempt every local release even if one component fails."""
        self.closing = True
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        ok = True
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), self.cleanup_step_timeout)
        except TimeoutError:
            ok = False
        for key, resource in list(self.streams.items()):
            try:
                await asyncio.wait_for(resource.aclose(), self.cleanup_step_timeout)
            except Exception:
                ok = False
            else:
                del self.streams[key]
                self._stream_tracks.pop(key, None)
                self._stream_tasks.pop(key, None)
        for resource in list(self.retired_streams):
            try:
                await asyncio.wait_for(resource.aclose(), self.cleanup_step_timeout)
            except Exception:
                ok = False
            else:
                self.retired_streams.remove(resource)
        for resource in list(self.sources):
            try:
                await asyncio.wait_for(resource.aclose(), self.cleanup_step_timeout)
            except Exception:
                ok = False
            else:
                self.sources.remove(resource)
        try:
            await asyncio.wait_for(self.room.disconnect(), self.cleanup_step_timeout)
        except Exception:
            ok = False
        return ok


class LiveKitAdapter:
    def __init__(
        self,
        livekit_url: str,
        api_key: str,
        api_secret: str,
        *,
        room_factory: Callable | None = None,
        api_factory: Callable | None = None,
        cleanup_step_timeout: float = 1.0,
    ) -> None:
        if not 0 < cleanup_step_timeout <= 60:
            raise ValueError("invalid_cleanup_step_timeout")
        self._cleanup_step_timeout = cleanup_step_timeout
        self._url, self._key, self._secret = livekit_url, api_key, api_secret
        self._room_factory = room_factory or rtc.Room
        self._api = (api_factory or api.LiveKitAPI)(url=livekit_url, api_key=api_key, api_secret=api_secret)
        self._handles: dict[str, MediaHandle] = {}

    def _token(self, spec: dict, identity: str) -> str:
        remaining = int(spec["expires_at"]) - int(time.time())
        if not 1 <= remaining <= 7200:
            raise ValueError("token_ttl_invalid")
        return (
            api.AccessToken(self._key, self._secret)
            .with_identity(identity)
            .with_ttl(timedelta(seconds=remaining))
            .with_grants(
                api.VideoGrants(
                    room_join=True,
                    room=spec["room"],
                    can_publish=True,
                    can_subscribe=True,
                    can_publish_data=False,
                )
            )
            .to_jwt()
        )

    def mint_worker_token(self, spec: dict) -> str:
        return self._token(spec, spec["worker_identity"])

    async def prepare(self, spec: dict) -> MediaHandle:
        name = spec["room"]
        if name in self._handles:
            raise ValueError("room_already_prepared")
        if spec["agent_identity"] == spec["worker_identity"]:
            raise ValueError("identities_not_distinct")
        room = self._room_factory()
        handle = MediaHandle(room, spec["worker_identity"], self._cleanup_step_timeout)
        self._handles[name] = handle  # before any side-effecting await
        handle.bind_events()
        await self._api.room.create_room(api.CreateRoomRequest(name=name))
        await room.connect(self._url, self._token(spec, spec["agent_identity"]))
        for participant in room.remote_participants.values():
            if participant.identity == spec["worker_identity"]:
                handle.counts["worker_present"] = True
        audio_source = rtc.AudioSource(SAMPLE_RATE, 1, queue_size_ms=100)
        handle.sources.append(audio_source)
        video_source = rtc.VideoSource(WIDTH, HEIGHT)
        handle.sources.append(video_source)
        audio_track = rtc.LocalAudioTrack.create_audio_track("reference-tone", audio_source)
        video_track = rtc.LocalVideoTrack.create_video_track("reference-bars", video_source)
        await room.local_participant.publish_track(
            audio_track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
        )
        handle.counts["published_audio"] = True
        await room.local_participant.publish_track(
            video_track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_CAMERA)
        )
        handle.counts["published_video"] = True
        handle.spawn(handle.audio(audio_source))
        handle.spawn(handle.video(video_source))
        # A scheduled task alone is not evidence of a successful first submission.
        # Coordinator owns the outer signed-decision deadline and cancellation.
        while not (handle.audio_submitted.is_set() and handle.video_submitted.is_set()):
            if handle.failed.is_set():
                raise RuntimeError("initial_media_failed")
            await asyncio.sleep(0.001)
        if handle.failed.is_set():
            raise RuntimeError("initial_media_failed")
        return handle

    async def close_room(self, room: str) -> None:
        handle = self._handles.get(room)
        ok = True
        try:
            if handle is not None:
                ok = await handle.close()
        finally:
            # Deletion is essential even when cancellation or disconnect fails.
            try:
                await asyncio.wait_for(
                    self._api.room.delete_room(api.DeleteRoomRequest(room=room)), self._cleanup_step_timeout
                )
            except api.TwirpError as error:
                if error.code != "not_found":
                    raise RuntimeError("room_delete_failed") from None
            except Exception:
                raise RuntimeError("room_delete_failed") from None
        remaining = await asyncio.wait_for(
            self._api.room.list_rooms(api.ListRoomsRequest(names=[room])), self._cleanup_step_timeout
        )
        if any(item.name == room for item in remaining.rooms):
            raise RuntimeError("room_delete_unverified")
        if not ok:
            raise RuntimeError("local_close_failed")
        self._handles.pop(room, None)

    async def aclose(self) -> None:
        errors = False
        for name in list(self._handles):
            try:
                await self.close_room(name)
            except Exception:
                errors = True
        await asyncio.wait_for(self._api.aclose(), self._cleanup_step_timeout)
        if errors:
            raise RuntimeError("adapter_close_failed")
