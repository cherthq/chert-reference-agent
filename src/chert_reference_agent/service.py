"""Ordinary optionally signed webhooks, bounded in-memory replay and durable cleanup."""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import hmac
import inspect
import json
import re
import time
import uuid

from .journal import Journal
from .observability import SafeLogger

DECLINE = {'action': 'decline'}


@dataclass
class Resource:
    call: str
    room: str
    deadline: float
    born: float
    task: object = None
    handle: object = None
    response: dict = field(default_factory=lambda: DECLINE.copy())
    terminal: bool = False
    cleaned: bool = False
    cleanup_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    progress: tuple = (0, 0)
    progress_at: float = 0
    had_worker: bool = False
    incoming_progress: int = 0
    incoming_progress_at: float = 0


class Service:
    def __init__(self, config, adapter, *, clock=time.time, logger=None):
        self.config = config
        self.adapter = adapter
        self.clock = clock
        self.log = logger or SafeLogger(config)
        self.journal = Journal(config.journal_path, config.project_fingerprint)
        self.resources = {}
        self.events = {}
        self.tombstones = {}
        self.ready = False
        self.stopping = False
        self._watcher = None
        self._cleanup_tasks = set()
        self._cleanup_by_room = {}
        self.started_at = datetime.fromtimestamp(self.clock(), timezone.utc).timestamp()
        self.started_at_iso = datetime.fromtimestamp(self.started_at, timezone.utc).isoformat()

    async def start(self):
        self.started_at = datetime.fromtimestamp(self.clock(), timezone.utc).timestamp()
        self.started_at_iso = datetime.fromtimestamp(self.started_at, timezone.utc).isoformat()
        self.journal.open()
        if any(not re.fullmatch(re.escape(self.config.namespace) + r'-[0-9a-f]{32}', room) for room in self.journal.receipts):
            self.journal.close()
            raise RuntimeError('invalid owned receipt')
        # Reconcile independently: one unavailable room must not delay the rest.
        await asyncio.gather(*(self._schedule_room_task(
            room, lambda room=room: self._delete(room)
        ) for room in list(self.journal.receipts)))
        self.ready = not self.journal.receipts
        self.log.emit('startup', 'ready' if self.ready else 'recovery_failed', receipts=len(self.journal.receipts))
        self._watcher = asyncio.create_task(self._watch())

    def _prune(self):
        now = self.clock()
        horizon = self.config.replay_horizon_seconds
        self.events = {k: v for k, v in self.events.items() if now - v[1] < horizon}
        self.tombstones = {k: v for k, v in self.tombstones.items() if now - v < horizon}

    async def handle(self, path, headers, body):
        if len(body) > self.config.max_body_bytes:
            self.log.emit('rejected', 'body_too_large')
            return 413, {'error': 'body_too_large'}
        if path not in self.config.webhook_routes:
            return 404, {'error': 'unknown_route'}
        if self.config.signing_enabled:
            headers = {k.lower(): v for k, v in headers.items()}
            version = headers.get('x-chert-signature-version', '')
            keys = self.config.webhook_routes[path]
            secret = keys.get(int(version)) if version.isascii() and version.isdecimal() and len(version) < 10 else None
            signature = headers.get('x-chert-signature', '')
            if not secret or not signature.isascii() or not hmac.compare_digest(signature, 'sha256=' + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()):
                self.log.emit('rejected', 'invalid_signature')
                return 401, {'error': 'invalid_signature'}
        try:
            obj = json.loads(body)
            event, kind, data = obj['event_id'], obj['type'], obj['data']
            call = data['call_id']
            line = data['line_id']
            media = data['media']
            if not isinstance(line, str) or not 0 < len(line) <= 240 or not isinstance(media, dict) or set(media) != {'audio', 'video'} or any(type(v) is not bool for v in media.values()):
                raise ValueError()
            created = datetime.fromisoformat(obj['created_at'].replace('Z', '+00:00'))
            if created.tzinfo is None or not isinstance(event, str) or not 0 < len(event) <= 240 or not isinstance(call, str) or not 0 < len(call) <= 240 or kind not in ('call.incoming', 'call.started', 'call.ended', 'call.failed'):
                raise ValueError()
            created_at = created.timestamp()
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
            self.log.emit('rejected', 'invalid_envelope')
            return 400, {'error': 'invalid_envelope'}
        self.log.emit('lifecycle', 'received', call, event_ref=event)
        self._prune()
        event_key = (path, event)
        call_key = (path, call)
        digest = hashlib.sha256(body).hexdigest()
        previous = self.events.get(event_key)
        if previous and previous[0] != digest:
            self.log.emit('rejected', 'event_conflict', event)
            return 409, {'error': 'event_conflict'}
        # Admission capacity never evicts terminal suppression or retained fingerprints.
        if not previous and len(self.events) >= self.config.capacity * 8:
            return 503, {'error': 'replay_capacity'}
        self.events[event_key] = (digest, self.clock())
        if kind in ('call.ended', 'call.failed'):
            if call_key not in self.tombstones and len(self.tombstones) >= self.config.capacity * 8:
                self.ready = False
                return 503, {'error': 'terminal_capacity'}
            self.log.emit('lifecycle', 'terminal', call)
            self.tombstones[call_key] = self.clock()
            resource = self.resources.get(call_key)
            if resource:
                resource.terminal = True
                await asyncio.shield(self._schedule_cleanup(resource, 'terminal'))
            return 200, {'ok': True}
        if kind == 'call.started':
            self.log.emit('lifecycle', 'started', call)
            return 200, {'ok': True}
        now = self.clock()
        deadline = created_at + self.config.decision_budget_ms / 1000
        if not self.ready or self.stopping or call_key in self.tombstones or created_at < self.started_at or created_at > now + self.config.future_tolerance_seconds or now >= deadline:
            self.log.emit('decision', 'decline', call, event_ref=event)
            return 200, DECLINE.copy()
        resource = self.resources.get(call_key)
        if resource is None:
            if len(self.resources) >= self.config.capacity:
                return 200, DECLINE.copy()
            resource = Resource(call, self.config.namespace + '-' + uuid.uuid4().hex, deadline, time.monotonic())
            self.resources[call_key] = resource
            resource.task = asyncio.create_task(self._prepare(resource))
        try:
            await asyncio.shield(resource.task)
        except asyncio.CancelledError:
            # A client cancellation must not cancel another coalesced delivery.
            if not resource.task.cancelled():
                raise
        if resource.terminal or self.clock() >= resource.deadline:
            return 200, DECLINE.copy()
        return 200, resource.response.copy()

    async def _prepare(self, resource):
        try:
            self.journal.add(resource.room)
        except Exception:
            self.ready = False
            resource.terminal = True
            self.log.emit('decision', 'journal_failed', resource.call)
            return
        spec = dict(room=resource.room, agent_identity=self.config.namespace + '-agent-' + uuid.uuid4().hex, worker_identity=self.config.namespace + '-worker-' + uuid.uuid4().hex, expires_at=int(self.clock()) + self.config.token_ttl_seconds)
        try:
            async with asyncio.timeout(max(0, resource.deadline - self.clock())):
                resource.handle = await self.adapter.prepare(spec)
                token = self.adapter.mint_worker_token(spec)
                if inspect.isawaitable(token):
                    token = await token
            if resource.terminal or resource.handle.failed.is_set() or self.clock() >= resource.deadline:
                raise TimeoutError()
            resource.progress_at = time.monotonic()
            resource.incoming_progress_at = time.monotonic()
            resource.response = dict(action='accept', livekit_url=self.config.livekit_url, participant_token=token, remote_participant_identity=spec['agent_identity'])
            self.log.emit('decision', 'accept', resource.call, elapsed_ms=(time.monotonic() - resource.born) * 1000)
        except (Exception, asyncio.CancelledError):
            resource.terminal = True
            self.log.emit('decision', 'prepare_failed', resource.call)
            self._schedule_cleanup(resource, 'prepare_failed')

    def _schedule_cleanup(self, resource, reason):
        resource.terminal = True
        resource.response = DECLINE.copy()
        self.ready = False
        async def cleanup():
            if resource.task and not resource.task.done():
                resource.task.cancel()
                await asyncio.gather(resource.task, return_exceptions=True)
            await self._cleanup(resource, reason)
        return self._schedule_room_task(resource.room, cleanup)

    def _schedule_room_task(self, room, operation):
        current = self._cleanup_by_room.get(room)
        if current is not None and not current.done():
            return current
        task = asyncio.create_task(operation())
        self._cleanup_by_room[room] = task
        self._cleanup_tasks.add(task)
        def finished(done):
            self._cleanup_tasks.discard(done)
            if self._cleanup_by_room.get(room) is done:
                self._cleanup_by_room.pop(room, None)
            if not done.cancelled() and done.exception() is not None:
                self.ready = False
                self.log.emit('cleanup', 'failed', receipts=len(self.journal.receipts))
        task.add_done_callback(finished)
        return task

    async def _delete(self, room):
        for attempt in range(self.config.cleanup_attempts):
            try:
                async with asyncio.timeout(self.config.cleanup_timeout_seconds):
                    await self.adapter.close_room(room)
                self.journal.remove(room)
                return True
            except Exception:
                self.ready = False
                if attempt + 1 < self.config.cleanup_attempts:
                    await asyncio.sleep(min(0.1 * 2 ** attempt, 1))
        return False

    async def _cleanup(self, resource, reason):
        async with resource.cleanup_lock:
            if resource.cleaned:
                return
            resource.terminal = True
            resource.response = DECLINE.copy()
            resource.cleaned = await self._delete(resource.room)
            self.log.emit('cleanup', 'success' if resource.cleaned else 'failed', resource.call, receipts=len(self.journal.receipts))

    async def _watch(self):
        try:
            while True:
                await asyncio.sleep(self.config.watch_interval_seconds)
                now = time.monotonic()
                for key, resource in list(self.resources.items()):
                    if resource.terminal:
                        if not resource.cleaned:
                            self._schedule_cleanup(resource, 'failed')
                        if resource.cleaned:
                            self.tombstones[key] = self.clock()
                            self.resources.pop(key, None)
                        continue
                    reason = None
                    snapshot = resource.handle.snapshot() if resource.handle else {}
                    if now - resource.born >= self.config.resource_ttl_seconds:
                        reason = 'ttl'
                    elif resource.handle:
                        if resource.handle.failed.is_set():
                            reason = 'agent_failed'
                        elif resource.had_worker and not snapshot.get('worker_present'):
                            reason = 'worker_departed'
                        elif not snapshot.get('worker_present') and now - resource.born >= self.config.worker_arrival_timeout_seconds:
                            reason = 'missing_worker'
                        resource.had_worker = resource.had_worker or bool(snapshot.get('worker_present'))
                        progress = (snapshot.get('outgoing_audio_frames', 0), snapshot.get('outgoing_video_frames', 0))
                        if all(a > b for a, b in zip(progress, resource.progress)):
                            resource.progress, resource.progress_at = progress, now
                        elif now - resource.progress_at >= self.config.stall_timeout_seconds:
                            reason = reason or 'producer_stall'
                        self.log.emit('watch', 'summary', resource.call, **snapshot)
                        incoming = snapshot.get('incoming_audio_frames', 0)
                        if incoming > resource.incoming_progress:
                            resource.incoming_progress, resource.incoming_progress_at = incoming, now
                        if snapshot.get('worker_present') and now - resource.incoming_progress_at >= self.config.stall_timeout_seconds:
                            self.log.emit('watch', 'missing_media' if not incoming else 'incoming_stall', resource.call)
                    if reason:
                        self.log.emit('watch', reason, resource.call)
                        resource.terminal = True
                        self._schedule_cleanup(resource, reason)
                # Orphans recovered after transient failure; never admit while debt remains.
                active = {r.room for r in self.resources.values() if not r.terminal}
                for room in set(self.journal.receipts) - active - {r.room for r in self.resources.values()}:
                    self._schedule_room_task(room, lambda room=room: self._delete(room))
                self.ready = not self.stopping and not (set(self.journal.receipts) - active)
                self._prune()
        except asyncio.CancelledError:
            return
        except Exception:
            self.ready = False
            self.log.emit('watch', 'failed')

    async def shutdown(self):
        self.stopping = True
        self.ready = False
        if self._watcher:
            self._watcher.cancel()
            await asyncio.gather(self._watcher, return_exceptions=True)
        for resource in self.resources.values():
            resource.terminal = True
            self._schedule_cleanup(resource, 'shutdown')
        if self._cleanup_tasks:
            await asyncio.gather(*self._cleanup_tasks, return_exceptions=True)
        try:
            async with asyncio.timeout(self.config.cleanup_timeout_seconds):
                await self.adapter.aclose()
        finally:
            self.journal.close()
        self.log.emit('shutdown', 'shutdown', receipts=len(self.journal.receipts))
