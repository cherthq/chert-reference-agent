import asyncio
import hashlib
import hmac
import json
import time

import pytest

from chert_reference_agent.config import Config
from chert_reference_agent.service import Service


class Handle:
    def __init__(self):
        self.failed = asyncio.Event()
    def snapshot(self):
        return {'worker_present': True, 'audio_frames_sent': 10, 'video_frames_sent': 10}


class Adapter:
    def __init__(self):
        self.prepared = []
        self.closed = []
        self.gate = None
        self.fail_close = False
    async def prepare(self, spec):
        self.prepared.append(spec)
        if self.gate:
            await self.gate.wait()
        return Handle()
    def mint_worker_token(self, spec):
        return 'synthetic-token'
    async def close_room(self, room):
        self.closed.append(room)
        if self.fail_close:
            raise RuntimeError('secret must never be logged')
    async def aclose(self):
        pass


def config(tmp_path, **kw):
    kw.setdefault('signing_enabled', True)
    return Config(webhook_routes={'/hooks/customer': {1: 'test-secret'}}, livekit_url='wss://example.test', livekit_api_key='test-key', livekit_api_secret='test-api-secret', namespace='test', journal_path=str(tmp_path / 'receipts.json'), **kw)


def envelope(kind='call.incoming', call='call-1', event='event-1', created=None):
    return {'event_id': event, 'type': kind, 'created_at': created or time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'data': {'call_id': call, 'line_id': 'line', 'media': {'audio': True, 'video': False}}}


async def send(service, obj, version='1', secret='test-secret', path='/hooks/customer'):
    body = json.dumps(obj).encode()
    headers = {'X-Chert-Signature-Version': version, 'X-Chert-Signature': 'sha256=' + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()}
    return await service.handle(path, headers, body)


@pytest.mark.asyncio
async def test_concurrent_replay_and_terminal_cleanup(tmp_path):
    adapter = Adapter()
    service = Service(config(tmp_path), adapter)
    await service.start()
    obj = envelope(created=service.started_at_iso)
    results = await asyncio.gather(*(send(service, obj) for _ in range(10)))
    assert all(r == results[0] for r in results)
    assert results[0][1]['action'] == 'accept'
    assert len(adapter.prepared) == 1
    assert service.journal.receipts
    await send(service, envelope('call.ended', event='ended'))
    assert len(adapter.closed) == 1
    assert not service.journal.receipts
    assert (await send(service, obj))[1] == {'action': 'decline'}
    await service.shutdown()


@pytest.mark.asyncio
async def test_signature_scope_conflicts_and_old_deadline(tmp_path):
    adapter = Adapter()
    service = Service(config(tmp_path), adapter)
    await service.start()
    obj = envelope(created=service.started_at_iso)
    assert (await send(service, obj, version='2'))[0] == 401
    assert (await send(service, obj, path='/hooks/other'))[0] == 404
    assert (await send(service, envelope(created='2000-01-01T00:00:00Z')))[1] == {'action': 'decline'}
    obj['event_id'] = 'new'
    await send(service, obj)
    changed = dict(obj, data={**obj['data'], 'call_id': 'other'})
    assert (await send(service, changed))[0] == 409
    await service.shutdown()


@pytest.mark.asyncio
async def test_terminal_cancels_preparation(tmp_path):
    adapter = Adapter()
    adapter.gate = asyncio.Event()
    service = Service(config(tmp_path), adapter)
    await service.start()
    task = asyncio.create_task(send(service, envelope(created=service.started_at_iso)))
    while not adapter.prepared:
        await asyncio.sleep(0)
    await send(service, envelope('call.failed', event='terminal'))
    assert (await task)[1] == {'action': 'decline'}
    assert adapter.closed and not service.journal.receipts
    await service.shutdown()


@pytest.mark.asyncio
async def test_receipt_before_effect_and_recovery_failure_closed(tmp_path):
    adapter = Adapter()
    service = Service(config(tmp_path), adapter)
    original = adapter.prepare
    async def prepare(spec):
        assert spec['room'] in service.journal.receipts
        assert (tmp_path / 'receipts.json').stat().st_mode & 0o777 == 0o600
        return await original(spec)
    adapter.prepare = prepare
    await service.start()
    await send(service, envelope(created=service.started_at_iso))
    adapter.fail_close = True
    await service.shutdown()
    second = Service(config(tmp_path), adapter)
    await second.start()
    assert not second.ready
    assert second.journal.receipts
    assert (await send(second, envelope(event='new', created=second.started_at_iso)))[1] == {'action': 'decline'}
    adapter.fail_close = False
    await second.shutdown()


@pytest.mark.parametrize('field', ['resource_ttl_seconds', 'future_tolerance_seconds', 'watch_interval_seconds'])
def test_nonfinite_limits_rejected(tmp_path, field):
    with pytest.raises(ValueError):
        config(tmp_path, **{field: float('nan')})


@pytest.mark.asyncio
async def test_deadline_cancels_and_does_not_renew_on_retry(tmp_path):
    adapter = Adapter()
    adapter.gate = asyncio.Event()
    service = Service(config(tmp_path, decision_budget_ms=30), adapter)
    await service.start()
    obj = envelope(created=service.started_at_iso)
    assert (await send(service, obj))[1] == {'action': 'decline'}
    assert len(adapter.prepared) == 1 and len(adapter.closed) == 1
    assert not service.journal.receipts
    assert (await send(service, obj))[1] == {'action': 'decline'}
    assert len(adapter.prepared) == 1
    await service.shutdown()


@pytest.mark.asyncio
async def test_terminal_before_incoming_suppresses_start(tmp_path):
    adapter = Adapter()
    service = Service(config(tmp_path), adapter)
    await service.start()
    await send(service, envelope('call.ended', event='terminal', created='2000-01-01T00:00:00Z'))
    assert (await send(service, envelope(created=service.started_at_iso)))[1] == {'action': 'decline'}
    assert not adapter.prepared
    await service.shutdown()


@pytest.mark.asyncio
async def test_worker_departure_cleans_up(tmp_path):
    adapter = Adapter()
    state = {'worker_present': True, 'outgoing_audio_frames': 1, 'outgoing_video_frames': 1}
    handle = Handle()
    handle.snapshot = lambda: dict(state)
    async def prepare(spec):
        return handle
    adapter.prepare = prepare
    service = Service(config(tmp_path, watch_interval_seconds=0.005), adapter)
    await service.start()
    await send(service, envelope(created=service.started_at_iso))
    await asyncio.sleep(0.02)
    state['worker_present'] = False
    await asyncio.sleep(0.02)
    assert adapter.closed
    await service.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario', ['ttl', 'missing_worker', 'producer_stall', 'agent_failed'])
async def test_watchdog_cleanup(tmp_path, scenario):
    adapter = Adapter()
    handle = Handle()
    state = {'worker_present': scenario != 'missing_worker', 'outgoing_audio_frames': 1, 'outgoing_video_frames': 1}
    handle.snapshot = lambda: state
    if scenario == 'agent_failed':
        handle.failed.set()
    async def prepare(spec):
        return handle
    adapter.prepare = prepare
    cfg = config(tmp_path, watch_interval_seconds=0.005,
                 resource_ttl_seconds=0.01 if scenario == 'ttl' else 100,
                 worker_arrival_timeout_seconds=0.01 if scenario == 'missing_worker' else 100,
                 stall_timeout_seconds=0.01 if scenario == 'producer_stall' else 100)
    service = Service(cfg, adapter)
    await service.start()
    await send(service, envelope(created=service.started_at_iso))
    await asyncio.sleep(0.05)
    assert adapter.closed
    assert not service.journal.receipts
    await service.shutdown()


@pytest.mark.asyncio
async def test_request_cancellation_does_not_cancel_shared_preparation(tmp_path):
    adapter = Adapter()
    adapter.gate = asyncio.Event()
    service = Service(config(tmp_path), adapter)
    await service.start()
    obj = envelope(created=service.started_at_iso)
    first = asyncio.create_task(send(service, obj))
    while not adapter.prepared:
        await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    adapter.gate.set()
    assert (await send(service, obj))[1]['action'] == 'accept'
    assert len(adapter.prepared) == 1
    await service.shutdown()


@pytest.mark.asyncio
async def test_journal_failure_prevents_side_effects(tmp_path):
    adapter = Adapter()
    service = Service(config(tmp_path), adapter)
    await service.start()
    def fail_save():
        raise OSError('private disk detail')
    original = service.journal._save
    service.journal._save = fail_save
    assert (await send(service, envelope(created=service.started_at_iso)))[1] == {'action': 'decline'}
    assert not adapter.prepared
    assert not service.ready
    service.journal._save = original
    await service.shutdown()


@pytest.mark.asyncio
async def test_deadline_declines_before_slow_cleanup_finishes(tmp_path):
    adapter = Adapter()
    adapter.gate = asyncio.Event()
    cleanup_gate = asyncio.Event()
    async def close(room):
        await cleanup_gate.wait()
    adapter.close_room = close
    service = Service(config(tmp_path, decision_budget_ms=20), adapter)
    await service.start()
    started = time.monotonic()
    request = asyncio.create_task(send(service, envelope(created=service.started_at_iso)))
    await asyncio.sleep(0.07)
    completed_promptly = request.done()
    cleanup_gate.set()
    await request
    await service.shutdown()
    assert completed_promptly, f'decline blocked on cleanup for {time.monotonic() - started}'


@pytest.mark.asyncio
async def test_recovery_deletes_only_receipted_rooms(tmp_path):
    cfg = config(tmp_path)
    from chert_reference_agent.journal import Journal
    journal = Journal(cfg.journal_path, cfg.project_fingerprint)
    journal.open()
    room = 'test-' + 'a' * 32
    journal.add(room)
    journal.close()
    adapter = Adapter()
    service = Service(cfg, adapter)
    await service.start()
    assert adapter.closed == [room]
    assert service.ready and not service.journal.receipts
    await service.shutdown()


@pytest.mark.asyncio
async def test_unknown_receipt_is_never_deleted(tmp_path):
    cfg = config(tmp_path)
    from chert_reference_agent.journal import Journal
    journal = Journal(cfg.journal_path, cfg.project_fingerprint)
    journal.open()
    journal.add('unknown-room')
    journal.close()
    adapter = Adapter()
    service = Service(cfg, adapter)
    with pytest.raises(RuntimeError):
        await service.start()
    assert not adapter.closed and not service.ready


@pytest.mark.parametrize('url', ['wss://example.test/path', 'wss://example.test/?secret=1'])
def test_endpoint_requires_origin(tmp_path, url):
    from dataclasses import replace
    with pytest.raises(ValueError):
        replace(config(tmp_path), livekit_url=url)


def test_replay_horizon_cannot_expire_during_decision(tmp_path):
    with pytest.raises(ValueError):
        config(tmp_path, replay_horizon_seconds=1)


@pytest.mark.asyncio
async def test_immediate_agent_failure_cannot_accept(tmp_path):
    adapter = Adapter()
    async def prepare(spec):
        handle = Handle()
        handle.failed.set()
        return handle
    adapter.prepare = prepare
    service = Service(config(tmp_path), adapter)
    await service.start()
    assert (await send(service, envelope(created=service.started_at_iso)))[1] == {'action': 'decline'}
    await service.shutdown()
    assert adapter.closed


@pytest.mark.asyncio
async def test_nonascii_signature_rejected(tmp_path):
    service = Service(config(tmp_path), Adapter())
    status, _ = await service.handle('/hooks/customer', {'X-Chert-Signature-Version': '1', 'X-Chert-Signature': 'sha256=é'}, b'{}')
    assert status == 401


@pytest.mark.asyncio
async def test_stalled_return_media_warns_without_closing_active_producer(tmp_path):
    from chert_reference_agent.observability import SafeLogger
    adapter = Adapter()
    handle = Handle()
    count = 0
    def snapshot():
        nonlocal count
        count += 1
        return {'worker_present': True, 'outgoing_audio_frames': count, 'outgoing_video_frames': count, 'incoming_audio_frames': 1}
    handle.snapshot = snapshot
    async def prepare(spec):
        return handle
    adapter.prepare = prepare
    cfg = config(tmp_path, watch_interval_seconds=0.005, stall_timeout_seconds=0.01)
    logs = []
    service = Service(cfg, adapter, logger=SafeLogger(cfg, logs.append))
    await service.start()
    await send(service, envelope(created=service.started_at_iso))
    await asyncio.sleep(0.04)
    assert any(json.loads(line)['reason'] == 'incoming_stall' for line in logs)
    assert not adapter.closed
    await service.shutdown()


@pytest.mark.asyncio
async def test_slow_cleanup_does_not_block_other_resource_watchers(tmp_path):
    adapter = Adapter()
    handles = []
    async def prepare(spec):
        adapter.prepared.append(spec)
        handle = Handle()
        handles.append(handle)
        return handle
    adapter.prepare = prepare
    gate = asyncio.Event()
    entered = []
    async def close(room):
        entered.append(room)
        if room == adapter.prepared[0]['room']:
            await gate.wait()
    adapter.close_room = close
    cfg = config(tmp_path, watch_interval_seconds=0.005, resource_ttl_seconds=0.03)
    service = Service(cfg, adapter)
    await service.start()
    for i in range(2):
        await send(service, envelope(call=f'call-{i}', event=f'event-{i}', created=service.started_at_iso))
    handles[0].failed.set()
    await asyncio.sleep(0.07)
    second_observed = adapter.prepared[1]['room'] in entered
    debt_ready = service.ready
    first_attempts = entered.count(adapter.prepared[0]['room'])
    gate.set()
    await service.shutdown()
    assert second_observed, 'first deletion stalled observation/TTL cleanup of second resource'
    assert not debt_ready
    assert first_attempts == 1


@pytest.mark.asyncio
async def test_shutdown_cleans_rooms_concurrently(tmp_path):
    adapter = Adapter()
    gate = asyncio.Event()
    entered = []
    async def close(room):
        entered.append(room)
        await gate.wait()
    adapter.close_room = close
    service = Service(config(tmp_path), adapter)
    await service.start()
    for i in range(2):
        await send(service, envelope(call=f'call-{i}', event=f'event-{i}', created=service.started_at_iso))
    shutdown = asyncio.create_task(service.shutdown())
    await asyncio.sleep(0.02)
    entered_together = len(entered) == 2
    gate.set()
    await shutdown
    assert entered_together
    assert not service.journal.receipts


@pytest.mark.asyncio
async def test_startup_recovery_does_not_block_other_orphans(tmp_path):
    from chert_reference_agent.journal import Journal
    cfg = config(tmp_path)
    rooms = ['test-' + 'a' * 32, 'test-' + 'b' * 32]
    journal = Journal(cfg.journal_path, cfg.project_fingerprint)
    journal.open()
    for room in rooms:
        journal.add(room)
    journal.close()
    adapter = Adapter()
    gate = asyncio.Event()
    second = asyncio.Event()
    async def close(room):
        if room == rooms[0]:
            await gate.wait()
        else:
            second.set()
    adapter.close_room = close
    service = Service(cfg, adapter)
    startup = asyncio.create_task(service.start())
    try:
        await asyncio.wait_for(second.wait(), 0.1)
        assert not service.ready
        assert rooms[0] in service.journal.receipts
    finally:
        gate.set()
        await startup
        await service.shutdown()
    assert not service.journal.receipts
