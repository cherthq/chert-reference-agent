"""Both receiver modes share admission, replay and durable cleanup behavior."""
import asyncio
from dataclasses import replace
import json

import pytest
from aiohttp.test_utils import TestClient, TestServer

from chert_reference_agent.config import Config
from chert_reference_agent.http import create_app
from chert_reference_agent.service import Service
from test_core_service import Adapter, config, envelope, send


def environment(monkeypatch, tmp_path):
    for key, value in {
        'CHERT_WEBHOOK_ROUTES_JSON': '{"/hooks/customer":{}}',
        'LIVEKIT_URL': 'wss://example.test', 'LIVEKIT_API_KEY': 'test-key',
        'LIVEKIT_API_SECRET': 'test-api-secret', 'CHERT_NAMESPACE': 'test',
        'CHERT_JOURNAL_PATH': str(tmp_path / 'receipts.json'),
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv('CHERT_SIGNING_ENABLED', raising=False)


@pytest.mark.parametrize('value,expected', [(None, False), ('false', False), ('true', True)])
def test_mode_environment(monkeypatch, tmp_path, value, expected):
    environment(monkeypatch, tmp_path)
    if value is not None:
        monkeypatch.setenv('CHERT_SIGNING_ENABLED', value)
    if expected:
        monkeypatch.setenv('CHERT_WEBHOOK_ROUTES_JSON', '{"/hooks/customer":{"1":"test-secret"}}')
    assert Config.from_env().signing_enabled is expected


@pytest.mark.parametrize('value', ['', '0', '1', 'yes', 'flase'])
def test_invalid_mode_fails_startup(monkeypatch, tmp_path, value):
    environment(monkeypatch, tmp_path)
    monkeypatch.setenv('CHERT_SIGNING_ENABLED', value)
    with pytest.raises(ValueError):
        Config.from_env()


def test_signed_requires_keys_on_every_route(monkeypatch, tmp_path):
    environment(monkeypatch, tmp_path)
    monkeypatch.setenv('CHERT_SIGNING_ENABLED', 'true')
    with pytest.raises(ValueError):
        Config.from_env()
    with pytest.raises(ValueError):
        replace(config(tmp_path), signing_enabled=True,
                webhook_routes={'/hooks/customer': {1: 'test-secret'}, '/empty': {}})


@pytest.mark.parametrize('signed_mode', [False, True])
async def test_http_modes_duplicates_and_cleanup(tmp_path, signed_mode):
    cfg = replace(config(tmp_path), signing_enabled=signed_mode,
                  webhook_routes={'/hooks/customer': {1: 'test-secret'} if signed_mode else {}})
    adapter = Adapter()
    service = Service(cfg, adapter)
    async with TestClient(TestServer(create_app(service))) as client:
        obj = envelope(created=service.started_at_iso)
        async def deliver(obj):
            if signed_mode:
                from scripts.livekit_probe import signed
                body, headers = signed(obj, 'test-secret', 1)
            else:
                body, headers = json.dumps(obj).encode(), {}
            response = await client.post('/hooks/customer', data=body, headers=headers)
            return response.status, await response.json()
        replies = await asyncio.gather(*(deliver(obj) for _ in range(8)))
        assert all(reply == replies[0] for reply in replies)
        assert replies[0][0] == 200 and replies[0][1]['action'] == 'accept'
        assert len(adapter.prepared) == 1
        assert await deliver(envelope('call.started', event='started')) == (200, {'ok': True})
        assert (await client.post('/unknown', json=obj)).status == 404
        assert (await service.handle('/unknown', {}, json.dumps(obj).encode()))[0] == 404
        assert await deliver(envelope('call.ended', event='ended')) == (200, {'ok': True})
        assert len(adapter.closed) == 1 and not service.journal.receipts
        assert (await deliver(obj))[1] == {'action': 'decline'}


@pytest.mark.parametrize('body', [b'{', b'null', b'[]', b'{}', b'\xff',
                                  b'{"data":null}', b'{"type":"unknown"}'])
async def test_unsigned_malformed_requests(tmp_path, body):
    adapter = Adapter()
    service = Service(replace(config(tmp_path), signing_enabled=False), adapter)
    await service.start()
    try:
        assert (await service.handle('/hooks/customer', {}, body))[0] == 400
        assert not adapter.prepared and not service.events
    finally:
        await service.shutdown()


@pytest.mark.parametrize('headers', [{}, {'X-Chert-Signature-Version': '1'},
    {'X-Chert-Signature-Version': '1', 'X-Chert-Signature': 'sha256=bad'},
    {'X-Chert-Signature-Version': 'x', 'X-Chert-Signature': 'sha256=bad'}])
async def test_signed_never_falls_back(tmp_path, headers):
    adapter = Adapter()
    service = Service(replace(config(tmp_path), signing_enabled=True), adapter)
    await service.start()
    try:
        obj = envelope(created=service.started_at_iso)
        assert (await service.handle('/hooks/customer', headers, json.dumps(obj).encode()))[0] == 401
        assert not adapter.prepared and not service.events
        assert (await send(service, obj))[1]['action'] == 'accept'
    finally:
        await service.shutdown()


async def test_unsigned_ignores_signature_headers_and_recovers_receipts(monkeypatch, tmp_path):
    environment(monkeypatch, tmp_path)
    cfg = Config.from_env()
    adapter = Adapter()
    service = Service(cfg, adapter)
    await service.start()
    obj = envelope(created=service.started_at_iso)
    assert (await service.handle('/hooks/customer', {'X-Chert-Signature': 'bad'},
                                 json.dumps(obj).encode()))[1]['action'] == 'accept'
    adapter.fail_close = True
    await service.shutdown()
    assert service.journal.receipts
    recovered = Adapter()
    service = Service(cfg, recovered)
    await service.start()
    assert service.ready and len(recovered.closed) == 1 and not service.journal.receipts
    await service.shutdown()


async def test_signed_exact_bytes_versions_and_route_key_isolation(tmp_path):
    from scripts.livekit_probe import signed
    cfg = replace(config(tmp_path), webhook_routes={
        '/hooks/customer': {1: 'test-secret', 2: 'rotated-secret'}, '/other': {1: 'other-secret'}})
    adapter = Adapter()
    service = Service(cfg, adapter)
    await service.start()
    try:
        obj = envelope(created=service.started_at_iso)
        body, headers = signed(obj, 'test-secret', 1)
        assert (await service.handle('/hooks/customer', headers, body + b' '))[0] == 401
        assert (await service.handle('/other', headers, body))[0] == 401
        for version in ['2', '99', '-1', '1.0', '١', '9' * 100]:
            wrong = dict(headers, **{'X-Chert-Signature-Version': version})
            assert (await service.handle('/hooks/customer', wrong, body))[0] == 401
        assert not adapter.prepared
        body, headers = signed(obj, 'rotated-secret', 2)
        assert (await service.handle('/hooks/customer', headers, body))[1]['action'] == 'accept'
    finally:
        await service.shutdown()


async def test_unsigned_limits_conflicts_failure_and_log_privacy(tmp_path, capsys):
    cfg = replace(config(tmp_path), signing_enabled=False, capacity=1, max_body_bytes=1000)
    adapter = Adapter()
    service = Service(cfg, adapter)
    await service.start()
    async def deliver(obj):
        return await service.handle('/hooks/customer', {}, json.dumps(obj).encode())
    try:
        assert (await service.handle('/hooks/customer', {}, b'x' * 1001))[0] == 413
        assert (await deliver(envelope(created='2000-01-01T00:00:00Z')))[1]['action'] == 'decline'
        obj = envelope(event='fresh', call='private-caller', created=service.started_at_iso)
        assert (await deliver(obj))[1]['action'] == 'accept'
        other = envelope(event='other', call='second', created=service.started_at_iso)
        assert (await deliver(other))[1]['action'] == 'decline'
        assert (await deliver(dict(obj, data=other['data'])))[0] == 409
        assert len(adapter.prepared) == 1
        assert (await deliver(envelope('call.failed', call='private-caller', event='failed')))[0] == 200
        assert not service.journal.receipts
    finally:
        await service.shutdown()
    logs = capsys.readouterr().out
    assert all(secret not in logs for secret in ('private-caller', 'test-secret', 'synthetic-token', 'call_id'))


@pytest.mark.parametrize('mode', [False, True])
def test_probe_matches_receiver_mode(tmp_path, mode):
    from scripts.livekit_probe import request_payload, signed
    cfg = replace(config(tmp_path), signing_enabled=mode,
                  webhook_routes={'/hooks/customer': {1: 'test-secret'} if mode else {}})
    obj = envelope()
    body, headers = request_payload(cfg, '/hooks/customer', obj)
    assert json.loads(body) == obj
    if mode:
        assert (body, headers) == signed(obj, 'test-secret', 1)
    else:
        assert headers == {'Content-Type': 'application/json'}
