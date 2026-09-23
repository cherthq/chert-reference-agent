"""Synthetic LiveKit probe checks over loopback HTTP, with no provider connection."""
import asyncio
from dataclasses import replace
import importlib.util
from pathlib import Path
import subprocess
import sys

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
import pytest

from chert_reference_agent.http import create_app
from chert_reference_agent.livekit_adapter import LiveKitAdapter
from chert_reference_agent.observability import SafeLogger
from chert_reference_agent.service import Service
from test_core_service import config
from test_livekit_adapter import FakeAPI, FakeRoom, claims

spec = importlib.util.spec_from_file_location('livekit_probe', Path(__file__).parents[1] / 'scripts/livekit_probe.py')
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def test_live_probe_requires_explicit_opt_in():
    result = subprocess.run([sys.executable, 'scripts/livekit_probe.py'], capture_output=True, text=True)
    assert result.returncode == 2
    assert 'No connection made' in result.stderr
    assert not result.stdout


@pytest.mark.asyncio
async def test_signed_loopback_http_concurrency_binding_cleanup(tmp_path):
    cfg = replace(config(tmp_path), livekit_api_secret='0' * 32)
    logs = []
    provider = FakeAPI()
    rooms = []
    def room_factory():
        room = FakeRoom()
        rooms.append(room)
        return room
    adapter = LiveKitAdapter(cfg.livekit_url, cfg.livekit_api_key, cfg.livekit_api_secret,
                             room_factory=room_factory, api_factory=lambda **kw: provider)
    service = Service(cfg, adapter, logger=SafeLogger(cfg, logs.append))
    async with TestServer(create_app(service), host='127.0.0.1') as server:
        async with aiohttp.ClientSession() as session:
            endpoint = str(server.make_url('/hooks/customer'))
            obj = probe.envelope()
            body, headers = probe.signed(obj, 'test-secret', 1)
            bad = dict(headers, **{'X-Chert-Signature': 'sha256=' + '0' * 64})
            assert (await probe.post(session, endpoint, body, bad))[0] == 401
            assert not provider.created and not service.journal.receipts
            results = await asyncio.gather(*(probe.post(session, endpoint, body, headers) for _ in range(8)))
            assert all(s == 200 and r == results[0][1] for s, r, _ in results)
            accepted = results[0][1]
            assert accepted['action'] == 'accept'
            assert len(provider.created) == len(rooms) == 1
            worker = claims(accepted['participant_token'])
            agent = claims(rooms[0].token)
            assert worker['video']['room'] == agent['video']['room'] == provider.created[0]
            assert worker['sub'] != agent['sub'] == accepted['remote_participant_identity']
            handle = next(iter(adapter._handles.values()))
            terminal, th = probe.signed(probe.envelope('call.ended', obj['data']['call_id']), 'test-secret', 1)
            assert (await probe.post(session, endpoint, terminal, th))[0] == 200
            assert not service.journal.receipts and not adapter._handles
            assert not handle.tasks and not handle.sources and not rooms[0].connected
            assert (await probe.post(session, endpoint, body, headers))[1] == {'action': 'decline'}
    assert provider.closed
    for forbidden in (obj['data']['call_id'], accepted['participant_token'], rooms[0].token,
                      cfg.livekit_url, cfg.livekit_api_secret, provider.created[0]):
        assert forbidden not in '\n'.join(logs)


@pytest.mark.asyncio
async def test_probe_does_not_forward_credentials_on_redirect():
    received = []
    app = web.Application()
    async def redirect(request):
        raise web.HTTPTemporaryRedirect('/destination')
    async def destination(request):
        received.append(True)
        return web.json_response({})
    app.router.add_post('/redirect', redirect)
    app.router.add_post('/destination', destination)
    async with TestServer(app, host='127.0.0.1') as server:
        async with aiohttp.ClientSession() as session:
            with pytest.raises(aiohttp.ContentTypeError):
                await probe.post(session, str(server.make_url('/redirect')), b'{}', {})
    assert not received
