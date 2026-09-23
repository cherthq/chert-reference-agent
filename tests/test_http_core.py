import hashlib
import hmac
import json
from types import SimpleNamespace

import pytest
from chert_reference_agent.http import create_app
from chert_reference_agent.observability import SafeLogger
from test_core_service import Adapter, config, envelope
from chert_reference_agent.service import Service


class Stream:
    def __init__(self, body):
        self.body = body
    async def iter_chunked(self, size):
        for i in range(0, len(self.body), size):
            yield self.body[i:i + size]


@pytest.mark.asyncio
async def test_http_routes_signed_body_and_size_without_sockets(tmp_path):
    cfg = config(tmp_path, max_body_bytes=1000)
    service = Service(cfg, Adapter())
    app = create_app(service)
    await service.start()
    route = next(r for r in app.router.routes() if r.method == 'POST')
    body = json.dumps(envelope(created=service.started_at_iso)).encode()
    headers = {'X-Chert-Signature-Version': '1', 'X-Chert-Signature': 'sha256=' + hmac.new(b'test-secret', body, hashlib.sha256).hexdigest()}
    response = await route.handler(SimpleNamespace(content=Stream(body), path='/hooks/customer', headers=headers))
    assert response.status == 200
    assert json.loads(response.body)['action'] == 'accept'
    response = await route.handler(SimpleNamespace(content=Stream(b'x' * 1001), path='/hooks/customer', headers=headers))
    assert response.status == 413
    await service.shutdown()


def test_logging_uses_only_allowlisted_values(tmp_path):
    lines = []
    logger = SafeLogger(config(tmp_path), lines.append)
    logger.emit('watch', 'summary', 'private-call-id', participant_token='secret-jwt', room='private-room', incoming_audio_samples=15, arbitrary='secret', outgoing_audio_frames=float('nan'))
    record = json.loads(lines[0])
    assert record['incoming_audio_samples'] == 15
    assert len(record['correlation']) == 24
    assert not any(secret in lines[0] for secret in ('private-call-id', 'secret-jwt', 'private-room', 'arbitrary', 'NaN'))
    with pytest.raises(ValueError):
        logger.emit('secret-event', 'summary')
