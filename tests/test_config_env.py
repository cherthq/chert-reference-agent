"""Runtime environment parsing uses declared types, including fractional seconds."""
import json

from chert_reference_agent.config import Config


def test_fractional_intervals_from_environment(monkeypatch, tmp_path):
    for key, value in {
        'CHERT_WEBHOOK_ROUTES_JSON': json.dumps({'/test': {'1': 'synthetic'}}),
        'LIVEKIT_URL': 'wss://synthetic.example',
        'LIVEKIT_API_KEY': 'synthetic-key',
        'LIVEKIT_API_SECRET': 'synthetic-secret',
        'CHERT_NAMESPACE': 'synthetic',
        'CHERT_JOURNAL_PATH': str(tmp_path / 'rooms.json'),
        'CHERT_WATCH_INTERVAL_SECONDS': '0.25',
        'CHERT_CLEANUP_TIMEOUT_SECONDS': '0.5',
        'CHERT_RESOURCE_TTL_SECONDS': '12.5',
        'CHERT_CAPACITY': '4',
    }.items():
        monkeypatch.setenv(key, value)
    cfg = Config.from_env()
    assert cfg.watch_interval_seconds == 0.25
    assert cfg.cleanup_timeout_seconds == 0.5
    assert cfg.resource_ttl_seconds == 12.5
    assert cfg.capacity == 4 and type(cfg.capacity) is int
