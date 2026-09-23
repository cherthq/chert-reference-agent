"""Entrypoint failures must not serialize configuration or SDK exceptions."""
import os
from pathlib import Path
import subprocess
import sys


def test_malformed_environment_never_prints_values():
    env = {"PATH": os.environ.get("PATH", ""), "CHERT_WEBHOOK_ROUTES_JSON": "[]",
           "LIVEKIT_URL": "wss://fixture.example.test", "LIVEKIT_API_KEY": "fixture-key",
           "LIVEKIT_API_SECRET": "synthetic-private-sentinel", "CHERT_NAMESPACE": "fixture",
           "CHERT_JOURNAL_PATH": "/unused/synthetic-private-sentinel"}
    result = subprocess.run([sys.executable, '-m', 'chert_reference_agent'], env=env,
                            cwd=Path(__file__).parents[1], capture_output=True, text=True, timeout=5)
    assert result.returncode != 0
    assert 'Traceback' not in result.stderr
    assert 'synthetic-private-sentinel' not in result.stderr
    assert 'configuration_invalid' in result.stderr
