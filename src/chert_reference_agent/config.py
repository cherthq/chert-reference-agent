"""Immutable configuration loaded from the deployment environment."""
from dataclasses import dataclass
import hashlib
import json
import math
import os
import re
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Config:
    webhook_routes: dict[str, dict[int, str]]
    livekit_url: str
    livekit_api_key: str
    livekit_api_secret: str
    namespace: str
    journal_path: str
    signing_enabled: bool = False
    decision_budget_ms: int = 4000
    token_ttl_seconds: int = 3600
    resource_ttl_seconds: float = 1800
    worker_arrival_timeout_seconds: float = 30
    watch_interval_seconds: float = 1
    stall_timeout_seconds: float = 10
    cleanup_timeout_seconds: float = 5
    cleanup_attempts: int = 3
    capacity: int = 100
    max_body_bytes: int = 65536
    future_tolerance_seconds: float = 1
    replay_horizon_seconds: float = 7200
    source_sha: str = 'unknown'
    image_digest: str = 'unknown'
    config_revision: str = 'unknown'

    def __post_init__(self):
        u = urlsplit(self.livekit_url)
        if u.path not in ('', '/') or u.query or u.scheme != 'wss' or not u.hostname or u.username or u.password or u.fragment or u.port not in (None, 443) or len(self.livekit_url.encode()) > 2048:
            raise ValueError('invalid LiveKit URL')
        if not re.fullmatch(r'[a-zA-Z0-9_-]{1,48}', self.namespace):
            raise ValueError('invalid namespace')
        if not self.livekit_api_key or not self.livekit_api_secret or not self.webhook_routes:
            raise ValueError('missing credentials')
        if type(self.signing_enabled) is not bool:
            raise ValueError('invalid signing mode')
        for path, keys in self.webhook_routes.items():
            if not path.startswith('/') or path in ('/healthz', '/readyz') or not isinstance(keys, dict) or (self.signing_enabled and not keys) or any(type(v) is not int or v < 1 or not isinstance(s, str) or not s for v, s in keys.items()):
                raise ValueError('invalid webhook routes')
        if not 1 <= self.token_ttl_seconds <= 7200 or not 0 < self.resource_ttl_seconds <= self.token_ttl_seconds:
            raise ValueError('invalid token/resource TTL')
        for field in ('decision_budget_ms', 'worker_arrival_timeout_seconds', 'watch_interval_seconds', 'stall_timeout_seconds', 'cleanup_timeout_seconds', 'cleanup_attempts', 'capacity', 'max_body_bytes', 'replay_horizon_seconds'):
            if not math.isfinite(getattr(self, field)) or getattr(self, field) <= 0:
                raise ValueError('invalid positive limit')
        if not math.isfinite(self.future_tolerance_seconds) or self.future_tolerance_seconds < 0:
            raise ValueError('invalid clock tolerance')
        for name in ('decision_budget_ms', 'token_ttl_seconds', 'cleanup_attempts', 'capacity', 'max_body_bytes'):
            if type(getattr(self, name)) is not int:
                raise ValueError('invalid integer limit')
        if self.replay_horizon_seconds < self.decision_budget_ms / 1000 + self.future_tolerance_seconds:
            raise ValueError('replay horizon shorter than decision freshness')
        for value in (self.source_sha, self.image_digest, self.config_revision):
            if not re.fullmatch(r'[a-zA-Z0-9_.:-]{1,128}', value):
                raise ValueError('invalid revision label')

    @property
    def project_fingerprint(self):
        return hashlib.sha256(json.dumps([self.namespace, self.livekit_url, self.livekit_api_key, self.livekit_api_secret]).encode()).hexdigest()

    @classmethod
    def from_env(cls):
        routes = json.loads(os.environ['CHERT_WEBHOOK_ROUTES_JSON'])
        args = dict(webhook_routes={p: {int(v): s for v, s in keys.items()} for p, keys in routes.items()}, livekit_url=os.environ['LIVEKIT_URL'], livekit_api_key=os.environ['LIVEKIT_API_KEY'], livekit_api_secret=os.environ['LIVEKIT_API_SECRET'], namespace=os.environ['CHERT_NAMESPACE'], journal_path=os.environ['CHERT_JOURNAL_PATH'])
        for name, field in cls.__dataclass_fields__.items():
            env = 'CHERT_' + name.upper()
            if name not in args and env in os.environ:
                if field.type is bool:
                    value = os.environ[env].strip().lower()
                    if value not in ('true', 'false'):
                        raise ValueError('invalid boolean setting')
                    args[name] = value == 'true'
                else:
                    args[name] = field.type(os.environ[env])
        return cls(**args)
