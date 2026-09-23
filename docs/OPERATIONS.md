# Run your receiver

This repository contains a customer-side webhook receiver and LiveKit media
fixture. The fixture publishes a tone and changing color bars; it is not a
conversational agent. Connect your own agent in place of that fixture. See the
[customer contract](CONTRACT.md) for requests and responses.

## Local checks

Use Python 3.11–3.13 on macOS or Linux:

```sh
python3.11 -m venv .venv
.venv/bin/pip install -r requirements-dev.lock
.venv/bin/pip install --no-deps -e .
.venv/bin/python -m pytest -q
.venv/bin/ruff check src tests scripts
```

Tests use synthetic credentials and fake provider transports. Some tests open
loopback HTTP listeners. They do not connect to a real LiveKit project or make
FaceTime calls. Test success does not establish live media compatibility.

## Configuration

Supply environment variables through your host's protected configuration tools.
[.env.example](../.env.example) lists supported example settings; the service does
not load dotenv files automatically. Set your LiveKit URL, API key, API secret,
namespace, cleanup journal path, and allowed webhook routes. All example values
are placeholders. Do not commit your actual configuration.

Use an origin-only secure WebSocket URL, for example
`wss://project.example.test`, without a query string or non-root path. Choose a
distinct namespace and persistent storage location for each receiver deployment.

### Webhook signing

New receivers default to unsigned mode:

```sh
CHERT_SIGNING_ENABLED=false
CHERT_WEBHOOK_ROUTES_JSON='{"/webhooks/reference":{}}'
```

Leave **Enable webhook signing** unchecked in Chert. Unsigned mode validates
requests but does not authenticate their sender; anyone able to reach the route
can submit events. Signature headers are ignored in this mode.

To authenticate requests, enable signing in Chert and install its generated
numeric key version and secret through your host's secret configuration:

```sh
CHERT_SIGNING_ENABLED=true
CHERT_WEBHOOK_ROUTES_JSON='{"/webhooks/reference":{"1":"replace-with-signing-secret"}}'
```

Replace the example version and secret with the values supplied by Chert. Missing
or invalid signatures return 401; signed mode never falls back to unsigned.
Reverse proxies must preserve the exact request bytes. Unknown routes are rejected.

The signing mode applies to every route in one process and is read at startup.
Use separate deployments when you need different modes. Changing the Chert
checkbox does not configure your receiver. Before upgrading an existing signed
receiver, explicitly retain `CHERT_SIGNING_ENABLED=true`, its routes, and its key
versions. Keep previous keys available while calls or delayed events need them.
Verify the deployed mode with synthetic requests; health checks alone do not prove it.

## Hosting

The Dockerfile runs the receiver as UID 10001 on port 8080. Run one always-on
process/container and mount writable persistent storage at `/state`. Verify that
the mounted directory is writable by that UID. Terminate HTTPS at your hosting
platform and expose your configured POST route. Use `/healthz` for liveness and
`/readyz` for readiness. Readiness returns 503 during unresolved recovery or cleanup.

The default journal supports one process with an exclusive lock. Do not enable
replica autoscaling or use ephemeral storage. Request-scoped serverless execution
is unsuitable for this implementation's long-lived media connections.

To run without Docker, provide the environment and start:

```sh
python -m chert_reference_agent
```

Starting the service may contact LiveKit to recover previously owned rooms.
Use your intended project configuration. Drain active calls before configuration
changes; outstanding cleanup receipts require the original project configuration.
Do not delete journal data to clear a recovery failure. Allow shutdown time for
media teardown and room cleanup.

Use synchronized clocks. Confirm the decision deadline with your Chert onboarding
configuration. The receiver's default budget is four seconds, measured from the
event's `created_at`; the request does not supply the actual Chert deadline.
Allow network margin and measure preparation time before assigning the integration.
Retries do not reset the budget. Prewarm slow agent providers when needed.

Set the resource lifetime for your intended call duration, no longer than token
validity. Missing connection or terminal events are bounded by connection timeout
and resource expiry. Failed cleanup remains in persistent storage for retry;
cleanup depends on provider availability and intact storage.

Keep API secrets and tokens out of logs. Disable platform request/response body
capture and avoid logging webhook paths or caller media. The receiver emits
allowlisted structured logs and does not record media.

## Verification and Chert setup

Verify HTTPS, health, readiness, request validation, configured signing behavior,
timely acceptance, room/token binding, and cleanup on your deployed receiver.
Use a test project and synthetic calls for checks that allocate provider resources.

For the unmodified tone/color-bar fixture, an optional probe is available:

```sh
# Configure CHERT_WEBHOOK_URL and the same receiver/LiveKit environment securely.
.venv/bin/python scripts/livekit_probe.py --execute-approved-live
```

The probe sends synthetic webhooks, joins the allocated LiveKit room, decodes test
media, publishes generated return audio, and checks room deletion. It expects the
fixture's media pattern; adapt the checks after connecting your own agent. It
does not make a FaceTime call. Its `partial` result still requires receiver-side
return-audio observation and cleanup verification. Published tracks alone do not
prove audible speech or visible FaceTime video.

Register your exact public HTTPS POST URL in Chert with the matching signing mode.
Use `customer_decides`, leave the Chert Assistant unassigned, and assign the
integration after receiver verification. Confirm audio and any avatar video in
a separate supervised FaceTime call. Do not assume caller camera input is available.
