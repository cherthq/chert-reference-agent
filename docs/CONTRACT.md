# Customer receiver contract

This describes the interface used by this reference implementation. Confirm your
Chert account's current onboarding settings before deploying. Example identifiers,
hosts, and credentials below are placeholders.

## Request

Chert sends JSON in an HTTPS POST to the webhook route you register:

```json
{
  "event_id": "example-event-id",
  "type": "call.incoming",
  "created_at": "2026-01-01T12:00:00Z",
  "data": {
    "call_id": "example-call-id",
    "line_id": "example-line-id",
    "media": {"audio": true, "video": true},
    "occurred_at": "2026-01-01T12:00:00Z"
  }
}
```

The receiver validates `event_id`, `type`, timezone-aware `created_at`, and
`data.call_id`, `data.line_id`, and `data.media`. Additional fields may be present;
do not log request bodies. Use a fresh timestamp for synthetic incoming-call tests.

Supported events are `call.incoming`, `call.started`, `call.ended`, and
`call.failed`. Retries may repeat events. Coalesce concurrent incoming events for
the same call and keep resources isolated across calls and configured routes.
Terminal events stop the session and release its resources. Timeouts and expiry
must also release resources if a terminal event is not received.

## Incoming-call response

Return HTTP 200 with either acceptance or decline within the configured decision
deadline. The agent must already be ready in the supplied room before acceptance.

Acceptance:

```json
{
  "action": "accept",
  "livekit_url": "wss://project.example.test",
  "participant_token": "<fresh-room-scoped-LiveKit-JWT>",
  "remote_participant_identity": "<your-agent-participant-identity>"
}
```

Supply a LiveKit token with `roomJoin`, `canPublish`, and `canSubscribe` enabled
for exactly that room. Its subject must identify a participant distinct from your
agent. Use short-lived tokens with valid integer `nbf` and `exp` timestamps and
a validity interval no longer than two hours. Keep the LiveKit API secret on your
server; only the scoped participant token belongs in this response.

Publish the agent's speech and optional avatar video from the same participant.
Keep a single agent audio publisher. Subscribe to incoming caller audio and feed
it to your conversation engine. Caller video availability requires separate
confirmation and must not be a prerequisite for ordinary audio startup.

Decline:

```json
{"action": "decline"}
```

The actual Chert decision deadline is not included in the event. Configure your
receiver's budget from your onboarding settings, allowing network margin. This
reference defaults to four seconds from `created_at`. Retries do not renew it.
Decline if preparation cannot finish in time.

For recognized lifecycle notifications, the reference returns HTTP 200 with
`{"ok": true}`. Invalid envelopes return 400, unknown routes 404, oversized
requests 413, and conflicting reuse of an event ID 409. The reference can return
503 when it cannot safely retain more lifecycle/replay state.

## Optional signature verification

When signing is enabled, verify before parsing the request. The headers are:

```text
X-Chert-Signature: sha256=<hex HMAC-SHA256(secret, exact request bytes)>
X-Chert-Signature-Version: <configured positive integer version>
```

Use the secret for that route and exact numeric version. Compare signatures in
constant time. Missing signatures, unknown versions, or invalid signatures return
401. Do not reserialize JSON before verification or try a different key version
as a fallback. Match the receiver's mode to the dashboard setting; see
[operations](OPERATIONS.md).
