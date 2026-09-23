Connect my existing real-time AI agent to Chert FaceTime using Bring Your Own
Room (BYOR). Build and deploy the integration, including the webhook receiver,
and return the exact public HTTPS webhook URL to enter in Chert.

Complete the implementation and deployment. Do not stop at a plan, sample code,
or instructions for me to build the receiver.

1. **Inspect and preserve my agent.**

   Inspect this workspace and its instructions. Identify the conversation engine,
   audio input/output, optional avatar/video provider, session lifecycle, and
   deployment setup. Preserve existing prompts, workflows, tools, state transitions,
   timing, and completion behavior. Add FaceTime as another way to use this agent.

   Read the customer receiver reference at
   https://github.com/cherthq/chert-reference-agent, especially
   `docs/CONTRACT.md`, `docs/OPERATIONS.md`, source, and tests. It publishes test
   media; replace that fixture with my actual agent. Use the documented customer
   interface and current Chert onboarding configuration. Identify missing contract
   details rather than guessing. Do not require access to Chert's private source.

2. **Connect the media.**

   Reuse an existing LiveKit integration when available. Otherwise, connect the
   agent's supported media interfaces: route incoming caller audio to the agent
   and publish its generated speech into the room. If it has an avatar, publish
   its actual generated video from the same participant. Keep one agent audio
   publisher and preserve supported interruption and turn-taking behavior.

   Verify that the provider exposes a usable media path; a browser preview alone
   is insufficient. Do not assume caller video is available. If the provider
   cannot support this connection, identify the exact blocker without silently
   replacing my agent, provider, or avatar with a demo.

3. **Build the webhook receiver.**

   Implement a public HTTPS POST endpoint for Chert call events. On a valid
   `call.incoming`, prepare an isolated session and LiveKit room, ensure the agent
   is ready, and respond within the configured Chert decision deadline with
   `action="accept"`, `livekit_url`, `participant_token`, and
   `remote_participant_identity` as documented in `docs/CONTRACT.md`.

   Use the agent's identity for `remote_participant_identity`. Supply Chert a
   fresh, room-scoped token with the required permissions and a distinct identity.
   Never share conversation state between callers. If startup is too slow, use
   bounded preparation ahead of calls and exclusive session assignment. Decline
   when preparation fails; never accept an unready agent or extend the deadline.

   Handle duplicate/concurrent events, `call.started`, `call.ended`, `call.failed`,
   connection timeout, expiry, and restart-safe cleanup. Release agent/provider
   sessions and rooms on termination. Use Chert's supported call lifecycle.

   For a new receiver, follow the reference default:
   `CHERT_SIGNING_ENABLED=false` with explicit webhook routes. If signing is
   requested or already enabled, preserve it and verify the documented HMAC
   signature over exact request bytes using the configured numeric key version.
   Never fall back to unsigned mode. Keep secrets server-side and out of chat,
   source control, and logs. Add health/readiness endpoints. Do not add recordings
   or log caller media, tokens, or webhook request/response bodies.

4. **Deploy and verify.**

   Use my existing configured hosting if it supports the runtime requirements.
   The reference service requires one always-on process with writable persistent
   cleanup storage. Follow `docs/OPERATIONS.md`; do not put its long-lived media
   process into a request-scoped serverless function.

   Use existing authorized deployment tools and securely configured credentials.
   Ask only for genuinely missing access or configuration, a hosting choice if
   none is available, or approval for new spending. Never ask me to paste secrets
   into chat.

   Test validation, the configured signing mode, timely acceptance, room/token/
   identity binding, duplicate delivery, session isolation, and cleanup. Deploy
   and verify the actual public HTTPS POST route, health, and readiness. Run
   authorized synthetic lifecycle and LiveKit checks for outgoing audio, optional
   avatar video, incoming audio handling, and cleanup. Report what actually passed.

5. **Return the onboarding result.**

   Put the exact deployed webhook URL first, including its full route path.
   Do not substitute a frontend URL, LiveKit URL, localhost URL, placeholder,
   or deployment dashboard link. Then provide deployment/test status, the matching
   Chert signing setting, and any remaining setup or blockers.

   For unsigned mode, tell me to leave “Enable webhook signing” unchecked. For
   signed mode, securely install Chert's generated secret/version on the receiver.
   Changing the dashboard checkbox does not configure the server. Verify the
   deployed receiver's mode before reporting it.

   Tell me to use `customer_decides`, leave the Chert Assistant unassigned, and
   assign the integration after receiver setup is complete. Do not change line
   assignments or place a FaceTime call. Distinguish synthetic/LiveKit verification
   from a supervised real FaceTime test.

   If deployment is blocked, finish everything possible locally and identify the
   exact missing prerequisite. Never invent a webhook URL or claim an unverified
   integration is ready.
