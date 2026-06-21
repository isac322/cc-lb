//! Writer stream W2 — credentials and incident response.
//!
//! Hosts the W2 feature scenarios (F5, F7, F8, F10, F11A, F11B, F11C;
//! 66 scenarios total in the v5.2 corpus). Each feature lives in its
//! own `mod w2_f<n>` submodule. See `docs/cc-lb-bdd-fast-subset.md`
//! for the W2 fast-subset budget (9 scenarios).

use cc_lb_bdd_tests::{
    KillswitchState, W2CompatibilityCacheResult, W2CredentialIncidentResult, W2KillswitchResult,
    W2OAuthConsentResult, W2QuotaVisibilityResult, W2UpstreamOutageResult, W2WarmupResult,
    bdd_scenario,
};

mod w2_f5 {
    use super::*;

    bdd_scenario! {
        id: "F5.1",
        fn_name: fast_f5_1,
        persona: Charlie,
        title: "cc-lb automatically rotates a credential that is close to expiry",
        description:
            "Charlie observes an expiring OAuth credential during the rotation cycle. A new credential is stored, the expiry moves forward, Bob's calls continue, and the rotation is recorded in audit.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_rotate_expiring_credential().await? },
        then: |result, ctx| {
            let result: W2CredentialIncidentResult = result;
            ctx.assert(result.credential_stored, "rotated credential was not stored");
            ctx.assert(result.refreshed_expiry > result.previous_expiry, "credential expiry did not move forward");
            ctx.assert(result.calls_continue, "calls were interrupted during rotation");
            ctx.assert(result.audit_recorded, "credential rotation audit row missing");
        },
    }

    bdd_scenario! {
        id: "F5.2",
        fn_name: f5_2,
        persona: Charlie,
        title: "cc-lb notifies the operator when automatic rotation fails repeatedly",
        description:
            "Charlie observes repeated automatic rotation failures. The next failure produces a single operator notification that identifies the credential and asks for manual intervention.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_notify_rotation_failure().await? },
        then: |result, ctx| {
            let result: W2CredentialIncidentResult = result;
            ctx.assert(result.notification_sent, "rotation failure notification was not sent");
            ctx.assert(result.guidance.contains("manual intervention"), "manual intervention guidance missing");
        },
    }

    bdd_scenario! {
        id: "F5.3",
        fn_name: f5_3,
        persona: Charlie,
        title: "cc-lb increases the backoff interval when automatic rotation fails repeatedly",
        description:
            "Charlie observes repeated rotation failures for one credential. The next failed attempt increases the retry backoff so Anthropic is not hammered by repeated refresh attempts.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_increase_rotation_backoff().await? },
        then: |result, ctx| {
            let result: W2CredentialIncidentResult = result;
            ctx.assert(result.backoff_increased, "rotation backoff did not increase after repeated failures");
        },
    }

    bdd_scenario! {
        id: "F5.4",
        fn_name: fast_f5_4,
        persona: Charlie,
        title: "A malformed credential is rejected at registration time",
        description:
            "Charlie submits a malformed credential through the operator path. Registration is rejected with clear guidance, nothing is stored, and Bob's call path cannot use that credential.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_reject_malformed_credential().await? },
        then: |result, ctx| {
            let result: W2CredentialIncidentResult = result;
            ctx.assert(result.malformed_rejected, "malformed credential was not rejected");
            ctx.assert(!result.credential_stored, "malformed credential was stored");
            ctx.assert(result.calls_blocked, "malformed credential reached Bob's call path");
        },
    }

    bdd_scenario! {
        id: "F5.5",
        fn_name: fast_f5_5,
        persona: Charlie,
        title: "Revoking a credential immediately stops all calls that used it",
        description:
            "Charlie revokes credential K during an incident. New calls cannot use K, in-progress work cannot advance through K, and Bob receives an administrator-revoked message.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_revoke_credential().await? },
        then: |result, ctx| {
            let result: W2CredentialIncidentResult = result;
            ctx.assert(result.calls_blocked, "revoked credential still accepted calls");
            ctx.assert(result.audit_recorded, "credential revoke audit row missing");
            ctx.assert(result.guidance.contains("revoked"), "revoked-credential guidance missing");
        },
    }

    bdd_scenario! {
        id: "F5.6",
        fn_name: f5_6,
        persona: Charlie,
        title: "Audit records for a revoked credential remain intact",
        description:
            "Charlie checks the revoked credential history after time has passed. The plaintext credential stays hidden, while timestamps, outcomes, and audit history remain visible and untampered.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_preserve_revoked_credential_audit().await? },
        then: |result, ctx| {
            let result: W2CredentialIncidentResult = result;
            ctx.assert(result.audit_recorded, "revoked credential audit history missing");
            ctx.assert(result.secret_hidden, "revoked credential plaintext was exposed");
            ctx.assert(result.history_visible, "revoked credential history was not visible");
        },
    }

    bdd_scenario! {
        id: "F5.7",
        fn_name: f5_7,
        persona: Charlie,
        title: "The operator is notified when the permission on the credential protection key drifts",
        description:
            "Charlie detects that the protection key permission drifted from the safe state. Alice and Dana are notified, and new credential registration is blocked until permission is restored.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_detect_protection_permission_drift().await? },
        then: |result, ctx| {
            let result: W2CredentialIncidentResult = result;
            ctx.assert(!result.protection_permission_ok, "protection permission drift was not detected");
            ctx.assert(result.notification_sent, "protection permission notification missing");
            ctx.assert(result.registration_blocked, "credential registration remained open during permission drift");
        },
    }

    bdd_scenario! {
        id: "F5.8",
        fn_name: f5_8,
        persona: Charlie,
        title: "The state of a credential is displayed in a human-readable form in a single view",
        description:
            "Charlie opens a credential detail view. The credential state is rendered as a human-readable status and the same view gives guidance for the next operator action.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_display_credential_state().await? },
        then: |result, ctx| {
            let result: W2CredentialIncidentResult = result;
            ctx.assert(result.credential_stored, "credential detail did not find stored credential");
            ctx.assert(result.status_label == "found and healthy", format!("unexpected credential status label: {}", result.status_label));
            ctx.assert(!result.guidance.is_empty(), "credential detail guidance missing");
        },
    }

    bdd_scenario! {
        id: "F5.9",
        fn_name: f5_9,
        persona: Charlie,
        title: "When two people try to edit the same credential simultaneously, only one is accepted",
        description:
            "Charlie observes two near-simultaneous edits to credential K. The first edit applies, the second edit is rejected, and the rejected operator is told to reread and retry.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_reject_second_credential_edit().await? },
        then: |result, ctx| {
            let result: W2CredentialIncidentResult = result;
            ctx.assert(result.first_edit_applied, "first credential edit did not apply");
            ctx.assert(result.second_edit_rejected, "second credential edit was not rejected");
            ctx.assert(result.guidance.contains("retry"), "concurrent edit retry guidance missing");
        },
    }
}

mod w2_f7 {
    use super::*;

    bdd_scenario! {
        id: "F7.1",
        fn_name: fast_f7_1,
        persona: Charlie,
        title: "Charlie engaging the killswitch flips the global signal on",
        description:
            "Charlie flips the emergency killswitch. The global flag reports enabled=true so the request pipeline can reject subsequent calls. The storage-side invariant is that the flag actually flips.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.enable_killswitch().await? },
        then: |state, ctx| {
            let state: KillswitchState = state;
            ctx.assert(state.enabled, format!("killswitch did not engage: expected enabled=true, actual={}", state.enabled));
        },
    }

    bdd_scenario! {
        id: "F7.2",
        fn_name: fast_f7_2,
        persona: Charlie,
        title: "Charlie disengaging the killswitch returns the system to normal",
        description:
            "Charlie first engages the killswitch, then disengages it. The global flag returns to enabled=false so traffic can be accepted again without bouncing the process.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.enable_killswitch().await?; charlie.disable_killswitch().await? },
        then: |state, ctx| {
            let state: KillswitchState = state;
            ctx.assert(!state.enabled, format!("killswitch did not disengage: expected enabled=false, actual={}", state.enabled));
        },
    }

    bdd_scenario! {
        id: "F7.3",
        fn_name: f7_3,
        persona: Charlie,
        title: "The emergency killswitch state persists across a cc-lb restart",
        description:
            "Charlie replaces the cc-lb process while the killswitch is active. When cc-lb becomes ready again, the killswitch remains active and the first call remains rejected.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_persist_killswitch_after_restart().await? },
        then: |result, ctx| {
            let result: W2KillswitchResult = result;
            ctx.assert(result.enabled, "killswitch was not enabled before restart");
            ctx.assert(result.persisted_after_restart, "killswitch did not persist after restart");
            ctx.assert(result.call_rejected, "first call after restart was not rejected");
        },
    }

    bdd_scenario! {
        id: "F7.4",
        fn_name: f7_4,
        persona: Charlie,
        title: "The management screen and dashboard remain operational during an emergency killswitch",
        description:
            "Charlie keeps the emergency killswitch active while management surfaces remain available. The dashboard opens, credential revocation remains possible, and deactivation remains possible.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_keep_management_available_during_killswitch().await? },
        then: |result, ctx| {
            let result: W2KillswitchResult = result;
            ctx.assert(result.enabled, "killswitch was not active for management availability check");
            ctx.assert(result.dashboard_available, "dashboard unavailable during killswitch");
            ctx.assert(result.management_available, "management surface unavailable during killswitch");
            ctx.assert(result.revoke_available, "credential revocation unavailable during killswitch");
        },
    }

    bdd_scenario! {
        id: "F7.5",
        fn_name: fast_f7_5,
        persona: Charlie,
        title: "Responses rejected by the killswitch clearly indicate an operator-imposed block",
        description:
            "Charlie verifies a call rejected by the active killswitch. The response says the operator temporarily blocked traffic and distinguishes the decision from an upstream outage.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_killswitch_rejection_message().await? },
        then: |result, ctx| {
            let result: W2KillswitchResult = result;
            ctx.assert(result.call_rejected, "killswitch did not reject the call");
            ctx.assert(result.response_message.contains("operator"), "killswitch response did not mention operator decision");
            ctx.assert(result.operator_decision_visible, "operator decision was not distinguishable from outage");
        },
    }

    bdd_scenario! {
        id: "F7.6",
        fn_name: f7_6,
        persona: Charlie,
        title: "Activation and deactivation require a two-step confirmation",
        description:
            "Charlie verifies the killswitch confirmation guard. A single click is not enough, and both activation and deactivation require the second confirmation step.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_require_killswitch_confirmation().await? },
        then: |result, ctx| {
            let result: W2KillswitchResult = result;
            ctx.assert(result.two_step_required, "killswitch did not require two-step confirmation");
            ctx.assert(result.activated_after_second_confirm, "activation did not complete after second confirmation");
            ctx.assert(result.deactivated_after_second_confirm, "deactivation did not complete after second confirmation");
        },
    }

    bdd_scenario! {
        id: "F7.7",
        fn_name: f7_7,
        persona: Charlie,
        title: "The reason for each killswitch activation and deactivation is subject to audit",
        description:
            "Charlie enters a human-readable reason during the killswitch decision. The reason, decision-maker, and timestamp are recorded so Dana can trace the incident later.",
        given: |ctx| { ctx.charlie().await },
        when: |charlie| { charlie.charlie_w2_audit_killswitch_reason().await? },
        then: |result, ctx| {
            let result: W2KillswitchResult = result;
            ctx.assert(result.reason_audited, "killswitch reason was not audited");
            ctx.assert(result.reason_traceable, "killswitch reason was not traceable later");
        },
    }
}

mod w2_f8 {
    use super::*;

    bdd_scenario! { id: "F8.1", fn_name: f8_1, persona: Charlie, title: "When the upstream slows temporarily, users are notified of the delay", description: "Charlie observes a slow upstream. The user is not dropped and receives a delay notice with guidance to wait briefly.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_notify_slow_upstream().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.user_notified, "slow upstream user notice missing"); ctx.assert(result.retry_guidance, "slow upstream guidance missing"); }, }
    bdd_scenario! { id: "F8.2", fn_name: f8_2, persona: Charlie, title: "When the upstream slows, the reason is shown on the operator dashboard", description: "Charlie observes a slow upstream from the operator surface. The dashboard reason is recorded as upstream delay.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_upstream_delay_reason().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.dashboard_reason == "upstream delay", format!("unexpected dashboard reason: {}", result.dashboard_reason)); }, }
    bdd_scenario! { id: "F8.3", fn_name: f8_3, persona: Charlie, title: "Anthropic's rate-limit-exceeded response is passed through to users as-is", description: "Charlie observes a rate-limit response for one credential. The same meaning reaches Bob unchanged and calls using other credentials continue.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_pass_rate_limit_response().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.passed_through, "rate-limit response was not passed through"); ctx.assert(result.other_credentials_healthy, "other credentials were affected by rate limit"); }, }
    bdd_scenario! { id: "F8.4", fn_name: f8_4, persona: Charlie, title: "When the same credential fails consecutively, that credential alone is temporarily blocked", description: "Charlie observes consecutive failures on credential K. Only K is temporarily blocked while other credentials continue normally.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_block_failing_credential_only().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.credential_blocked, "failing credential was not blocked"); ctx.assert(result.other_credentials_healthy, "unrelated credentials were affected"); }, }
    bdd_scenario! { id: "F8.5", fn_name: f8_5, persona: Charlie, title: "New calls during the temporary block are rejected quickly", description: "Charlie checks a credential while its circuit breaker is open. The call is rejected quickly without upstream traffic and includes retry guidance.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_fast_reject_blocked_credential().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.fast_reject, "blocked credential was not rejected quickly"); ctx.assert(result.retry_guidance, "blocked credential retry guidance missing"); }, }
    bdd_scenario! { id: "F8.6", fn_name: f8_6, persona: Charlie, title: "When Anthropic responds with 5xx, users are informed of the temporary outage", description: "Charlie observes a temporary outage from the upstream. Bob receives a human-readable outage message and guidance that retrying soon is safe.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_report_temporary_outage().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.outage_message.contains("temporary outage"), "temporary outage message missing"); ctx.assert(result.retry_guidance, "temporary outage retry guidance missing"); }, }
    bdd_scenario! { id: "F8.7", fn_name: fast_f8_7, persona: Charlie, title: "When one upstream goes down, traffic is automatically rerouted to another upstream", description: "Charlie has multiple upstreams for one credential. When one upstream stops responding, new calls use a healthy upstream and Bob receives normal responses.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_reroute_to_healthy_upstream().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.failover_used, "healthy upstream was not selected after failure"); ctx.assert(result.normal_response, "Bob did not receive a normal response after failover"); }, }
    bdd_scenario! { id: "F8.8", fn_name: f8_8, persona: Charlie, title: "When all upstreams go down simultaneously, a consistent response is returned", description: "Charlie observes all upstreams for a credential down at once. Every user receives the same unreachable-upstream message during the incident.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_return_consistent_all_upstreams_down().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.all_upstreams_consistent, "all-upstreams-down response was not consistent"); ctx.assert(result.outage_message.contains("unreachable"), "unreachable-upstream message missing"); }, }
    bdd_scenario! { id: "F8.9", fn_name: f8_9, persona: Charlie, title: "When the routing trace becomes too long, it is shown with a truncation indicator", description: "Charlie opens a long routing trace. Major routing steps remain visible and the screen clearly marks where extra detail was truncated.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_truncate_long_routing_trace().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.trace_visible, "routing trace was not visible"); ctx.assert(result.trace_truncated, "routing trace truncation marker missing"); }, }
    bdd_scenario! { id: "F8.10", fn_name: f8_10, persona: Charlie, title: "When backpressure is applied, new calls are rejected gracefully", description: "Charlie observes cc-lb at concurrent-call capacity. The next call is rejected gracefully while calls already in progress continue unchanged.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_reject_backpressure_gracefully().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.backpressure_graceful, "backpressure rejection was not graceful"); ctx.assert(result.in_progress_unchanged, "in-progress calls were affected by backpressure"); }, }
    bdd_scenario! { id: "F8.11", fn_name: f8_11, persona: Charlie, title: "Each upstream has its own concurrent call bulkhead", description: "Charlie observes two upstreams on one credential. One upstream reaches its bulkhead while calls through the other upstream proceed normally.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_isolate_upstream_bulkheads().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.bulkhead_isolated, "upstream bulkheads were not isolated"); ctx.assert(result.other_credentials_healthy, "other upstream path was affected"); }, }
    bdd_scenario! { id: "F8.12", fn_name: f8_12, persona: Charlie, title: "Only idempotent calls are retried automatically", description: "Charlie checks retry classification after a temporary upstream outage. Idempotent calls are retried, while calls that cannot be classified as safe are not retried automatically.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_retry_only_idempotent_calls().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.idempotent_retried, "idempotent call was not retried"); ctx.assert(result.non_idempotent_not_retried, "non-idempotent call was retried automatically"); }, }
    bdd_scenario! { id: "F8.13", fn_name: f8_13, persona: Charlie, title: "Rate-limit guidance headers from Anthropic are passed through to users as-is", description: "Charlie observes an upstream response carrying rate-limit guidance. The guidance header reaches Bob unmodified and untruncated.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_preserve_rate_limit_guidance_headers().await? }, then: |result, ctx| { let result: W2UpstreamOutageResult = result; ctx.assert(result.guidance_header_preserved, "rate-limit guidance header changed"); ctx.assert(result.passed_through, "rate-limit guidance response was not passed through"); }, }
}

mod w2_f10 {
    use super::*;

    bdd_scenario! { id: "F10.1", fn_name: fast_f10_1, persona: Alice, title: "The operator consents to Anthropic through a browser", description: "Alice starts OAuth consent and follows the provided browser address. The flow returns to the callback and cannot be taken over by someone who did not start the session.", given: |ctx| { ctx.alice().await }, when: |alice| { alice.alice_w2_start_oauth_consent().await? }, then: |result, ctx| { let result: W2OAuthConsentResult = result; ctx.assert(result.auth_url_created, "OAuth consent address was not created"); ctx.assert(result.returned_to_callback, "OAuth consent did not return to callback"); ctx.assert(result.takeover_blocked, "OAuth consent session takeover was possible"); }, }
    bdd_scenario! { id: "F10.2", fn_name: f10_2, persona: Alice, title: "The callback is accepted only after cc-lb validates it", description: "Alice completes consent and the callback arrives. cc-lb accepts only callbacks matching an issued session and rejected callbacks create no credential.", given: |ctx| { ctx.alice().await }, when: |alice| { alice.alice_w2_validate_oauth_callback().await? }, then: |result, ctx| { let result: W2OAuthConsentResult = result; ctx.assert(result.callback_validated, "valid OAuth callback was not accepted"); ctx.assert(result.invalid_callback_rejected, "invalid OAuth callback was not rejected"); ctx.assert(!result.credential_created, "invalid callback created a credential"); }, }
    bdd_scenario! { id: "F10.3", fn_name: f10_3, persona: Alice, title: "When consent completes, the credential is registered and marked active", description: "Alice completes OAuth consent successfully. The credential is stored as active and immediately available for Bob's calls through the same slot.", given: |ctx| { ctx.alice().await }, when: |alice| { alice.alice_w2_complete_oauth_consent().await? }, then: |result, ctx| { let result: W2OAuthConsentResult = result; ctx.assert(result.credential_created, "OAuth credential was not created"); ctx.assert(result.credential_active, "OAuth credential was not marked active"); ctx.assert(result.immediately_usable, "OAuth credential was not immediately usable"); }, }
    bdd_scenario! { id: "F10.4", fn_name: f10_4, persona: Alice, title: "An invalid callback address is rejected", description: "Alice returns through an unregistered callback address. The request is rejected with a clear explanation, no credential is created, and the attempt is audited.", given: |ctx| { ctx.alice().await }, when: |alice| { alice.alice_w2_reject_invalid_callback_address().await? }, then: |result, ctx| { let result: W2OAuthConsentResult = result; ctx.assert(result.invalid_callback_rejected, "invalid callback address was not rejected"); ctx.assert(!result.credential_created, "invalid callback address created a credential"); ctx.assert(result.audit_recorded, "invalid callback audit row missing"); }, }
    bdd_scenario! { id: "F10.5", fn_name: f10_5, persona: Alice, title: "A missing or tampered session marker is rejected", description: "Alice returns with a missing or tampered session marker. cc-lb creates no credential and tells Alice to restart the consent flow.", given: |ctx| { ctx.alice().await }, when: |alice| { alice.alice_w2_reject_tampered_session_marker().await? }, then: |result, ctx| { let result: W2OAuthConsentResult = result; ctx.assert(result.tampered_marker_rejected, "tampered OAuth marker was not rejected"); ctx.assert(!result.credential_created, "tampered OAuth marker created a credential"); ctx.assert(result.restart_guidance, "OAuth restart guidance missing"); }, }
    bdd_scenario! { id: "F10.6", fn_name: f10_6, persona: Alice, title: "If the operator cancels consent, no credential is created", description: "Alice denies consent on the Anthropic screen. The callback creates no credential and the operator receives a message that consent was not granted.", given: |ctx| { ctx.alice().await }, when: |alice| { alice.alice_w2_cancel_oauth_consent().await? }, then: |result, ctx| { let result: W2OAuthConsentResult = result; ctx.assert(result.cancellation_prevents_credential, "cancelled OAuth consent created a credential"); ctx.assert(result.cancellation_message, "OAuth cancellation message missing"); }, }
    bdd_scenario! { id: "F10.7", fn_name: f10_7, persona: Alice, title: "Two operators conducting OAuth consent simultaneously do not interfere with each other", description: "Alice and another operator start OAuth flows at nearly the same time. Each callback matches only its own session and cannot capture the other credential.", given: |ctx| { ctx.alice().await }, when: |alice| { alice.alice_w2_isolate_parallel_oauth_sessions().await? }, then: |result, ctx| { let result: W2OAuthConsentResult = result; ctx.assert(result.sessions_isolated, "parallel OAuth sessions were not isolated"); ctx.assert(result.takeover_blocked, "one OAuth session could capture another credential"); }, }
    bdd_scenario! { id: "F10.8", fn_name: f10_8, persona: Alice, title: "A consent session expires after a set period of time", description: "Alice starts OAuth consent but the callback arrives too late. The marker is no longer valid and Alice is told to restart the consent flow.", given: |ctx| { ctx.alice().await }, when: |alice| { alice.alice_w2_expire_oauth_consent_session().await? }, then: |result, ctx| { let result: W2OAuthConsentResult = result; ctx.assert(result.expired_marker_rejected, "expired OAuth marker was not rejected"); ctx.assert(result.restart_guidance, "expired OAuth marker restart guidance missing"); ctx.assert(!result.credential_created, "expired OAuth marker created a credential"); }, }
}

mod w2_f11a {
    use super::*;

    bdd_scenario! { id: "F11A.1", fn_name: fast_f11a_1, persona: Charlie, title: "Current usage against the 5-hour quota is visible", description: "Charlie opens the detail view for an active credential. The 5-hour quota usage and the window bounds are visible together.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_five_hour_quota().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.five_hour_usage_visible, "5-hour quota usage missing"); ctx.assert(result.window_bounds_visible, "5-hour quota window bounds missing"); }, }
    bdd_scenario! { id: "F11A.2", fn_name: f11a_2, persona: Charlie, title: "Current usage against the 7-day quota is visible", description: "Charlie opens the detail view for an active credential. The 7-day quota usage and the next renewal time are visible together.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_seven_day_quota().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.seven_day_usage_visible, "7-day quota usage missing"); ctx.assert(result.renewal_visible, "7-day quota renewal time missing"); }, }
    bdd_scenario! { id: "F11A.3", fn_name: f11a_3, persona: Charlie, title: "The base quota and overage quota are displayed separately", description: "Charlie views a credential with base and overage quotas. Usage against each quota is displayed separately rather than collapsed into one number.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_separate_base_and_overage_quota().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.base_overage_separated, "base and overage quota were not separated"); }, }
    bdd_scenario! { id: "F11A.4", fn_name: f11a_4, persona: Charlie, title: "The fact that the overage quota has been entered is explicitly shown to the operator", description: "Charlie views a credential that entered overage quota. The overage state is explicit and the remaining overage quota is visible in the same screen.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_overage_entry_state().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.overage_entered_visible, "overage entry state missing"); ctx.assert(result.overage_remaining_visible, "remaining overage quota missing"); }, }
    bdd_scenario! { id: "F11A.5", fn_name: f11a_5, persona: Charlie, title: "A warning is shown on screen when usage approaches 80%", description: "Charlie observes usage approaching 80 percent of the quota window. The row is highlighted and the notification channel receives a single warning.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_warn_at_eighty_percent_quota().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.warning_visible, "quota warning highlight missing"); ctx.assert(result.notification_sent, "quota warning notification missing"); }, }
    bdd_scenario! { id: "F11A.6", fn_name: f11a_6, persona: Charlie, title: "The operator refreshes quota metadata immediately", description: "Charlie triggers an immediate quota metadata refresh. The latest quota is fetched without waiting for the automatic cycle and appears right away.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_refresh_quota_metadata_now().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.refreshed_now, "quota metadata was not refreshed immediately"); ctx.assert(result.new_quota_visible, "new quota was not visible after refresh"); }, }
    bdd_scenario! { id: "F11A.7", fn_name: f11a_7, persona: Charlie, title: "The operator chooses the quota aggregation mode", description: "Charlie selects per-credential and organization aggregation modes. The quota display redraws consistently and can switch back at any time.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_choose_quota_aggregation_mode().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.aggregation_mode_applied, "quota aggregation mode was not applied"); ctx.assert(result.can_switch_modes, "quota aggregation mode could not be switched"); }, }
    bdd_scenario! { id: "F11A.8", fn_name: f11a_8, persona: Charlie, title: "Usage by time slot within the 5-hour window is shown separately", description: "Charlie expands the 5-hour quota usage bar. Usage by time slot is shown and the highest-volume slot is easy to identify.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_quota_time_slots().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.time_slots_visible, "quota time slots missing"); ctx.assert(result.highest_slot_visible, "highest quota slot not visible"); }, }
    bdd_scenario! { id: "F11A.9", fn_name: f11a_9, persona: Charlie, title: "Usage restrictions attached to a credential are displayed in a human-readable form", description: "Charlie views usage restrictions communicated for a credential. The guidance is human-readable and explains which calls can and cannot use the credential.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_usage_restrictions().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.restrictions_visible, "usage restrictions were not visible"); ctx.assert(result.allowed_calls_visible, "allowed call guidance missing"); }, }
    bdd_scenario! { id: "F11A.10", fn_name: f11a_10, persona: Charlie, title: "When the quota is exhausted, the shortfall is shown to the operator", description: "Charlie views an exhausted quota window. The shortfall is shown as a human-readable number so the missing quota amount is clear.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_quota_shortfall().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.shortfall_visible, "quota shortfall was not visible"); }, }
    bdd_scenario! { id: "F11A.11", fn_name: f11a_11, persona: Charlie, title: "The last known quota value is temporarily retained after a credential is deleted", description: "Charlie deletes credential K and immediately registers a new credential in the same slot. The previous quota value remains temporarily available for comparison.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_retain_last_quota_after_delete().await? }, then: |result, ctx| { let result: W2QuotaVisibilityResult = result; ctx.assert(result.retained_after_delete, "last quota value was not retained after credential delete"); }, }
}

mod w2_f11b {
    use super::*;

    bdd_scenario! { id: "F11B.1", fn_name: f11b_1, persona: Charlie, title: "Anthropic is signaled periodically to keep the quota active", description: "Charlie observes the warmup cycle for an active credential. A small warmup signal is sent, the next cycle follows the 5-hour window, and the quota window remains alive.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_send_periodic_warmup_signal().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.signal_sent, "warmup signal was not sent"); ctx.assert(result.next_cycle_from_window, "next warmup cycle was not derived from quota window"); ctx.assert(result.quota_window_alive, "quota window was not kept alive"); }, }
    bdd_scenario! { id: "F11B.2", fn_name: f11b_2, persona: Charlie, title: "Warmup calls are not counted toward usage or cost", description: "Charlie checks usage and cost during warmup. Warmup signals are excluded from usage and cost, while Bob's real calls remain counted.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_exclude_warmup_from_usage_and_cost().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.usage_excluded, "warmup was counted as usage"); ctx.assert(result.cost_excluded, "warmup was counted as cost"); }, }
    bdd_scenario! { id: "F11B.3", fn_name: f11b_3, persona: Charlie, title: "When Anthropic signals to back off, the polling interval is increased", description: "Charlie observes upstream backoff guidance for warmup. The polling interval increases and later returns to normal when guidance returns to normal.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_increase_warmup_poll_interval().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.backoff_increased, "warmup polling interval did not increase"); ctx.assert(result.normal_interval_restored, "warmup polling interval did not return to normal"); }, }
    bdd_scenario! { id: "F11B.4", fn_name: f11b_4, persona: Charlie, title: "Only one of multiple replica nodes performs warmup", description: "Charlie observes three replica nodes at the warmup cycle. Exactly one replica sends the warmup signal and duplicate signals are prevented in that cycle.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_allow_single_replica_warmup().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.single_replica, "more than one replica performed warmup"); ctx.assert(result.duplicate_prevented, "duplicate warmup signal was not prevented"); }, }
    bdd_scenario! { id: "F11B.5", fn_name: f11b_5, persona: Charlie, title: "When warmup fails, the next attempt uses a backoff interval", description: "Charlie observes a failed warmup attempt. The next attempt waits for a backoff interval, and a later success shrinks the interval back to normal.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_apply_warmup_failure_backoff().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.backoff_applied, "warmup failure did not apply backoff"); ctx.assert(result.normal_interval_restored, "warmup interval did not restore after success"); }, }
    bdd_scenario! { id: "F11B.6", fn_name: f11b_6, persona: Charlie, title: "The operator views warmup status on screen", description: "Charlie expands a credential row on the operations screen. Last successful warmup time, next attempt time, and quota window end are shown together.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_warmup_status().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.status_visible, "warmup status was not visible"); ctx.assert(result.quota_window_alive, "warmup status did not show live quota window"); }, }
    bdd_scenario! { id: "F11B.7", fn_name: f11b_7, persona: Charlie, title: "Changes that occurred while the subscription was disconnected are caught up by the reconciler after reconnection", description: "Charlie observes a subscription reconnect after a credential revoke and upstream registration. The reconciler sweeps changes, removes revoked credential routing, and includes the new upstream.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_reconcile_after_subscription_reconnect().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.reconciled, "subscription reconnect did not reconcile changes"); ctx.assert(result.revoked_credential_removed, "revoked credential stayed in routing"); ctx.assert(result.new_upstream_included, "new upstream was not included after reconnect"); }, }
    bdd_scenario! { id: "F11B.8", fn_name: f11b_8, persona: Charlie, title: "Warmup is suspended during an emergency killswitch", description: "Charlie activates the emergency killswitch before the warmup cycle. No warmup signal is sent while active, and warmup resumes on a later cycle after deactivation.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_suspend_warmup_during_killswitch().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.killswitch_suspended, "warmup was not suspended during killswitch"); ctx.assert(result.resumes_after_killswitch, "warmup did not resume after killswitch"); }, }
    bdd_scenario! { id: "F11B.9", fn_name: f11b_9, persona: Charlie, title: "The operator views the warmup target upstream and next attempt time", description: "Charlie expands multiple upstreams bound to one credential. Each upstream shows its next warmup attempt time and last result separately.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_warmup_target_upstream().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.target_upstream_visible, "warmup target upstream missing"); ctx.assert(result.per_upstream_schedule_visible, "per-upstream warmup schedule missing"); }, }
    bdd_scenario! { id: "F11B.10", fn_name: f11b_10, persona: Charlie, title: "The operator is shown that warmup applies only to OAuth credentials", description: "Charlie views warmup status for a credential not registered through OAuth. The screen explains that warmup does not apply and an OAuth credential is required.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_warmup_oauth_only().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.oauth_only_guidance, "OAuth-only warmup guidance missing"); }, }
    bdd_scenario! { id: "F11B.11", fn_name: f11b_11, persona: Charlie, title: "Upstream address changes at Anthropic are shown to the operator and calls are not interrupted", description: "Charlie observes an upstream address change on the automatic cycle. Bob's in-progress calls continue and Charlie sees the new upstream address line.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_upstream_address_change().await? }, then: |result, ctx| { let result: W2WarmupResult = result; ctx.assert(result.address_change_visible, "upstream address change was not visible"); ctx.assert(result.in_progress_uninterrupted, "in-progress calls were interrupted by upstream address change"); }, }
}

mod w2_f11c {
    use super::*;

    bdd_scenario! { id: "F11C.1", fn_name: f11c_1, persona: Charlie, title: "The compatibility cache is automatically refreshed on a one-hour cycle", description: "Charlie observes the compatibility cache cycle. New values replace old values without interrupting Bob's calls between refreshes.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_refresh_compatibility_cache_cycle().await? }, then: |result, ctx| { let result: W2CompatibilityCacheResult = result; ctx.assert(result.refreshed, "compatibility cache was not refreshed"); ctx.assert(result.calls_uninterrupted, "calls were interrupted during compatibility refresh"); }, }
    bdd_scenario! { id: "F11C.2", fn_name: f11c_2, persona: Charlie, title: "Organization metadata is stored in a way that allows quota differences to be traced", description: "Charlie stores organization metadata for credentials with different tiers. The tier and billing type remain traceable so quota differences can be explained.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_store_organization_metadata_traceably().await? }, then: |result, ctx| { let result: W2CompatibilityCacheResult = result; ctx.assert(result.metadata_traceable, "organization metadata was not traceable"); ctx.assert(result.tier_visible, "organization tier was not visible"); }, }
    bdd_scenario! { id: "F11C.3", fn_name: f11c_3, persona: Charlie, title: "The operator manually refreshes subscription metadata", description: "Charlie triggers a manual subscription metadata refresh. cc-lb fetches metadata without waiting for the automatic cycle and shows the new information immediately.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_refresh_subscription_metadata_manually().await? }, then: |result, ctx| { let result: W2CompatibilityCacheResult = result; ctx.assert(result.manual_refresh_done, "subscription metadata manual refresh did not run"); ctx.assert(result.metadata_traceable, "refreshed subscription metadata was not traceable"); }, }
    bdd_scenario! { id: "F11C.4", fn_name: f11c_4, persona: Charlie, title: "Process restart markers are not mistaken for quota spikes", description: "Charlie replaces the cc-lb process and usage is recalculated. The restart marker is shown distinctly and is not drawn as a quota usage spike.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_keep_restart_marker_out_of_quota_spikes().await? }, then: |result, ctx| { let result: W2CompatibilityCacheResult = result; ctx.assert(result.restart_marker_distinct, "restart marker was treated as a quota spike"); }, }
    bdd_scenario! { id: "F11C.5", fn_name: f11c_5, persona: Charlie, title: "When a compatibility cache refresh fails, the previous value is retained", description: "Charlie observes a compatibility cache refresh failure. cc-lb keeps the previous value rather than replacing it with an empty or failed value.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_retain_compatibility_cache_on_failure().await? }, then: |result, ctx| { let result: W2CompatibilityCacheResult = result; ctx.assert(result.retained_previous_on_failure, "compatibility cache previous value was not retained after failure"); }, }
    bdd_scenario! { id: "F11C.6", fn_name: f11c_6, persona: Charlie, title: "The time of the last successful compatibility cache refresh is shown to the operator", description: "Charlie views compatibility cache detail after one failed refresh. The time of the last successful refresh remains visible to the operator.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_last_successful_compatibility_refresh().await? }, then: |result, ctx| { let result: W2CompatibilityCacheResult = result; ctx.assert(result.last_success_visible, "last successful compatibility refresh time missing"); }, }
    bdd_scenario! { id: "F11C.7", fn_name: f11c_7, persona: Charlie, title: "The last attempt time and last success time are shown separately", description: "Charlie opens subscription metadata detail. The last attempt time and last success time are separate so the freshness of displayed values is clear.", given: |ctx| { ctx.charlie().await }, when: |charlie| { charlie.charlie_w2_show_attempt_and_success_times_separately().await? }, then: |result, ctx| { let result: W2CompatibilityCacheResult = result; ctx.assert(result.attempt_success_separated, "last attempt and last success times were not separated"); }, }
}
