# cc-lb True BDD v5.2: Part 3: Policy & Plugin

- Date: 2026-06-18
- Scope: 6 features / 63 scenarios
- Source: `cc-lb-true-bdd-3-policy-plugin-v5.md` (62), `cc-lb-bdd-final-report-v5.md` §3 sample table, `cc-lb-bdd-verify-C-scenario-quality.md` (Verification C results), `bdd-research.md` §A to §H, `cc-lb-bdd-v3-final-consensus.md`
- Changes from v5 (Changelog)
  - Split the multi-rule pattern in scenario F29.3, which previously handled two business rules at once, (1) fallback behavior on external connection loss and (2) the audit identifier for that fallback behavior, into two independent scenarios F29.3a and F29.3b. This aligns with the final report §3 sample table fix.
  - The business rules, text, and vocabulary of the other 61 scenarios remain unchanged from v5.
- Changes from v4 (Cumulative Changelog)
  - Split the multi-rule pattern in scenario F12.10, which previously handled three business rules at once, preparation, application, and cleanup, into three independent scenarios F12.10a, F12.10b, and F12.10c. This aligns with the Verification C §5 priority 1 fix.
  - Applied vocabulary replacements as follows: `chain` (plugin chain) to `plugin queue`, `bulkhead` to `functional partition`, `endpoint` or `terminal` to `screen`.
  - `repository` is accepted as a business term and kept as is, which was marked only as borderline in Verification C §2 S18.
  - There are no business rule changes in the scenario bodies of F9, F21, F25, F27, and F29. Only the vocabulary in the headers and bodies has been aligned.
- Rules of Application
  - Only four personas appear: Alice (operator), Bob (developer and plugin author), Charlie (SRE), and Dana (auditor).
  - Each scenario covers exactly one business rule. Success and failure cases are not mixed in a single scenario.
  - HTTP status, AEAD algorithm names, schema constants, hook method names, and SQL expressions do not appear in the scenarios.
  - Vocabulary replacements follow the mapping in the header, such as plugin filter chain to "routing and response processing rule set defined by the operator".

---

## Feature: F9: Routing Policies and Rule Sets Attached to Teams

Personas: Alice (operator), Charlie (SRE)
Domain value: The operator centrally defines how calls route to specific models and plugins for each team.

### Scenario: F9.1 Attaching a policy to a team applies it immediately to subsequent calls

- Given Alice attached a routing and response processing rule set to a team
- When Bob from that team sends the next call
- Then the call routes and the response is processed according to the attached rule set
- And the attachment is recorded as a single line in the audit log

### Scenario: F9.2 Detaching a policy only affects subsequent calls

- Given a routing and response processing rule set is attached to a team
- And a call from that team is already in progress
- When Alice detaches the rule set from that team
- Then the in-progress call is processed to completion using the previously attached rules
- And subsequent new calls route using the default rules

### Scenario: F9.3 Malformed policies are rejected during the save phase

- Given Alice created a draft routing and response processing rule set
- And the draft is malformed in a shape that cc-lb cannot understand
- When Alice attempts to save the draft
- Then the save is rejected and the issue is displayed on the operator screen
- And it is not attached to any team

### Scenario: F9.4 Policy changes apply to subsequent calls without restarting cc-lb

- Given a routing and response processing rule set is attached to a team
- When Alice modifies and applies the rule set
- Then the new rules apply to subsequent calls without restarting cc-lb
- And the change is recorded chronologically in the audit log

### Scenario: F9.5 Policy changes for one team do not affect calls from other teams

- Given different routing and response processing rule sets are attached to two teams
- When Alice modifies the rule set of only one team
- Then subsequent calls from the modified team route according to the new rules
- And calls from the other team route as originally configured

### Scenario: F9.6 Global rules apply first, followed by team rules

- Given Alice configures a global rule set that applies to all teams
- And attaches an additional team-specific rule set to one of the teams
- When a call from that team arrives
- Then the call passes through the global rules first and then the team rules
- And if there are conflicting decisions of the same type, the team rule takes precedence as the final decision

### Scenario: F9.8 Operators can validate the flow before attaching a policy

- Given Alice has a draft of a new routing and response processing rule set
- And it is not yet attached
- When Alice runs a validation using the draft against recent call samples from a team
- Then the operator screen displays how those calls would have been routed and processed under the new rules
- And the actual call flow and audit logs remain unaffected

---

## Feature: F12: Plugin Upload, Registration, and Plugin Queue Ordering

Personas: Alice (operator), Bob (plugin author)
Domain value: The operator safely imports, reorders, and removes unused routing and response processing rule sets.

### Scenario: F12.1 Uploading a plugin with the same signature twice rejects the second upload

- Given Alice has already uploaded a plugin file to cc-lb
- When Alice uploads the same file again
- Then the second upload is rejected with a message stating it is identical to the existing one
- And only a single copy of the plugin is retained

### Scenario: F12.2 Plugins are managed and distinguished by label and version

- Given Alice uploads two versions of a plugin with the same label
- When Alice selects one of the versions and attaches it to a team rule set
- Then only the attached version is used in calls for that team
- And the other version remains in the repository

### Scenario: F12.3 Reordering the plugin queue applies the new order to subsequent calls

- Given multiple plugins are attached in order to a team rule set
- When Alice changes and applies a different order
- Then the plugins operate in the new order starting from the next call
- And in-progress calls are processed to completion using the previous order

### Scenario: F12.4 Plugins currently in use cannot be deleted

- Given a plugin uploaded by Bob is attached to a team rule set
- When Alice attempts to delete the plugin from the repository
- Then the deletion is rejected and the screen displays which team is using it
- And a message is displayed stating that all attachments must be detached first

### Scenario: F12.5 Exceeding the maximum number of plugins in a plugin queue is rejected

- Given a team rule set already has the maximum number of plugins allowed by cc-lb
- When Alice attempts to attach one more plugin to the rule set
- Then the attachment is rejected with a message stating the limit is exceeded
- And the existing attachments remain unchanged

### Scenario: F12.6 The number of plugins that can be uploaded per minute is limited

- Given Alice is uploading multiple plugins in rapid succession
- When the number of uploads within a single minute exceeds the limit
- Then further uploads within that minute are rejected with a message to try again later
- And uploads are allowed again when the next minute begins

### Scenario: F12.7 Currently cc-lb only accepts plugins for response shaping slots

- Given Bob created a plugin designed for either a response shaping slot, a filter slot, or an observability slot
- When Bob uploads a plugin for a slot other than response shaping
- Then the registration is rejected and the screen displays the slot types currently accepted by cc-lb
- And plugins for response shaping slots are accepted normally

### Scenario: F12.8 Plugins with unverified signatures are rejected during registration

- Given Alice attempts to upload a plugin file received from an external source
- And the signature on the file is not from a publisher trusted by cc-lb
- When Alice uploads the file
- Then the registration is rejected and the signature verification failure is displayed on the operator screen
- And the plugin remains in a state where it cannot be attached to any team rule set

### Scenario: F12.9 A plugin exhausting its resource limit does not affect other plugins

- Given multiple plugins are attached to a team rule set
- And one of the plugins reaches its resource limit defined by cc-lb
- When other plugins in the same call perform their tasks
- Then the plugin that reached its limit is safely stopped for that call
- And the operations and resource tracking of other plugins remain unaffected

### Scenario: F12.10a The old rule set is used during the preparation phase of a plugin queue change

- Given Alice initiated a change to replace multiple attached plugins at once in a team rule set
- When cc-lb places the new attachments into a preparation phase before application
- Then in-progress calls and new incoming calls for that team are still processed using the old rule set
- And the start of the preparation phase is recorded as a single line in the audit log

### Scenario: F12.10b The new rule set is applied all at once at the application point

- Given Alice's plugin queue change has passed the preparation phase
- When Alice triggers the application
- Then all new calls after that point are processed using the same new rule set
- And the application point is recorded as a single line in the audit log

### Scenario: F12.10c Unused old attachments are cleaned up from the repository after application

- Given Alice's plugin queue change has been applied
- And all calls processed under the old rule set have completed
- When cc-lb enters the cleanup phase
- Then old attachments no longer used by any team calls are removed from the repository
- And the list of cleaned items is recorded as a single line in the audit log

### Scenario: F12.11 Operators can insert a new plugin at a specified position in the plugin queue

- Given multiple plugins are already attached in order to a team rule set
- When Alice inserts a new plugin into the rule set by specifying its position either before or after an existing plugin name
- Then the new plugin is inserted at the specified position, and the order of other plugins remains unchanged
- And the insertion is recorded in the audit log along with where and how it was inserted

### Scenario: F12.12 Changes are rejected if slot types do not match during pre-application checks

- Given Alice created a draft of a plugin queue change
- And the draft includes a plugin for a slot type that cc-lb does not currently accept
- When Alice runs a pre-check before application
- Then the check results display where the slot types do not match
- And the draft cannot proceed to the application phase

### Scenario: F12.13 Unattached plugin queue items are displayed in a separate list

- Given there is a plugin queue change history on the operator screen
- And some items remain that are no longer attached anywhere
- When Alice views the repository
- Then the unattached items are displayed together in a separate list
- And items from that list can be selected and removed from the repository all at once

### Scenario: F12.14 Operators can reorder all items in the plugin queue at once

- Given multiple plugins are attached in order to a team rule set
- When Alice specifies and applies a new order all at once
- Then the plugins operate in the new order starting from the next call
- And the reordering is recorded in the audit log along with the mapping from the old order to the new order

### Scenario: F12.15 Attaching a second plugin to a single-capacity slot is rejected

- Given a plugin is already attached to a single-capacity slot in a team rule set
- When Alice attempts to attach another plugin to the same slot
- Then the attachment is rejected with a message stating that the slot can only hold a single plugin
- And the existing attachment remains unchanged

---

## Feature: F21: Observability Logging Operates on Every Lifecycle Event

Personas: Charlie (SRE), Dana (auditor)
Domain value: Events such as the start, middle, and end of a call, as well as authentication and drops, appear as consistent facts on the operator dashboard and audit logs.

### Scenario: F21.1 Each call is counted separately by principal and model

- Given Charlie views who used which model and how much on the operator dashboard
- When a key from a team calls a model once
- Then the count corresponding to that team, key, and model increases by one
- And the counts for other teams and models remain unchanged

### Scenario: F21.2 The start, completion, or error of a call is recorded exactly once

- Given a call arrives at cc-lb
- When the call completes normally or ends with an error
- Then one start event and one completion or error event are recorded in the observability logs
- And completion and error events are not recorded simultaneously for the same call

### Scenario: F21.3 The number of partial transmissions for streaming responses is aggregated on the operator dashboard

- Given Bob makes a call that receives a streaming response
- When the response is delivered sequentially in multiple parts
- Then the number of delivered parts for that call is aggregated on the operator dashboard
- And disconnected or interrupted events are displayed separately from the delivery count

### Scenario: F21.4 Authentication failures are counted by failure reason

- Given Dana views authentication failures on a minute-by-minute basis
- When failures occur due to different reasons such as an invalid key, expired credentials, or lack of authorization
- Then the count for each reason increases separately
- And failures with the same reason are grouped together on the same line

### Scenario: F21.5 Calls dropped due to backpressure are recorded on the operator dashboard

- Given cc-lb is close to its configured processing limit
- When some incoming calls are dropped because the queue is full
- Then the number of dropped calls increases on the operator dashboard
- And the count of successfully processed calls remains unaffected

### Scenario: F21.7 The recipient of the response also sees the call identifier

- Given Bob sends a call through cc-lb
- When the response is returned to Bob
- Then Bob can verify the call identifier in the response
- And the identifier matches the one in the operator logs for the same call

### Scenario: F21.8 The same event is transmitted to an external observability tool configured by the operator

- Given Charlie has connected an external observability tool to cc-lb
- When a call event occurs within cc-lb
- Then the same event arrives at the external observability tool
- And the counts on the operator dashboard and the external tool align within a certain timeframe

### Scenario: F21.9 The call identifier is identical across audit logs, operator logs, external traces, and responses

- Given Dana performs a post-incident analysis on a call
- When Dana views the audit logs, operator logs, external tracing tools, and the response together
- Then the call identifier appears as the same value in all four places
- And it is not mixed with identifiers of other calls

### Scenario: F21.11 The duration of each call phase is included in the usage report

- Given Charlie searches for the cause of slow calls
- When a call completes
- Then the duration of each phase of that call is included in the usage report
- And the sum of the phase durations is consistent with the total duration of the call

### Scenario: F21.12 Failure to transmit events to an external observability tool does not affect call processing

- Given Charlie has connected an external observability tool to cc-lb
- When transmission to the external observability tool temporarily fails
- Then the core call processing of cc-lb continues unaffected by the failure
- And the transmission failure is displayed separately on the operator dashboard

### Scenario: F21.13 Dropped batches are displayed separately when the observability event queue is full

- Given there is a queue in cc-lb where observability events are collected
- When the queue becomes full and some event batches are dropped
- Then the number of dropped batches is displayed on a separate line on the operator dashboard
- And the count of successfully processed events remains unaffected

### Scenario: F21.14 One team's observability chain exhausting its resources does not affect other teams' observability chains

- Given separate observability chains are attached to two teams
- When the observability chain of one team reaches its resource limit
- Then the observability chain of that team is safely stopped from that point onward
- And the observability chain of the other team continues to operate unaffected

---

## Feature: F25: Plugin Authors Rely on Stable Commitments

Personas: Bob (plugin author)
Domain value: Bob can rely on his plugins registering and operating safely without needing to know the deep internals of cc-lb.

### Scenario: F25.1 Plugins built within the plugin format supported by cc-lb are accepted

- Given Bob created a plugin matching the plugin format supported by cc-lb
- When Bob uploads the plugin to cc-lb
- Then the plugin introduces itself, and cc-lb verifies its capabilities and completes registration
- And the plugin operates starting from subsequent calls

### Scenario: F25.2 Plugins with unsupported formats are rejected

- Given Bob created a plugin in a format not supported by cc-lb
- When Bob uploads the plugin to cc-lb
- Then the registration is rejected with a message stating that cc-lb does not support the format
- And the plugin does not intercept any calls

### Scenario: F25.3 Plugins with identical content share the same signature

- Given Bob has two plugin files built twice from the same source
- When Bob compares the signatures of the two files
- Then the two signatures are identical
- And cc-lb treats files with the same signature as the same plugin

### Scenario: F25.5 Plugins failing pre-checks are blocked during registration

- Given Bob's plugin must undergo pre-checks such as naming rules, self-checks, and capability declarations
- When any of the pre-checks fail
- Then the registration is rejected and Bob is notified of where it failed
- And the failed plugin does not intercept any calls

### Scenario: F25.7 Authors can predefine fallback behaviors for each function on failure

- Given Bob predefines the fallback behavior for each function of his plugin on failure
- When the function fails during a call
- Then cc-lb follows the behavior predefined by Bob, which is one of rejection, silent bypass, default value usage, or transparent pass-through
- And functions without predefined behaviors follow the default behavior of cc-lb

### Scenario: F25.8 The most compatible generation is negotiated when cc-lb supports multiple plugin format generations

- Given cc-lb supports multiple generations of plugin formats simultaneously
- And Bob's plugin is compatible with one or more of those generations
- When Bob's plugin introduces itself
- Then the most recent generation that both sides can agree on is negotiated
- And the negotiated generation is recorded in the registration log

### Scenario: F25.9 Authors communicate with the external environment only through auxiliary functions provided by cc-lb

- Given Bob's plugin requires auxiliary functions such as random values, current time, or short-term memory
- When Bob's plugin uses those functions
- Then the plugin receives those values only through the auxiliary functions exposed by cc-lb
- And it cannot access the external environment through any other path

### Scenario: F25.11 Registered plugins display their name, version, and capabilities to the operator

- Given Bob's plugin is registered
- When Alice views the plugin list
- Then the name, version, and capabilities of the plugin, along with which slot it occupies, are displayed
- And other versions with the same name are grouped together on the same line

### Scenario: F25.12 Having many registered plugins does not delay booting during cc-lb restart

- Given multiple plugins created by Bob are already registered
- When Charlie restarts cc-lb
- Then cc-lb completes its preparation to receive incoming calls within a designated timeframe
- And the introduction of one plugin does not delay the introduction of other plugins

### Scenario: F25.13 If the core phase of a plugin exceeds the designated time, the call completes with a fallback behavior

- Given Bob's plugin is attached to a team rule set
- And there is a designated maximum time for the core phase of the plugin
- When the core phase exceeds the designated time during a call
- Then cc-lb completes the call using the fallback behavior predefined by Bob for that function
- And the event is recorded in the operator logs and audit logs with the same call identifier

### Scenario: F25.14 Secrets are masked at the cc-lb boundary before being passed to plugins

- Given Bob's plugin is attached to a slot that views the call body and response
- And the call contains secrets such as credentials or tokens
- When cc-lb passes the call to the plugin
- Then masked placeholders are inserted in place of the secrets before being passed to the plugin
- And the plugin cannot access the original secret values regardless of how it processes the body and response

### Scenario: F25.15 Plugins requesting negotiation for a lower generation than supported are rejected

- Given Bob's plugin introduces itself
- And the range of format generations accepted by cc-lb is defined
- When Bob's plugin requests negotiation for a generation lower than that range in its introduction
- Then cc-lb rejects the request and displays the accepted range of generations
- And the plugin remains unregistered

### Scenario: F25.16 Plugins missing capabilities required by a slot are rejected

- Given Bob's plugin declares capabilities targeting a specific slot
- And the slot has a list of capabilities required by cc-lb
- When any of those capabilities are missing from Bob's declaration
- Then the registration is rejected and Bob is notified of which capabilities are missing
- And the plugin does not intercept any calls

### Scenario: F25.17 Plugins with format generations outside the range accepted by cc-lb are rejected

- Given Bob's plugin declares the format generation it follows
- And the generation is lower than the lowest or higher than the highest generation accepted by cc-lb
- When Bob uploads the plugin
- Then the registration is rejected and the accepted range of generations is displayed
- And the plugin remains unregistered

---

## Feature: F27: Administrators are Protected from Mistakes and Attacks

Personas: Alice (operator), Dana (auditor)
Domain value: Assuming the operator screen is an attack surface, the system protects the operator from invalid inputs, traffic spikes, and accidental high-risk clicks.

### Scenario: F27.3 Rapid plugin upload spikes within a short timeframe are temporarily throttled

- Given the operator screen receives multiple upload requests within a short timeframe
- When uploads exceeding the designated limit per minute arrive
- Then further uploads are throttled with a message to try again later
- And uploads are accepted again when the next time window opens

### Scenario: F27.4 Emergency shutdown takes effect only after two-step verification

- Given Alice clicks the emergency shutdown button
- When the second verification step is not yet completed
- Then the emergency shutdown does not take effect
- And it takes effect immediately and is recorded in the audit log only when the second verification is completed

### Scenario: F27.5 Admin sessions require re-authentication before high-risk operations after a certain period of inactivity

- Given Alice is logged into the operator screen with an admin token
- And a designated period has passed since the last activity
- When Alice attempts a high-risk operation such as emergency shutdown or plugin deletion
- Then the operation does not execute immediately, and a step requiring Alice to re-authenticate is interposed
- And the operation executes and is recorded as a single line in the audit log only after re-authentication is completed

### Scenario: F27.6 Admin requests originating from other sources are not executed unintentionally

- Given Alice is visiting other web pages while logged into the operator screen
- When the other page attempts to trigger a high-risk operation using Alice's admin privileges
- Then cc-lb verifies that the request did not originate from the operator screen and rejects execution
- And the rejected attempt is recorded in the audit log along with its origin

### Scenario: F27.7 Admin token values are never displayed in plaintext anywhere on the operator screen

- Given Alice inspects where the admin token was loaded from on the operator screen
- When the operator screen displays its source, length, and last modified time
- Then the token value itself is displayed only in a masked format
- And the plaintext is not visible even when the same token is recorded in operator logs or audit logs

---

## Feature: F29: cc-lb Tolerates Intentional Fault Injection in a Defined Manner

Personas: Charlie (SRE), Dana (auditor)
Domain value: Even when the operator intentionally injects faults to test resilience, secrets are not leaked, and the system recovers in a defined manner.

### Scenario: F29.1 Secrets are masked even in sudden crash messages

- Given a component within cc-lb crashes unexpectedly
- When the cause of the crash is propagated to logs, audit logs, and external traces
- Then secrets such as credentials or tokens are displayed only in a masked format
- And the event itself is recorded chronologically without being masked

### Scenario: F29.2 Operators intentionally inject faults to test resilience

- Given Charlie configures intentional fault injection to test resilience
- When faults occur at the designated rate and points
- Then cc-lb safely completes the call or reroutes it in a predefined manner
- And a method to withdraw the fault injection is provided on the same screen when the test ends

### Scenario: F29.3a Calls complete with a designated fallback behavior even if the external connection is suddenly lost

- Given a call has already routed to an external model provider
- When the external connection is suddenly lost in the middle of the response
- Then cc-lb completes the call using a predefined fallback behavior
- And the response indicates that the call completed with a fallback behavior rather than the original external response

### Scenario: F29.3b Fallback behavior due to external connection loss is recorded in operator logs and audit logs with the same call identifier

- Given a call completed with a fallback behavior according to F29.3a
- When Dana views the operator logs and audit logs from that timeframe together
- Then the external connection loss event and the application of the fallback behavior are recorded in both places with the same call identifier
- And the identifier matches the one in the response for the same call

### Scenario: F29.4 Ongoing tracking continues even if the limit engine undergoes a cold restart

- Given the component tracking minute, hour, and daily limits undergoes a cold restart
- When there is tracking in progress at that moment
- Then the tracking continues uninterrupted within the same time window after the restart
- And no tracking interval is double-counted or omitted

### Scenario: F29.5 Limiting fault injection to a single team does not affect calls from other teams

- Given Charlie limits the scope of fault injection to a single team when enabling it
- When fault injection occurs at the designated rate and points
- Then only calls from the scoped team experience faults and complete with the fallback behavior
- And calls from other teams are processed normally, unaffected by the enabled fault injection

### Scenario: F29.6 Enabling and disabling fault injection is recorded in the audit log

- Given Charlie enables fault injection and disables it after a certain period
- When Dana views the audit logs from that timeframe
- Then who enabled what type of fault, when, for which scope, and when they disabled it are displayed chronologically line by line
- And the identifiers of calls affected by the fault injection can be traced in the operator logs from the same timeframe

### Scenario: F29.7 Fault injection occurs only at predefined points

- Given the points allowed for fault injection are predefined within cc-lb
- When Charlie attempts to enable faults at a point not in the allowed list
- Then the attempt is rejected and the allowed points are displayed
- And call flows at unauthorized points do not experience any faults
