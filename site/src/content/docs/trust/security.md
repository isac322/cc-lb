---
title: Security
description: Report vulnerabilities privately and keep operator secrets outside source control.
slug: docs/trust/security
---

Use GitHub's private vulnerability reporting route linked from [`/.well-known/security.txt`](/.well-known/security.txt). Include the affected version, a minimal reproduction, and the impact you observed. Do not include live credentials or customer data.

## Operational safeguards

- Keep `CC_LB_MASTER_KEY`, admin tokens, and upstream credentials in a secret store.
- Use HTTPS at the public boundary and restrict admin access to the operator network.
- Rotate proxy keys through the issue, verify, and revoke lifecycle.
- Keep logs and metrics bounded so prompts and authorization material do not become labels or failure payloads.
- Review plugin metadata and artifacts before attaching them to a principal chain.

cc-lb does not claim an external security certification or a service-level guarantee. The repository's trust boundary is the source code, published license, release metadata, and operator-controlled deployment.
