# /// script
# requires-python = ">=3.11"
# dependencies = ["google-genai==3.0.0", "httpx==0.28.1"]
# ///
"""Actual SDK serialization with a local mock transport, never provider execution.

Run through run.mjs --capture-google-genai for the redacted observer projection.
The camelCase tier is emitted by native GenerateContentConfig.service_tier.
The snake_case compatibility alias uses public HttpOptions.extra_body; that case
does not claim native typed alias support or provider acceptance.
The pinned SDK emits nested ThinkingConfig fields in snake_case. Capture those
wire keys without normalizing them to the server's camelCase request shape.
Cases prefixed extra-body- use public HttpOptions.extra_body for documented
camelCase API fields, not native typed ThinkingConfig serialization.
Use --raw-body CASE --request-index 0 to emit the original captured request bytes
unchanged for local backend replay (request indexes 1 and 2 retain signed history).
"""

import argparse
import base64
import hashlib
import importlib.metadata
import json
import sys

import httpx
from google import genai
from google.genai import types

SDK_VERSION = "3.0.0"
assert importlib.metadata.version("google-genai") == SDK_VERSION
SIGNATURE = base64.b64encode(b"offline-google-genai-signature").decode()
CASES = [
    ("camel-level", {"serviceTier": "flex"}, {"thinking_level": types.ThinkingLevel.LOW, "include_thoughts": True}),
    ("snake-budget", {"service_tier": "priority"}, {"thinking_budget": 1024, "include_thoughts": False}),
    ("automatic-budget", {}, {"thinking_budget": -1, "include_thoughts": True}),
    ("disabled-budget", {}, {"thinking_budget": 0, "include_thoughts": False}),
    ("omitted-config", {}, None),
    ("extra-body-level", {"serviceTier": "flex"}, None),
    ("extra-body-budget", {"service_tier": "priority"}, None),
    ("extra-body-automatic-budget", {}, None),
    ("extra-body-disabled-budget", {}, None),
]
EXTRA_BODY_THINKING = {
    "extra-body-level": {"thinkingLevel": "LOW", "includeThoughts": True},
    "extra-body-budget": {"thinkingBudget": 1024, "includeThoughts": False},
    "extra-body-automatic-budget": {"thinkingBudget": -1, "includeThoughts": True},
    "extra-body-disabled-budget": {"thinkingBudget": 0, "includeThoughts": False},
}
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--raw-body", choices=[case[0] for case in CASES])
parser.add_argument("--request-index", type=int, choices=range(3), default=0)
args = parser.parse_args()

captures = []
for name, aliases, thinking in CASES:
    requests = []

    def respond(request):
        # Capture bytes at the actual SDK's HTTP boundary, not a hand-built payload.
        requests.append({
            "method": request.method,
            "url": str(request.url),
            "headers": {key: value for key, value in request.headers.items() if key in ("content-type", "user-agent", "x-goog-api-client")},
            "body_base64": base64.b64encode(request.content).decode(),
        })
        parts = [{"text": "offline fixture complete"}]
        if len(requests) == 1:
            parts = [{"functionCall": {"name": "probe", "args": {}}, "thoughtSignature": SIGNATURE}]
        return httpx.Response(200, json={
            "candidates": [{"content": {"role": "model", "parts": parts}, "finishReason": "STOP"}],
            "responseId": f"offline-{name}-{len(requests)}",
        })

    # No sockets, environment credentials, ADC, retries, automatic tools or model calls.
    transport = httpx.MockTransport(respond)
    with httpx.Client(transport=transport, trust_env=False) as http_client:
        with genai.Client(vertexai=False, api_key="offline-not-a-provider-key", http_options=types.HttpOptions(
            base_url="http://127.0.0.1:1", api_version="v1beta", httpx_client=http_client,
        )) as client:
            extra_body = {key: value for key, value in aliases.items() if key != "serviceTier"}
            if name in EXTRA_BODY_THINKING:
                extra_body["generationConfig"] = {"thinkingConfig": EXTRA_BODY_THINKING[name]}
            config = types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(**thinking) if thinking is not None else None,
                http_options=types.HttpOptions(extra_body=extra_body),
                service_tier=types.ServiceTier(aliases["serviceTier"]) if "serviceTier" in aliases else None,
                tools=[types.Tool(function_declarations=[types.FunctionDeclaration(name="probe", description="Offline fixture", parameters={"type": "OBJECT", "properties": {}})])],
                automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
            )
            model = "gemini-3.1-pro-preview" if (thinking and "thinking_level" in thinking) or "thinkingLevel" in EXTRA_BODY_THINKING.get(name, {}) else "gemini-2.5-flash"
            history = [types.Content(role="user", parts=[types.Part(text="Call probe")])]
            first = client.models.generate_content(model=model, contents=history, config=config)
            signed_content = first.candidates[0].content
            assert signed_content.parts[0].thought_signature == b"offline-google-genai-signature"
            history.extend([signed_content, types.Content(role="user", parts=[types.Part.from_function_response(name="probe", response={"ok": True})])])
            second = client.models.generate_content(model=model, contents=history, config=config)
            history.extend([second.candidates[0].content, types.Content(role="user", parts=[types.Part(text="Continue the same history")])])
            client.models.generate_content(model=model, contents=history, config=config)
    assert len(requests) == 3
    captures.append({
        "case": name,
        "service_tier_source": "GenerateContentConfig.service_tier" if "serviceTier" in aliases else "HttpOptions.extra_body" if aliases else "omitted",
        "requests": requests,
    })

if args.raw_body:
    capture = next(capture for capture in captures if capture["case"] == args.raw_body)
    sys.stdout.buffer.write(base64.b64decode(capture["requests"][args.request_index]["body_base64"]))
    sys.exit(0)

# Read only the bytes observed by MockTransport; never echo the input config.
# run.mjs deliberately projects the server's camelCase request shape separately.
sdk_thinking_config_wire = []
for capture in captures:
    requests = []
    for request in capture["requests"]:
        raw = base64.b64decode(request["body_base64"])
        requests.append({
            "thinking_config": json.loads(raw).get("generationConfig", {}).get("thinkingConfig"),
            "body_bytes": len(raw),
            "body_sha12": hashlib.sha256(raw).hexdigest()[:12],
        })
    sdk_thinking_config_wire.append({"case": capture["case"], "requests": requests})

print(json.dumps({
    "classification": "SUPPORTED_SDK_OFFLINE_WIRE_CAPTURE_NOT_PROVIDER_QA",
    "client": {"name": "google-genai", "version": SDK_VERSION, "actual_sdk_invoked": True},
    "transport": "httpx.MockTransport",
    "provider_calls": 0,
    "service_tier_source": "native typed camelCase tier plus public extra_body snake_case compatibility alias",
    "captures": captures,
    "thinking_config_sources": {
        name: "HttpOptions.extra_body" if name in EXTRA_BODY_THINKING else "GenerateContentConfig.thinking_config" if thinking is not None else "omitted"
        for name, _, thinking in CASES
    },
    "sdk_thinking_config_wire": sdk_thinking_config_wire,
}))
