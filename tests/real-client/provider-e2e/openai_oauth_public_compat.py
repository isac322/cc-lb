#!/usr/bin/env python3
"""Run public OpenAI SDK compatibility checks against an existing cc-lb.

The proxy must already be configured with only real ``openai_oauth`` upstreams.
No server resources are created and no credentials are written to disk.
"""
from __future__ import annotations

import asyncio
import base64
import inspect
import io
import json
import os
import ssl
import sys
from dataclasses import dataclass
from typing import Any, Callable

import httpx
from openai import OpenAI

PROXY = os.environ.get("CC_LB_PROXY_URL", "").rstrip("/")
API_KEY = os.environ.get("CC_LB_API_KEY")
MODEL = os.environ.get("CC_LB_MODEL", "gpt-6.2-sol")
CA_CERT = os.environ.get("CC_LB_CA_CERT")
ADMIN_URL = os.environ.get("CC_LB_ADMIN_URL", "").rstrip("/")
ADMIN_TOKEN = os.environ.get("CC_LB_ADMIN_TOKEN")
PRINCIPAL_ID = os.environ.get("CC_LB_PRINCIPAL_ID")
BASE_URL = f"{PROXY}/openai/v1"


@dataclass
class Result:
    qa_id: str
    name: str
    status: str
    detail: str = ""


results: list[Result] = []


def require(value: Any, message: str) -> None:
    if not value:
        raise AssertionError(message)


def status_of(error: BaseException) -> int | None:
    return getattr(error, "status_code", None) or getattr(error, "status", None)


def error_body(error: BaseException) -> dict[str, Any]:
    body = getattr(error, "body", None)
    if isinstance(body, dict):
        return body.get("error", body)
    return {}


def output_text(response: Any) -> str:
    value = getattr(response, "output_text", None)
    if isinstance(value, str):
        return value
    pieces: list[str] = []
    for item in getattr(response, "output", None) or []:
        for content in getattr(item, "content", None) or []:
            if getattr(content, "type", None) == "output_text":
                pieces.append(getattr(content, "text", ""))
    return "".join(pieces)


def run(qa_id: str, name: str, function: Callable[[], None]) -> None:
    try:
        function()
        results.append(Result(qa_id, name, "PASS"))
        print(f"PASS {qa_id} {name}")
    except Exception as error:  # noqa: BLE001 - a case must not hide later results
        detail = str(error)
        results.append(Result(qa_id, name, "FAIL", detail))
        print(f"FAIL {qa_id} {name}: {detail}")


def skip(qa_id: str, name: str, reason: str) -> None:
    results.append(Result(qa_id, name, "SKIP", reason))
    print(f"SKIP {qa_id} {name}: {reason}")


def admin_request(client: httpx.Client, method: str, path: str, body: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> tuple[httpx.Response, Any]:
    require(ADMIN_URL and ADMIN_TOKEN, "CC_LB_ADMIN_URL and CC_LB_ADMIN_TOKEN are required for admin operations")
    request_headers = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
    if body is not None:
        request_headers["content-type"] = "application/json"
    request_headers.update(headers or {})
    response = client.request(method, f"{ADMIN_URL}{path}", headers=request_headers, json=body)
    try:
        value = response.json()
    except ValueError:
        value = None
    return response, value


def set_override(http: httpx.Client, value: dict[str, Any] | None) -> Any:
    require(PRINCIPAL_ID, "CC_LB_PRINCIPAL_ID is required for override testing")
    response, current = admin_request(http, "GET", f"/admin/v1/principals/{PRINCIPAL_ID}")
    require(response.is_success, f"principal GET failed: {response.status_code} {response.text}")
    etag = response.headers.get("etag") or f'"{current["revision"]}"'
    response, updated = admin_request(
        http,
        "PATCH",
        f"/admin/v1/principals/{PRINCIPAL_ID}",
        {"openai_oauth_compat": value},
        {"if-match": etag},
    )
    require(response.is_success, f"principal PATCH failed: {response.status_code} {response.text}")
    return updated


def override_case(client: OpenAI, http: httpx.Client) -> None:
    response, current = admin_request(http, "GET", f"/admin/v1/principals/{PRINCIPAL_ID}")
    require(response.is_success, f"principal GET failed: {response.status_code} {response.text}")
    original = current.get("openai_oauth_compat")
    try:
        set_override(http, {"store_true": "reject"})
        caught: BaseException | None = None
        try:
            client.responses.create(model=MODEL, input="override must reject store", store=True, max_output_tokens=4)
        except BaseException as error:  # SDK's APIError is version-dependent
            caught = error
        require(caught is not None, "store:true override unexpectedly succeeded")
        require(status_of(caught) == 400, f"expected HTTP 400, got {status_of(caught)}")
        body = error_body(caught)
        require(body.get("param") == "store", f"expected error.param=store, got {body}")
    finally:
        set_override(http, original)


def png_bytes() -> bytes:
    return base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")


def raw_post(client: OpenAI, path: str, body: dict[str, Any]) -> Any:
    require(hasattr(client, "post"), f"installed OpenAI SDK has no raw POST helper for {path}")
    return client.post(path, body=body)


def input_token_count(client: OpenAI) -> Any:
    api = getattr(client.responses, "input_tokens", None)
    if api is not None and hasattr(api, "count"):
        return api.count(model=MODEL, input="Count these short input tokens.")
    api = getattr(client.responses, "inputTokens", None)
    if api is not None and hasattr(api, "count"):
        return api.count(model=MODEL, input="Count these short input tokens.")
    return raw_post(client, "/responses/input_tokens", {"model": MODEL, "input": "Count these short input tokens."})


def compact(client: OpenAI) -> Any:
    body = {"model": MODEL, "input": [{"role": "user", "content": [{"type": "input_text", "text": "Keep only this short fact: the color is blue."}]}], "store": False}
    if hasattr(client.responses, "compact"):
        return client.responses.compact(**body)
    return raw_post(client, "/responses/compact", body)


async def _exercise_sdk_websocket_async(connection: Any) -> None:
    events: list[dict[str, Any]] = []

    async def send(value: dict[str, Any]) -> None:
        result = connection.send(json.dumps(value))
        if inspect.isawaitable(result):
            await result

    async def wait_for(predicate: Callable[[dict[str, Any]], bool], label: str) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + 60
        while asyncio.get_running_loop().time() < deadline:
            raw = await asyncio.wait_for(connection.recv(), timeout=max(0.1, deadline - asyncio.get_running_loop().time()))
            if isinstance(raw, bytes):
                raw = raw.decode()
            try:
                event = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
            events.append(event)
            if predicate(event):
                return event
        raise AssertionError(f"timed out waiting for {label}")

    await send({"type": "response.create", "model": MODEL, "stream_id": "lane_a", "input": "Say exactly A", "max_output_tokens": 4})
    await send({"type": "response.create", "model": MODEL, "stream_id": "lane_b", "input": "Say exactly B", "max_output_tokens": 4})
    first_a = await wait_for(lambda event: event.get("type") == "response.completed" and event.get("stream_id") == "lane_a", "lane_a completion")
    first_b = await wait_for(lambda event: event.get("type") == "response.completed" and event.get("stream_id") == "lane_b", "lane_b completion")
    require(first_a.get("response", {}).get("id") and first_b.get("response", {}).get("id"), "SDK WebSocket lanes did not return response ids")
    await send({"type": "response.create", "model": MODEL, "stream_id": "lane_a", "previous_response_id": first_a["response"]["id"], "input": "Say exactly C", "max_output_tokens": 4})
    await wait_for(lambda event: event.get("type") == "response.completed" and event.get("stream_id") == "lane_a" and event.get("response", {}).get("id") != first_a["response"]["id"], "same-lane continuation")
    require(any(event.get("stream_id") == "lane_a" for event in events) and any(event.get("stream_id") == "lane_b" for event in events), "SDK WebSocket events lost stream_id lane routing")
    require(not any(event.get("type") == "response.metadata" for event in events), "SDK WebSocket emitted response.metadata")


def websocket_sdk_available(client: OpenAI) -> bool:
    websocket = getattr(client.responses, "websocket", None)
    if not callable(websocket):
        return False
    connection: Any = None
    try:
        connection = websocket(model=MODEL)
        if inspect.isawaitable(connection):
            connection = asyncio.run(connection)
        require(hasattr(connection, "send") and hasattr(connection, "recv"), "SDK websocket object has no send/recv")
        asyncio.run(_exercise_sdk_websocket_async(connection))
        return True
    except Exception:  # SDK versions expose different websocket APIs; use raw fallback.
        return False
    finally:
        close = getattr(connection, "close", None)
        if callable(close):
            close()


async def websocket_raw_async() -> None:
    try:
        import websockets  # type: ignore
    except ImportError as error:
        raise AssertionError("neither SDK websocket mode nor Python websockets is installed") from error
    websocket_url = f"{PROXY.replace('https://', 'wss://').replace('http://', 'ws://')}/openai/v1/responses"
    ssl_context = None
    if websocket_url.startswith("wss://"):
        ssl_context = ssl.create_default_context(cafile=CA_CERT or None)
    headers = {"Authorization": f"Bearer {API_KEY}", "OpenAI-Beta": "responses_websockets=2025-02-01"}
    parameters = inspect.signature(websockets.connect).parameters
    header_arg = "additional_headers" if "additional_headers" in parameters else "extra_headers"
    connect_kwargs: dict[str, Any] = {header_arg: headers, "open_timeout": 30, "close_timeout": 5}
    if ssl_context is not None:
        connect_kwargs["ssl"] = ssl_context
    events: list[dict[str, Any]] = []
    async with websockets.connect(websocket_url, **connect_kwargs) as socket:
        async def send(value: dict[str, Any]) -> None:
            await socket.send(json.dumps(value))

        async def wait_for(predicate: Callable[[dict[str, Any]], bool], label: str) -> dict[str, Any]:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 60
            while loop.time() < deadline:
                try:
                    raw = await asyncio.wait_for(socket.recv(), timeout=max(0.1, deadline - loop.time()))
                except asyncio.TimeoutError as error:
                    raise AssertionError(f"timed out waiting for {label}") from error
                if isinstance(raw, bytes):
                    raw = raw.decode()
                try:
                    event = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    continue
                events.append(event)
                if predicate(event):
                    return event
            raise AssertionError(f"timed out waiting for {label}")

        await send({"type": "response.create", "model": MODEL, "stream_id": "lane_a", "input": "Say exactly A", "max_output_tokens": 4})
        await send({"type": "response.create", "model": MODEL, "stream_id": "lane_b", "input": "Say exactly B", "max_output_tokens": 4})
        first_a = await wait_for(lambda event: event.get("type") == "response.completed" and event.get("stream_id") == "lane_a", "lane_a completion")
        first_b = await wait_for(lambda event: event.get("type") == "response.completed" and event.get("stream_id") == "lane_b", "lane_b completion")
        require(first_a.get("response", {}).get("id") and first_b.get("response", {}).get("id"), "WebSocket lanes did not return response ids")
        await send({"type": "response.create", "model": MODEL, "stream_id": "lane_a", "previous_response_id": first_a["response"]["id"], "input": "Say exactly C", "max_output_tokens": 4})
        await wait_for(lambda event: event.get("type") == "response.completed" and event.get("stream_id") == "lane_a" and event.get("response", {}).get("id") != first_a["response"]["id"], "same-lane continuation")
    require(any(event.get("stream_id") == "lane_a" for event in events) and any(event.get("stream_id") == "lane_b" for event in events), "WebSocket events lost stream_id lane routing")
    require(not any(event.get("type") == "response.metadata" for event in events), "public WebSocket emitted response.metadata")


def websocket_case(client: OpenAI) -> None:
    if websocket_sdk_available(client):
        return
    asyncio.run(websocket_raw_async())


def main() -> int:
    require(PROXY, "CC_LB_PROXY_URL is required")
    require(API_KEY, "CC_LB_API_KEY is required")
    http_client = httpx.Client(verify=CA_CERT or True, timeout=90)
    sdk_kwargs: dict[str, Any] = {"api_key": API_KEY, "base_url": BASE_URL, "max_retries": 0, "timeout": 90}
    if CA_CERT:
        sdk_kwargs["http_client"] = httpx.Client(verify=CA_CERT, timeout=90)
    client = OpenAI(**sdk_kwargs)
    run("QA-C-143", "responses non-stream string input returns output_text", lambda: require(bool(output_text(client.responses.create(model=MODEL, input="Reply with one short word: ok", max_output_tokens=4))), "Responses output_text was empty"))

    def responses_stream() -> None:
        events = list(client.responses.create(model=MODEL, input="Reply with one short word: stream", max_output_tokens=4, stream=True))
        types = [event.type for event in events]
        require({"response.created", "response.in_progress", "response.completed"}.issubset(types), f"unexpected event sequence: {types}")
        require("response.output_text.delta" in types, "stream had no output text delta")
        require("response.metadata" not in types, "stream emitted response.metadata")
    run("QA-C-42", "responses stream event sequence has no response.metadata", responses_stream)
    run("QA-C-12", "store:true is coerced and succeeds", lambda: require(bool(output_text(client.responses.create(model=MODEL, input="store coercion control: say ok", store=True, max_output_tokens=4))), "store coercion response was empty"))
    if ADMIN_URL and ADMIN_TOKEN and PRINCIPAL_ID:
        run("QA-C-04", "store:true override rejects and restores", lambda: override_case(client, http_client))
    else:
        skip("QA-C-04", "store:true override rejects and restores", "admin override environment not supplied")
    run("QA-C-13", "temperature is dropped", lambda: require(bool(output_text(client.responses.create(model=MODEL, input="temperature drop control: say ok", temperature=0.2, max_output_tokens=4))), "temperature request failed"))

    def metadata_case() -> None:
        metadata = {"qa_case": "metadata_echo", "short": "1"}
        response = client.responses.create(model=MODEL, input="Echo metadata control: say ok", metadata=metadata, max_output_tokens=4)
        require(getattr(response, "metadata", None) == metadata, f"metadata mismatch: {getattr(response, 'metadata', None)}")
    run("QA-C-17", "metadata is echoed", metadata_case)
    run("QA-C-16", "web_search_preview tool works", lambda: web_search_case(client))
    run("QA-C-104", "function tool calling loop accepts function_call_output", lambda: function_tool_case(client))
    run("QA-C-100", "chat.completions non-stream parses", lambda: chat_nonstream(client))
    run("QA-C-101", "chat.completions stream parses", lambda: chat_stream(client))
    run("QA-C-104-chat", "chat.completions tools round-trip", lambda: chat_tool(client))
    run("QA-C-109", "chat.completions n=2 returns two choices", lambda: chat_n(client))
    run("QA-C-112", "chat.completions stop cuts output", lambda: chat_stop(client))
    run("QA-C-102", "chat.completions include_usage emits usage chunk", lambda: chat_usage(client))
    run("QA-C-68", "models.list contains configured model", lambda: models_list(client))
    run("QA-C-69", "models.retrieve works", lambda: require(client.models.retrieve(MODEL).id == MODEL, "models.retrieve returned the wrong model"))
    run("QA-C-66", "responses.compact returns usable compaction output", lambda: compact_case(client))
    run("QA-C-126", "input_tokens.count returns a number", lambda: input_tokens_case(client))
    run("QA-C-71", "images.generate returns b64_json and stream completes", lambda: images_generate(client))
    run("QA-C-72", "images.edit multipart returns b64_json", lambda: images_edit(client))
    run("QA-C-80/82/85", "responses WebSocket lanes and same-lane continuation", lambda: websocket_case(client))
    run("QA-R-42", "transcription disabled returns a public 4xx/503", lambda: transcription_disabled(client))

    failures = [result for result in results if result.status == "FAIL"]
    print("\nQA ID | STATUS | CASE")
    for result in results:
        suffix = f" — {result.detail}" if result.detail else ""
        print(f"{result.qa_id} | {result.status} | {result.name}{suffix}")
    try:
        client.close()
    finally:
        http_client.close()
    return 1 if failures else 0


def web_search_case(client: OpenAI) -> None:
    response = client.responses.create(model=MODEL, input="Use web search to find the current weekday, then answer briefly.", tools=[{"type": "web_search_preview"}], tool_choice="required", max_output_tokens=8)
    require(bool(output_text(response)), "web search response was empty")
    require(any("web_search" in str(getattr(item, "type", "")) for item in getattr(response, "output", None) or []), "response contained no web search call")


def function_tool_case(client: OpenAI) -> None:
    tool = {"type": "function", "name": "lookup", "description": "Look up a city.", "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"], "additionalProperties": False}}
    first = client.responses.create(model=MODEL, input="Call lookup with city=Paris, then wait for the result.", tools=[tool], tool_choice={"type": "function", "name": "lookup"}, max_output_tokens=8)
    call = next((item for item in first.output if item.type == "function_call"), None)
    require(call and call.call_id, "model did not return a function call")
    second = client.responses.create(model=MODEL, input=[*first.output, {"type": "function_call_output", "call_id": call.call_id, "output": json.dumps({"temperature": 18})}, {"role": "user", "content": [{"type": "input_text", "text": "Answer in one word."}]}], max_output_tokens=8)
    require(bool(output_text(second)), "function_call_output follow-up was empty")


def chat_nonstream(client: OpenAI) -> None:
    response = client.chat.completions.create(model=MODEL, messages=[{"role": "user", "content": "Reply with one short word: chat"}], max_tokens=4)
    require(bool(response.choices[0].message.content), "chat non-stream content was empty")


def chat_stream(client: OpenAI) -> None:
    chunks = list(client.chat.completions.create(model=MODEL, messages=[{"role": "user", "content": "Reply with one short word: stream"}], max_tokens=4, stream=True))
    require(len(chunks) > 1 and any(chunk.choices and chunk.choices[0].delta.content for chunk in chunks), "chat stream had no content deltas")
    require(chunks[-1].choices and chunks[-1].choices[0].finish_reason, "chat stream had no terminal finish_reason")


def chat_tool(client: OpenAI) -> None:
    response = client.chat.completions.create(model=MODEL, messages=[{"role": "user", "content": "Call lookup for Paris."}], tools=[{"type": "function", "function": {"name": "lookup", "description": "Look up a city.", "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}}}], tool_choice={"type": "function", "function": {"name": "lookup"}}, max_tokens=8)
    require(response.choices[0].message.tool_calls and response.choices[0].message.tool_calls[0].function.name == "lookup", "chat tool call was not returned")


def chat_n(client: OpenAI) -> None:
    response = client.chat.completions.create(model=MODEL, messages=[{"role": "user", "content": "Reply with one short word: choices"}], n=2, max_tokens=4)
    require(len(response.choices) == 2, f"expected two choices, got {len(response.choices)}")


def chat_stop(client: OpenAI) -> None:
    response = client.chat.completions.create(model=MODEL, messages=[{"role": "user", "content": "Reply exactly ALPHA-BETA."}], stop=["-"], max_tokens=8)
    content = response.choices[0].message.content or ""
    require("-" not in content, f"stop sequence was not cut: {content}")


def chat_usage(client: OpenAI) -> None:
    chunks = list(client.chat.completions.create(model=MODEL, messages=[{"role": "user", "content": "Reply with one short word: usage"}], max_tokens=4, stream=True, stream_options={"include_usage": True}))
    require(any(chunk.usage and chunk.usage.total_tokens is not None for chunk in chunks), "include_usage stream had no usage chunk")


def models_list(client: OpenAI) -> None:
    require(any(item.id == MODEL for item in client.models.list()), f"models.list did not include {MODEL}")


def compact_case(client: OpenAI) -> None:
    compacted = compact(client)
    item = next((entry for entry in compacted.output if entry.type == "compaction"), None)
    require(item and item.encrypted_content, "compact response had no encrypted compaction item")
    followup = client.responses.create(model=MODEL, input=[*compacted.output, {"role": "user", "content": [{"type": "input_text", "text": "What color was the fact? One word."}]}], max_output_tokens=4)
    require(bool(output_text(followup)), "compaction follow-up was empty")


def input_tokens_case(client: OpenAI) -> None:
    value = input_token_count(client)
    count = getattr(value, "input_tokens", None)
    if count is None and isinstance(value, dict):
        count = value.get("input_tokens") or value.get("inputTokens")
    require(isinstance(count, (int, float)) and count > 0, f"invalid input token count: {value}")


def images_generate(client: OpenAI) -> None:
    response = client.images.generate(model=MODEL, prompt="A tiny blue dot", n=1, size="1024x1024", quality="low", response_format="b64_json")
    require(bool(response.data and response.data[0].b64_json), "images.generate had no b64_json")
    streamed = client.images.generate(model=MODEL, prompt="A tiny red dot", n=1, size="1024x1024", quality="low", response_format="b64_json", stream=True)
    events = list(streamed) if hasattr(streamed, "__iter__") and not hasattr(streamed, "data") else [streamed]
    require(any(getattr(event, "data", None) and event.data[0].b64_json for event in events), "images stream had no b64_json event")


def images_edit(client: OpenAI) -> None:
    image = ("pixel.png", io.BytesIO(png_bytes()), "image/png")
    response = client.images.edit(model=MODEL, image=image, prompt="Keep this single pixel blue", size="1024x1024", quality="low", response_format="b64_json")
    require(bool(response.data and response.data[0].b64_json), "images.edit had no b64_json")


def transcription_disabled(client: OpenAI) -> None:
    wav = base64.b64decode("UklGRiQAAABXQVZFZm10IBAAAAABAAEAgD4AAAB9AAACABAAZGFjdGEAAAAA")
    caught: BaseException | None = None
    try:
        client.audio.transcriptions.create(model=MODEL, file=("silence.wav", io.BytesIO(wav), "audio/wav"), response_format="json")
    except BaseException as error:  # noqa: BLE001
        caught = error
    require(caught is not None, "transcription unexpectedly succeeded while disabled")
    require(status_of(caught) in {400, 404, 503}, f"unexpected transcription status {status_of(caught)}")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:  # noqa: BLE001
        print(f"FAIL openai OAuth public compatibility: {error}", file=sys.stderr)
        sys.exit(1)
