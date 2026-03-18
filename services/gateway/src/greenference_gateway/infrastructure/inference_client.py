from __future__ import annotations

import json
import os
import socket
from collections.abc import Iterator
from urllib import request
from urllib.error import HTTPError, URLError

from greenference_protocol import ChatCompletionRequest, ChatCompletionResponse, DeploymentRecord


class InferenceUpstreamError(RuntimeError):
    pass


class InferenceTimeoutError(InferenceUpstreamError):
    pass


class InferenceConnectionError(InferenceUpstreamError):
    pass


class InferenceBadResponseError(InferenceUpstreamError):
    pass


class HttpInferenceClient:
    def __init__(
        self,
        upstream_timeout_seconds: float | None = None,
        health_timeout_seconds: float | None = None,
    ) -> None:
        self.upstream_timeout_seconds = upstream_timeout_seconds or float(
            os.getenv("GREENFERENCE_UPSTREAM_TIMEOUT_SECONDS", "10.0")
        )
        self.health_timeout_seconds = health_timeout_seconds or float(
            os.getenv("GREENFERENCE_HEALTH_TIMEOUT_SECONDS", "2.0")
        )

    def check_deployment_health(self, deployment: DeploymentRecord) -> bool:
        if not deployment.endpoint:
            return False
        upstream = request.Request(
            url=f"{deployment.endpoint.rstrip('/')}/healthz",
            method="GET",
        )
        try:
            with request.urlopen(upstream, timeout=self.health_timeout_seconds) as response:  # noqa: S310
                return 200 <= getattr(response, "status", 200) < 300
        except (HTTPError, URLError, TimeoutError, socket.timeout):
            return False

    def invoke_chat_completion(
        self,
        deployment: DeploymentRecord,
        payload: ChatCompletionRequest,
        *,
        request_id: str | None = None,
    ) -> ChatCompletionResponse:
        if not deployment.endpoint:
            raise InferenceUpstreamError(f"deployment endpoint missing: {deployment.deployment_id}")

        upstream = request.Request(
            url=f"{deployment.endpoint.rstrip('/')}/v1/chat/completions",
            data=payload.model_dump_json().encode(),
            headers={
                "content-type": "application/json",
                **({"x-request-id": request_id} if request_id is not None else {}),
            },
            method="POST",
        )
        try:
            with request.urlopen(upstream, timeout=self.upstream_timeout_seconds) as response:  # noqa: S310
                body = json.loads(response.read().decode())
        except (TimeoutError, socket.timeout) as exc:
            raise InferenceTimeoutError(
                f"upstream timed out for deployment={deployment.deployment_id}"
            ) from exc
        except (HTTPError, URLError) as exc:
            if isinstance(exc, URLError) and isinstance(exc.reason, TimeoutError | socket.timeout):
                raise InferenceTimeoutError(
                    f"upstream timed out for deployment={deployment.deployment_id}"
                ) from exc
            if isinstance(exc, URLError):
                raise InferenceConnectionError(
                    f"upstream connection failed for deployment={deployment.deployment_id}"
                ) from exc
            raise InferenceUpstreamError(
                f"upstream invocation failed for deployment={deployment.deployment_id}"
            ) from exc
        try:
            return ChatCompletionResponse(**body)
        except Exception as exc:  # noqa: BLE001
            raise InferenceBadResponseError(
                f"upstream returned invalid response for deployment={deployment.deployment_id}"
            ) from exc

    def stream_chat_completion(
        self,
        deployment: DeploymentRecord,
        payload: ChatCompletionRequest,
        *,
        request_id: str | None = None,
    ) -> Iterator[str]:
        if not deployment.endpoint:
            raise InferenceUpstreamError(f"deployment endpoint missing: {deployment.deployment_id}")

        upstream = request.Request(
            url=f"{deployment.endpoint.rstrip('/')}/v1/chat/completions",
            data=payload.model_copy(update={"stream": True}).model_dump_json().encode(),
            headers={
                "content-type": "application/json",
                **({"x-request-id": request_id} if request_id is not None else {}),
            },
            method="POST",
        )
        try:
            with request.urlopen(upstream, timeout=self.upstream_timeout_seconds) as response:  # noqa: S310
                while True:
                    line = response.readline()
                    if not line:
                        break
                    yield line.decode()
        except (TimeoutError, socket.timeout) as exc:
            raise InferenceTimeoutError(
                f"upstream timed out for deployment={deployment.deployment_id}"
            ) from exc
        except (HTTPError, URLError) as exc:
            if isinstance(exc, URLError) and isinstance(exc.reason, TimeoutError | socket.timeout):
                raise InferenceTimeoutError(
                    f"upstream timed out for deployment={deployment.deployment_id}"
                ) from exc
            if isinstance(exc, URLError):
                raise InferenceConnectionError(
                    f"upstream connection failed for deployment={deployment.deployment_id}"
                ) from exc
            raise InferenceUpstreamError(
                f"upstream invocation failed for deployment={deployment.deployment_id}"
            ) from exc
