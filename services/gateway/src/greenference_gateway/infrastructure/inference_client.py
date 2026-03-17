from __future__ import annotations

import json
from urllib import request
from urllib.error import HTTPError, URLError

from greenference_protocol import ChatCompletionRequest, ChatCompletionResponse, DeploymentRecord


class InferenceUpstreamError(RuntimeError):
    pass


class HttpInferenceClient:
    def check_deployment_health(self, deployment: DeploymentRecord) -> bool:
        if not deployment.endpoint:
            return False
        upstream = request.Request(
            url=f"{deployment.endpoint.rstrip('/')}/healthz",
            method="GET",
        )
        try:
            with request.urlopen(upstream) as response:  # noqa: S310
                return 200 <= getattr(response, "status", 200) < 300
        except (HTTPError, URLError):
            return False

    def invoke_chat_completion(
        self,
        deployment: DeploymentRecord,
        payload: ChatCompletionRequest,
    ) -> ChatCompletionResponse:
        if not deployment.endpoint:
            raise InferenceUpstreamError(f"deployment endpoint missing: {deployment.deployment_id}")

        upstream = request.Request(
            url=f"{deployment.endpoint.rstrip('/')}/v1/chat/completions",
            data=payload.model_dump_json().encode(),
            headers={"content-type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(upstream) as response:  # noqa: S310
                body = json.loads(response.read().decode())
        except (HTTPError, URLError) as exc:
            raise InferenceUpstreamError(
                f"upstream invocation failed for deployment={deployment.deployment_id}"
            ) from exc
        return ChatCompletionResponse(**body)
