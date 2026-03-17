from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import StreamingResponse

from greenference_protocol import (
    APIKeyCreateRequest,
    BuildRequest,
    ChatCompletionRequest,
    DeploymentCreateRequest,
    UserRegistrationRequest,
    WorkloadCreateRequest,
)
from greenference_gateway.application.services import service
from greenference_gateway.domain.routing import NoReadyDeploymentError
from greenference_gateway.infrastructure.inference_client import InferenceTimeoutError, InferenceUpstreamError
from greenference_gateway.transport.security import enforce_rate_limit, metrics, require_api_key

router = APIRouter()


@router.post("/platform/api-keys")
def create_api_key(payload: APIKeyCreateRequest) -> dict:
    return service.create_api_key(payload).model_dump(mode="json")


@router.post("/platform/register")
def register_user(payload: UserRegistrationRequest) -> dict:
    return service.register_user(payload).model_dump(mode="json")


@router.post("/platform/images")
def build_image(
    payload: BuildRequest,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict:
    api_key = require_api_key(authorization, x_api_key)
    enforce_rate_limit("build_image", api_key.key_id, limit=30, window_seconds=60)
    return service.start_build(payload).model_dump(mode="json")


@router.get("/platform/images")
def list_images(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> list[dict]:
    require_api_key(authorization, x_api_key)
    return [build.model_dump(mode="json") for build in service.list_builds()]


@router.post("/platform/workloads")
def create_workload(
    payload: WorkloadCreateRequest,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict:
    api_key = require_api_key(authorization, x_api_key)
    enforce_rate_limit("create_workload", api_key.key_id, limit=30, window_seconds=60)
    return service.create_workload(payload).model_dump(mode="json")


@router.get("/platform/workloads")
def list_workloads(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> list[dict]:
    require_api_key(authorization, x_api_key)
    return [workload.model_dump(mode="json") for workload in service.list_workloads()]


@router.post("/platform/deployments")
def create_deployment(
    payload: DeploymentCreateRequest,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict:
    api_key = require_api_key(authorization, x_api_key)
    enforce_rate_limit("create_deployment", api_key.key_id, limit=30, window_seconds=60)
    return service.create_deployment(payload).model_dump(mode="json")


@router.get("/platform/deployments")
def list_deployments(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> list[dict]:
    require_api_key(authorization, x_api_key)
    return [deployment.model_dump(mode="json") for deployment in service.list_deployments()]


@router.post("/v1/chat/completions")
def chat_completions(
    payload: ChatCompletionRequest,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict:
    api_key = require_api_key(authorization, x_api_key)
    enforce_rate_limit("chat_completions", api_key.key_id, limit=60, window_seconds=60)
    try:
        if payload.stream:
            metrics.increment("invoke.stream")
            return StreamingResponse(
                service.stream_chat_completion(payload),
                media_type="text/event-stream",
                headers={"cache-control": "no-cache"},
            )
        response = service.invoke_chat_completion(payload).model_dump(mode="json")
        metrics.increment("invoke.success")
        return response
    except NoReadyDeploymentError as exc:
        metrics.increment("invoke.failure.no_ready_deployment")
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except InferenceTimeoutError as exc:
        metrics.increment("invoke.failure.timeout")
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except InferenceUpstreamError as exc:
        metrics.increment("invoke.failure.upstream")
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/v1/completions")
def completions(
    payload: dict,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict:
    request = ChatCompletionRequest(
        model=payload["model"],
        messages=[{"role": "user", "content": payload.get("prompt", "")}],
        max_tokens=payload.get("max_tokens", 128),
        temperature=payload.get("temperature", 0.7),
        stream=payload.get("stream", False),
    )
    return chat_completions(request, authorization=authorization, x_api_key=x_api_key)


@router.post("/v1/embeddings")
def embeddings(
    payload: dict,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict:
    api_key = require_api_key(authorization, x_api_key)
    enforce_rate_limit("embeddings", api_key.key_id, limit=60, window_seconds=60)
    text = payload.get("input", "")
    vector = [round(((ord(char) % 32) / 31.0), 6) for char in str(text)[:16]]
    return {
        "object": "list",
        "data": [{"object": "embedding", "index": 0, "embedding": vector}],
        "model": payload.get("model", "greenference-embedding"),
    }


@router.get("/platform/v1/debug/route/{model}")
def debug_route(
    model: str,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict:
    require_api_key(authorization, x_api_key, admin_required=True)
    try:
        workload_id = service._resolve_workload_id(model)
        deployment = service.control_plane.resolve_ready_deployment(workload_id)
        return {
            "model": model,
            "workload_id": workload_id,
            "deployment": deployment.model_dump(mode="json") if deployment else None,
        }
    except NoReadyDeploymentError as exc:
        return {"model": model, "error": str(exc), "deployment": None}


@router.get("/platform/v1/metrics")
def platform_metrics(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict:
    require_api_key(authorization, x_api_key, admin_required=True)
    return metrics.snapshot()
