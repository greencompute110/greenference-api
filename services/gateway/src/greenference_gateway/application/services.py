from __future__ import annotations

import secrets
from collections.abc import Iterator
from json import dumps

from greenference_builder.application.services import BuilderService, service as default_builder_service
from greenference_control_plane.application.services import (
    ControlPlaneService,
    service as default_control_plane_service,
)
from greenference_protocol import (
    APIKeyCreateRequest,
    APIKeyRecord,
    BuildRecord,
    BuildRequest,
    ChatCompletionRequest,
    DeploymentCreateRequest,
    DeploymentRecord,
    UserRecord,
    UserRegistrationRequest,
    UsageRecord,
    WorkloadCreateRequest,
    WorkloadSpec,
)
from greenference_gateway.domain.routing import NoReadyDeploymentError
from greenference_gateway.infrastructure.inference_client import HttpInferenceClient
from greenference_gateway.infrastructure.repository import GatewayRepository


class GatewayService:
    def __init__(
        self,
        repository: GatewayRepository | None = None,
        control_plane: ControlPlaneService | None = None,
        builder: BuilderService | None = None,
        inference_client: HttpInferenceClient | None = None,
    ) -> None:
        self.repository = repository or GatewayRepository()
        self.control_plane = control_plane or default_control_plane_service
        self.builder = builder or default_builder_service
        self.inference_client = inference_client or HttpInferenceClient()

    def register_user(self, request: UserRegistrationRequest) -> UserRecord:
        user = UserRecord(username=request.username, email=request.email)
        return self.repository.save_user(user)

    def create_api_key(self, request: APIKeyCreateRequest) -> APIKeyRecord:
        api_key = APIKeyRecord(
            name=request.name,
            user_id=request.user_id,
            admin=request.admin,
            scopes=request.scopes,
            secret=f"gk_{secrets.token_urlsafe(24)}",
        )
        return self.repository.save_api_key(api_key)

    def start_build(self, request: BuildRequest) -> BuildRecord:
        return self.builder.start_build(request)

    def list_builds(self) -> list[BuildRecord]:
        return self.builder.list_builds()

    def create_workload(self, request: WorkloadCreateRequest) -> WorkloadSpec:
        workload = WorkloadSpec(**request.model_dump())
        return self.control_plane.upsert_workload(workload)

    def list_workloads(self) -> list[WorkloadSpec]:
        return self.control_plane.list_workloads()

    def create_deployment(self, request: DeploymentCreateRequest | dict) -> DeploymentRecord:
        payload = request if isinstance(request, DeploymentCreateRequest) else DeploymentCreateRequest(**request)
        return self.control_plane.create_deployment(payload)

    def list_deployments(self) -> list[DeploymentRecord]:
        return self.control_plane.list_deployments()

    def invoke_chat_completion(self, request: ChatCompletionRequest):
        workload_id = self._resolve_workload_id(request.model)
        candidates = [
            deployment
            for deployment in self.control_plane.list_ready_deployments(workload_id)
            if deployment.endpoint is not None
        ]
        if not candidates:
            raise NoReadyDeploymentError(f"no ready deployment for model={request.model}")

        last_upstream_error: RuntimeError | None = None
        for deployment in candidates:
            if not self.inference_client.check_deployment_health(deployment):
                self.control_plane.record_deployment_health_failure(
                    deployment.deployment_id,
                    f"deployment endpoint unhealthy: {deployment.endpoint}",
                )
                continue
            try:
                response = self.inference_client.invoke_chat_completion(deployment, request)
            except RuntimeError as exc:
                self.control_plane.record_deployment_health_failure(deployment.deployment_id, str(exc))
                last_upstream_error = exc
                continue
            self.control_plane.clear_deployment_health_failures(deployment.deployment_id)
            self.control_plane.record_usage(
                UsageRecord(
                    deployment_id=deployment.deployment_id,
                    workload_id=deployment.workload_id,
                    hotkey=deployment.hotkey or "unknown",
                    request_count=1,
                    compute_seconds=0.25,
                    latency_ms_p95=42.0,
                    occupancy_seconds=0.25,
                )
            )
            return response

        if last_upstream_error is not None:
            raise last_upstream_error
        raise NoReadyDeploymentError(f"no healthy deployment available for model={request.model}")

    def stream_chat_completion(self, request: ChatCompletionRequest) -> Iterator[str]:
        response = self.invoke_chat_completion(request.model_copy(update={"stream": False}))
        words = response.content.split()
        for index, word in enumerate(words):
            event = {
                "id": response.id,
                "object": "chat.completion.chunk",
                "model": response.model,
                "deployment_id": response.deployment_id,
                "routed_hotkey": response.routed_hotkey,
                "choices": [{"index": 0, "delta": {"content": word if index == 0 else f" {word}"}}],
            }
            yield f"data: {dumps(event)}\n\n"
        done_event = {
            "id": response.id,
            "object": "chat.completion.chunk",
            "model": response.model,
            "deployment_id": response.deployment_id,
            "routed_hotkey": response.routed_hotkey,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }
        yield f"data: {dumps(done_event)}\n\n"
        yield "data: [DONE]\n\n"

    def _resolve_workload_id(self, model: str) -> str:
        workload = self.control_plane.repository.get_workload(model)
        if workload is not None:
            return workload.workload_id
        named = self.control_plane.find_workload_by_name(model)
        if named is not None:
            return named.workload_id
        for workload in self.control_plane.list_workloads():
            if workload.name == model:
                return workload.workload_id
        raise NoReadyDeploymentError(f"unknown model={model}")


service = GatewayService()
