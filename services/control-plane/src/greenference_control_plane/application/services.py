from __future__ import annotations

from datetime import UTC, datetime, timedelta

from greenference_persistence import WorkflowEventRepository, get_metrics_store
from greenference_protocol import (
    CapacityUpdate,
    DeploymentCreateRequest,
    DeploymentRecord,
    DeploymentState,
    DeploymentStatusUpdate,
    Heartbeat,
    LeaseAssignment,
    MinerRegistration,
    UsageRecord,
    WorkloadSpec,
)
from greenference_control_plane.config import settings
from greenference_control_plane.domain.metering import UsageAggregator
from greenference_control_plane.domain.scheduler import PlacementPolicy
from greenference_control_plane.domain.state import transition_state
from greenference_control_plane.infrastructure.repository import ControlPlaneRepository


class ControlPlaneService:
    def __init__(
        self,
        repository: ControlPlaneRepository | None = None,
        workflow_repository: WorkflowEventRepository | None = None,
    ) -> None:
        self.repository = repository or ControlPlaneRepository()
        self.workflow_repository = workflow_repository or WorkflowEventRepository(
            engine=self.repository.engine,
            session_factory=self.repository.session_factory,
        )
        self.placement_policy = PlacementPolicy()
        self.usage_aggregator = UsageAggregator()
        self.metrics = get_metrics_store("greenference-control-plane")

    def register_miner(self, registration: MinerRegistration) -> MinerRegistration:
        return self.repository.upsert_miner(registration)

    def record_heartbeat(self, heartbeat: Heartbeat) -> Heartbeat:
        return self.repository.upsert_heartbeat(heartbeat)

    def update_capacity(self, update: CapacityUpdate) -> CapacityUpdate:
        return self.repository.upsert_capacity(update)

    def upsert_workload(self, workload: WorkloadSpec) -> WorkloadSpec:
        return self.repository.upsert_workload(workload)

    def list_workloads(self) -> list[WorkloadSpec]:
        return self.repository.list_workloads()

    def find_workload_by_name(self, name: str) -> WorkloadSpec | None:
        return self.repository.find_workload_by_name(name)

    def create_deployment(self, request: DeploymentCreateRequest | dict) -> DeploymentRecord:
        payload = request if isinstance(request, DeploymentCreateRequest) else DeploymentCreateRequest(**request)
        workload = self.repository.get_workload(payload.workload_id)
        if workload is None:
            raise KeyError(f"workload not found: {payload.workload_id}")
        deployment = DeploymentRecord(
            workload_id=payload.workload_id,
            requested_instances=payload.requested_instances,
        )
        self.repository.create_deployment(deployment)
        self.workflow_repository.publish(
            "deployment.requested",
            {
                "deployment_id": deployment.deployment_id,
                "workload_id": deployment.workload_id,
                "requested_instances": deployment.requested_instances,
            },
        )
        self.metrics.increment("deployment.requested")
        return deployment

    def _assign_lease(self, workload: WorkloadSpec, deployment_id: str) -> LeaseAssignment | None:
        nodes = []
        for update in self.repository.list_capacities():
            heartbeat = self.repository.get_heartbeat(update.hotkey)
            if heartbeat and not heartbeat.healthy:
                continue
            nodes.extend(update.nodes)
        assignment = self.placement_policy.assign_lease(workload, deployment_id, nodes)
        if assignment is None:
            return None
        self.repository.adjust_node_capacity(
            assignment.hotkey,
            assignment.node_id,
            -workload.requirements.gpu_count,
        )
        assignment.expires_at = datetime.now(UTC) + timedelta(seconds=settings.default_lease_ttl_seconds)
        return assignment

    def list_leases(self, hotkey: str) -> list[LeaseAssignment]:
        return self.repository.list_assignments(hotkey, statuses=["assigned", "activating"])

    def list_deployments(self) -> list[DeploymentRecord]:
        return self.repository.list_deployments()

    def update_deployment_status(self, update: DeploymentStatusUpdate) -> DeploymentRecord:
        deployment = self.repository.get_deployment(update.deployment_id)
        if deployment is None:
            raise KeyError(f"deployment not found: {update.deployment_id}")
        deployment.state = transition_state(deployment.state, update.state)
        deployment.ready_instances = update.ready_instances if update.state == DeploymentState.READY else 0
        deployment.endpoint = update.endpoint or deployment.endpoint
        deployment.last_error = update.error
        if update.state == DeploymentState.READY:
            deployment.health_check_failures = 0
        deployment.updated_at = update.observed_at
        self.repository.add_deployment_event(update)
        assignment_status = {
            DeploymentState.PULLING: "activating",
            DeploymentState.STARTING: "activating",
            DeploymentState.READY: "active",
            DeploymentState.FAILED: "failed",
            DeploymentState.TERMINATED: "terminated",
        }.get(update.state)
        if assignment_status is not None:
            self.repository.update_assignment_status(update.deployment_id, assignment_status)
        saved = self.repository.update_deployment(deployment)
        subject = {
            DeploymentState.READY: "deployment.ready",
            DeploymentState.FAILED: "deployment.failed",
            DeploymentState.TERMINATED: "deployment.terminated",
        }.get(update.state, "deployment.status.updated")
        self.workflow_repository.publish(
            subject,
            {
                "deployment_id": saved.deployment_id,
                "state": saved.state.value,
                "hotkey": saved.hotkey,
                "endpoint": saved.endpoint,
                "error": saved.last_error,
            },
        )
        self.metrics.increment(f"deployment.state.{saved.state.value}")
        return saved

    def list_ready_deployments(self, workload_id: str) -> list[DeploymentRecord]:
        return sorted(
            self.repository.list_ready_deployments(workload_id),
            key=lambda item: item.updated_at,
            reverse=True,
        )

    def resolve_ready_deployment(self, workload_id: str) -> DeploymentRecord | None:
        ready = self.list_ready_deployments(workload_id)
        return ready[0] if ready else None

    def record_deployment_health_failure(self, deployment_id: str, error: str) -> DeploymentRecord:
        deployment = self.repository.get_deployment(deployment_id)
        if deployment is None:
            raise KeyError(f"deployment not found: {deployment_id}")
        deployment.health_check_failures += 1
        deployment.last_error = error
        deployment.updated_at = datetime.now(UTC)
        if deployment.health_check_failures >= settings.deployment_health_failure_threshold:
            self.repository.update_deployment(deployment)
            return self.update_deployment_status(
                DeploymentStatusUpdate(
                    deployment_id=deployment_id,
                    state=DeploymentState.FAILED,
                    error=error,
                    observed_at=deployment.updated_at,
                )
            )
        self.metrics.increment("deployment.health.failed")
        return self.repository.update_deployment(deployment)

    def clear_deployment_health_failures(self, deployment_id: str) -> DeploymentRecord:
        deployment = self.repository.get_deployment(deployment_id)
        if deployment is None:
            raise KeyError(f"deployment not found: {deployment_id}")
        if deployment.health_check_failures == 0 and deployment.last_error is None:
            return deployment
        deployment.health_check_failures = 0
        deployment.last_error = None
        deployment.updated_at = datetime.now(UTC)
        self.metrics.increment("deployment.health.recovered")
        return self.repository.update_deployment(deployment)

    def record_usage(self, record: UsageRecord) -> UsageRecord:
        self.workflow_repository.publish(
            "usage.recorded",
            record.model_dump(mode="json"),
        )
        self.metrics.increment("usage.queued")
        return record

    def usage_summary(self) -> dict[str, dict[str, float]]:
        return self.usage_aggregator.aggregate(self.repository.list_usage_records())

    def process_timeouts(self, now: datetime | None = None) -> list[DeploymentRecord]:
        observed_at = now or datetime.now(UTC)
        expired: list[DeploymentRecord] = []
        for assignment in self.repository.list_assignments(statuses=["assigned", "activating"]):
            expires_at = assignment.expires_at
            if expires_at is None:
                continue
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=UTC)
            if expires_at > observed_at:
                continue
            deployment = self.repository.get_deployment(assignment.deployment_id)
            if deployment is None or deployment.state in {
                DeploymentState.READY,
                DeploymentState.FAILED,
                DeploymentState.TERMINATED,
            }:
                continue
            expired.append(
                self.update_deployment_status(
                    DeploymentStatusUpdate(
                        deployment_id=deployment.deployment_id,
                        state=DeploymentState.FAILED,
                        error="lease expired before deployment became ready",
                        observed_at=observed_at,
                    )
                )
            )
        return expired

    def process_pending_events(self, limit: int = 10) -> dict[str, list]:
        events = self.workflow_repository.claim_pending(["deployment.requested", "usage.recorded"], limit=limit)
        scheduled: list[DeploymentRecord] = []
        usage_records: list[UsageRecord] = []

        for event in events:
            if event.subject == "deployment.requested":
                processed = self._process_deployment_request(event)
                if processed is not None:
                    scheduled.append(processed)
                continue
            if event.subject == "usage.recorded":
                usage_record = self._process_usage_record(event)
                if usage_record is not None:
                    usage_records.append(usage_record)
                continue
            self.workflow_repository.mark_failed(event.event_id, f"unsupported workflow subject={event.subject}")

        self.metrics.set_gauge(
            "workflow.pending.deployment.requested",
            float(len(self.workflow_repository.list_events(subjects=["deployment.requested"], statuses=["pending"]))),
        )
        self.metrics.set_gauge(
            "workflow.pending.usage.recorded",
            float(len(self.workflow_repository.list_events(subjects=["usage.recorded"], statuses=["pending"]))),
        )
        return {
            "deployments": [item.model_dump(mode="json") for item in scheduled],
            "usage_records": [item.model_dump(mode="json") for item in usage_records],
        }

    def _process_deployment_request(self, event) -> DeploymentRecord | None:
        deployment = self.repository.get_deployment(str(event.payload["deployment_id"]))
        if deployment is None:
            self.workflow_repository.mark_failed(event.event_id, "deployment not found")
            return None
        if deployment.state != DeploymentState.PENDING:
            self.workflow_repository.mark_completed(event.event_id)
            return None

        workload = self.repository.get_workload(deployment.workload_id)
        if workload is None:
            self.workflow_repository.mark_failed(event.event_id, "workload not found")
            return None

        assignment = self._assign_lease(workload, deployment.deployment_id)
        if assignment is None:
            deployment.retry_count = max(0, event.attempts)
            deployment.last_error = "no compatible miner capacity available"
            deployment.updated_at = datetime.now(UTC)
            self.repository.update_deployment(deployment)
            if event.attempts >= settings.deployment_request_retry_limit:
                failed = self.update_deployment_status(
                    DeploymentStatusUpdate(
                        deployment_id=deployment.deployment_id,
                        state=DeploymentState.FAILED,
                        error=(
                            "no compatible miner capacity available after "
                            f"{settings.deployment_request_retry_limit} attempts"
                        ),
                        observed_at=deployment.updated_at,
                    )
                )
                self.workflow_repository.mark_failed(event.event_id, failed.last_error or "deployment scheduling failed")
                return None
            self.workflow_repository.mark_failed(
                event.event_id,
                "no compatible miner capacity available",
                retryable=True,
                retry_after_seconds=float(settings.deployment_request_retry_delay_seconds),
            )
            return None

        deployment.hotkey = assignment.hotkey
        deployment.node_id = assignment.node_id
        deployment.state = DeploymentState.SCHEDULED
        deployment.retry_count = max(0, event.attempts - 1)
        deployment.health_check_failures = 0
        deployment.last_error = None
        deployment.updated_at = datetime.now(UTC)
        self.repository.save_assignment(assignment)
        saved = self.repository.update_deployment(deployment)
        self.workflow_repository.publish(
            "deployment.scheduled",
            {
                "deployment_id": saved.deployment_id,
                "workload_id": saved.workload_id,
                "hotkey": saved.hotkey,
                "node_id": saved.node_id,
            },
        )
        self.workflow_repository.mark_completed(event.event_id)
        self.metrics.increment("deployment.scheduled")
        return saved

    def _process_usage_record(self, event) -> UsageRecord | None:
        record = UsageRecord(**event.payload)
        saved = self.repository.add_usage_record(record)
        self.workflow_repository.mark_completed(event.event_id)
        self.metrics.increment("usage.persisted")
        return saved


service = ControlPlaneService()
