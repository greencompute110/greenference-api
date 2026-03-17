from __future__ import annotations

from datetime import UTC, datetime
from urllib.parse import urlparse

from greenference_persistence import (
    SubjectBus,
    WorkflowEventRepository,
    create_subject_bus,
    get_metrics_store,
    load_runtime_settings,
)
from greenference_protocol import BuildRecord, BuildRequest
from greenference_builder.infrastructure.repository import BuilderRepository


class BuilderService:
    def __init__(
        self,
        repository: BuilderRepository | None = None,
        workflow_repository: WorkflowEventRepository | None = None,
        bus: SubjectBus | None = None,
    ) -> None:
        self.repository = repository or BuilderRepository()
        self.workflow_repository = workflow_repository or WorkflowEventRepository(
            engine=self.repository.engine,
            session_factory=self.repository.session_factory,
        )
        self.settings = load_runtime_settings("greenference-builder")
        self.bus = bus or create_subject_bus(
            engine=self.workflow_repository.engine,
            session_factory=self.workflow_repository.session_factory,
            workflow_repository=self.workflow_repository,
            nats_url=self.settings.nats_url,
            transport=self.settings.bus_transport,
        )
        self.metrics = get_metrics_store("greenference-builder")

    def start_build(self, request: BuildRequest) -> BuildRecord:
        build = BuildRecord(
            image=request.image,
            context_uri=request.context_uri,
            dockerfile_path=request.dockerfile_path,
            public=request.public,
            status="accepted",
            artifact_uri=None,
        )
        build.updated_at = datetime.now(UTC)
        saved = self.repository.save_build(build)
        self.bus.publish(
            "build.accepted",
            {
                "build_id": saved.build_id,
                "image": saved.image,
            },
        )
        self.metrics.increment("build.accepted")
        return saved

    def list_builds(self) -> list[BuildRecord]:
        return self.repository.list_builds()

    def process_pending_events(self, limit: int = 10) -> list[BuildRecord]:
        events = self.bus.claim_pending("builder-worker", ["build.accepted"], limit=limit)
        processed: list[BuildRecord] = []
        registry_ref = urlparse(self.settings.registry_url).netloc or self.settings.registry_url.replace(
            "http://", ""
        ).replace("https://", "")

        for event in events:
            build = self.repository.get_build(str(event.payload["build_id"]))
            if build is None:
                self.bus.mark_failed(event.delivery_id, "build not found")
                self.metrics.increment("build.failed")
                continue
            build.status = "published"
            build.artifact_uri = f"oci://{registry_ref.rstrip('/')}/{build.image}"
            build.updated_at = datetime.now(UTC)
            self.repository.save_build(build)
            self.bus.publish(
                "build.published",
                {
                    "build_id": build.build_id,
                    "artifact_uri": build.artifact_uri,
                },
            )
            self.bus.mark_completed(event.delivery_id)
            self.metrics.increment("build.published")
            processed.append(build)
        pending_count = len(
            self.bus.list_deliveries(
                consumer="builder-worker",
                subjects=["build.accepted"],
                statuses=["pending"],
            )
        )
        self.metrics.set_gauge("workflow.pending.build.accepted", float(pending_count))
        return processed


service = BuilderService()
