from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from urllib.parse import urlparse

from greenference_persistence import (
    SubjectBus,
    WorkflowEventRepository,
    create_subject_bus,
    get_metrics_store,
    load_runtime_settings,
)
from greenference_protocol import BuildContextRecord, BuildEventRecord, BuildRecord, BuildRequest
from greenference_builder.infrastructure.execution import (
    ObjectStoreAdapter,
    RegistryAdapter,
    SimulatedObjectStoreAdapter,
    SimulatedRegistryAdapter,
)
from greenference_builder.infrastructure.repository import BuilderRepository


class BuilderService:
    def __init__(
        self,
        repository: BuilderRepository | None = None,
        workflow_repository: WorkflowEventRepository | None = None,
        bus: SubjectBus | None = None,
        object_store: ObjectStoreAdapter | None = None,
        registry: RegistryAdapter | None = None,
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
        self.object_store = object_store or SimulatedObjectStoreAdapter(self.settings)
        self.registry = registry or SimulatedRegistryAdapter(self.settings)
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
        self.repository.save_build_context(
            BuildContextRecord(
                build_id=saved.build_id,
                source_uri=request.context_uri,
                normalized_context_uri=self._normalized_context_uri(request.context_uri),
                dockerfile_path=request.dockerfile_path,
                dockerfile_object_uri=self._dockerfile_object_uri(request.context_uri, request.dockerfile_path),
                context_digest=self._context_digest(saved.build_id, request.context_uri, request.dockerfile_path),
            )
        )
        self.repository.add_build_event(
            BuildEventRecord(
                build_id=saved.build_id,
                stage="accepted",
                message="build request accepted and queued",
            )
        )
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

    def get_build(self, build_id: str) -> BuildRecord | None:
        return self.repository.get_build(build_id)

    def get_build_context(self, build_id: str) -> BuildContextRecord | None:
        return self.repository.get_build_context(build_id)

    def list_build_events(self, build_id: str) -> list[BuildEventRecord]:
        return self.repository.list_build_events(build_id)

    def list_image_history(self, image: str) -> list[BuildRecord]:
        return self.repository.list_builds(image=image)

    def process_pending_events(self, limit: int = 10) -> list[BuildRecord]:
        events = self.bus.claim_pending("builder-worker", ["build.accepted"], limit=limit)
        processed: list[BuildRecord] = []

        for event in events:
            build = self.repository.get_build(str(event.payload["build_id"]))
            if build is None:
                self.bus.mark_failed(event.delivery_id, "build not found")
                self.metrics.increment("build.failed")
                continue
            started_at = datetime.now(UTC)
            try:
                build.status = "building"
                build.failure_reason = None
                build.executor_name = None
                build.updated_at = datetime.now(UTC)
                self.repository.save_build(build)
                self.repository.add_build_event(
                    BuildEventRecord(
                        build_id=build.build_id,
                        stage="building",
                        message="validated build context and prepared registry target",
                    )
                )
                self.bus.publish(
                    "build.started",
                    {
                        "build_id": build.build_id,
                        "image": build.image,
                    },
                )
                self.metrics.increment("build.started")

                self._validate_context_uri(build.context_uri)
                context = self.repository.get_build_context(build.build_id)
                if context is None:
                    raise ValueError("build context not found")
                staged = self.object_store.stage_context(build, context)
                self.repository.save_build_context(staged.context)
                self.repository.add_build_event(
                    BuildEventRecord(
                        build_id=build.build_id,
                        stage="staged",
                        message=staged.message,
                    )
                )
                published = self.registry.publish(build, staged.context)
                build.status = "published"
                build.registry_repository = published.registry_repository
                build.image_tag = published.image_tag
                build.artifact_digest = published.artifact_digest
                build.artifact_uri = published.artifact_uri
                build.registry_manifest_uri = published.registry_manifest_uri
                build.build_log_uri = staged.log_uri
                build.executor_name = published.executor_name
                build.build_duration_seconds = max(
                    (datetime.now(UTC) - started_at).total_seconds(),
                    0.001,
                )
                build.updated_at = datetime.now(UTC)
                self.repository.save_build(build)
                self.repository.add_build_event(
                    BuildEventRecord(
                        build_id=build.build_id,
                        stage="published",
                        message=published.message,
                    )
                )
                self.bus.publish(
                    "build.published",
                    {
                        "build_id": build.build_id,
                        "artifact_uri": build.artifact_uri,
                        "artifact_digest": build.artifact_digest,
                        "registry_manifest_uri": build.registry_manifest_uri,
                    },
                )
                self.metrics.increment("build.published")
                processed.append(build)
            except ValueError as exc:
                build.status = "failed"
                build.failure_reason = str(exc)
                build.build_log_uri = self.object_store.build_log_uri(build.build_id)
                build.build_duration_seconds = max(
                    (datetime.now(UTC) - started_at).total_seconds(),
                    0.001,
                )
                build.updated_at = datetime.now(UTC)
                self.repository.save_build(build)
                self.repository.add_build_event(
                    BuildEventRecord(
                        build_id=build.build_id,
                        stage="failed",
                        message=build.failure_reason,
                    )
                )
                self.bus.publish(
                    "build.failed",
                    {
                        "build_id": build.build_id,
                        "image": build.image,
                        "reason": build.failure_reason,
                    },
                )
                self.metrics.increment("build.failed")
            finally:
                self.bus.mark_completed(event.delivery_id)
        pending_count = len(
            self.bus.list_deliveries(
                consumer="builder-worker",
                subjects=["build.accepted"],
                statuses=["pending"],
            )
        )
        self.metrics.set_gauge("workflow.pending.build.accepted", float(pending_count))
        return processed

    @staticmethod
    def _validate_context_uri(context_uri: str) -> None:
        parsed = urlparse(context_uri)
        if parsed.scheme not in {"s3", "minio", "file", "http", "https"}:
            raise ValueError(f"unsupported build context scheme: {parsed.scheme or 'missing'}")
        if parsed.scheme != "file" and not parsed.netloc:
            raise ValueError("build context uri missing object store or host component")
        if not parsed.path and parsed.scheme == "file":
            raise ValueError("build context uri missing object path")
        if parsed.scheme in {"s3", "minio"} and not (parsed.netloc or parsed.path.lstrip("/")):
            raise ValueError("build context uri missing object path")

    @staticmethod
    def _normalized_context_uri(context_uri: str) -> str:
        parsed = urlparse(context_uri)
        path = parsed.path or ""
        return f"{parsed.scheme}://{parsed.netloc}{path}"

    @staticmethod
    def _dockerfile_object_uri(context_uri: str, dockerfile_path: str) -> str:
        normalized = BuilderService._normalized_context_uri(context_uri).rstrip("/")
        return f"{normalized}.{dockerfile_path.replace('/', '_')}"

    @staticmethod
    def _context_digest(build_id: str, context_uri: str, dockerfile_path: str) -> str:
        return f"sha256:{hashlib.sha256(f'{build_id}:{context_uri}:{dockerfile_path}'.encode()).hexdigest()}"

service = BuilderService()
