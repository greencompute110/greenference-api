from __future__ import annotations

import hashlib
from dataclasses import dataclass
from urllib.parse import urlparse

from greenference_persistence import RuntimeSettings
from greenference_protocol import BuildContextRecord, BuildRecord


@dataclass(slots=True)
class StagedContext:
    context: BuildContextRecord
    log_uri: str
    message: str


@dataclass(slots=True)
class PublishedImage:
    registry_repository: str
    image_tag: str
    artifact_uri: str
    artifact_digest: str
    registry_manifest_uri: str
    executor_name: str
    message: str


class ObjectStoreAdapter:
    def stage_context(self, build: BuildRecord, context: BuildContextRecord) -> StagedContext:
        raise NotImplementedError

    def build_log_uri(self, build_id: str) -> str:
        raise NotImplementedError


class RegistryAdapter:
    def publish(self, build: BuildRecord, context: BuildContextRecord) -> PublishedImage:
        raise NotImplementedError


class SimulatedObjectStoreAdapter(ObjectStoreAdapter):
    def __init__(self, settings: RuntimeSettings) -> None:
        self.settings = settings

    def stage_context(self, build: BuildRecord, context: BuildContextRecord) -> StagedContext:
        staged_context_uri = (
            f"s3://greenference-build-artifacts/contexts/{build.build_id}/context.tar.gz"
        )
        context_manifest_uri = (
            f"s3://greenference-build-artifacts/manifests/{build.build_id}.json"
        )
        updated_context = context.model_copy(
            update={
                "staged_context_uri": staged_context_uri,
                "context_manifest_uri": context_manifest_uri,
            }
        )
        return StagedContext(
            context=updated_context,
            log_uri=self.build_log_uri(build.build_id),
            message=f"staged build context at {staged_context_uri}",
        )

    @staticmethod
    def build_log_uri(build_id: str) -> str:
        return f"s3://greenference-build-artifacts/build-logs/{build_id}.log"


class SimulatedRegistryAdapter(RegistryAdapter):
    def __init__(self, settings: RuntimeSettings) -> None:
        self.settings = settings

    def publish(self, build: BuildRecord, context: BuildContextRecord) -> PublishedImage:
        registry_ref = urlparse(self.settings.registry_url).netloc or self.settings.registry_url.replace(
            "http://", ""
        ).replace("https://", "")
        repository, image_tag = split_image_ref(build.image)
        digest = hashlib.sha256(
            (
                f"{build.build_id}:{build.image}:{context.context_digest}:"
                f"{context.staged_context_uri}:{context.dockerfile_path}"
            ).encode()
        ).hexdigest()
        artifact_uri = f"oci://{registry_ref.rstrip('/')}/{build.image}"
        manifest_uri = f"{artifact_uri}@sha256:{digest}"
        return PublishedImage(
            registry_repository=repository,
            image_tag=image_tag,
            artifact_uri=artifact_uri,
            artifact_digest=f"sha256:{digest}",
            registry_manifest_uri=manifest_uri,
            executor_name="simulated-buildkit",
            message=f"published registry manifest {manifest_uri}",
        )


def split_image_ref(image: str) -> tuple[str, str]:
    last_slash = image.rfind("/")
    last_colon = image.rfind(":")
    if last_colon > last_slash:
        return image[:last_colon], image[last_colon + 1 :]
    return image, "latest"
