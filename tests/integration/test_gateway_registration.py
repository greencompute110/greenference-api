from greenference_gateway.application.services import GatewayService
from greenference_gateway.infrastructure.repository import GatewayRepository
from greenference_control_plane.application.services import ControlPlaneService
from greenference_control_plane.infrastructure.repository import ControlPlaneRepository
from greenference_protocol import (
    APIKeyCreateRequest,
    UserProfileUpdateRequest,
    UserRegistrationRequest,
    UserSecretCreateRequest,
    WorkloadCreateRequest,
    WorkloadShareCreateRequest,
)


def test_gateway_registers_user_and_persists_api_key():
    gateway = GatewayService(
        repository=GatewayRepository(database_url="sqlite+pysqlite:///:memory:", bootstrap=True)
    )
    user = gateway.register_user(UserRegistrationRequest(username="alice", email="alice@example.com"))
    api_key = gateway.create_api_key(APIKeyCreateRequest(name="default", user_id=user.user_id))

    assert user.user_id
    assert api_key.user_id == user.user_id
    assert api_key.secret.startswith("gk_")


def test_gateway_persists_product_foundation_entities():
    shared_db = "sqlite+pysqlite:///:memory:"
    gateway = GatewayService(repository=GatewayRepository(database_url=shared_db, bootstrap=True))
    gateway.control_plane = ControlPlaneService(ControlPlaneRepository(database_url=shared_db, bootstrap=True))

    owner = gateway.register_user(UserRegistrationRequest(username="owner", email="owner@example.com"))
    peer = gateway.register_user(UserRegistrationRequest(username="peer", email="peer@example.com"))

    updated = gateway.update_user_profile(
        owner.user_id,
        UserProfileUpdateRequest(display_name="Owner Name", bio="gpu builder", metadata={"team": "alpha"}),
    )
    assert updated.display_name == "Owner Name"
    assert updated.metadata["team"] == "alpha"

    secret = gateway.create_secret(owner.user_id, UserSecretCreateRequest(name="HF_TOKEN", value="token-123"))
    assert [item.name for item in gateway.list_secrets(owner.user_id)] == ["HF_TOKEN"]
    deleted = gateway.delete_secret(secret.secret_id, user_id=owner.user_id)
    assert deleted.secret_id == secret.secret_id

    workload = gateway.create_workload(
        WorkloadCreateRequest(
            name="private-model",
            image="greenference/private:latest",
            requirements={"gpu_count": 1},
            public=False,
        ),
        owner.user_id,
    )
    assert gateway.list_workloads(user_id=peer.user_id) == []

    share = gateway.share_workload(
        workload.workload_id,
        WorkloadShareCreateRequest(shared_with_user_id=peer.user_id),
        actor_user_id=owner.user_id,
    )
    assert share.shared_with_user_id == peer.user_id

    peer_visible = gateway.list_workloads(user_id=peer.user_id)
    assert [item.workload_id for item in peer_visible] == [workload.workload_id]

    deployment = gateway.create_deployment(
        {"workload_id": workload.workload_id, "requested_instances": 1},
        user_id=peer.user_id,
    )
    assert deployment.owner_user_id == peer.user_id
    assert [item.deployment_id for item in gateway.list_deployments(user_id=peer.user_id)] == [deployment.deployment_id]
