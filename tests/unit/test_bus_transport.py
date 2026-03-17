from __future__ import annotations

from greenference_persistence import NatsJetStreamBus, SubjectBus, WorkflowEventRepository, create_subject_bus


def test_create_subject_bus_uses_durable_for_auto_without_nats() -> None:
    shared_db = "sqlite+pysqlite:///:memory:"
    workflow_repository = WorkflowEventRepository(database_url=shared_db, bootstrap=True)

    bus = create_subject_bus(
        database_url=shared_db,
        bootstrap=True,
        workflow_repository=workflow_repository,
        transport="auto",
    )

    assert isinstance(bus, SubjectBus)


def test_create_subject_bus_returns_nats_wrapper_when_requested() -> None:
    shared_db = "sqlite+pysqlite:///:memory:"
    workflow_repository = WorkflowEventRepository(database_url=shared_db, bootstrap=True)

    bus = create_subject_bus(
        database_url=shared_db,
        bootstrap=True,
        workflow_repository=workflow_repository,
        transport="nats",
        nats_url="nats://nats:4222",
    )

    assert isinstance(bus, NatsJetStreamBus)
    assert bus.active_transport in {"nats", "durable"}
