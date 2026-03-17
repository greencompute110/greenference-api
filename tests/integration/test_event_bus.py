from greenference_persistence import SubjectBus, WorkflowEventRepository
from greenference_protocol import NodeCapability, ProbeResult
from greenference_validator.application.services import ValidatorService
from greenference_validator.infrastructure.repository import ValidatorRepository


def test_validator_worker_acks_deliveries_once() -> None:
    shared_db = "sqlite+pysqlite:///:memory:"
    workflow_repository = WorkflowEventRepository(database_url=shared_db, bootstrap=True)
    bus = SubjectBus(database_url=shared_db, bootstrap=True, workflow_repository=workflow_repository)
    validator = ValidatorService(
        ValidatorRepository(database_url=shared_db, bootstrap=True),
        workflow_repository=workflow_repository,
        bus=bus,
    )

    capability = NodeCapability(
        hotkey="miner-a",
        node_id="node-a",
        gpu_model="a100",
        gpu_count=1,
        available_gpus=1,
        vram_gb_per_gpu=80,
        cpu_cores=32,
        memory_gb=128,
        performance_score=1.2,
    )
    validator.register_capability(capability)
    challenge = validator.create_probe("miner-a", "node-a")
    validator.submit_probe_result(
        ProbeResult(
            challenge_id=challenge.challenge_id,
            hotkey="miner-a",
            node_id="node-a",
            latency_ms=90.0,
            throughput=180.0,
            benchmark_signature="bus-sig",
        )
    )

    pending = bus.list_deliveries(
        consumer="validator-worker",
        subjects=["probe.result.recorded"],
        statuses=["pending"],
    )
    assert len(pending) == 1

    first = validator.process_pending_events()
    second = validator.process_pending_events()
    completed = bus.list_deliveries(
        consumer="validator-worker",
        subjects=["probe.result.recorded"],
        statuses=["completed"],
    )

    assert len(first) == 1
    assert second == []
    assert len(completed) == 1
