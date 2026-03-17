from __future__ import annotations

from greenference_persistence import SubjectBus, WorkflowEventRepository, create_subject_bus, get_metrics_store
from greenference_persistence.runtime import load_runtime_settings
from greenference_protocol import NodeCapability, ProbeChallenge, ProbeResult, ScoreCard, WeightSnapshot
from greenference_validator.domain.scoring import ScoreEngine
from greenference_validator.infrastructure.repository import ValidatorRepository


class UnknownCapabilityError(KeyError):
    pass


class UnknownProbeChallengeError(KeyError):
    pass


class InvalidProbeResultError(ValueError):
    pass


class ValidatorService:
    def __init__(
        self,
        repository: ValidatorRepository | None = None,
        workflow_repository: WorkflowEventRepository | None = None,
        bus: SubjectBus | None = None,
    ) -> None:
        self.repository = repository or ValidatorRepository()
        self.workflow_repository = workflow_repository or WorkflowEventRepository(
            engine=self.repository.engine,
            session_factory=self.repository.session_factory,
        )
        runtime_settings = load_runtime_settings("greenference-validator")
        self.bus = bus or create_subject_bus(
            engine=self.workflow_repository.engine,
            session_factory=self.workflow_repository.session_factory,
            workflow_repository=self.workflow_repository,
            nats_url=runtime_settings.nats_url,
            transport=runtime_settings.bus_transport,
        )
        self.scoring = ScoreEngine()
        self.metrics = get_metrics_store("greenference-validator")

    def register_capability(self, capability: NodeCapability) -> NodeCapability:
        return self.repository.upsert_capability(capability)

    def create_probe(self, hotkey: str, node_id: str, kind: str = "latency") -> ProbeChallenge:
        capability = self.repository.get_capability(hotkey)
        if capability is None:
            raise UnknownCapabilityError(f"capability not found for hotkey={hotkey}")
        if capability.node_id != node_id:
            raise InvalidProbeResultError(f"node mismatch for hotkey={hotkey}: expected={capability.node_id}")
        challenge = ProbeChallenge(hotkey=hotkey, node_id=node_id, kind=kind)
        return self.repository.save_challenge(challenge)

    def submit_probe_result(self, result: ProbeResult) -> ScoreCard:
        challenge = self.repository.get_challenge(result.challenge_id)
        if challenge is None:
            raise UnknownProbeChallengeError(f"challenge not found: {result.challenge_id}")
        if challenge.hotkey != result.hotkey or challenge.node_id != result.node_id:
            raise InvalidProbeResultError(f"challenge mismatch for hotkey={result.hotkey} node={result.node_id}")
        if self.repository.get_result(result.challenge_id, result.hotkey) is not None:
            raise InvalidProbeResultError(f"duplicate result for challenge={result.challenge_id} hotkey={result.hotkey}")

        capability = self.repository.get_capability(result.hotkey)
        if capability is None:
            raise UnknownCapabilityError(f"capability not found for hotkey={result.hotkey}")

        self.repository.add_result(result)
        scorecard = self.scoring.compute_scorecard(capability, self.repository.list_results(result.hotkey))
        saved = self.repository.save_scorecard(scorecard)
        self.bus.publish(
            "probe.result.recorded",
            {
                "challenge_id": result.challenge_id,
                "hotkey": result.hotkey,
                "node_id": result.node_id,
                "final_score": saved.final_score,
            },
        )
        self.metrics.increment("probe.result.recorded")
        return saved

    def publish_weight_snapshot(self, netuid: int = 64) -> WeightSnapshot:
        scorecards: dict[str, ScoreCard] = {}
        for hotkey, capability in sorted(self.repository.list_capabilities().items()):
            results = self.repository.list_results(hotkey)
            if not results:
                continue
            scorecard = self.scoring.compute_scorecard(capability, results)
            scorecards[hotkey] = self.repository.save_scorecard(scorecard)
        weights = {
            hotkey: scorecard.final_score
            for hotkey, scorecard in sorted(scorecards.items())
        }
        snapshot = WeightSnapshot(netuid=netuid, weights=weights)
        saved = self.repository.save_snapshot(snapshot)
        self.bus.publish(
            "validator.weights.published",
            {
                "snapshot_id": saved.snapshot_id,
                "netuid": saved.netuid,
                "weights": saved.weights,
            },
        )
        self.metrics.increment("weights.published")
        return saved

    def process_pending_events(self, limit: int = 10) -> list[dict]:
        events = self.bus.claim_pending(
            "validator-worker",
            ["probe.result.recorded", "validator.weights.published"],
            limit=limit,
        )
        processed: list[dict] = []
        for event in events:
            if event.subject == "probe.result.recorded":
                self.bus.mark_completed(event.delivery_id)
                self.metrics.increment("probe.result.delivered")
                processed.append({"subject": event.subject, "hotkey": event.payload["hotkey"]})
                continue
            if event.subject == "validator.weights.published":
                self.bus.mark_completed(event.delivery_id)
                self.metrics.increment("weights.delivered")
                processed.append({"subject": event.subject, "snapshot_id": event.payload["snapshot_id"]})
                continue
            self.bus.mark_failed(event.delivery_id, f"unsupported workflow subject={event.subject}")
        self.metrics.set_gauge(
            "workflow.pending.validator",
            float(
                len(
                    self.bus.list_deliveries(
                        consumer="validator-worker",
                        subjects=["probe.result.recorded", "validator.weights.published"],
                        statuses=["pending"],
                    )
                )
            ),
        )
        return processed


service = ValidatorService()
