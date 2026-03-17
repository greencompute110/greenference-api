from __future__ import annotations

import importlib.util
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from greenference_persistence.db import (
    create_db_engine,
    create_session_factory,
    init_database,
    needs_bootstrap,
    session_scope,
)
from greenference_persistence.orm import BusDeliveryORM, WorkflowEventORM
from greenference_persistence.workflow import WorkflowEvent, WorkflowEventRepository


def utcnow() -> datetime:
    return datetime.now(UTC)


SUBJECT_CONSUMERS: dict[str, list[str]] = {
    "build.accepted": ["builder-worker"],
    "deployment.requested": ["control-plane-worker"],
    "usage.recorded": ["control-plane-worker"],
    "probe.result.recorded": ["validator-worker"],
    "validator.weights.published": ["validator-worker"],
}


class BusMessage(BaseModel):
    delivery_id: int
    event_id: str
    consumer: str
    subject: str
    payload: dict[str, Any]
    attempts: int = 0
    available_at: datetime = Field(default_factory=utcnow)
    last_error: str | None = None


class SubjectBus:
    def __init__(
        self,
        database_url: str | None = None,
        bootstrap: bool | None = None,
        engine: Engine | None = None,
        session_factory: sessionmaker[Session] | None = None,
        workflow_repository: WorkflowEventRepository | None = None,
    ) -> None:
        resolved_engine = engine
        resolved_session_factory = session_factory
        if workflow_repository is not None and resolved_engine is None and resolved_session_factory is None:
            resolved_engine = workflow_repository.engine
            resolved_session_factory = workflow_repository.session_factory
        self.engine = resolved_engine or create_db_engine(database_url)
        self.session_factory = resolved_session_factory or create_session_factory(self.engine)
        if engine is None and session_factory is None and needs_bootstrap(str(self.engine.url), bootstrap):
            init_database(self.engine)
        self.workflow_repository = workflow_repository or WorkflowEventRepository(
            engine=self.engine,
            session_factory=self.session_factory,
        )

    def publish(self, subject: str, payload: dict[str, Any]) -> WorkflowEvent:
        event = self.workflow_repository.publish(subject, payload)
        consumers = SUBJECT_CONSUMERS.get(subject, [])
        if not consumers:
            return event
        with session_scope(self.session_factory) as session:
            for consumer in consumers:
                session.add(
                    BusDeliveryORM(
                        event_id=event.event_id,
                        consumer=consumer,
                        subject=subject,
                        status="pending",
                        attempts=0,
                        available_at=event.available_at,
                        last_error=None,
                        created_at=event.created_at,
                        updated_at=event.updated_at,
                    )
                )
        return event

    def claim_pending(self, consumer: str, subjects: list[str], limit: int = 10) -> list[BusMessage]:
        now = utcnow()
        with session_scope(self.session_factory) as session:
            rows = session.scalars(
                select(BusDeliveryORM)
                .where(
                    BusDeliveryORM.consumer == consumer,
                    BusDeliveryORM.subject.in_(subjects),
                    BusDeliveryORM.status == "pending",
                    BusDeliveryORM.available_at <= now,
                )
                .order_by(BusDeliveryORM.created_at.asc())
                .limit(limit)
            ).all()
            messages: list[BusMessage] = []
            for row in rows:
                row.status = "processing"
                row.attempts += 1
                row.updated_at = now
                session.add(row)
                event_row = session.scalar(select(WorkflowEventORM).where(WorkflowEventORM.event_id == row.event_id))
                if event_row is None:
                    continue
                messages.append(self._to_message(row, event_row))
            return messages

    def mark_completed(self, delivery_id: int) -> BusMessage | None:
        return self._update_status(delivery_id, status="completed")

    def mark_failed(
        self,
        delivery_id: int,
        error: str,
        retryable: bool = False,
        retry_after_seconds: float | None = None,
    ) -> BusMessage | None:
        status = "pending" if retryable else "failed"
        return self._update_status(delivery_id, status=status, error=error, retry_after_seconds=retry_after_seconds)

    def list_deliveries(
        self,
        consumer: str | None = None,
        subjects: list[str] | None = None,
        statuses: list[str] | None = None,
    ) -> list[BusMessage]:
        with session_scope(self.session_factory) as session:
            stmt = select(BusDeliveryORM)
            if consumer:
                stmt = stmt.where(BusDeliveryORM.consumer == consumer)
            if subjects:
                stmt = stmt.where(BusDeliveryORM.subject.in_(subjects))
            if statuses:
                stmt = stmt.where(BusDeliveryORM.status.in_(statuses))
            rows = session.scalars(stmt.order_by(BusDeliveryORM.created_at.asc())).all()
            messages: list[BusMessage] = []
            for row in rows:
                event_row = session.scalar(select(WorkflowEventORM).where(WorkflowEventORM.event_id == row.event_id))
                if event_row is None:
                    continue
                messages.append(self._to_message(row, event_row))
            return messages

    def _update_status(
        self,
        delivery_id: int,
        status: str,
        error: str | None = None,
        retry_after_seconds: float | None = None,
    ) -> BusMessage | None:
        with session_scope(self.session_factory) as session:
            row = session.get(BusDeliveryORM, delivery_id)
            if row is None:
                return None
            row.status = status
            row.last_error = error
            if retry_after_seconds is not None:
                row.available_at = utcnow() + timedelta(seconds=retry_after_seconds)
            row.updated_at = utcnow()
            session.add(row)
            session.flush()
            event_row = session.scalar(select(WorkflowEventORM).where(WorkflowEventORM.event_id == row.event_id))
            if event_row is None:
                return None
            event_row.status = status
            event_row.last_error = error
            if retry_after_seconds is not None:
                event_row.available_at = row.available_at
            event_row.updated_at = row.updated_at
            session.add(event_row)
            return self._to_message(row, event_row)

    @staticmethod
    def _to_message(row: BusDeliveryORM, event_row: WorkflowEventORM) -> BusMessage:
        return BusMessage(
            delivery_id=row.id,
            event_id=row.event_id,
            consumer=row.consumer,
            subject=row.subject,
            payload=event_row.payload,
            attempts=row.attempts,
            available_at=row.available_at,
            last_error=row.last_error,
        )


class NatsJetStreamBus:
    def __init__(
        self,
        durable_bus: SubjectBus,
        nats_url: str,
        enabled: bool = True,
    ) -> None:
        self.durable_bus = durable_bus
        self.nats_url = nats_url
        self.enabled = enabled
        self.client_available = importlib.util.find_spec("nats") is not None

    @property
    def active_transport(self) -> str:
        return "nats" if self.enabled and self.client_available else "durable"

    def publish(self, subject: str, payload: dict[str, Any]) -> WorkflowEvent:
        event = self.durable_bus.publish(subject, payload)
        return event

    def claim_pending(self, consumer: str, subjects: list[str], limit: int = 10) -> list[BusMessage]:
        return self.durable_bus.claim_pending(consumer, subjects, limit=limit)

    def mark_completed(self, delivery_id: int) -> BusMessage | None:
        return self.durable_bus.mark_completed(delivery_id)

    def mark_failed(
        self,
        delivery_id: int,
        error: str,
        retryable: bool = False,
        retry_after_seconds: float | None = None,
    ) -> BusMessage | None:
        return self.durable_bus.mark_failed(
            delivery_id,
            error,
            retryable=retryable,
            retry_after_seconds=retry_after_seconds,
        )

    def list_deliveries(
        self,
        consumer: str | None = None,
        subjects: list[str] | None = None,
        statuses: list[str] | None = None,
    ) -> list[BusMessage]:
        return self.durable_bus.list_deliveries(consumer=consumer, subjects=subjects, statuses=statuses)


def create_subject_bus(
    *,
    database_url: str | None = None,
    bootstrap: bool | None = None,
    engine: Engine | None = None,
    session_factory: sessionmaker[Session] | None = None,
    workflow_repository: WorkflowEventRepository | None = None,
    nats_url: str = "nats://127.0.0.1:4222",
    transport: str = "auto",
):
    durable = SubjectBus(
        database_url=database_url,
        bootstrap=bootstrap,
        engine=engine,
        session_factory=session_factory,
        workflow_repository=workflow_repository,
    )
    if transport == "durable":
        return durable
    nats_bus = NatsJetStreamBus(durable_bus=durable, nats_url=nats_url, enabled=transport in {"auto", "nats"})
    if transport == "nats" and not nats_bus.client_available:
        return nats_bus
    if transport == "auto" and not nats_bus.client_available:
        return durable
    return nats_bus
