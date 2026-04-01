from pydantic import BaseModel, Field


class Settings(BaseModel):
    service_name: str = "greenference-validator"
    score_alpha: float = Field(default=1.0, ge=0.0)
    score_beta: float = Field(default=1.3, ge=0.0)
    score_gamma: float = Field(default=1.1, ge=0.0)
    score_delta: float = Field(default=0.8, ge=0.0)  # utilization exponent
    rental_revenue_bonus_cap: float = Field(default=0.1, ge=0.0)

    # Miner whitelist — when enabled, only whitelisted hotkeys receive weight
    whitelist_enabled: bool = True

    # Flux orchestrator
    flux_inference_floor_pct: float = Field(default=0.20, ge=0.0, le=1.0)
    flux_rental_floor_pct: float = Field(default=0.10, ge=0.0, le=1.0)
    flux_rebalance_interval_seconds: float = Field(default=30.0, ge=1.0)

    # Bittensor chain integration (gated by flag)
    bittensor_enabled: bool = False
    bittensor_network: str = "test"  # test | finney | local | ws://...
    bittensor_netuid: int = 16
    bittensor_wallet_path: str | None = None
    metagraph_sync_interval_seconds: float = Field(default=60.0, ge=5.0)


settings = Settings()

