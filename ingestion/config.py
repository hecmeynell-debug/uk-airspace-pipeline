"""Configuration, loaded from the environment or a local .env file.

Two database identities are exposed deliberately. Migrations run as
``airspace_owner`` because they issue DDL; the ingestion job itself connects as
``airspace_ingest``, which has no DDL rights at all. Keeping them separate here
means the loader physically cannot acquire schema-modifying privileges by
accident.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Role names are created by db/init/01_roles_and_schemas.sh and are not
# user-configurable; the passwords are.
OWNER_ROLE = "airspace_owner"
INGEST_ROLE = "airspace_ingest"


def credit_cost_for_area(area_sq_deg: float) -> int:
    """OpenSky charges /states/all by bounding-box area.

    Documented bands: <=25 sq deg costs 1 credit, 25-100 costs 2, 100-400
    costs 3, and anything larger (or a global query) costs 4.
    """
    if area_sq_deg <= 25:
        return 1
    if area_sq_deg <= 100:
        return 2
    if area_sq_deg <= 400:
        return 3
    return 4


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- database ---------------------------------------------------------
    # 127.0.0.1 rather than "localhost" on purpose. On Windows, localhost
    # resolves to ::1 first, but docker-compose publishes the port on the IPv4
    # loopback only, so every connection would stall for the full TCP timeout
    # before falling back to IPv4.
    postgres_host: str = "127.0.0.1"
    postgres_port: int = 5432
    postgres_db: str = "airspace"
    airspace_owner_password: str = ""
    airspace_ingest_password: str = ""
    # Fail fast rather than hanging when the database is unreachable.
    postgres_connect_timeout: int = 10

    # --- OpenSky ----------------------------------------------------------
    # Blank credentials mean anonymous access: 400 credits/day, 10s
    # resolution, live state vectors only. Usable for a smoke test, not for
    # sustained ingestion.
    opensky_client_id: str = ""
    opensky_client_secret: str = ""
    opensky_token_url: str = (
        "https://auth.opensky-network.org/auth/realms/opensky-network"
        "/protocol/openid-connect/token"
    )
    opensky_api_base: str = "https://opensky-network.org/api"
    opensky_timeout_seconds: float = 30.0
    opensky_max_attempts: int = 4

    # --- region of interest ----------------------------------------------
    opensky_lat_min: float = Field(default=49.0, ge=-90.0, le=90.0)
    opensky_lat_max: float = Field(default=61.0, ge=-90.0, le=90.0)
    opensky_lon_min: float = Field(default=-10.0, ge=-180.0, le=180.0)
    opensky_lon_max: float = Field(default=3.0, ge=-180.0, le=180.0)

    poll_interval_seconds: int = Field(default=120, ge=10)
    log_level: str = "INFO"

    @model_validator(mode="after")
    def _check_bbox(self) -> Settings:
        if self.opensky_lat_min >= self.opensky_lat_max:
            raise ValueError("opensky_lat_min must be less than opensky_lat_max")
        if self.opensky_lon_min >= self.opensky_lon_max:
            raise ValueError("opensky_lon_min must be less than opensky_lon_max")
        return self

    # --- derived ----------------------------------------------------------
    @property
    def auth_mode(self) -> Literal["anonymous", "oauth2_client_credentials"]:
        if self.opensky_client_id and self.opensky_client_secret:
            return "oauth2_client_credentials"
        return "anonymous"

    @property
    def bbox_area_sq_deg(self) -> float:
        return (self.opensky_lat_max - self.opensky_lat_min) * (
            self.opensky_lon_max - self.opensky_lon_min
        )

    @property
    def credit_cost(self) -> int:
        return credit_cost_for_area(self.bbox_area_sq_deg)

    @property
    def daily_credit_budget(self) -> int:
        """Credits this configuration would consume over 24h of polling."""
        polls_per_day = 86_400 // self.poll_interval_seconds
        return polls_per_day * self.credit_cost

    @property
    def bbox_params(self) -> dict[str, float]:
        return {
            "lamin": self.opensky_lat_min,
            "lamax": self.opensky_lat_max,
            "lomin": self.opensky_lon_min,
            "lomax": self.opensky_lon_max,
        }

    def _dsn(self, user: str, password: str) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={user} password={password} "
            f"connect_timeout={self.postgres_connect_timeout}"
        )

    @property
    def owner_dsn(self) -> str:
        """Connection used for migrations only. Holds DDL rights."""
        return self._dsn(OWNER_ROLE, self.airspace_owner_password)

    @property
    def ingest_dsn(self) -> str:
        """Connection used by the loader. SELECT/INSERT on raw, nothing more."""
        return self._dsn(INGEST_ROLE, self.airspace_ingest_password)
