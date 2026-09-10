"""Configuration validation and the API credit budget arithmetic."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ingestion.config import Settings, credit_cost_for_area


@pytest.mark.parametrize(
    ("area", "expected"),
    [
        (1.0, 1),
        (25.0, 1),
        (25.1, 2),
        (100.0, 2),
        (100.1, 3),
        (156.0, 3),  # the configured UK FIR + Atlantic box
        (400.0, 3),
        (400.1, 4),
        (64_800.0, 4),  # global
    ],
)
def test_credit_cost_bands_match_the_documented_tariff(area, expected):
    assert credit_cost_for_area(area) == expected


def test_default_bounding_box_costs_three_credits():
    settings = Settings()
    assert settings.bbox_area_sq_deg == pytest.approx(156.0)
    assert settings.credit_cost == 3


def test_default_cadence_stays_within_the_registered_allowance():
    """120s polling must leave headroom against the documented 4000/day."""
    settings = Settings(poll_interval_seconds=120)
    assert settings.daily_credit_budget == 2160
    assert settings.daily_credit_budget < 4000


def test_sixty_second_cadence_would_exceed_the_allowance():
    """Guards the reasoning in ADR-0001; if this passes, the ADR is wrong."""
    assert Settings(poll_interval_seconds=60).daily_credit_budget > 4000


@pytest.mark.parametrize(
    "overrides",
    [
        {"opensky_lat_min": 61.0, "opensky_lat_max": 49.0},
        {"opensky_lon_min": 3.0, "opensky_lon_max": -10.0},
        {"opensky_lat_min": 50.0, "opensky_lat_max": 50.0},
    ],
)
def test_inverted_or_empty_bounding_boxes_are_rejected(overrides):
    with pytest.raises(ValidationError):
        Settings(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"opensky_lat_max": 91.0},
        {"opensky_lat_min": -91.0},
        {"opensky_lon_max": 181.0},
        {"opensky_lon_min": -181.0},
    ],
)
def test_out_of_range_coordinates_are_rejected(overrides):
    with pytest.raises(ValidationError):
        Settings(**overrides)


def test_auth_mode_requires_both_halves_of_the_credential():
    assert Settings(opensky_client_id="", opensky_client_secret="").auth_mode == "anonymous"
    assert Settings(opensky_client_id="id", opensky_client_secret="").auth_mode == "anonymous"
    assert Settings(opensky_client_id="", opensky_client_secret="s").auth_mode == "anonymous"
    assert (
        Settings(opensky_client_id="id", opensky_client_secret="s").auth_mode
        == "oauth2_client_credentials"
    )


def test_the_two_database_identities_are_distinct():
    """Migrations and loading must never share a connection identity."""
    settings = Settings(airspace_owner_password="owner-pw", airspace_ingest_password="ingest-pw")

    assert "user=airspace_owner" in settings.owner_dsn
    assert "user=airspace_ingest" in settings.ingest_dsn
    assert settings.owner_dsn != settings.ingest_dsn


def test_poll_interval_has_a_floor():
    with pytest.raises(ValidationError):
        Settings(poll_interval_seconds=1)
