"""Parsing and validation of OpenSky state vectors.

OpenSky returns each aircraft as a positional array, not an object, so field
order is load-bearing and is asserted here rather than assumed at the call
site. Rows that cannot be trusted are rejected with a machine-readable reason
instead of being silently coerced - the reject counts are a data-quality
signal that Phase 3 surfaces as a metric.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple

# Index of every field in the /states/all response array, per the OpenSky REST
# documentation. `sensors` (12) is only populated for sensor-filtered queries
# and is deliberately not stored.
IDX_ICAO24 = 0
IDX_CALLSIGN = 1
IDX_ORIGIN_COUNTRY = 2
IDX_TIME_POSITION = 3
IDX_LAST_CONTACT = 4
IDX_LONGITUDE = 5
IDX_LATITUDE = 6
IDX_BARO_ALTITUDE = 7
IDX_ON_GROUND = 8
IDX_VELOCITY = 9
IDX_TRUE_TRACK = 10
IDX_VERTICAL_RATE = 11
IDX_SENSORS = 12
IDX_GEO_ALTITUDE = 13
IDX_SQUAWK = 14
IDX_SPI = 15
IDX_POSITION_SOURCE = 16
IDX_CATEGORY = 17

# `category` was added after the original schema, so older or proxied
# responses can be one element short. Anything shorter than this is malformed.
MIN_FIELDS = 17

# Timestamps outside this window indicate a malformed row rather than a real
# observation. ADS-B predates neither.
_MIN_PLAUSIBLE = datetime(2000, 1, 1, tzinfo=UTC)
_MAX_SKEW = timedelta(days=1)


class RejectedRow(NamedTuple):
    """A row that failed validation, kept for counting and diagnosis."""

    reason: str
    raw: list[Any]


@dataclass(frozen=True, slots=True)
class StateVector:
    """One validated observation of one aircraft at one instant.

    `icao24` is present because it is the natural key required to deduplicate
    and to attribute provenance. It does not leave the raw schema: no mart
    exposes per-airframe identity. See CONSTRAINTS.md.
    """

    icao24: str
    observed_at: datetime
    time_position: datetime | None
    last_contact: datetime
    callsign: str | None
    origin_country: str | None
    longitude: float | None
    latitude: float | None
    baro_altitude_m: float | None
    geo_altitude_m: float | None
    on_ground: bool
    velocity_ms: float | None
    true_track_deg: float | None
    vertical_rate_ms: float | None
    squawk: str | None
    spi: bool
    position_source: int | None
    category: int | None

    @property
    def dedup_key(self) -> tuple[str, datetime]:
        return (self.icao24, self.observed_at)


class ParseError(ValueError):
    """Raised for a single unusable row; carries a stable reason code."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _epoch_to_utc(value: Any, field: str, *, now: datetime) -> datetime:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ParseError(f"{field}_not_numeric")
    try:
        parsed = datetime.fromtimestamp(float(value), tz=UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise ParseError(f"{field}_out_of_range") from exc
    if parsed < _MIN_PLAUSIBLE or parsed > now + _MAX_SKEW:
        raise ParseError(f"{field}_implausible")
    return parsed


def _optional_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ParseError(f"{field}_not_numeric")
    result = float(value)
    # NaN and infinities round-trip badly through Postgres double precision
    # and carry no meaning here.
    if result != result or result in (float("inf"), float("-inf")):
        raise ParseError(f"{field}_not_finite")
    return result


def _optional_text(value: Any) -> str | None:
    """OpenSky pads callsigns to a fixed width with trailing spaces."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ParseError("text_field_not_string")
    stripped = value.strip()
    return stripped or None


def _optional_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ParseError(f"{field}_not_numeric")
    return int(value)


def parse_state_vector(row: Any, *, now: datetime | None = None) -> StateVector:
    """Validate one positional state-vector array.

    Raises ParseError with a stable reason code if the row is unusable.
    """
    now = now or datetime.now(tz=UTC)

    if not isinstance(row, (list, tuple)):
        raise ParseError("row_not_array")
    if len(row) < MIN_FIELDS:
        raise ParseError("row_too_short")

    icao24 = row[IDX_ICAO24]
    if not isinstance(icao24, str) or not icao24.strip():
        raise ParseError("icao24_missing")
    icao24 = icao24.strip().lower()

    # last_contact is documented as non-nullable and is the fallback that makes
    # every row addressable, so a missing value is fatal.
    last_contact = _epoch_to_utc(row[IDX_LAST_CONTACT], "last_contact", now=now)

    raw_time_position = row[IDX_TIME_POSITION]
    time_position = (
        None
        if raw_time_position is None
        else _epoch_to_utc(raw_time_position, "time_position", now=now)
    )

    on_ground = row[IDX_ON_GROUND]
    if not isinstance(on_ground, bool):
        raise ParseError("on_ground_not_boolean")

    longitude = _optional_float(row[IDX_LONGITUDE], "longitude")
    latitude = _optional_float(row[IDX_LATITUDE], "latitude")
    if longitude is not None and not -180.0 <= longitude <= 180.0:
        raise ParseError("longitude_out_of_range")
    if latitude is not None and not -90.0 <= latitude <= 90.0:
        raise ParseError("latitude_out_of_range")

    true_track = _optional_float(row[IDX_TRUE_TRACK], "true_track")
    if true_track is not None and not 0.0 <= true_track < 360.0:
        raise ParseError("true_track_out_of_range")

    spi = row[IDX_SPI]
    if not isinstance(spi, bool):
        spi = bool(spi)

    return StateVector(
        icao24=icao24,
        # A position report may be older than the last radio contact. Preferring
        # time_position makes the key describe when the aircraft was actually
        # observed at that point, and falling back to last_contact keeps rows
        # without a position addressable rather than silently dropping them.
        observed_at=time_position or last_contact,
        time_position=time_position,
        last_contact=last_contact,
        callsign=_optional_text(row[IDX_CALLSIGN]),
        origin_country=_optional_text(row[IDX_ORIGIN_COUNTRY]),
        longitude=longitude,
        latitude=latitude,
        baro_altitude_m=_optional_float(row[IDX_BARO_ALTITUDE], "baro_altitude"),
        geo_altitude_m=_optional_float(row[IDX_GEO_ALTITUDE], "geo_altitude"),
        on_ground=on_ground,
        velocity_ms=_optional_float(row[IDX_VELOCITY], "velocity"),
        true_track_deg=true_track,
        vertical_rate_ms=_optional_float(row[IDX_VERTICAL_RATE], "vertical_rate"),
        squawk=_optional_text(row[IDX_SQUAWK]),
        spi=spi,
        position_source=_optional_int(row[IDX_POSITION_SOURCE], "position_source"),
        category=(
            _optional_int(row[IDX_CATEGORY], "category") if len(row) > IDX_CATEGORY else None
        ),
    )


def parse_states(
    rows: list[Any] | None, *, now: datetime | None = None
) -> tuple[list[StateVector], list[RejectedRow]]:
    """Parse a whole response body.

    One bad row never fails the batch: it is rejected and counted, so a single
    malformed aircraft cannot stall ingestion.
    """
    if not rows:
        return [], []

    accepted: list[StateVector] = []
    rejected: list[RejectedRow] = []
    for row in rows:
        try:
            accepted.append(parse_state_vector(row, now=now))
        except ParseError as exc:
            raw = list(row) if isinstance(row, (list, tuple)) else [row]
            rejected.append(RejectedRow(reason=exc.reason, raw=raw))
    return accepted, rejected


def deduplicate(vectors: list[StateVector]) -> tuple[list[StateVector], int]:
    """Collapse repeats of the same (icao24, observed_at) within one batch.

    A single snapshot should not contain the same aircraft twice, but the load
    must be deterministic regardless. First occurrence wins.
    """
    seen: set[tuple[str, datetime]] = set()
    unique: list[StateVector] = []
    duplicates = 0
    for vector in vectors:
        if vector.dedup_key in seen:
            duplicates += 1
            continue
        seen.add(vector.dedup_key)
        unique.append(vector)
    return unique, duplicates
