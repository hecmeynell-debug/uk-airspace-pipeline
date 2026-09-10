"""Parsing and validation of OpenSky state vectors."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion.models import (
    ParseError,
    deduplicate,
    parse_state_vector,
    parse_states,
)


def test_parses_a_well_formed_row(sample_states, parse_now):
    vector = parse_state_vector(sample_states[0], now=parse_now)

    assert vector.icao24 == "4009f5"
    assert vector.callsign == "BAW123", "trailing API padding must be stripped"
    assert vector.origin_country == "United Kingdom"
    assert vector.latitude == pytest.approx(51.47)
    assert vector.on_ground is False
    assert vector.category == 1


def test_icao24_is_normalised_to_lowercase(sample_states, parse_now):
    sample_states[0][0] = "  4009F5  "
    assert parse_state_vector(sample_states[0], now=parse_now).icao24 == "4009f5"


def test_observed_at_prefers_time_position(sample_states, parse_now):
    vector = parse_state_vector(sample_states[0], now=parse_now)
    assert vector.observed_at == vector.time_position
    assert vector.observed_at != vector.last_contact


def test_observed_at_falls_back_to_last_contact_when_no_position(sample_states, parse_now):
    """A row without a position report must stay addressable, not be dropped."""
    vector = parse_state_vector(sample_states[2], now=parse_now)

    assert vector.time_position is None
    assert vector.observed_at == vector.last_contact
    assert vector.latitude is None


def test_empty_callsign_becomes_null(sample_states, parse_now):
    sample_states[0][1] = "        "
    assert parse_state_vector(sample_states[0], now=parse_now).callsign is None


def test_row_without_category_field_is_accepted(sample_states, parse_now):
    """`category` was added after the original schema; 17-element rows are valid."""
    short_row = sample_states[0][:17]
    vector = parse_state_vector(short_row, now=parse_now)
    assert vector.category is None
    assert vector.icao24 == "4009f5"


@pytest.mark.parametrize(
    ("mutate", "expected_reason"),
    [
        (lambda r: r.__setitem__(0, None), "icao24_missing"),
        (lambda r: r.__setitem__(0, "   "), "icao24_missing"),
        (lambda r: r.__setitem__(4, None), "last_contact_not_numeric"),
        (lambda r: r.__setitem__(6, 91.0), "latitude_out_of_range"),
        (lambda r: r.__setitem__(5, -181.0), "longitude_out_of_range"),
        (lambda r: r.__setitem__(8, "no"), "on_ground_not_boolean"),
        (lambda r: r.__setitem__(10, 361.0), "true_track_out_of_range"),
        (lambda r: r.__setitem__(9, float("nan")), "velocity_not_finite"),
        (lambda r: r.__setitem__(4, 100), "last_contact_implausible"),
    ],
)
def test_invalid_rows_are_rejected_with_a_reason(sample_states, parse_now, mutate, expected_reason):
    row = sample_states[0]
    mutate(row)

    with pytest.raises(ParseError) as exc_info:
        parse_state_vector(row, now=parse_now)

    assert exc_info.value.reason == expected_reason


def test_future_timestamps_beyond_tolerated_skew_are_rejected(sample_states, parse_now):
    far_future = int(parse_now.timestamp()) + 60 * 60 * 48
    sample_states[0][4] = far_future

    with pytest.raises(ParseError) as exc_info:
        parse_state_vector(sample_states[0], now=parse_now)

    assert exc_info.value.reason == "last_contact_implausible"


@pytest.mark.parametrize("bad_row", [None, "not-a-row", 42, []])
def test_structurally_broken_rows_are_rejected(bad_row, parse_now):
    with pytest.raises(ParseError):
        parse_state_vector(bad_row, now=parse_now)


def test_one_bad_row_does_not_fail_the_batch(sample_states, parse_now):
    """A single malformed aircraft must never stall ingestion."""
    sample_states[1][6] = 999.0  # latitude out of range

    accepted, rejected = parse_states(sample_states, now=parse_now)

    assert len(accepted) == 2
    assert len(rejected) == 1
    assert rejected[0].reason == "latitude_out_of_range"


def test_parse_states_handles_empty_and_null_payloads(parse_now):
    assert parse_states(None, now=parse_now) == ([], [])
    assert parse_states([], now=parse_now) == ([], [])


def test_deduplicate_collapses_repeats_keeping_the_first(sample_states, parse_now):
    accepted, _ = parse_states([*sample_states, sample_states[0]], now=parse_now)
    assert len(accepted) == 4

    unique, duplicates = deduplicate(accepted)

    assert len(unique) == 3
    assert duplicates == 1
    assert len({v.dedup_key for v in unique}) == 3


def test_deduplicate_treats_different_timestamps_as_distinct(sample_states, parse_now):
    """Same aircraft, later position report - a genuinely new observation."""
    later = [list(sample_states[0])]
    later[0][3] = sample_states[0][3] + 5

    accepted, _ = parse_states([sample_states[0], later[0]], now=parse_now)
    unique, duplicates = deduplicate(accepted)

    assert duplicates == 0
    assert len(unique) == 2


def test_epoch_conversion_is_utc(sample_states, parse_now):
    vector = parse_state_vector(sample_states[0], now=parse_now)
    assert vector.last_contact.tzinfo is not None
    assert vector.last_contact.utcoffset() == datetime.now(UTC).utcoffset()
