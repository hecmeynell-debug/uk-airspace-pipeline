-- The aggregate-only constraint, enforced rather than asserted.
--
-- CONSTRAINTS.md says no mart exposes per-airframe identity. A document cannot
-- stop someone adding `icao24` to a group by six months from now; this test
-- can. It inspects the actual built schema, so it catches a violation whatever
-- route it arrives by - a new model, an edited one, or a column renamed into
-- existence.
--
-- origin_country is included in the forbidden list deliberately. Aggregating
-- traffic by country of registration would be defensible statistics in another
-- project; in a defence-adjacent one it invites exactly the reading this
-- project exists to avoid. Excluding it costs nothing.
--
-- A dbt test fails when it returns rows, so any row here is a violation.

select
    table_schema,
    table_name,
    column_name,
    'forbidden identity column present in a mart' as failure_reason

from information_schema.columns

where table_schema = '{{ target.schema }}_marts'
  and lower(column_name) in (
      'icao24',
      'callsign',
      'squawk',
      'registration',
      'tail_number',
      'serial_number',
      'origin_country',
      'spi',
      'sensors',
      'latitude',
      'longitude'
  )
