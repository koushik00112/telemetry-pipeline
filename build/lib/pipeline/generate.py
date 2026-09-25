"""Synthetic raw telemetry for one UTC day. All of it is labelled synthetic.

Scale 1 = 100 devices x 3 metrics x 1 reading/minute = 432,000 rows/day.
Scale 10 and 100 multiply the device count. Generation is vectorised and chunked by
device, so 100x (43 M rows) runs in bounded memory.

It deliberately injects the problems the quality checks must catch, and returns exact
counts of each, so tests can check that the checks find exactly what was injected:
  - dropouts: a device goes silent for 30-180 minutes
  - duplicates: an exact resend of a reading
  - invalid values: out-of-range spikes and NaNs

Output is deterministic for a given (day, scale, seed).
"""

import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import lake

DEVICES_PER_SCALE = 100
CHUNK_DEVICES = 1000
MINUTES = 24 * 60
METRICS = ("temperature_c", "humidity_pct", "vibration_rms")
NAMESPACE = uuid.UUID("6f1c1d8e-2f7e-4c55-9d0e-7a3c2b9e1f00")

DROPOUT_PROB = 0.05  # chance per device-day of one silent gap
DUPLICATE_RATE = 0.001
SPIKE_RATE = 0.0005
NAN_RATE = 0.0001


@dataclass
class Injected:
    rows_written: int = 0
    distinct_keys: int = 0
    duplicates: int = 0
    out_of_range: int = 0
    nans: int = 0
    dropout_devices: int = 0
    devices: int = 0
    by_chunk: list[int] = field(default_factory=list)


def device_ids(scale: int, seed: int) -> list[str]:
    return [str(uuid.uuid5(NAMESPACE, f"{seed}-{i}")) for i in range(DEVICES_PER_SCALE * scale)]


def _as_strings(indices: np.ndarray, values: list[str]) -> pa.Array:
    """Dictionary-encode then decode in C++: avoids building millions of Python strings."""
    return pa.DictionaryArray.from_arrays(pa.array(indices), pa.array(values)).cast(pa.string())


def _chunk(day: date, ids: list[str], rng: np.random.Generator, out: Injected) -> pa.Table:
    n_dev = len(ids)
    start = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1_000_000)
    minute = np.arange(MINUTES)
    # Each device reports at its own second within the minute.
    offset_us = rng.integers(0, 60, n_dev) * 1_000_000
    present = np.ones((n_dev, MINUTES), dtype=bool)
    drop = rng.random(n_dev) < DROPOUT_PROB
    for d in np.flatnonzero(drop):
        length = int(rng.integers(30, 181))
        begin = int(rng.integers(0, MINUTES - length))
        present[d, begin : begin + length] = False
    out.dropout_devices += int(drop.sum())

    tables = []
    hour = minute / 60.0
    for metric in METRICS:
        if metric == "temperature_c":
            base = 22 + 3 * np.sin(2 * np.pi * (hour - 9) / 24)
            values = base[None, :] + rng.normal(0, 0.2, (n_dev, MINUTES))
        elif metric == "humidity_pct":
            values = np.clip(45 + rng.normal(0, 1.0, (n_dev, MINUTES)), 0, 100)
        else:
            values = np.abs(0.5 + rng.normal(0, 0.05, (n_dev, MINUTES)))
        dev, mins = np.nonzero(present)
        v = values[dev, mins]
        ts = start + mins.astype(np.int64) * 60_000_000 + offset_us[dev]

        # Invalid values: spikes far outside the physical range, and NaNs.
        n = len(v)
        spikes = rng.random(n) < SPIKE_RATE
        v[spikes] = np.where(rng.random(int(spikes.sum())) < 0.5, 999.0, -999.0)
        nans = (rng.random(n) < NAN_RATE) & ~spikes
        v[nans] = np.nan
        out.out_of_range += int(spikes.sum())
        out.nans += int(nans.sum())
        out.distinct_keys += n

        # Exact duplicates (a device resending after a timeout).
        dup = np.flatnonzero(rng.random(n) < DUPLICATE_RATE)
        out.duplicates += len(dup)
        dev = np.concatenate([dev, dev[dup]])
        v = np.concatenate([v, v[dup]])
        ts = np.concatenate([ts, ts[dup]])

        tables.append(
            pa.table(
                {
                    "device_id": _as_strings(dev.astype(np.int32), ids),
                    "metric": _as_strings(np.zeros(len(v), dtype=np.int8), [metric]),
                    "value": pa.array(v, type=pa.float64()),
                    "ts": pa.array(ts, type=pa.timestamp("us", tz="UTC")),
                }
            )
        )
    table = pa.concat_tables(tables)
    out.rows_written += table.num_rows
    out.devices += n_dev
    return table


def write_day(lake_root: Path, day: date, scale: int = 1, seed: int = 0) -> Injected:
    """Generate one day and replace its raw partition. Returns the injected-issue counts."""
    ids = device_ids(scale, seed)
    injected = Injected()

    def write(tmp: Path) -> None:
        for c, first in enumerate(range(0, len(ids), CHUNK_DEVICES)):
            rng = np.random.default_rng([seed, day.toordinal(), c])
            table = _chunk(day, ids[first : first + CHUNK_DEVICES], rng, injected)
            pq.write_table(
                table.cast(lake.RAW_SCHEMA), tmp / f"part-{c:05d}.parquet", compression="zstd"
            )
            injected.by_chunk.append(table.num_rows)

    lake.replace_partition(
        lake_root,
        lake.RAW,
        day,
        write,
        manifest={
            "source": "synthetic",
            "scale": scale,
            "seed": seed,
            "note": "SYNTHETIC telemetry, generated",
            "injected": injected,
        },
    )
    return injected
