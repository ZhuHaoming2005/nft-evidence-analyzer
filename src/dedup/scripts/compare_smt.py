#!/usr/bin/env python3
"""Compare one worker per physical core with all SMT siblings on Linux."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path


def linux_topology() -> tuple[list[int], list[int]]:
    if platform.system() != "Linux":
        raise RuntimeError("this benchmark script supports Linux only")
    allowed = sorted(os.sched_getaffinity(0))
    cores: dict[tuple[str, str], list[int]] = {}
    for cpu in allowed:
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        package = (topology / "physical_package_id").read_text().strip()
        core = (topology / "core_id").read_text().strip()
        cores.setdefault((package, core), []).append(cpu)
    if not cores:
        raise RuntimeError("no online CPUs are available in the current cpuset")
    physical = sorted(min(siblings) for siblings in cores.values())
    return physical, allowed


def direct_bm25_seconds(manifest: dict) -> float | None:
    for timing in manifest.get("phase_timings", []):
        if timing.get("stage") == "metadata" and timing.get("phase") == "direct_bm25":
            return float(timing["elapsed_secs"])
    return None


def median(values: list[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return statistics.median(present) if present else None


def choose_mode(physical: float | None, smt: float | None, margin: float) -> str:
    if physical is None or smt is None:
        return "inconclusive"
    if physical < smt * (1.0 - margin):
        return "physical"
    if smt < physical * (1.0 - margin):
        return "smt"
    return "inconclusive"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run identical dedup jobs with one logical CPU per physical core and with "
            "all SMT siblings. Arguments after -- are passed to dedup."
        )
    )
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument(
        "--decision-margin",
        type=float,
        default=0.02,
        help="Minimum median improvement required for a recommendation (default: 0.02)",
    )
    parser.add_argument("dedup_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    if not 0.0 <= args.decision_margin < 1.0:
        parser.error("--decision-margin must be in [0, 1)")
    dedup_args = args.dedup_args[1:] if args.dedup_args[:1] == ["--"] else args.dedup_args
    if not dedup_args:
        parser.error("dedup arguments are required after --")
    if "--threads" in dedup_args or "--output-dir" in dedup_args:
        parser.error("do not pass --threads or --output-dir; the script injects both")
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error(f"binary does not exist: {binary}")
    if shutil.which("taskset") is None:
        parser.error("taskset is required")

    try:
        physical_cpus, logical_cpus = linux_topology()
    except (OSError, RuntimeError) as error:
        parser.error(str(error))
    if len(physical_cpus) == len(logical_cpus):
        parser.error(
            "SMT is disabled or hidden: every visible physical core has one logical CPU"
        )

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    result_path = output_root / "smt_comparison.json"
    if result_path.exists():
        parser.error(f"refusing to overwrite existing result: {result_path}")

    modes = {
        "physical": physical_cpus,
        "smt": logical_cpus,
    }
    runs: list[dict] = []
    for repetition in range(1, args.repetitions + 1):
        order = ["physical", "smt"] if repetition % 2 == 1 else ["smt", "physical"]
        for mode in order:
            cpus = modes[mode]
            output_dir = output_root / f"{mode}-{repetition}"
            if output_dir.exists():
                parser.error(f"refusing to reuse output directory: {output_dir}")
            command = [
                "taskset",
                "--cpu-list",
                ",".join(map(str, cpus)),
                str(binary),
                *dedup_args,
                "--output-dir",
                str(output_dir),
                "--threads",
                str(len(cpus)),
            ]
            print(f"\n[{mode} run {repetition}] {' '.join(command)}", flush=True)
            started = time.perf_counter()
            completed = subprocess.run(command, check=False)
            wall_elapsed = time.perf_counter() - started
            if completed.returncode != 0:
                print(
                    f"dedup exited with code {completed.returncode}; partial outputs remain in "
                    f"{output_dir}",
                    file=sys.stderr,
                )
                return completed.returncode

            manifest_path = output_dir / "run_manifest.json"
            if not manifest_path.is_file():
                raise RuntimeError(f"missing manifest: {manifest_path}")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            reported_threads = int(manifest.get("threads", 0))
            if reported_threads != len(cpus):
                raise RuntimeError(
                    f"manifest reported {reported_threads} threads; expected {len(cpus)}"
                )
            runs.append(
                {
                    "mode": mode,
                    "repetition": repetition,
                    "threads": len(cpus),
                    "cpu_affinity": cpus,
                    "wall_elapsed_secs": wall_elapsed,
                    "manifest_elapsed_secs": float(manifest["elapsed_secs"]),
                    "direct_bm25_elapsed_secs": direct_bm25_seconds(manifest),
                    "output_dir": str(output_dir),
                }
            )

    summaries: dict[str, dict] = {}
    for mode, cpus in modes.items():
        selected = [run for run in runs if run["mode"] == mode]
        summaries[mode] = {
            "threads": len(cpus),
            "median_wall_elapsed_secs": median(
                [run["wall_elapsed_secs"] for run in selected]
            ),
            "median_manifest_elapsed_secs": median(
                [run["manifest_elapsed_secs"] for run in selected]
            ),
            "median_direct_bm25_elapsed_secs": median(
                [run["direct_bm25_elapsed_secs"] for run in selected]
            ),
        }

    recommendation = choose_mode(
        summaries["physical"]["median_direct_bm25_elapsed_secs"],
        summaries["smt"]["median_direct_bm25_elapsed_secs"],
        args.decision_margin,
    )
    recommendation_basis = "direct_bm25"
    if recommendation == "inconclusive":
        recommendation = choose_mode(
            summaries["physical"]["median_manifest_elapsed_secs"],
            summaries["smt"]["median_manifest_elapsed_secs"],
            args.decision_margin,
        )
        recommendation_basis = "end_to_end"

    result = {
        "schema_version": 1,
        "machine": {
            "platform": platform.platform(),
            "physical_core_count": len(physical_cpus),
            "logical_cpu_count": len(logical_cpus),
            "physical_cpu_affinity": physical_cpus,
            "smt_cpu_affinity": logical_cpus,
        },
        "repetitions": args.repetitions,
        "decision_margin": args.decision_margin,
        "runs": runs,
        "summaries": summaries,
        "recommendation": recommendation,
        "recommendation_basis": recommendation_basis,
    }
    result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"\nrecommendation: {recommendation} ({recommendation_basis})")
    print(f"result: {result_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
