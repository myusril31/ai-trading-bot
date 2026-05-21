#!/usr/bin/env python3
"""Offline/manual trainer for v0.20a shadow dataset.

This script is intentionally decoupled from app/main.py runtime and is never
invoked automatically by webhook handling or bot startup.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def _eligible(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if not bool(row.get("include_ml", False)):
            continue
        if row.get("sample_type") != "FORWARD_SHADOW_PAPER":
            continue
        out.append(row)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline/manual ML shadow trainer (placeholder)")
    parser.add_argument("--dataset", default="logs/ml_dataset_rows.jsonl", help="Path to ml_dataset_rows.jsonl")
    parser.add_argument("--out", default="state/ml_models/logistic_v1.placeholder.json", help="Output model artifact path")
    args = parser.parse_args()

    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        print(json.dumps({"ok": False, "reason": "dataset_missing", "dataset": str(dataset_path)}))
        return 1

    rows = _load_jsonl(dataset_path)
    eligible = _eligible(rows)

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    artifact = {
        "ok": True,
        "type": "SHADOW_ONLY_PLACEHOLDER",
        "dataset_path": str(dataset_path),
        "rows_total": len(rows),
        "rows_eligible": len(eligible),
        "notes": "No runtime gating. Manual/offline training only.",
    }
    output_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "artifact": str(output_path), "rows_eligible": len(eligible)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
