#!/usr/bin/env python3
"""
Analyze a .json or .jsonl file, list every unique key (dot-path style),
and for each key produce the unique values seen and their occurrence counts.

Features:
- Auto-detect .json vs .jsonl (falls back to JSONL if single-load fails).
- CLI options: --max-values, --truncate, --json-output.
- Interactive key selection: after scanning keys, show list and let user pick which keys to analyze.
  - Enter comma-separated key indices or ranges (e.g. 1,3-5,8).
  - Enter "all" to select every key.
- Non-interactive mode (--non-interactive) to analyze all keys automatically.

Behavior notes:
- Arrays are always aggregated under their parent key (no numeric index keys).
- Keys that only ever hold container values (only dict/list JSON signatures and no primitive
  values like strings/numbers/bools/nulls) are omitted from the reported key list and summaries
  (their nested subkeys are still recorded and shown).
- "Count" is the number of times a specific value was observed at that key.
- "Percent" = (Count for this value) / (Total value observations for that key) * 100.
  Percentages are relative to the key's value observations, not the number of documents or
  the whole file.
"""

import sys
import json
import os
import argparse
from collections import defaultdict, Counter
from typing import Any, Dict, Iterable, Tuple, List

MAX_VALUE_DISPLAY = 200
DEFAULT_MAX_UNIQUE_TO_SHOW = 200
PROGRESS_EVERY = 10000
KEY_LABEL_WIDTH = 30  # width used when printing "Key:" header


def iter_json_objects(path: str) -> Iterable[Any]:
    _, ext = os.path.splitext(path.lower())
    if ext in (".jsonl", ".ndjson"):
        with open(path, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    sys.stderr.write(f"Warning: skipping invalid JSON on line {line_no}\n")
    else:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        try:
            data = json.loads(text)
            if isinstance(data, list):
                for item in data:
                    yield item
            else:
                yield data
        except json.JSONDecodeError:
            with open(path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        sys.stderr.write(f"Warning: skipping invalid JSON on line {line_no}\n")


def normalize_key_path(parent: str, key: str) -> str:
    return f"{parent}.{key}" if parent else key


def value_signature(value: Any) -> Tuple[str, Any]:
    if value is None:
        return ("null", None)
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, float)):
        return ("number", value)
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, dict) or isinstance(value, list):
        try:
            rep = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            return ("json", rep)
        except (TypeError, ValueError):
            return ("repr", repr(value))
    return ("repr", repr(value))


def display_value(sig: Tuple[str, Any], truncate: int) -> str:
    typ, rep = sig
    if typ == "null":
        s = "null"
    elif typ == "bool":
        s = "true" if rep else "false"
    elif typ == "number":
        s = str(rep)
    elif typ == "string":
        s = rep
    elif typ == "json":
        s = rep
    else:
        s = str(rep)
    if truncate and len(s) > truncate:
        return s[:truncate] + "...(truncated)"
    return s


def walk(obj: Any, counters: Dict[str, Counter], parent: str = "") -> None:
    """
    Recursively walk the JSON object. List elements are always aggregated under the parent key
    (without using index paths). Non-list/non-dict primitive values are recorded on their dotted path.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = normalize_key_path(parent, k)
            if isinstance(v, list):
                for item in v:
                    sig_item = value_signature(item)
                    counters[path][sig_item] += 1
                    if isinstance(item, (dict, list)):
                        walk(item, counters, path)
            else:
                sig = value_signature(v)
                counters[path][sig] += 1
                if isinstance(v, dict):
                    walk(v, counters, path)
    elif isinstance(obj, list):
        if parent:
            for item in obj:
                sig = value_signature(item)
                counters[parent][sig] += 1
                if isinstance(item, (dict, list)):
                    walk(item, counters, parent)
        else:
            for idx, item in enumerate(obj):
                path = f"[{idx}]"
                sig = value_signature(item)
                counters[path][sig] += 1
                if isinstance(item, (dict, list)):
                    walk(item, path)
    else:
        path = parent or "<root>"
        sig = value_signature(obj)
        counters[path][sig] += 1


def is_primitive_signature(sig: Tuple[str, Any]) -> bool:
    return sig[0] in ("string", "number", "bool", "null", "repr")


def filter_container_only_keys(counters: Dict[str, Counter]) -> List[str]:
    result = []
    for key, counter in counters.items():
        has_primitive = any(is_primitive_signature(sig) for sig in counter.keys())
        if has_primitive:
            result.append(key)
    return sorted(result)


def summarize_file_keys(path: str) -> List[str]:
    counters: Dict[str, Counter] = defaultdict(Counter)
    for i, obj in enumerate(iter_json_objects(path), 1):
        if i % PROGRESS_EVERY == 0:
            print(f"Processed {i} objects...", file=sys.stderr)
        walk(obj, counters, "")
    return filter_container_only_keys(counters)


def summarize_file_full(path: str) -> Dict[str, Counter]:
    counters: Dict[str, Counter] = defaultdict(Counter)
    for i, obj in enumerate(iter_json_objects(path), 1):
        if i % PROGRESS_EVERY == 0:
            print(f"Processed {i} objects...", file=sys.stderr)
        walk(obj, counters, "")
    return counters


def print_intro_explainers() -> None:
    lines = [
        "Summary metrics explanation:",
        "  - Count : number of times the given value was observed for that key (i.e., occurrences of that specific value at that key).",
        "  - Percent: (Count for this value / total value observations for that key) * 100",
        "             (percentages are relative to the key's value observations, not the number of documents or the whole file).",
        "  - Arrays  : elements are aggregated under their parent key; array elements contribute to the parent's value counts.",
        "  - Container-only parent keys (objects/arrays only) are omitted from the key list."
    ]
    for ln in lines:
        print(ln)
    print()


def print_keys_menu(keys: List[str]) -> None:
    print_intro_explainers()
    print("Keys identified:")
    idx_width = len(str(len(keys)))
    for idx, key in enumerate(keys, 1):
        print(f"  {idx:>{idx_width}}. {key}")
    print("\nEnter comma-separated indices/ranges (e.g. 1,3-5,8), or 'all' to select every key:")


def parse_selection(selection: str, n_keys: int) -> List[int]:
    sel = selection.strip().lower()
    if sel == "all":
        return list(range(1, n_keys + 1))
    parts = [p.strip() for p in sel.split(",") if p.strip()]
    indices = set()
    for p in parts:
        if "-" in p:
            try:
                a, b = p.split("-", 1)
                ia = int(a)
                ib = int(b)
                if ia <= ib:
                    for i in range(max(1, ia), min(n_keys, ib) + 1):
                        indices.add(i)
            except ValueError:
                continue
        else:
            try:
                i = int(p)
                if 1 <= i <= n_keys:
                    indices.add(i)
            except ValueError:
                continue
    return sorted(indices)


def print_summary_for_keys(
    counters: Dict[str, Counter], keys: List[str], max_unique: int, truncate: int
) -> None:
    # column widths for aligned table
    count_w = 8
    pct_w = 8
    header = f"{'Count':<{count_w}}  {'Percent':<{pct_w}}   Value"
    for key in keys:
        counter = counters.get(key, Counter())
        total = sum(counter.values())
        # nicely aligned key header
        key_label = f"Key: {key}"
        occ_label = f"total value observations: {total}"
        print()
        print(f"{key_label}")
        print(f"{occ_label}")
        print(f"  Note: 'Percent' = (Count for this value / total value observations for this key) * 100")
        items = sorted(counter.items(), key=lambda x: (-x[1], display_value(x[0], truncate)))
        unique_count = len(items)
        to_show = items[:max_unique]
        if unique_count > max_unique:
            print(f"  Showing top {max_unique} of {unique_count} unique values (by count):")
        else:
            print(f"  Showing {unique_count} unique value(s):")
        print(f"  {header}")
        for sig, cnt in to_show:
            pct = (cnt / total) * 100 if total else 0.0
            valstr = display_value(sig, truncate)
            print(f"  {cnt:<{count_w}d}  {pct:6.2f}%   {valstr}")
    if not keys:
        print("No keys selected or found.")


def summary_to_json(
    counters: Dict[str, Counter], keys: List[str], max_unique: int, truncate: int
) -> Dict[str, Any]:
    out = {
        "_explainers": {
            "count": "number of times the specific value was observed for the key (occurrences of that value at the key)",
            "percent": "count / total_value_observations_for_key * 100 (percent of value observations for that key)",
            "note": "arrays are aggregated under parent keys; container-only parent keys are omitted"
        }
    }
    for key in keys:
        counter = counters.get(key, Counter())
        total = sum(counter.values())
        items = sorted(counter.items(), key=lambda x: (-x[1], display_value(x[0], truncate)))
        to_show = items[:max_unique]
        out[key] = {
            "total_occurrences": total,
            "unique_count": len(items),
            "unique_values": [
                {"value": display_value(sig, truncate), "count": cnt, "percent": (cnt / total) * 100 if total else 0.0}
                for sig, cnt in to_show
            ],
        }
    return out


def parse_args(argv):
    p = argparse.ArgumentParser(description="Summarize unique JSON keys and values with counts.")
    p.add_argument("input", help="Input .json or .jsonl file")
    p.add_argument("--max-values", type=int, default=DEFAULT_MAX_UNIQUE_TO_SHOW,
                   help=f"Max unique values to display per key (default {DEFAULT_MAX_UNIQUE_TO_SHOW})")
    p.add_argument("--truncate", type=int, default=MAX_VALUE_DISPLAY,
                   help=f"Truncate displayed values to this length (default {MAX_VALUE_DISPLAY})")
    p.add_argument("--aggregate-arrays", action="store_true",
                   help="(no-op) arrays are always aggregated in this version")
    p.add_argument("--json-output", type=str, default=None,
                   help="Write the summary as JSON to this file instead of printing plain text")
    p.add_argument("--non-interactive", action="store_true",
                   help="Do not prompt; analyze all keys automatically (useful for scripts)")
    return p.parse_args(argv[1:])


def main(argv):
    args = parse_args(argv)
    path = args.input
    if not os.path.exists(path):
        print(f"Error: file not found: {path}", file=sys.stderr)
        return 2

    # First pass: discover keys only (container-only keys are filtered out)
    keys = summarize_file_keys(path)
    if not keys:
        print("No keys found (empty or invalid JSON).", file=sys.stderr)
        return 1

    if args.non_interactive:
        selected_keys = keys
    else:
        print_keys_menu(keys)
        try:
            selection = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nSelection cancelled.", file=sys.stderr)
            return 1
        indices = parse_selection(selection, len(keys))
        if not indices:
            print("No valid selection made. Exiting.", file=sys.stderr)
            return 1
        selected_keys = [keys[i - 1] for i in indices]

    # Second pass: build full counters and present only selected keys
    counters = summarize_file_full(path)
    if args.json_output:
        out = summary_to_json(counters, selected_keys, args.max_values, args.truncate)
        with open(args.json_output, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"Wrote JSON summary to {args.json_output}")
    else:
        print_summary_for_keys(counters, selected_keys, args.max_values, args.truncate)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
