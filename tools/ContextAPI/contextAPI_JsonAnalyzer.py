#!/usr/bin/env python3
"""
Analyze a .json or .jsonl file, list every unique key (dot-path style),
and for each key produce the unique values seen and their occurrence counts.

This is a variant of contextAPI_JsonAnalyzer.py that adds a fourth export
format: a self-contained "Spur Intelligence IP Context Classification" HTML report
(--html-output). Keys
are grouped into the categories used by the Context API response schema
(geography, device & network, anonymization/proxy signals, risk
classification); anything outside that known schema falls into an "Other
Keys" group so the report still degrades gracefully on unfamiliar input.
The plain-text/JSON/Markdown behavior is unchanged from the original script.

Features:
- Auto-detect .json vs .jsonl (falls back to JSONL if single-load fails).
- CLI options: --max-values, --truncate, --json-output, --md-output, --html-output.
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
- "Percent" = (Count for this value) / (Total number of input objects (IPs analyzed)) * 100.
  Percentages are relative to the total number of input objects processed, not the key's value observations.
- In the HTML report specifically, bar *length* is scaled relative to each key's own top value
  (so a key that's individually rare doesn't render as a wall of invisible slivers); the "Percent"
  number printed next to each bar remains the true value above.
- The HTML report's Risk Classification section is pinned near the top (under the headline
  stat tiles, before the other key groups) and combines the raw "risks" key with two curated
  cross-key detectors, both defined as editable constants near the top of this file:
    - Malware Proxies: sums occurrences of a fixed list of known malware proxy operator names
      (MALWARE_PROXY_VALUES) wherever they appear in client.proxies or tunnels.operator. Percent
      is of total objects analyzed, same basis as every other bar in the report.
    - Sanctioned Countries: sums occurrences of OFAC-sanctioned country codes (SANCTIONED_COUNTRIES)
      wherever they appear in location.country or client.concentration.country. Percent is each
      country's share of the combined total value observations across both keys (not total objects,
      and not unique-country cardinality — that denominator can exceed the numerator and produce
      percentages over 100%, so it's deliberately avoided).
"""

import sys
import json
import os
import re
import math
import argparse
import platform
import shutil
import subprocess
import tempfile
from collections import defaultdict, Counter
from typing import Any, Dict, Iterable, Optional, Tuple, List

MAX_VALUE_DISPLAY = 200
DEFAULT_MAX_UNIQUE_TO_SHOW = 200
DEFAULT_HTML_TOP_N = 6
PROGRESS_EVERY = 10000
KEY_LABEL_WIDTH = 30  # width used when printing "Key:" header

# Keys dropped entirely, for every output format — never discovered, selected, or shown.
# "ip" is a duplicate of "IP" (same dataset, sometimes represented twice in the source file).
EXCLUDED_KEYS = {"ip"}

# Known Context API key groups, in display order. Any selected key not listed
# here falls into a trailing "Other Keys" group so unfamiliar schemas still render.
# NOTE: "risks" is deliberately absent from this list — the Risk Classification
# section is rendered separately near the top of the report by render_risk_section().
#
# Most groups use a fixed "keys" list. "Client Devices" instead uses "prefix" (with
# "exclude_prefix") to catch every client.* key EXCEPT client.concentration.* and
# client.proxies — including any not explicitly known here — so an unfamiliar client.*
# field still lands in the right place instead of falling through to "Other Keys".
# Order matters: Anonymization & Proxy Signals and Geography must stay ahead of Client
# Devices in this list so client.proxies and client.concentration.* are claimed by their
# fixed lists first, before the client.* wildcard ever sees them.
#
# A group may optionally carry "subsections": a list of {"title", "keys"} dicts that
# further split its cards into labeled clusters. A subsection with no "keys" is the
# catch-all for whatever the group matched that no earlier subsection claimed — used by
# Client Devices so an unfamiliar client.* field still lands somewhere labeled.
KEY_GROUPS = [
    {
        "title": "Anonymization & Proxy Signals",
        "description": "VPN/Proxy Server Tunnels & client callback proxies",
        "keys": [
            "tunnels.operator", "tunnels.anonymous", "tunnels.entries", "tunnels.exits", "tunnels.type",
            "services", "client.proxies",
        ],
        "subsections": [
            {"title": "VPN & Proxy Services", "keys": ["tunnels.operator", "client.proxies"]},
            {"title": "Tunnels", "keys": ["tunnels.anonymous", "tunnels.entries", "tunnels.exits", "tunnels.type"]},
            {"title": "Services", "keys": ["services"]},
        ],
    },
    {
        "title": "AI Crawlers & Agents",
        "description": "Automated crawlers and AI agents detected",
        "keys": ["ai.operator", "ai.types"],
    },
    {
        "title": "Geography",
        "description": "Where traffic concentrates",
        "keys": [
            "location.country", "location.city", "location.state",
            "client.concentration.country", "client.concentration.city", "client.concentration.state",
            "client.concentration.geohash", "client.countries", "client.spread",
        ],
        "subsections": [
            {"title": "IP Geolocation", "keys": ["location.country", "location.city", "location.state"]},
            {"title": "Client Concentration", "keys": [
                "client.concentration.country", "client.concentration.city",
                "client.concentration.state", "client.concentration.geohash",
            ]},
            {"title": "Distribution", "keys": ["client.countries", "client.spread"]},
        ],
    },
    {
        "title": "Client Devices",
        "description": "What's connecting, and from where",
        "prefix": "client.",
        "exclude_prefix": "client.concentration",
        "subsections": [
            {"title": "Behavior Signals", "keys": ["client.behaviors"]},
            {"title": "Device Signals"},
        ],
    },
    {
        "title": "Network & Infrastructure",
        "description": "Who owns the network behind this traffic",
        "keys": ["as.number", "as.organization", "organization", "infrastructure", "IP"],
        "subsections": [
            {"title": "Hosting & Identifiers", "keys": ["infrastructure", "IP", "organization"]},
            {"title": "Autonomous System & Ownership", "keys": ["as.number", "as.organization"]},
        ],
    },
]
OTHER_GROUP_TITLE = "Other Keys"
OTHER_GROUP_DESCRIPTION = "Additional fields present in this dataset"

# Keys handled specially by render_risk_section() rather than the normal group flow;
# excluded from build_groups() so they never land in "Other Keys" either.
RISK_SECTION_KEYS = {"risks"}

# Headline stat tiles at the top of the HTML report: (key_path, label).
# Only rendered when the key is present among the selected keys.
HEADLINE_STATS = [
    ("location.country", "Unique IP geolocations"),
    ("client.concentration.country", "Unique client geolocations"),
    ("tunnels.operator", "Unique tunnels"),
    ("client.proxies", "Unique callback proxies"),
]

# Curated cross-key risk detectors, rendered as extra red "Risk Classification" cards.
MALWARE_PROXY_VALUES = {
    "ARACHNET_PROXY", "BHPROXIES_PROXY", "9PROXY_PROXY", "DICHVUSOCKS_PROXY",
    "DOPPELGANGER_PROXY", "GENERIC_MALWARE_PROXY", "CLOUDROUTER_PROXY",
    "IPROXYSHOP_PROXY", "SOCKSESCORT_PROXY", "KIMWOLF_PROXY", "TRUESOCKS_PROXY",
    "SOKOSIK_PROXY", "5SOCKS_PROXY", "PURE_PROXY", "TISOCKS_PROXY",
    "SOCKSHUB_PROXY", "AWM_PROXY", "NSOCKS_PROXY", "PREMSOCKS_PROXY",
    "FACELESS_PROXY", "911_PROXY",
}
MALWARE_PROXY_SOURCE_KEYS = ["client.proxies", "tunnels.operator"]

# OFAC-sanctioned countries (ISO 3166-1 alpha-2): comprehensive-embargo programs
# (Cuba, Iran, North Korea, Syria) plus broad sectoral programs (Russia, Belarus).
# Edit this set if your compliance team's definition differs.
SANCTIONED_COUNTRIES = {"CU", "IR", "KP", "SY", "RU", "BY"}
SANCTIONED_COUNTRY_SOURCE_KEYS = ["location.country", "client.concentration.country"]


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


def summarize_file_keys(path: str) -> (List[str], int):
    counters: Dict[str, Counter] = defaultdict(Counter)
    total = 0
    for i, obj in enumerate(iter_json_objects(path), 1):
        total = i
        if i % PROGRESS_EVERY == 0:
            print(f"Processed {i} objects...", file=sys.stderr)
        walk(obj, counters, "")
    return filter_container_only_keys(counters), total


class KeyCounter(Counter):
    """Value counts for one key, plus how many input records had at least one value for it."""
    present = 0


def summarize_file_full(path: str) -> (Dict[str, Counter], int):
    counters: Dict[str, KeyCounter] = defaultdict(KeyCounter)
    total = 0
    for i, obj in enumerate(iter_json_objects(path), 1):
        total = i
        if i % PROGRESS_EVERY == 0:
            print(f"Processed {i} objects...", file=sys.stderr)
        record: Dict[str, Counter] = defaultdict(Counter)
        walk(obj, record, "")
        for path_key, c in record.items():
            if c:
                counters[path_key].update(c)
                counters[path_key].present += 1
    return counters, total


def print_intro_explainers(total_objects: int) -> None:
    lines = [
        "Summary metrics explanation:",
        "  - Count : number of times the given value was observed for that key (i.e., occurrences of that specific value at that key).",
        "  - Percent: (Count for this value / total number of input objects (IPs analyzed)) * 100",
        f"             (percentages are relative to the total objects analyzed: {total_objects}).",
        "  - Arrays  : elements are aggregated under their parent key; array elements contribute to the parent's value counts.",
        "  - Container-only parent keys (objects/arrays only) are omitted from the key list."
    ]
    for ln in lines:
        print(ln)
    print()


def print_keys_menu(keys: List[str], total_objects: int) -> None:
    print_intro_explainers(total_objects)
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
    counters: Dict[str, Counter], keys: List[str], max_unique: int, truncate: int, total_objects: int
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
        print(f"  Note: 'Percent' = (Count for this value / total number of input objects) * 100")
        items = sorted(counter.items(), key=lambda x: (-x[1], display_value(x[0], truncate)))
        unique_count = len(items)
        to_show = items[:max_unique]
        if unique_count > max_unique:
            print(f"  Showing top {max_unique} of {unique_count} unique values (by count):")
        else:
            print(f"  Showing {unique_count} unique value(s):")
        print(f"  {header}")
        for sig, cnt in to_show:
            pct = (cnt / total_objects) * 100 if total_objects else 0.0
            valstr = display_value(sig, truncate)
            print(f"  {cnt:<{count_w}d}  {pct:6.2f}%   {valstr}")
    if not keys:
        print("No keys selected or found.")


def summary_to_json(
    counters: Dict[str, Counter], keys: List[str], max_unique: int, truncate: int, total_objects: int
) -> Dict[str, Any]:
    out = {
        "_explainers": {
            "count": "number of times the specific value was observed for the key (occurrences of that value at the key)",
            "percent": "count / total_number_of_input_objects * 100 (percent of input objects where this value appeared at the key)",
            "note": "arrays are aggregated under parent keys; container-only parent keys are omitted"
        },
        "_total_input_objects": total_objects
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
                {
                    "value": display_value(sig, truncate),
                    "count": cnt,
                    "percent": (cnt / total_objects) * 100 if total_objects else 0.0
                }
                for sig, cnt in to_show
            ],
        }
    return out


def summary_to_markdown(
    counters: Dict[str, Counter], keys: List[str], max_unique: int, truncate: int, total_objects: int
) -> str:
    lines = []
    lines.append(f"# JSON Analysis Summary")
    lines.append(f"Analyzed **{total_objects}** total objects from the input file.")
    lines.append("")
    lines.append("## Summary Metrics Explanation")
    lines.append("- **Count**: Number of times the given value was observed for that key.")
    lines.append(f"- **Percent**: `(Count / Total Objects) * 100`. Percentages are relative to the total of **{total_objects}** objects.")
    lines.append("- **Arrays**: Elements are aggregated under their parent key.")
    lines.append("- **Container-only keys** (which only ever contain objects/arrays) are omitted from the key list.")
    lines.append("")

    for key in keys:
        counter = counters.get(key, Counter())
        total_observations = sum(counter.values())

        lines.append(f"## Key: `{key}`")
        lines.append(f"Total value observations: **{total_observations}**")
        lines.append("")

        items = sorted(counter.items(), key=lambda x: (-x[1], display_value(x[0], truncate)))
        unique_count = len(items)
        to_show = items[:max_unique]

        if unique_count > max_unique:
            lines.append(f"*Showing top **{max_unique}** of **{unique_count}** unique values (by count):*")
        else:
            lines.append(f"*Showing **{unique_count}** unique value(s):*")
        lines.append("")

        # Markdown table
        lines.append("| Count | Percent | Value |")
        lines.append("|:------|:--------|:------|")

        for sig, cnt in to_show:
            pct = (cnt / total_objects) * 100 if total_objects else 0.0
            valstr = display_value(sig, truncate)
            # Escape characters for markdown table and wrap in backticks
            valstr_md = valstr.replace('`', r'\`').replace('|', r'\|').replace('\n', '<br>').replace('\r', '')
            lines.append(f"| {cnt} | {pct:.2f}% | `{valstr_md}` |")

        lines.append("")  # Add a newline after each key's section

    if not keys:
        lines.append("No keys selected or found.")

    return "\n".join(lines)


# --------------------------------------------------------------------------
# HTML "Spur Intelligence IP Context Classification" report
# --------------------------------------------------------------------------

def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def slugify_key(key: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", key).strip("-").lower()
    return f"card-{slug}" if slug else "card"


def is_boolean_counter(counter: Counter) -> bool:
    return len(counter) > 0 and all(sig[0] == "bool" for sig in counter.keys())


def build_groups(selected_keys: List[str]) -> List[Dict[str, Any]]:
    """Assign selected keys to their known group; anything left over goes into 'Other Keys'."""
    remaining = set(selected_keys) - RISK_SECTION_KEYS
    groups = []
    for g in KEY_GROUPS:
        if "prefix" in g:
            exclude_prefix = g.get("exclude_prefix")
            group_keys = sorted(
                k for k in remaining
                if k.startswith(g["prefix"]) and not (exclude_prefix and k.startswith(exclude_prefix))
            )
        else:
            group_keys = [k for k in g["keys"] if k in remaining]
        if group_keys:
            groups.append({**g, "keys": group_keys})
            remaining -= set(group_keys)
    if remaining:
        groups.append({
            "title": OTHER_GROUP_TITLE,
            "description": OTHER_GROUP_DESCRIPTION,
            "keys": sorted(remaining),
        })
    return groups


PIE_TOP_N = 3  # capped for colorblind-safe all-pairs distinguishability; see palette validation notes


def build_pie_slices(
    items: List[Tuple[Tuple[str, Any], int]], key_total: int, truncate: int, total_objects: int
) -> List[Dict[str, Any]]:
    """
    Top 3 real values (validated categorical colors) + a single 'Others' slice for the rest.
    "pct" (shown in the legend) is out of all input records, matching the bars; "ring_pct"
    (the drawn slice size) is the value's share of this key's counted values, so the ring
    always totals 100% even when the key is missing from some records or holds lists.
    """
    top = items[:PIE_TOP_N]
    slices = []
    for i, (sig, cnt) in enumerate(top):
        slices.append({
            "css_class": f"c{i + 1}",
            "label": html_escape(display_value(sig, truncate)),
            "count": cnt,
            "pct": (cnt / total_objects * 100) if total_objects else 0.0,
            "ring_pct": (cnt / key_total * 100) if key_total else 0.0,
        })
    if len(items) > PIE_TOP_N:
        rest = items[PIE_TOP_N:]
        rest_count = sum(cnt for _, cnt in rest)
        slices.append({
            "css_class": "other",
            "label": f"Others (+{len(rest):,})",
            "count": rest_count,
            "pct": (rest_count / total_objects * 100) if total_objects else 0.0,
            "ring_pct": (rest_count / key_total * 100) if key_total else 0.0,
        })
    return slices


DONUT_SIZE = 100
DONUT_STROKE_WIDTH = 28
DONUT_RADIUS = (DONUT_SIZE / 2) - (DONUT_STROKE_WIDTH / 2)
DONUT_CENTER = DONUT_SIZE / 2
DONUT_CIRCUMFERENCE = 2 * math.pi * DONUT_RADIUS


def render_tip(title: str, paragraphs: List[str], equation: str = "") -> str:
    """
    A small ⓘ button with a hover/focus/click explainer. Text is trusted static markup
    (it may contain <b>/<code>), so callers must pre-escape any dynamic values.
    """
    body = f'<b class="t">{title}</b>' + "".join(f"<p>{p}</p>" for p in paragraphs)
    if equation:
        body += f'<span class="eq">{equation}</span>'
    return (
        f'<span class="tip"><button type="button" class="tip-btn" '
        f'aria-label="What does {title.lower()} mean?" aria-expanded="false">i</button>'
        f'<span class="tip-body" role="tooltip">{body}</span></span>'
    )


def obs_tip(total_objects: int, boolean: bool = False) -> str:
    n = f"{total_objects:,}"
    if boolean:
        return render_tip("Observations", [
            f"The number of IP records where this key is present, out of the <b>{n}</b> IPs in this report. "
            f"The % beside true and false is out of all {n} IPs."
        ])
    return render_tip("Observations", [
        f"The number of IP records where this key is present with at least one value, out of the <b>{n}</b> "
        f"IPs in this report. An IP with a list of values still counts once."
    ], f"Bar % = value count ÷ all {n} IPs")


def legend_tip(total_objects: int) -> str:
    return render_tip("Legend %", [
        f"Same basis as the bars: value count ÷ all <b>{total_objects:,}</b> IPs. "
        f"The ring shows how this key's values split between them."
    ])


def render_donut_and_legend(slices: List[Dict[str, Any]], tip_html: str = "") -> str:
    # Built as plain SVG stroked circles (stroke-dasharray per slice), not CSS conic-gradient.
    # A conic-gradient forces Chromium's print/PDF export to emit a PDF shading pattern, which
    # Apple's Preview/Quartz renderer doesn't reliably support — it falls back to a flat gray
    # disc there even though the same PDF renders correctly elsewhere (e.g. poppler). Plain
    # circle strokes convert to ordinary vector path-stroke operations, which every PDF viewer
    # (including Preview) handles identically, and the donut hole falls out naturally from the
    # stroke itself — no separate cutout element needed.
    circles = []
    cum = 0.0
    for s in slices:
        length = (s["ring_pct"] / 100) * DONUT_CIRCUMFERENCE
        offset = -(cum / 100) * DONUT_CIRCUMFERENCE
        gap = max(DONUT_CIRCUMFERENCE - length, 0)
        circles.append(
            f'<circle class="{s["css_class"]}" cx="{DONUT_CENTER}" cy="{DONUT_CENTER}" r="{DONUT_RADIUS:.3f}" '
            f'fill="none" stroke-width="{DONUT_STROKE_WIDTH}" '
            f'stroke-dasharray="{length:.3f} {gap:.3f}" stroke-dashoffset="{offset:.3f}"></circle>'
        )
        cum += s["ring_pct"]
    donut_svg = (
        f'<svg class="donut" viewBox="0 0 {DONUT_SIZE} {DONUT_SIZE}" width="{DONUT_SIZE}" height="{DONUT_SIZE}">'
        f'<g transform="rotate(-90 {DONUT_CENTER} {DONUT_CENTER})">{"".join(circles)}</g>'
        f'</svg>'
    )

    legend_rows = "\n              ".join(
        f'<div class="legend-row"><span class="swatch {s["css_class"]}"></span>'
        f'<span class="lbl">{s["label"]}</span>'
        f'<span class="val"><b>{s["pct"]:.2f}%</b> · {s["count"]:,}</span></div>'
        for s in slices
    )
    cap_html = f'<div class="legend-cap"><span>% of IPs</span>{tip_html}</div>' if tip_html else ""
    return f"""
          <div class="pie-col">
            {donut_svg}
            {cap_html}
            <div class="legend">
              {legend_rows}
            </div>
          </div>"""


def render_key_card(
    key: str, counter: Counter, total_objects: int, truncate: int, top_n: int, max_unique: int,
    critical: bool, full_width: bool
) -> str:
    value_total = sum(counter.values())
    # "obs" in the card header = IP records that have this key with at least one value (not the
    # sum of value counts, which exceeds the record count for list-valued keys).
    total_obs = getattr(counter, "present", 0) or value_total
    key_esc = html_escape(key)
    anchor_id = slugify_key(key)
    badge = '<span class="badge">critical path</span>' if critical else ""
    style_attr = ' style="grid-column: 1 / -1;"' if full_width else ""

    if is_boolean_counter(counter):
        true_count = counter.get(("bool", True), 0)
        false_count = counter.get(("bool", False), 0)
        denom = true_count + false_count
        true_w = (true_count / denom * 100) if denom else 0.0
        false_w = 100.0 - true_w
        true_pct = (true_count / total_objects * 100) if total_objects else 0.0
        false_pct = (false_count / total_objects * 100) if total_objects else 0.0
        body = f"""
        <div class="split-labels">
          <span class="true-l"><span class="dot on"></span>true</span>
          <span class="false-l">false<span class="dot off" style="margin-left:6px;margin-right:0"></span></span>
        </div>
        <div class="split-track">
          <div class="split-true" style="width:{true_w:.2f}%"></div>
          <div class="split-false" style="width:{false_w:.2f}%"></div>
        </div>
        <div class="split-figures">
          <span class="tnum">{true_pct:.2f}% · {true_count:,}</span>
          <span class="tnum">{false_pct:.2f}% · {false_count:,}</span>
        </div>"""
    else:
        items = sorted(counter.items(), key=lambda x: (-x[1], display_value(x[0], truncate)))
        unique_count = len(items)
        shown = items[:top_n]
        max_count = shown[0][1] if shown else 1

        bar_rows = []
        for sig, cnt in shown:
            valstr = html_escape(display_value(sig, truncate))
            pct = (cnt / total_objects * 100) if total_objects else 0.0
            bar_w = (cnt / max_count * 100) if max_count else 0.0
            bar_rows.append(
                f'<div class="bar-row row"><div><div class="label">{valstr}</div>'
                f'<div class="bar-track"><div class="bar-fill" style="width:{bar_w:.2f}%"></div></div></div>'
                f'<div class="figures"><span class="pct">{pct:.2f}%</span> · {cnt:,}</div></div>'
            )

        if unique_count > top_n:
            display_cap = max(top_n, max_unique)
            rest = items[top_n:display_cap]
            leftover_count = unique_count - min(unique_count, display_cap)

            remaining_count = unique_count - len(shown)
            remaining_sum = value_total - sum(cnt for _, cnt in shown)
            remaining_pct = (remaining_sum / total_objects * 100) if total_objects else 0.0

            rest_rows = []
            for sig, cnt in rest:
                valstr = html_escape(display_value(sig, truncate))
                pct = (cnt / total_objects * 100) if total_objects else 0.0
                bar_w = (cnt / max_count * 100) if max_count else 0.0
                rest_rows.append(
                    f'<div class="bar-row row"><div><div class="label">{valstr}</div>'
                    f'<div class="bar-track"><div class="bar-fill" style="width:{bar_w:.2f}%"></div></div></div>'
                    f'<div class="figures"><span class="pct">{pct:.2f}%</span> · {cnt:,}</div></div>'
                )
            if leftover_count > 0:
                rest_rows.append(
                    f'<div class="tail-note">+ {leftover_count:,} further value(s) not shown '
                    f'(capped at {display_cap:,} unique values per key).</div>'
                )
            bar_rows.append(
                "<details class=\"tail\">\n"
                f'              <summary>+ <strong>{remaining_count:,} more</strong> values · '
                f'{remaining_sum:,} counts ({remaining_pct:.1f}% of IPs combined)</summary>\n'
                '              <div class="tail-body">\n                '
                + "\n                ".join(rest_rows) +
                "\n              </div>\n            </details>"
            )

        bars_html = "\n            " + "\n            ".join(bar_rows)

        if unique_count >= 2:
            slices = build_pie_slices(items, value_total, truncate, total_objects)
            pie_html = render_donut_and_legend(slices, legend_tip(total_objects))
            body = f"""
        <div class="card-body">
          <div class="bars-col">{bars_html}
          </div>{pie_html}
        </div>"""
        else:
            body = "\n        " + "\n        ".join(bar_rows)

    return f"""
      <div class="card" id="{anchor_id}" data-key="{key_esc}" data-total="{total_obs}"{style_attr}>
        <div class="card-head">
          <span class="key">{key_esc}{badge}</span>
          <span class="total tnum">{total_obs:,} obs{obs_tip(total_objects, is_boolean_counter(counter))}</span>
        </div>{body}
      </div>"""


def partition_into_subsections(
    group_keys: List[str], subsections_spec: List[Dict[str, Any]]
) -> List[Tuple[str, List[str]]]:
    """
    Split a group's keys into labeled clusters per subsections_spec, in spec order.
    A spec entry with no "keys" is the catch-all: it takes whatever no earlier entry claimed.
    """
    remaining = list(group_keys)
    result = []
    for sub in subsections_spec:
        if "keys" in sub:
            sub_keys = [k for k in sub["keys"] if k in remaining]
        else:
            sub_keys = list(remaining)
        result.append((sub["title"], sub_keys))
        remaining = [k for k in remaining if k not in sub_keys]
    return result


def render_cards_grid(
    keys: List[str], counters: Dict[str, Counter], total_objects: int, truncate: int, top_n: int,
    max_unique: int, critical: bool
) -> str:
    full_width = len(keys) == 1
    cards = "\n".join(
        render_key_card(k, counters.get(k, Counter()), total_objects, truncate, top_n, max_unique, critical, full_width)
        for k in keys
    )
    return f"""    <div class="cards">
{cards}
    </div>"""


def render_group(
    group: Dict[str, Any], counters: Dict[str, Counter], total_objects: int, truncate: int, top_n: int,
    max_unique: int
) -> str:
    critical = bool(group.get("critical", False))
    group_class = "group risk" if critical else "group"

    if "subsections" in group:
        parts = []
        for title, sub_keys in partition_into_subsections(group["keys"], group["subsections"]):
            if not sub_keys:
                continue
            grid = render_cards_grid(sub_keys, counters, total_objects, truncate, top_n, max_unique, critical)
            parts.append(f"""    <div class="subsection">
      <div class="subsection-head">{html_escape(title)}</div>
{grid}
    </div>""")
        body = "\n".join(parts)
    else:
        body = render_cards_grid(group["keys"], counters, total_objects, truncate, top_n, max_unique, critical)

    return f"""
  <section class="{group_class}">
    <div class="group-head">
      <h2>{html_escape(group['title'])}</h2>
      <span class="desc">{html_escape(group['description'])}</span>
    </div>
{body}
  </section>"""


def gather_cross_key_matches(
    counters: Dict[str, Counter], source_keys: List[str], truncate: int, match_set: set
) -> Counter:
    """Sum counts for every value in match_set, wherever it appears across source_keys."""
    merged = Counter()
    for key in source_keys:
        counter = counters.get(key)
        if not counter:
            continue
        for sig, cnt in counter.items():
            val = display_value(sig, truncate)
            if val in match_set:
                merged[val] += cnt
    return merged


def total_observations_across_keys(counters: Dict[str, Counter], source_keys: List[str]) -> int:
    """Sum of total value observations across source_keys combined (the shared percent denominator)."""
    return sum(sum(counters[key].values()) for key in source_keys if key in counters)


def render_flagged_card(
    card_id: str, title: str, badge_text: str, merged: Counter, denominator: int,
    empty_note: str, full_width: bool, tip_html: str = ""
) -> str:
    """A red risk card for a curated cross-key match list (no pie — it's a filtered subset, not a full key breakdown)."""
    total_obs = sum(merged.values())
    style_attr = ' style="grid-column: 1 / -1;"' if full_width else ""
    badge = f'<span class="badge">{html_escape(badge_text)}</span>'

    if merged:
        items = sorted(merged.items(), key=lambda x: (-x[1], x[0]))
        max_count = items[0][1]
        rows = []
        for label, cnt in items:
            pct = (cnt / denominator * 100) if denominator else 0.0
            bar_w = (cnt / max_count * 100) if max_count else 0.0
            rows.append(
                f'<div class="bar-row row"><div><div class="label">{html_escape(label)}</div>'
                f'<div class="bar-track"><div class="bar-fill" style="width:{bar_w:.2f}%"></div></div></div>'
                f'<div class="figures"><span class="pct">{pct:.2f}%</span> · {cnt:,}</div></div>'
            )
        body = "\n        " + "\n        ".join(rows)
    else:
        body = f'\n        <div class="tail-note">{html_escape(empty_note)}</div>'

    return f"""
      <div class="card" id="{card_id}" data-key="{html_escape(title)}" data-total="{total_obs}"{style_attr}>
        <div class="card-head">
          <span class="key">{html_escape(title)}{badge}</span>
          <span class="total tnum">{total_obs:,} obs{tip_html}</span>
        </div>{body}
      </div>"""


def render_risk_section(
    counters: Dict[str, Counter], selected_keys: List[str], total_objects: int, truncate: int,
    top_n: int, max_unique: int
) -> str:
    """
    The Risk Classification section, pinned near the top of the report (right under the
    headline stat tiles) rather than in the normal key-group flow, since it's the section
    analysts should see first. Combines the raw 'risks' key (if present) with two curated
    cross-key detectors: known malware proxy operators and OFAC-sanctioned countries.
    """
    has_risks_key = "risks" in selected_keys and "risks" in counters
    malware_present = any(k in counters for k in MALWARE_PROXY_SOURCE_KEYS)
    sanctioned_present = any(k in counters for k in SANCTIONED_COUNTRY_SOURCE_KEYS)

    card_count = int(has_risks_key) + int(malware_present) + int(sanctioned_present)
    if card_count == 0:
        return ""
    full_width = card_count == 1

    cards = []
    if has_risks_key:
        cards.append(render_key_card(
            "risks", counters["risks"], total_objects, truncate, top_n, max_unique, True, full_width
        ))
    if malware_present:
        merged = gather_cross_key_matches(counters, MALWARE_PROXY_SOURCE_KEYS, truncate, MALWARE_PROXY_VALUES)
        cards.append(render_flagged_card(
            "card-malware-proxies", "Malware Proxies", "malware proxy", merged, total_objects,
            "No known malware proxy operators detected in client.proxies or tunnels.operator.", full_width,
            render_tip("Observations", [
                "Matches only: known malware proxy operators found in <code>client.proxies</code> and "
                "<code>tunnels.operator</code>, summed across both keys."
            ], f"% = match count ÷ all {total_objects:,} IPs")
        ))
    if sanctioned_present:
        merged = gather_cross_key_matches(counters, SANCTIONED_COUNTRY_SOURCE_KEYS, truncate, SANCTIONED_COUNTRIES)
        denom = total_observations_across_keys(counters, SANCTIONED_COUNTRY_SOURCE_KEYS)
        cards.append(render_flagged_card(
            "card-sanctioned-countries", "Sanctioned Countries", "sanctioned", merged, denom,
            "No OFAC-sanctioned countries detected in location.country or client.concentration.country.", full_width,
            render_tip("Observations", [
                "Matches only: OFAC-sanctioned countries (CU, IR, KP, SY, RU, BY) found in "
                "<code>location.country</code> and <code>client.concentration.country</code>."
            ], f"% = match count ÷ all observations across both keys ({denom:,})")
        ))

    return f"""
  <section class="group risk">
    <div class="group-head">
      <h2>Risk Classification</h2>
      <span class="desc">Flags raised on this dataset</span>
    </div>
    <div class="cards">
{chr(10).join(cards)}
    </div>
  </section>"""


def render_headline_stats(counters: Dict[str, Counter], selected_keys: List[str], total_objects: int) -> str:
    selected_set = set(selected_keys)
    tiles = [f"""
      <div class="stat total">
        <div class="n tnum">{total_objects:,}</div>
        <div class="l">Total IPs</div>
        <div class="k">records in this enrichment</div>
      </div>"""]
    for key, label in HEADLINE_STATS:
        if key in selected_set and key in counters:
            unique_count = len(counters[key])
            tiles.append(f"""
      <a class="stat" href="#{slugify_key(key)}">
        <div class="n tnum">{unique_count:,}</div>
        <div class="l">{html_escape(label)}</div>
        <div class="k">{html_escape(key)}</div>
      </a>""")
    return f"""
    <div class="stats" style="--cols:{len(tiles)}">{"".join(tiles)}
    </div>"""


HTML_HEAD = """<title>Spur Intelligence IP Context Classification</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600&family=Inter:wght@400;500;600;700&family=Fragment+Mono:ital@0;1&display=swap">

<style>
/*
  Committed single-theme dark palette, mirrored from spur.us's own brand
  system (black/gunmetal/ash/silver/platinum ramp + one neon accent).
  A deliberate match to Spur's real brand, not a default.
*/
:root {
  --bg: #141414;
  --surface: #1a1a1a;
  --surface-2: #222222;
  --ink: #fafafa;
  --ink-2: #b3b3b3;
  --ink-3: #8d8d8d;
  --border: rgba(255,255,255,0.09);
  --accent: #ceff00;
  --accent-ink: #141414;
  --accent-soft: rgba(206,255,0,0.14);
  --critical: #ff5c5c;
  --critical-soft: rgba(255,92,92,0.14);
  --shadow: none;
  --cat-1: #3987e5;
  --cat-2: #d95926;
  --cat-3: #199e70;
}

* { box-sizing: border-box; }
body {
  margin: 0;
  background:
    repeating-linear-gradient(0deg, rgba(255,255,255,0.035) 0 1px, transparent 1px 64px),
    repeating-linear-gradient(90deg, rgba(255,255,255,0.035) 0 1px, transparent 1px 64px),
    var(--bg);
  color: var(--ink);
  font-family: "Inter", system-ui, sans-serif;
  -webkit-font-smoothing: antialiased;
}
.wrap { max-width: 1180px; margin: 0 auto; padding: 48px 32px 80px; }

.mono { font-family: "Fragment Mono", ui-monospace, monospace; }
.tnum { font-variant-numeric: tabular-nums; }

/* Header */
header.top { margin-bottom: 36px; }
.masthead {
  display: flex;
  align-items: center;
  margin-bottom: 28px;
}
.masthead svg { height: 22px; width: auto; display: block; }
.eyebrow {
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 11.5px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--ink-3);
  margin: 0 0 14px;
  display: flex;
  align-items: center;
  gap: 9px;
}
.eyebrow::before {
  content: "";
  display: inline-block;
  width: 7px;
  height: 7px;
  background: var(--accent);
  flex-shrink: 0;
}
h1 {
  font-family: "Space Grotesk", "Inter", system-ui, sans-serif;
  font-size: 40px;
  font-weight: 300;
  margin: 0 0 12px;
  text-wrap: balance;
  letter-spacing: -0.01em;
}
.sub {
  color: var(--ink-2);
  font-size: 15px;
  max-width: 62ch;
  line-height: 1.55;
  margin: 0 0 28px;
}
.sub code {
  font-family: "Fragment Mono", ui-monospace, monospace;
  background: var(--surface-2);
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 13px;
  color: var(--ink);
}

html { scroll-behavior: smooth; }
@media (prefers-reduced-motion: reduce) { html { scroll-behavior: auto; } }

.stats {
  display: grid;
  grid-template-columns: repeat(var(--cols, 4), minmax(0, 1fr));
  gap: 12px;
}
@media (max-width: 720px) {
  .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .stat.total { grid-column: 1 / -1; }
}
.stat {
  display: block;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 3px;
  padding: 14px 16px;
  text-decoration: none;
  transition: border-color 0.15s ease;
}
.stat:hover { border-color: var(--accent); }
.stat.total:hover { border-color: var(--border); }
.stat.total .n { color: var(--accent); }
.stat .n {
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 24px;
  font-weight: 600;
  color: var(--ink);
}
.stat .l {
  font-size: 12.5px;
  color: var(--ink-2);
  margin-top: 3px;
}
.stat .k {
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 11px;
  color: var(--ink-3);
  margin-top: 6px;
}

/* Toolbar */
.toolbar {
  display: flex;
  gap: 12px;
  align-items: center;
  margin: 40px 0 28px;
  padding-bottom: 20px;
  border-bottom: 1px solid var(--border);
}
.toolbar input[type="text"] {
  flex: 1;
  max-width: 340px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 3px;
  padding: 9px 12px;
  font-family: "Inter", sans-serif;
  font-size: 14px;
  color: var(--ink);
}
.toolbar input[type="text"]::placeholder { color: var(--ink-3); }
.toolbar input[type="text"]:focus, .toolbar select:focus {
  outline: 2px solid var(--accent);
  outline-offset: 1px;
}
.bracket {
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 12.5px;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--ink-3);
  display: flex;
  align-items: center;
  gap: 4px;
}
.bracket .bk { color: var(--ink-3); }
.toolbar select {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 3px;
  padding: 9px 10px;
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 12.5px;
  letter-spacing: 0.02em;
  text-transform: uppercase;
  color: var(--ink);
}
.toolbar .tcount {
  margin-left: auto;
  font-size: 12.5px;
  letter-spacing: 0.02em;
  color: var(--ink-3);
  font-family: "Fragment Mono", ui-monospace, monospace;
  text-transform: uppercase;
}

/* Groups */
.group { margin-bottom: 40px; }
.group-head {
  display: flex;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 16px;
}
.group-head h2 {
  font-family: "Space Grotesk", "Inter", system-ui, sans-serif;
  font-size: 18px;
  font-weight: 500;
  margin: 0;
  display: flex;
  align-items: center;
  gap: 10px;
}
.group-head h2::before {
  content: "";
  display: inline-block;
  width: 8px;
  height: 8px;
  background: var(--accent);
  flex-shrink: 0;
}
.group-head .desc {
  font-size: 13px;
  color: var(--ink-3);
}
.group.risk .group-head h2 { color: var(--critical); }
.group.risk .group-head h2::before { background: var(--critical); }

.subsection { margin-bottom: 26px; }
.subsection:last-child { margin-bottom: 0; }
.subsection-head {
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 10.5px;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--ink-3);
  padding-bottom: 8px;
  margin-bottom: 14px;
  border-bottom: 1px solid var(--border);
}

.cards {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 16px;
}
@media (max-width: 720px) {
  .cards { grid-template-columns: 1fr; }
}

.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 3px;
  padding: 18px 20px 16px;
  transition: border-color 0.15s ease;
}
.card:hover { border-color: var(--accent); }
.group.risk .card { border-color: var(--critical-soft); }
.group.risk .card:hover { border-color: var(--critical); }

.card-head {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 14px;
  gap: 10px;
}
.card-head .key {
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 13px;
  font-weight: 500;
  color: var(--ink);
}
.card-head .total {
  font-size: 12px;
  color: var(--ink-3);
  white-space: nowrap;
}

.card-body {
  display: flex;
  gap: 22px;
  align-items: flex-start;
  flex-wrap: wrap;
}
.bars-col { flex: 1 1 240px; min-width: 0; }

.pie-col {
  flex: 0 0 156px;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 12px;
}
.donut {
  width: 100px;
  height: 100px;
  display: block;
  border-radius: 50%;
  box-shadow: 0 0 0 1px var(--border);
  flex-shrink: 0;
}
.donut circle.c1 { stroke: var(--cat-1); }
.donut circle.c2 { stroke: var(--cat-2); }
.donut circle.c3 { stroke: var(--cat-3); }
.donut circle.other { stroke: var(--ink-3); }
.legend {
  width: 100%;
  display: flex;
  flex-direction: column;
  gap: 7px;
}
.legend-row {
  display: flex;
  align-items: baseline;
  gap: 7px;
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 11px;
}
.legend-row .swatch {
  width: 8px;
  height: 8px;
  border-radius: 2px;
  flex-shrink: 0;
  position: relative;
  top: 1px;
}
.legend-row .swatch.c1 { background: var(--cat-1); }
.legend-row .swatch.c2 { background: var(--cat-2); }
.legend-row .swatch.c3 { background: var(--cat-3); }
.legend-row .swatch.other { background: var(--ink-3); opacity: 0.55; }
.legend-row .lbl {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--ink-2);
}
.legend-row .val {
  white-space: nowrap;
  color: var(--ink-3);
}
.legend-row .val b { color: var(--ink); font-weight: 500; }

.tail summary {
  cursor: pointer;
  list-style: none;
  display: flex;
  align-items: center;
  gap: 6px;
}
.tail summary::-webkit-details-marker { display: none; }
.tail summary::before {
  content: "›";
  display: inline-block;
  font-size: 14px;
  line-height: 1;
  color: var(--ink-3);
  transition: transform 0.15s ease;
}
.tail[open] summary::before { transform: rotate(90deg); }
.tail summary:hover { color: var(--ink-2); }
.tail summary:hover::before { color: var(--ink-2); }
@media (prefers-reduced-motion: reduce) {
  .tail summary::before { transition: none; }
}
.tail-body {
  margin-top: 10px;
  max-height: 260px;
  overflow-y: auto;
  padding-right: 2px;
}
.tail-note {
  font-style: italic;
  color: var(--ink-3);
  padding: 4px 0 2px;
  line-height: 1.5;
}

.badge {
  display: inline-block;
  font-size: 10.5px;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  font-family: "Fragment Mono", ui-monospace, monospace;
  padding: 2px 7px;
  border-radius: 2px;
  background: var(--critical-soft);
  border: 1px solid var(--critical);
  color: var(--critical);
  margin-left: 8px;
}

.row {
  display: grid;
  grid-template-columns: 1fr auto;
  align-items: center;
  gap: 10px;
  padding: 5px 0;
}
.row .label {
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 12.5px;
  color: var(--ink-2);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.row .figures {
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 12px;
  color: var(--ink-3);
  white-space: nowrap;
  text-align: right;
}
.row .figures .pct { color: var(--ink); font-weight: 500; }

.bar-row { margin-bottom: 4px; }
.bar-track {
  position: relative;
  height: 7px;
  background: var(--surface-2);
  border-radius: 4px;
  margin-top: 4px;
  overflow: hidden;
}
.bar-fill {
  position: absolute;
  inset: 0;
  width: 0;
  background: var(--accent);
  border-radius: 4px;
}
.group.risk .bar-fill { background: var(--critical); }

.tail {
  margin-top: 10px;
  padding-top: 10px;
  border-top: 1px dashed var(--border);
  font-size: 12px;
  color: var(--ink-3);
}
.tail strong { color: var(--ink-2); font-weight: 500; }

/* Binary split-bar card */
.split-labels {
  display: flex;
  justify-content: space-between;
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 12.5px;
  margin-bottom: 6px;
}
.split-labels .true-l { color: var(--ink); }
.split-labels .false-l { color: var(--ink-3); }
.dot {
  display: inline-block;
  width: 7px; height: 7px;
  border-radius: 50%;
  margin-right: 6px;
  position: relative;
  top: -1px;
}
.dot.on { background: var(--accent); }
.dot.off { background: var(--ink-3); opacity: 0.5; }
.split-track {
  display: flex;
  height: 16px;
  border-radius: 4px;
  overflow: hidden;
  background: var(--surface-2);
}
.split-true { background: var(--accent); }
.split-false { background: var(--surface-2); border-right: none; }
.split-figures {
  display: flex;
  justify-content: space-between;
  font-family: "Fragment Mono", ui-monospace, monospace;
  font-size: 11.5px;
  color: var(--ink-3);
  margin-top: 6px;
}

footer.note {
  margin-top: 44px;
  padding-top: 20px;
  border-top: 1px solid var(--border);
  font-size: 12.5px;
  color: var(--ink-3);
  line-height: 1.6;
}

/* Explainer tooltips (ⓘ next to "obs" and the donut legend) */
.tip { position: relative; display: inline-flex; vertical-align: middle; margin-left: 7px; }
.tip-btn {
  appearance: none; background: none; cursor: help; padding: 0;
  width: 15px; height: 15px; border-radius: 50%;
  border: 1px solid var(--ink-3); color: var(--ink-3);
  font: italic 600 10px/1 "Fragment Mono", ui-monospace, monospace;
  display: inline-flex; align-items: center; justify-content: center;
  transition: color .12s ease, border-color .12s ease, background .12s ease;
}
.tip:hover .tip-btn, .tip.open .tip-btn, .tip-btn:focus-visible {
  color: var(--accent-ink); background: var(--accent); border-color: var(--accent); outline: none;
}
.group.risk .tip:hover .tip-btn, .group.risk .tip.open .tip-btn, .group.risk .tip-btn:focus-visible {
  background: var(--critical); border-color: var(--critical); color: var(--bg);
}
.tip-body {
  position: absolute; z-index: 20; top: calc(100% + 9px); right: -6px;
  width: 292px; padding: 12px 14px 13px;
  background: var(--surface-2); border: 1px solid var(--accent); border-radius: 3px;
  box-shadow: 0 10px 30px rgba(0,0,0,.55);
  font: 400 12px/1.55 "Inter", system-ui, sans-serif; color: var(--ink-2);
  text-align: left; white-space: normal; letter-spacing: 0;
  opacity: 0; visibility: hidden; transform: translateY(-3px);
  transition: opacity .12s ease, transform .12s ease, visibility .12s;
  pointer-events: none;
}
.group.risk .tip-body { border-color: var(--critical); }
.tip-body::before {
  content: ""; position: absolute; top: -5px; right: 10px; width: 8px; height: 8px;
  background: var(--surface-2); border-left: 1px solid var(--accent); border-top: 1px solid var(--accent);
  transform: rotate(45deg);
}
.group.risk .tip-body::before { border-color: var(--critical); }
.tip:hover .tip-body, .tip.open .tip-body, .tip:focus-within .tip-body {
  opacity: 1; visibility: visible; transform: none; pointer-events: auto;
}
.tip-body b.t {
  display: block; margin-bottom: 5px;
  font: 500 10.5px/1 "Fragment Mono", ui-monospace, monospace;
  letter-spacing: .08em; text-transform: uppercase; color: var(--ink);
}
.tip-body p { margin: 0 0 6px; }
.tip-body p:last-child { margin-bottom: 0; }
.tip-body code {
  font: 400 11px "Fragment Mono", ui-monospace, monospace; color: var(--ink);
  background: rgba(255,255,255,.07); padding: 0 4px; border-radius: 2px;
}
.tip-body .eq { display: block; margin-top: 6px; padding-top: 6px; border-top: 1px dashed var(--border); color: var(--ink-3); }
.legend-cap {
  display: flex; align-items: center; justify-content: center;
  font: 500 10.5px/1 "Fragment Mono", ui-monospace, monospace;
  letter-spacing: .08em; text-transform: uppercase; color: var(--ink-3);
  margin: -2px 0 -4px;
}
.legend-cap .tip { margin-left: 6px; }
.legend-cap .tip-body { width: 250px; }
@media (max-width: 520px) { .tip-body { width: min(292px, 78vw); } }
@media print { .tip, .legend-cap { display: none !important; } }

/*
  Print / "Save as PDF" support. Two things break a page like this by default:
  (1) browsers skip CSS backgrounds when printing unless told otherwise, which erases
      every bar, donut, and card surface since none of them are real content pixels; and
  (2) the fixed 1180px 2-column dashboard layout doesn't fit a portrait page.
  This block addresses both; orientation is set to landscape rather than a fixed paper
  size so it still respects whatever paper size the user's print dialog is set to.
  Collapsed <details> tails are left collapsed for print by design — see HTML_SCRIPT —
  to keep the dense multi-card-per-page layout below from ballooning on high-cardinality
  cards; only the "+N more" summary line prints for those.
*/
@media print {
  /*
    margin: 0 — a nonzero @page margin leaves a strip of the page that's outside the
    printable "page area" entirely, which the position:fixed dark backdrop below can't
    reach (fixed-position elements are confined to the page area inside the margin box).
    That strip stays the PDF's default white — a white border around otherwise-dark pages.
    This is a digital export, not a home-printer page needing a safety margin, so go full
    bleed and let .wrap's own padding provide breathing room instead.
  */
  @page { size: landscape; margin: 0; }
  * {
    -webkit-print-color-adjust: exact !important;
    print-color-adjust: exact !important;
    color-adjust: exact !important;
  }
  /*
    Chromium's print/PDF pipeline only paints a box's background across that box's own
    fragment on a given page — if the actual content (html/body/wrap) ends partway down a
    page, the remaining space stays the PDF's default white, producing a dark rectangle
    floating on an otherwise white page. Setting html/body's background helped on some
    pages but not the first page or a page that starts from natural overflow rather than a
    forced .group break. The reliable fix is a `position: fixed` backdrop: fixed-position
    elements are repainted at the same spot on every printed page regardless of how much
    real content is on that page, which is exactly the "fill every page" guarantee needed.
  */
  html, body { background: var(--bg) !important; }
  body { background-image: none !important; }
  body::before {
    content: "";
    position: fixed;
    inset: 0;
    background: var(--bg);
    z-index: -1;
  }
  .wrap { max-width: none !important; padding: 24px 28px !important; }
  .toolbar { display: none !important; }
  .card, .stat { box-shadow: none !important; }

  /*
    Two-column dense grid: fits multiple cards per page instead of one-chart-per-page.
    Donuts/legends are shrunk to match so a card's pie column doesn't dominate its row.
    Tails stay natively collapsed for print (see HTML_SCRIPT — the old force-expand-on-
    print listeners were removed) so a high-cardinality card's "+N more" summary line
    doesn't balloon it to full-page height the way a fully expanded list would.
  */
  .cards { grid-template-columns: repeat(2, 1fr) !important; }
  .card-body { flex-wrap: nowrap !important; }
  .bars-col { flex: 1 1 auto !important; min-width: 0 !important; }
  .pie-col { flex: 0 0 130px !important; }
  .donut { width: 80px !important; height: 80px !important; }
  .legend-row { font-size: 9.5px !important; }

  /*
    Each top-level section starts on a fresh page, except Risk Classification which stays
    on page 1 with the top-of-report highlight tiles. A section's own subsections/cards are
    free to overflow onto the next page if they don't fit, but never mid-card or mid-donut.
    Note: break-inside:avoid belongs on .card (and its donut), not on the whole .subsection —
    a subsection can hold several cards and easily be taller than one page, so avoiding a
    break inside the *whole subsection* just pushes it entirely to the next page, stranding
    its heading alone with a large empty gap on the page before it. Letting the subsection
    break between cards (each card still individually protected) keeps pages full.
  */
  .group:not(.risk) { break-before: page; page-break-before: always; }
  .group { padding-top: 30px; }

  /*
    Padding above an element only takes effect in Chromium's print pipeline when that
    element itself carries an explicit forced break-before — a natural/engine-decided
    break never gets it, no matter what break-inside/box-decoration-break combination is
    tried on the parent. That's empirically confirmed (see project memory), so every
    subsection after the first in a section is forced onto its own page purely to get its
    padding-top respected; this trades some page-packing density for consistent spacing.
  */
  .subsection + .subsection { break-before: page; page-break-before: always; }
  .subsection { padding-top: 24px; }

  /*
    A lone trailing card left alone in an odd-count grid would otherwise stretch to fill
    the full row width at whatever the 2-column track width happens to be, which looks
    oversized and empty next to pages with 3-4 packed cards. Center it at a fixed width
    instead so single/lone-card pages look intentional rather than stretched. This applies
    even when it's the ONLY card in its subsection (nth-child(1) is also last & odd).
  */
  .card:last-child:nth-child(odd) {
    grid-column: 1 / -1 !important;
    width: 88%;
    max-width: 900px;
    margin-left: auto;
    margin-right: auto;
  }
  .card { grid-column: auto !important; }

  /*
    Margin-top on a short, non-fragmenting sibling box (break-inside:avoid keeps a .card
    from ever splitting) IS honored at a natural page break in Chromium, unlike padding-top
    on a *container* that can itself span multiple pages (.group/.subsection above) — that
    "only the first fragment gets top padding" rule is specific to a box being sliced, and a
    .card here is never sliced, just placed fresh on whichever page it lands on. So a lone
    trailing card only needs margin-top, no forced break-before. Skip it on :first-child so
    a singleton subsection (e.g. Services) isn't pushed an extra 30px below its already-
    spaced subsection heading.
  */
  .card:last-child:nth-child(odd):not(:first-child) {
    margin-top: 30px;
  }

  /*
    A run longer than one page's worth of cards (e.g. the Client Devices catch-all) could,
    in principle, naturally overflow at a different point than expected if actual card
    height varies from the ~4-per-page assumption. Forcing a break at a fixed, predictable
    row-start (every 2 rows / 4 cards) guarantees the margin-top below always lands on the
    exact card that starts a new page, rather than trusting natural overflow to land there
    — a small density trade for certainty. (Cards are always ordered by the report's
    default "most observations" sort — see HTML_SCRIPT's applySort(), which runs before the
    PDF snapshot is taken — independent of and unaffected by any of this pagination CSS.)
  */
  .cards > .card:nth-child(4n+1):not(:first-child) {
    break-before: page;
    page-break-before: always;
    margin-top: 30px;
  }

  /*
    Risk Classification cards render single-column and full-width (matching the headline
    stat tiles' full-bleed row above) instead of the 2-column grid every other section
    uses, and stay on page 1 stacked one under another — packing together whenever they
    fit — rather than each forced onto its own page. Being single-column, there's never a
    row of 2+ cards that needs to move together, so plain margin-top (no forced break) is
    enough regardless of how many cards end up overflowing to the next page. The generic
    lone-trailing-card rule above would otherwise also match the last risk card (if there
    are 3) and cap it at 900px/centered — override that back to full width here since risk
    cards should always match the stat tiles' width, not the centered-lone-card treatment
    used elsewhere.
  */
  .group.risk .cards { grid-template-columns: 1fr !important; }
  .group.risk .card:not(:first-child) { margin-top: 30px; }
  .group.risk .card:last-child:nth-child(odd) {
    grid-column: auto !important;
    width: auto;
    max-width: none;
    margin-left: 0;
    margin-right: 0;
  }

  .card, .pie-col, .donut {
    break-inside: avoid;
    page-break-inside: avoid;
  }
  .subsection-head { break-after: avoid; page-break-after: avoid; }
  .tail[open] summary::before { transform: none; }
}
</style>
"""

# Spur's own wordmark, inlined as static paths (fetched from spur.us's own asset CDN).
# Fill is hardcoded white to match this report's committed dark theme.
SPUR_LOGO_SVG = """<svg viewBox="0 0 103 31" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Spur">
<path d="M19.7852 0C20.1147 5.07404 20.293 10.1515 20.293 15.2256C20.293 20.2996 20.114 25.3742 19.7852 30.4482H18.7705C18.4517 25.3742 18.2627 20.2996 18.2627 15.2256C18.2627 10.1515 18.4517 5.07404 18.7705 0H19.7852ZM15.7314 13.6963C16.0219 14.1289 16.2379 14.6054 16.2393 15.1055C16.2378 15.6054 16.0219 16.0541 15.7314 16.4873L8.12012 27.3975H7.10449C7.11677 27.3747 12.4345 17.501 13.4482 16.9941V16.2334C11.9258 16.2334 0 15.7256 0 15.7256V14.7109C0 14.7109 11.9258 14.2031 13.4482 14.2031V13.4424C12.4333 12.4274 7.10449 3.03906 7.10449 3.03906H8.12012L15.7314 13.6963ZM31.46 3.03906C31.46 3.03906 26.1312 12.4274 25.1162 13.4424V14.2031C26.6386 14.2031 38.5645 14.7109 38.5645 14.7109V15.7256C38.5645 15.7256 26.6386 16.2334 25.1162 16.2334V16.9941C26.1301 18.008 31.4485 27.3772 31.46 27.3975H30.4443L22.833 16.4873C22.5426 16.0541 22.3266 15.6054 22.3252 15.1055C22.3265 14.6055 22.5425 14.1289 22.833 13.6963L30.4443 3.03906H31.46Z" fill="#FAFAFA"></path>
<path d="M55.1006 4.30478C57.3442 4.30478 59.1414 4.91644 60.3525 5.91415C61.4354 6.80621 62.0735 8.02574 62.1504 9.47177H60.7646C60.6621 8.28297 60.3506 7.26242 59.5117 6.53915C58.5822 5.7377 57.1174 5.40146 54.9531 5.40146C53.0971 5.40148 51.7113 5.68071 50.7871 6.37997C49.8259 7.10722 49.4668 8.20368 49.4668 9.55966C49.4668 10.6771 49.6973 11.5631 50.457 12.2403C51.1714 12.877 52.2923 13.2647 53.8721 13.6183V13.6192L57.4131 14.419H57.4141C59.2766 14.8373 60.549 15.4299 61.3525 16.1866C62.1365 16.9249 62.5136 17.8563 62.5137 19.0645C62.5137 20.7006 61.8356 22.1164 60.5938 23.1319C59.3448 24.1533 57.4945 24.793 55.1318 24.7931C52.7652 24.7931 50.8782 24.1665 49.6055 23.1349C48.4619 22.2078 47.7894 20.9337 47.7041 19.4112H49.0918C49.2128 20.6678 49.5676 21.7387 50.4658 22.4962C51.4547 23.3299 52.9937 23.6905 55.248 23.6905C57.2092 23.6905 58.6665 23.3959 59.6367 22.6778C60.6458 21.9309 61.0303 20.8054 61.0303 19.4151C61.0302 18.2689 60.7913 17.3773 60.0234 16.6974C59.3026 16.0592 58.175 15.6691 56.5977 15.3009H56.5967L52.9092 14.4464H52.9102C51.1363 14.0285 49.923 13.4509 49.1562 12.713C48.4086 11.9933 48.045 11.0821 48.0449 9.88583C48.0449 8.27841 48.7036 6.89983 49.8945 5.91415C51.0923 4.9229 52.8597 4.30484 55.1006 4.30478Z" fill="#FAFAFA" stroke="#FAFAFA"></path>
<path d="M66.499 10.6494V11.9268H67.7012C67.6543 11.9738 67.6077 12.0219 67.5635 12.0732C67.116 12.5936 66.8514 13.2894 66.6943 14.168C66.5373 15.0467 66.4795 16.1542 66.4795 17.5361C66.4795 18.918 66.5373 20.0263 66.6943 20.9072C66.8513 21.788 67.1149 22.4866 67.5615 23.0098C67.6145 23.0718 67.6707 23.1297 67.7275 23.1855H66.5L66.499 23.6855L66.4932 29.9424L65.4502 29.9385L65.4561 10.6494H66.499ZM72.5684 9.98438C74.5285 9.98441 76.0257 10.6369 77.043 11.8506C78.0714 13.0776 78.665 14.945 78.665 17.4736C78.665 19.9956 78.0376 21.9043 76.9824 23.1729C75.9365 24.4302 74.4272 25.1123 72.5371 25.1123C70.5635 25.1123 69.2779 24.5235 68.4463 23.6934C68.718 23.8297 69.0152 23.9335 69.3379 24.0078C70.0471 24.171 70.9049 24.2119 71.9121 24.2119C72.9204 24.2119 73.7816 24.1687 74.4961 24.0029C75.2225 23.8344 75.8272 23.5331 76.2881 23.001C76.7418 22.4771 77.0139 21.7786 77.1768 20.8975C77.3395 20.0169 77.4004 18.9113 77.4004 17.5361C77.4004 16.1533 77.3395 15.045 77.1768 14.165C77.0139 13.2847 76.7414 12.5888 76.2861 12.0693C75.8238 11.5418 75.2182 11.248 74.4932 11.085C73.7798 10.9246 72.9191 10.8848 71.9121 10.8848C70.9053 10.8848 70.0476 10.9244 69.3389 11.085C69.0375 11.1533 68.7582 11.2463 68.501 11.3682C69.364 10.5458 70.6765 9.98438 72.5684 9.98438Z" fill="#FAFAFA" stroke="#FAFAFA"></path>
<path d="M82.7783 10.6533V19.8164C82.7783 20.642 82.8128 21.3371 82.9316 21.9062C83.052 22.4823 83.2668 22.9693 83.6553 23.3408C84.0413 23.7099 84.5465 23.9146 85.1494 24.0312C85.7487 24.1472 86.4868 24.1846 87.3789 24.1846C88.4548 24.1846 89.343 24.1055 90.0479 23.8643C89.2365 24.5233 88.0898 25.0098 86.4258 25.0098C84.9127 25.0097 83.7482 24.6245 82.9629 23.8818C82.2511 23.2086 81.7888 22.1814 81.7129 20.7002V10.6533H82.7783ZM93.1328 24.6191H92.1523V23.2705H91.0791C91.1113 23.2407 91.1441 23.2113 91.1748 23.1797C91.9439 22.3864 92.0967 21.1426 92.0967 19.5215V10.6514L93.1328 10.6494V24.6191Z" fill="#FAFAFA" stroke="#FAFAFA"></path>
<path d="M97.4409 10.6504V11.9141H98.5288C98.5091 11.9318 98.4884 11.9484 98.4692 11.9668C97.6676 12.7364 97.483 13.9608 97.4829 15.5879L97.4429 24.6152H96.4106V10.6504H97.4409ZM102.5 11.0195H102.262C101.149 11.0195 100.228 11.0965 99.5015 11.3535C100.247 10.7126 101.197 10.1666 102.5 10.0215V11.0195Z" fill="#FAFAFA" stroke="#FAFAFA"></path>
</svg>"""

HTML_SCRIPT = """<script>
  const search = document.getElementById('search');
  const sort = document.getElementById('sort');
  const tcount = document.getElementById('tcount');
  const allCards = Array.from(document.querySelectorAll('.card'));

  function applyFilter() {
    const q = search.value.trim().toLowerCase();
    let visible = 0;
    allCards.forEach(card => {
      const match = card.dataset.key.toLowerCase().includes(q);
      card.style.display = match ? '' : 'none';
      if (match) visible++;
    });
    document.querySelectorAll('.group').forEach(group => {
      const anyVisible = Array.from(group.querySelectorAll('.card')).some(c => c.style.display !== 'none');
      group.style.display = anyVisible ? '' : 'none';
    });
    tcount.textContent = `${visible} of ${allCards.length} cards`;
  }

  function applySort() {
    const mode = sort.value;
    document.querySelectorAll('.cards').forEach(container => {
      const cards = Array.from(container.querySelectorAll('.card'));
      cards.sort((a, b) => {
        if (mode === 'name') return a.dataset.key.localeCompare(b.dataset.key);
        const av = +a.dataset.total, bv = +b.dataset.total;
        return mode === 'asc' ? av - bv : bv - av;
      });
      cards.forEach(c => container.appendChild(c));
    });
  }

  search.addEventListener('input', applyFilter);
  sort.addEventListener('change', applySort);
  applySort();

  // Explainer tooltips open on hover/focus via CSS; click/tap pins one open, Esc or an outside click closes it.
  document.addEventListener('click', e => {
    const btn = e.target.closest('.tip-btn');
    document.querySelectorAll('.tip.open').forEach(t => {
      if (!btn || t !== btn.parentElement) { t.classList.remove('open'); t.firstElementChild.setAttribute('aria-expanded', 'false'); }
    });
    if (btn) { const open = btn.parentElement.classList.toggle('open'); btn.setAttribute('aria-expanded', open); }
  });
  document.addEventListener('mouseover', e => {
    const hovered = e.target.closest('.tip');
    if (!hovered) return;
    document.querySelectorAll('.tip.open').forEach(t => {
      if (t !== hovered) { t.classList.remove('open'); t.firstElementChild.setAttribute('aria-expanded', 'false'); }
    });
  });
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') document.querySelectorAll('.tip.open').forEach(t => t.classList.remove('open'));
  });

  // Tails intentionally stay collapsed for print/PDF (showing only their "+N more"
  // summary line) so a high-cardinality card doesn't balloon to full-page height in the
  // dense multi-card-per-page print layout. Full itemization remains available on-screen
  // (expand the tail) or via the JSON/Markdown/HTML exports.
</script>
"""


def summary_to_html(
    counters: Dict[str, Counter], keys: List[str], top_n: int, truncate: int, total_objects: int, input_path: str,
    max_unique: int = DEFAULT_MAX_UNIQUE_TO_SHOW
) -> str:
    risk_section_html = render_risk_section(counters, keys, total_objects, truncate, top_n, max_unique)
    groups = build_groups(keys)
    groups_html = "\n".join(
        render_group(g, counters, total_objects, truncate, top_n, max_unique) for g in groups
    )
    stats_html = render_headline_stats(counters, keys, total_objects)
    basename_esc = html_escape(os.path.basename(input_path))
    n_keys = len(keys)

    # The toolbar's live count reflects every rendered .card (JS counts DOM nodes), which
    # includes the two synthetic cross-key risk cards alongside real JSON-key cards — so the
    # initial count must include them too, or it'll jump after the first search/sort.
    synthetic_card_count = (
        int(any(k in counters for k in MALWARE_PROXY_SOURCE_KEYS))
        + int(any(k in counters for k in SANCTIONED_COUNTRY_SOURCE_KEYS))
    )
    n_cards = n_keys + synthetic_card_count

    parts = [
        HTML_HEAD,
        '<div class="wrap">\n',
        '  <header class="top">\n',
        f'    <div class="masthead">{SPUR_LOGO_SVG}</div>\n',
        '    <p class="eyebrow">Context dataset report</p>\n',
        '    <h1>Spur Intelligence IP Context Classification</h1>\n',
        f'    <p class="sub">Breakdown of <code>{basename_esc}</code> across {n_keys} key(s).</p>\n',
        stats_html,
        '\n  </header>\n\n',
        '  <div class="toolbar">\n',
        '    <input type="text" id="search" placeholder="Filter keys… e.g. proxy, risk, country" />\n',
        '    <select id="sort">\n',
        '      <option value="desc">Sort: most observations</option>\n',
        '      <option value="asc">Sort: fewest observations</option>\n',
        '      <option value="name">Sort: key name A–Z</option>\n',
        '    </select>\n',
        f'    <span class="tcount"><span class="bk">[</span> <span id="tcount">{n_cards} of {n_cards} cards</span> <span class="bk">]</span></span>\n',
        '  </div>\n',
        risk_section_html,
        '\n\n',
        groups_html,
        '\n\n  <footer class="note">\n',
        f'    Generated from <code>{basename_esc}</code> — bar length is scaled within each key '
        f'(its top value fills the track); the percent alongside each bar is that value’s share of all '
        f'{total_objects:,} objects analyzed. Search and sort above are live.\n',
        '  </footer>\n',
        '</div>\n',
        HTML_SCRIPT,
    ]
    return "".join(parts)


# Chromium-based browser executable names/paths to try, in order, for headless PDF export.
# A real browser engine renders the report exactly as designed (fonts, conic-gradient donuts,
# flex/grid layout) — unlike a pure-Python HTML-to-PDF library, which typically can't.
_BROWSER_PATH_CANDIDATES_DARWIN = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
]
_BROWSER_PATH_CANDIDATES_WINDOWS_REL = [
    r"Google\Chrome\Application\chrome.exe",
    r"Chromium\Application\chrome.exe",
    r"BraveSoftware\Brave-Browser\Application\brave.exe",
    r"Microsoft\Edge\Application\msedge.exe",
]
_BROWSER_PATH_NAMES = [
    "google-chrome", "google-chrome-stable", "chromium", "chromium-browser",
    "brave-browser", "brave", "microsoft-edge", "microsoft-edge-stable", "msedge",
]


def find_chromium_browser() -> Optional[str]:
    """Locate an installed Chromium-based browser for headless PDF export."""
    system = platform.system()
    candidates = []
    if system == "Darwin":
        candidates = list(_BROWSER_PATH_CANDIDATES_DARWIN)
    elif system == "Windows":
        program_dirs = [
            os.environ.get("PROGRAMFILES", r"C:\Program Files"),
            os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"),
        ]
        candidates = [
            os.path.join(pf, rel) for pf in program_dirs for rel in _BROWSER_PATH_CANDIDATES_WINDOWS_REL
        ]
    for path in candidates:
        if os.path.isfile(path):
            return path
    for name in _BROWSER_PATH_NAMES:
        found = shutil.which(name)
        if found:
            return found
    return None


def render_html_to_pdf(html_path: str, pdf_path: str, browser_path: str) -> None:
    """Print a local HTML file to PDF via a headless Chromium-based browser."""
    file_url = "file://" + os.path.abspath(html_path)
    cmd = [
        browser_path,
        "--headless=new",
        "--disable-gpu",
        "--virtual-time-budget=4000",
        "--no-pdf-header-footer",
        f"--print-to-pdf={os.path.abspath(pdf_path)}",
        file_url,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except OSError as e:
        raise RuntimeError(f"Could not run browser executable {browser_path!r}: {e}")
    if result.returncode != 0 or not os.path.exists(pdf_path):
        raise RuntimeError(
            f"Headless browser PDF export failed (exit code {result.returncode}).\n"
            f"Command: {' '.join(cmd)}\n"
            f"stdout: {result.stdout.strip()}\n"
            f"stderr: {result.stderr.strip()}"
        )


def export_pdf(
    counters: Dict[str, Counter], keys: List[str], top_n: int, truncate: int, total_objects: int,
    input_path: str, pdf_path: str, max_unique: int, browser_path: Optional[str] = None
) -> None:
    """Render the HTML report to a temp file, then print that to PDF via a headless browser."""
    resolved_browser = browser_path or find_chromium_browser()
    if not resolved_browser:
        raise RuntimeError(
            "No Chromium-based browser found for PDF export. Install Google Chrome, Chromium, "
            "Brave, or Microsoft Edge, or pass --browser-path to point at one directly."
        )
    html_content = summary_to_html(counters, keys, top_n, truncate, total_objects, input_path, max_unique)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False, encoding="utf-8") as tmp:
        tmp.write(html_content)
        tmp_path = tmp.name
    try:
        render_html_to_pdf(tmp_path, pdf_path, resolved_browser)
    finally:
        os.unlink(tmp_path)


def prompt_for_export(
    counters: Dict[str, Counter], keys: List[str], max_unique: int, truncate: int, total_objects: int, input_path: str,
    browser_path: Optional[str] = None
) -> None:
    """After terminal output, prompt user to export results to a file."""
    print("\n" + "-" * 50)
    try:
        choice = input("Export these results to a file? (json / md / html / pdf / no): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\nExport cancelled.")
        return

    if choice not in ['json', 'md', 'html', 'pdf']:
        print("Not exporting.")
        return

    base_name = os.path.splitext(os.path.basename(input_path))[0]
    default_filename = f"{base_name}_analysis.{choice}"

    try:
        filename_input = input(f"Enter filename for export (default: {default_filename}): ").strip()
        filename = filename_input or default_filename
    except (EOFError, KeyboardInterrupt):
        print("\nExport cancelled.")
        return

    try:
        if choice == 'json':
            out = summary_to_json(counters, keys, max_unique, truncate, total_objects)
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
            print(f"Wrote JSON summary to {filename}")
        elif choice == 'md':
            md_content = summary_to_markdown(counters, keys, max_unique, truncate, total_objects)
            with open(filename, "w", encoding="utf-8") as f:
                f.write(md_content)
            print(f"Wrote Markdown summary to {filename}")
        elif choice == 'html':
            html_content = summary_to_html(
                counters, keys, DEFAULT_HTML_TOP_N, truncate, total_objects, input_path, max_unique
            )
            with open(filename, "w", encoding="utf-8") as f:
                f.write(html_content)
            print(f"Wrote HTML report to {filename}")
        elif choice == 'pdf':
            export_pdf(
                counters, keys, DEFAULT_HTML_TOP_N, truncate, total_objects, input_path, filename, max_unique,
                browser_path
            )
            print(f"Wrote PDF report to {filename}")
    except IOError as e:
        print(f"Error: Could not write to file {filename}: {e}", file=sys.stderr)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)


def parse_args(argv):
    p = argparse.ArgumentParser(description="Summarize unique JSON keys and values with counts.")
    p.add_argument("input", help="Input .json or .jsonl file")
    p.add_argument("--max-values", type=int, default=DEFAULT_MAX_UNIQUE_TO_SHOW,
                   help=f"Max unique values to display per key (default {DEFAULT_MAX_UNIQUE_TO_SHOW})")
    p.add_argument("--truncate", type=int, default=MAX_VALUE_DISPLAY,
                   help=f"Truncate displayed values to this length (default {MAX_VALUE_DISPLAY})")
    p.add_argument("--html-top-n", type=int, default=DEFAULT_HTML_TOP_N,
                   help=f"Max bars to show per key in the HTML report before collapsing into a "
                        f"'+N more' summary (default {DEFAULT_HTML_TOP_N})")
    p.add_argument("--aggregate-arrays", action="store_true",
                   help="(no-op) arrays are always aggregated in this version")
    output_group = p.add_mutually_exclusive_group()
    output_group.add_argument("--json-output", type=str, default=None,
                              help="Write the summary as JSON to this file instead of printing plain text")
    output_group.add_argument("--md-output", type=str, default=None,
                              help="Write the summary as Markdown to this file instead of printing plain text")
    output_group.add_argument("--html-output", type=str, default=None,
                              help="Write the summary as a self-contained HTML report to this file instead of printing plain text")
    output_group.add_argument("--pdf-output", type=str, default=None,
                              help="Write the summary as a PDF (renders the HTML report via a headless "
                                   "Chromium-based browser — Chrome, Chromium, Brave, or Edge) instead of "
                                   "printing plain text")
    p.add_argument("--browser-path", type=str, default=None,
                   help="Path to a Chromium-based browser executable for --pdf-output, if auto-detection fails")
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
    keys, total_keys_pass = summarize_file_keys(path)
    keys = [k for k in keys if k not in EXCLUDED_KEYS]
    if not keys:
        print("No keys found (empty or invalid JSON).", file=sys.stderr)
        return 1

    if args.non_interactive:
        selected_keys = keys
    else:
        print_keys_menu(keys, total_keys_pass)
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
    counters, total_objects = summarize_file_full(path)
    if args.json_output:
        out = summary_to_json(counters, selected_keys, args.max_values, args.truncate, total_objects)
        with open(args.json_output, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"Wrote JSON summary to {args.json_output}")
    elif args.md_output:
        md_content = summary_to_markdown(counters, selected_keys, args.max_values, args.truncate, total_objects)
        with open(args.md_output, "w", encoding="utf-8") as f:
            f.write(md_content)
        print(f"Wrote Markdown summary to {args.md_output}")
    elif args.html_output:
        html_content = summary_to_html(
            counters, selected_keys, args.html_top_n, args.truncate, total_objects, args.input, args.max_values
        )
        with open(args.html_output, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"Wrote HTML report to {args.html_output}")
    elif args.pdf_output:
        try:
            export_pdf(
                counters, selected_keys, args.html_top_n, args.truncate, total_objects, args.input,
                args.pdf_output, args.max_values, args.browser_path
            )
        except RuntimeError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 3
        print(f"Wrote PDF report to {args.pdf_output}")
    else:
        print_summary_for_keys(counters, selected_keys, args.max_values, args.truncate, total_objects)
        prompt_for_export(
            counters, selected_keys, args.max_values, args.truncate, total_objects, args.input, args.browser_path
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
