#!/usr/bin/env python3
"""
Markdown memory manager for WeatherBot.

Stores a lightweight long-running thread in markdown and provides deterministic
compaction to avoid unbounded growth and context rot.
"""

import os
from datetime import datetime, timezone

DEFAULT_MEMORY_FILE = "logs/weatherbot_memory.md"
DEFAULT_MAX_CONTEXT_CHARS = 3500
DEFAULT_MAX_FILE_BYTES = 262144
DEFAULT_KEEP_RECENT_RUNS = 192
DEFAULT_COMPACT_AT_RATIO = 0.9
DEFAULT_TARGET_RATIO = 0.75


def get_memory_settings():
    memory_path = os.getenv("WEATHERBOT_MEMORY_FILE", DEFAULT_MEMORY_FILE)
    max_context_chars = int(os.getenv("WEATHERBOT_MEMORY_MAX_CONTEXT_CHARS", str(DEFAULT_MAX_CONTEXT_CHARS)))
    max_file_bytes = int(os.getenv("WEATHERBOT_MEMORY_MAX_FILE_BYTES", str(DEFAULT_MAX_FILE_BYTES)))
    keep_recent_runs = int(os.getenv("WEATHERBOT_MEMORY_KEEP_RECENT_RUNS", str(DEFAULT_KEEP_RECENT_RUNS)))
    compact_at_ratio = float(os.getenv("WEATHERBOT_MEMORY_COMPACT_AT_RATIO", str(DEFAULT_COMPACT_AT_RATIO)))
    target_ratio = float(os.getenv("WEATHERBOT_MEMORY_TARGET_RATIO", str(DEFAULT_TARGET_RATIO)))

    compact_at_ratio = min(max(compact_at_ratio, 0.1), 1.0)
    target_ratio = min(max(target_ratio, 0.1), compact_at_ratio)

    return {
        "memory_path": memory_path,
        "max_context_chars": max_context_chars,
        "max_file_bytes": max_file_bytes,
        "keep_recent_runs": keep_recent_runs,
        "compact_at_ratio": compact_at_ratio,
        "target_ratio": target_ratio,
    }


def _ensure_parent_dir(file_path):
    parent = os.path.dirname(file_path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _read_text(file_path):
    if not os.path.exists(file_path):
        return ""
    with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _atomic_write(file_path, content):
    _ensure_parent_dir(file_path)
    temp_path = f"{file_path}.tmp"
    with open(temp_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)
    os.replace(temp_path, file_path)


def get_memory_context():
    settings = get_memory_settings()
    content = _read_text(settings["memory_path"])
    if not content:
        return ""

    max_chars = settings["max_context_chars"]
    if len(content) <= max_chars:
        return content

    tail = content[-max_chars:]
    marker = tail.find("## Run")
    if marker > 0:
        tail = tail[marker:]
    return tail


def get_recent_reasoning_openers(limit=8):
    settings = get_memory_settings()
    content = _read_text(settings["memory_path"])
    if not content:
        return []

    openers = []
    for line in content.splitlines():
        if line.startswith("- Reasoning excerpt:"):
            excerpt = line.split(":", 1)[1].strip()
            if not excerpt:
                continue

            sentence_end = excerpt.find(".")
            sentence = excerpt if sentence_end == -1 else excerpt[:sentence_end]
            words = sentence.split()
            opener = " ".join(words[:4]).strip().lower()
            if opener:
                openers.append(opener)

    # Keep newest unique openers only
    seen = set()
    ordered = []
    for opener in reversed(openers):
        if opener not in seen:
            seen.add(opener)
            ordered.append(opener)
        if len(ordered) >= limit:
            break

    return list(reversed(ordered))


def _render_header(existing_content):
    if existing_content.strip():
        return existing_content.rstrip() + "\n"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return (
        "# WeatherBot Memory Thread\n"
        "\n"
        "This file is managed by the weather report service to maintain continuity over periodic runs.\n"
        "The newest entries appear at the bottom.\n"
        "\n"
        "## Long-Term Facts\n"
        "- Bot runs periodically and generates weather reports from fresh observed measurements.\n"
        "- Memory influences narrative continuity only; fresh measurements and NWS data are authoritative.\n"
        f"- Thread initialized: {now}\n"
        "\n"
        "## Rolling Summary\n"
        "- Placeholder summary. Updated during compaction as needed.\n"
        "\n"
        "## Run History\n"
    )


def append_memory_entry(entry):
    settings = get_memory_settings()
    path = settings["memory_path"]
    current = _read_text(path)
    content = _render_header(current)

    timestamp = entry.get("timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
    location = entry.get("location", "unknown")
    confidence = entry.get("confidence", 0.0)
    analysis_window = entry.get("analysis_window_hours")
    analysis_window_str = f"{analysis_window}" if analysis_window is not None else "unknown"
    reasoning_excerpt = (entry.get("reasoning", "") or "").strip().replace("\n", " ")
    if len(reasoning_excerpt) > 500:
        reasoning_excerpt = reasoning_excerpt[:500].rstrip() + "..."

    run_lines = [
        "",
        f"## Run {timestamp}",
        f"- Location: {location}",
        f"- Analysis window hours: {analysis_window_str}",
        f"- Confidence: {confidence}",
    ]

    key_points = entry.get("key_points") or []
    if key_points:
        run_lines.append("- Key points:")
        for point in key_points[:8]:
            run_lines.append(f"  - {point}")

    if reasoning_excerpt:
        run_lines.append(f"- Reasoning excerpt: {reasoning_excerpt}")

    updated = content.rstrip() + "\n" + "\n".join(run_lines) + "\n"
    _atomic_write(path, updated)


def should_compact_memory():
    settings = get_memory_settings()
    path = settings["memory_path"]
    if not os.path.exists(path):
        return False
    compact_threshold = int(settings["max_file_bytes"] * settings["compact_at_ratio"])
    return os.path.getsize(path) >= compact_threshold


def _extract_sections(content):
    long_term_start = content.find("## Long-Term Facts")
    rolling_start = content.find("## Rolling Summary")
    history_start = content.find("## Run History")

    long_term = ""
    rolling = ""
    history = ""

    if long_term_start != -1:
        next_idx = rolling_start if rolling_start != -1 else history_start
        if next_idx != -1:
            long_term = content[long_term_start:next_idx].strip()
    if rolling_start != -1:
        next_idx = history_start
        if next_idx != -1:
            rolling = content[rolling_start:next_idx].strip()
    if history_start != -1:
        history = content[history_start:].strip()

    return long_term, rolling, history


def _split_runs(history_section):
    if not history_section:
        return []

    lines = history_section.splitlines()
    if not lines:
        return []

    runs = []
    current = []
    for line in lines[1:]:
        if line.startswith("## Run "):
            if current:
                runs.append("\n".join(current).strip())
            current = [line]
        elif current:
            current.append(line)

    if current:
        runs.append("\n".join(current).strip())
    return [run for run in runs if run]


def _trim_run_block(run_block):
    lines = run_block.splitlines()
    if not lines:
        return run_block

    trimmed = []
    key_point_count = 0
    in_key_points = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("- Reasoning excerpt:"):
            continue

        if stripped == "- Key points:":
            in_key_points = True
            trimmed.append(line)
            continue

        if in_key_points and line.startswith("  - "):
            if key_point_count < 3:
                trimmed.append(line)
                key_point_count += 1
            continue

        in_key_points = False
        trimmed.append(line)

    return "\n".join(trimmed).strip()


def _build_compacted_content(long_term, kept_runs, compacted_count):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    rolling = [
        "## Rolling Summary",
        f"- Last compaction: {now}",
        f"- Runs compacted this pass: {compacted_count}",
        f"- Recent runs retained: {len(kept_runs)}",
        "- Use recent runs for continuity; rely on fresh observations for current conditions.",
    ]

    new_parts = [
        "# WeatherBot Memory Thread",
        "",
        "This file is managed by the weather report service to maintain continuity over periodic runs.",
        "The newest entries appear at the bottom.",
        "",
        long_term if long_term else "## Long-Term Facts\n- Bot runs periodically and generates weather reports from fresh observed measurements.",
        "",
        "\n".join(rolling),
        "",
        "## Run History",
    ]

    if kept_runs:
        new_parts.append("\n\n".join(kept_runs))

    return "\n".join(new_parts).rstrip() + "\n"


def compact_memory_file():
    settings = get_memory_settings()
    path = settings["memory_path"]
    content = _read_text(path)
    if not content:
        return False

    long_term, _, history = _extract_sections(content)
    runs = _split_runs(history)
    if not runs:
        return False

    keep_recent = max(settings["keep_recent_runs"], 1)
    target_size = int(settings["max_file_bytes"] * settings["target_ratio"])
    kept_count = min(keep_recent, len(runs))

    while kept_count >= 1:
        kept_runs = runs[-kept_count:]
        compacted_count = max(len(runs) - kept_count, 0)
        candidate = _build_compacted_content(long_term, kept_runs, compacted_count)
        if len(candidate.encode("utf-8")) <= target_size or kept_count == 1:
            if len(candidate.encode("utf-8")) > target_size:
                compressed_runs = [_trim_run_block(run) for run in kept_runs]
                candidate = _build_compacted_content(long_term, compressed_runs, compacted_count)
            new_content = candidate
            break
        kept_count -= 1

    _atomic_write(path, new_content)
    return True
