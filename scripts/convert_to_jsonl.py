#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import io
import json
import os
import sys
from typing import Any, Dict, List, Optional


Role = str
Message = Dict[str, str]
Record = Dict[str, Any]


def normalize_role(value: str) -> Optional[Role]:
    v = (value or "").strip().lower()
    if v in {"system", "user", "assistant"}:
        return v
    if v in {"human"}:
        return "user"
    return None


def normalize_messages(obj: Record) -> Optional[List[Message]]:
    # Case 1: Already in OpenAI messages format
    if isinstance(obj.get("messages"), list):
        messages: List[Message] = []
        for m in obj["messages"]:
            if not isinstance(m, dict):
                continue
            role = normalize_role(m.get("role", ""))
            content = m.get("content")
            if role and isinstance(content, str):
                messages.append({"role": role, "content": content})
        return messages if messages else None

    # Case 2: conversations with {from|role, value|content}
    conv = obj.get("conversations") or obj.get("conversation")
    if isinstance(conv, list):
        messages: List[Message] = []
        for m in conv:
            if not isinstance(m, dict):
                continue
            role = normalize_role(m.get("role") or m.get("from") or "")
            content = m.get("content") if isinstance(m.get("content"), str) else m.get("value")
            if role and isinstance(content, str):
                messages.append({"role": role, "content": content})
        return messages if messages else None

    # Case 3: Top-level keys system/user/assistant as strings
    keys = [k for k in ("system", "user", "assistant") if isinstance(obj.get(k), str)]
    if keys:
        messages: List[Message] = []
        if isinstance(obj.get("system"), str):
            messages.append({"role": "system", "content": obj["system"]})
        if isinstance(obj.get("user"), str):
            messages.append({"role": "user", "content": obj["user"]})
        if isinstance(obj.get("assistant"), str):
            messages.append({"role": "assistant", "content": obj["assistant"]})
        return messages if messages else None

    return None


def convert_record(
    obj: Record,
    default_rejected: str,
    map_user_field: Optional[str] = None,
    map_chosen_field: Optional[str] = None,
    map_reject_field: Optional[str] = None,
    system_text: Optional[str] = None,
) -> Optional[Record]:
    # If explicit mapping is provided and fields exist, use it
    if map_user_field or map_chosen_field or map_reject_field:
        user_text = obj.get(map_user_field) if map_user_field else None
        chosen_text = obj.get(map_chosen_field) if map_chosen_field else None
        reject_text = obj.get(map_reject_field) if map_reject_field else None

        if isinstance(user_text, str) and isinstance(chosen_text, str):
            messages: List[Message] = []
            st = system_text if isinstance(system_text, str) and system_text != "" else None
            if st:
                messages.append({"role": "system", "content": st})
            messages.append({"role": "user", "content": user_text})
            messages.append({"role": "assistant", "content": chosen_text})

            rejected = reject_text if isinstance(reject_text, str) else default_rejected
            return {"messages": messages, "rejected_response": rejected}
        # If mapping specified but required fields missing, skip this record
        return None

    # Fallback: try to normalize generic formats
    messages = normalize_messages(obj)
    if not messages:
        return None
    rejected = obj.get("rejected_response")
    if not isinstance(rejected, str):
        rejected = default_rejected
    return {"messages": messages, "rejected_response": rejected}


def iter_json_items_from_stream(stream: io.TextIOBase) -> Any:
    # Try to read as JSON Lines first
    for ln in stream:
        line = ln.strip()
        if not line:
            continue
        # Heuristically detect JSON array file if first non-empty line starts with [
        if line.startswith("["):
            # Read the whole file content (already read first line)
            rest = [line] + [l for l in stream]
            text = "".join(rest)
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                raise SystemExit("Input appears to be a JSON array but failed to parse it as JSON.")
            if isinstance(data, list):
                for item in data:
                    yield item
                return
            else:
                raise SystemExit("Top-level JSON must be a list when using array format.")
        else:
            # Treat as JSONL: current line is a JSON object
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise SystemExit(f"Failed to parse JSONL line: {e}\nLine: {line[:200]}")
            yield obj
    return


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert conversation JSON/JSONL formats to {messages, rejected_response} JSONL.")
    parser.add_argument("--input", "-i", required=False, default="-", help="Input file path (.jsonl or .json array). Use '-' for STDIN.")
    parser.add_argument("--output", "-o", required=False, default="-", help="Output file path (JSONL). Use '-' for STDOUT.")
    parser.add_argument("--default-rejected", required=False, default="我不知道", help="Default rejected_response value when missing.")

    # Explicit field mapping options (e.g., for zh/en DPO data)
    parser.add_argument("--user-field", required=False, default=None, help="Field name for the user query (e.g., 'question').")
    parser.add_argument("--chosen-field", required=False, default=None, help="Field name for the chosen assistant reply (e.g., 'answer_zh').")
    parser.add_argument("--reject-field", required=False, default=None, help="Field name for the rejected response (e.g., 'answer_en').")
    parser.add_argument("--system-text", required=False, default=None, help="Optional system prompt to include as first message.")

    args = parser.parse_args()

    # Ensure UTF-8 IO
    try:
        sys.stdin.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    # Input stream
    if args.input == "-":
        in_stream = sys.stdin
        close_in = False
    else:
        in_stream = open(args.input, "r", encoding="utf-8")
        close_in = True

    # Output stream
    if args.output == "-":
        out_stream = sys.stdout
        close_out = False
    else:
        # Create parent dir if needed
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        out_stream = open(args.output, "w", encoding="utf-8")
        close_out = True

    num_in = 0
    num_out = 0
    try:
        for item in iter_json_items_from_stream(in_stream):
            num_in += 1
            converted = convert_record(
                item,
                args.default_rejected,
                map_user_field=args.user_field,
                map_chosen_field=args.chosen_field,
                map_reject_field=args.reject_field,
                system_text=args.system_text if args.system_text is not None else None,
            )
            if not converted:
                continue
            out_stream.write(json.dumps(converted, ensure_ascii=False, separators=(",", ":")) + "\n")
            num_out += 1
    finally:
        if close_in:
            in_stream.close()
        if close_out:
            out_stream.close()

    if num_out == 0:
        # Provide a helpful message to STDERR but keep STDOUT clean
        print("No valid conversation records found to convert.", file=sys.stderr)


if __name__ == "__main__":
    main() 