from __future__ import annotations

import json
from typing import Any

from openai import OpenAI

import core.config as config


def create_client() -> OpenAI:
    return OpenAI(api_key=config.OPENAI_API_KEY)


def generate_text(
    client: OpenAI,
    *,
    prompt: str,
    instructions: str | None = None,
    max_output_tokens: int | None = None,
) -> str:
    request: dict[str, Any] = {
        "model": config.OPENAI_MODEL,
        "input": prompt,
    }
    if instructions:
        request["instructions"] = instructions
    if max_output_tokens is not None:
        request["max_output_tokens"] = max_output_tokens

    response = client.responses.create(**request)
    text = (response.output_text or "").strip()
    if text:
        return text
    raise RuntimeError(_extract_refusal(response) or "OpenAI returned no text output.")


def generate_json(
    client: OpenAI,
    *,
    prompt: str,
    schema_name: str,
    schema: dict[str, Any],
    instructions: str | None = None,
    max_output_tokens: int | None = None,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "model": config.OPENAI_MODEL,
        "input": prompt,
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "strict": True,
                "schema": schema,
            }
        },
    }
    if instructions:
        request["instructions"] = instructions
    if max_output_tokens is not None:
        request["max_output_tokens"] = max_output_tokens

    response = client.responses.create(**request)
    text = (response.output_text or "").strip()
    if not text:
        raise RuntimeError(_extract_refusal(response) or "OpenAI returned no JSON output.")
    return json.loads(text)


def create_embedding(
    client: OpenAI,
    *,
    text: str,
    model: str,
    dimensions: int | None = None,
) -> list[float]:
    request: dict[str, Any] = {
        "input": text,
        "model": model,
    }
    if dimensions is not None:
        request["dimensions"] = dimensions

    response = client.embeddings.create(**request)
    return list(response.data[0].embedding)


def _extract_refusal(response: Any) -> str | None:
    for output in getattr(response, "output", []):
        if getattr(output, "type", None) != "message":
            continue
        for item in getattr(output, "content", []):
            if getattr(item, "type", None) == "refusal":
                return getattr(item, "refusal", None)
    return None
