"""Optional LLM narrative provider (Gemini or OpenAI).

This module ONLY generates the human-readable root-cause / diagnosis prose for
postmortems. It never decides the actual fix -- that is always computed
deterministically in ``diagnoser.py`` -- so the system heals identically with
or without an LLM.

Provider selection (auto):
  - GEMINI_API_KEY / GOOGLE_API_KEY set  -> Gemini
  - OPENAI_API_KEY set                   -> OpenAI
  - neither                              -> None (deterministic templates used)
Override with HOMEOSTAT_LLM_PROVIDER=gemini|openai.
"""

import json
import os

SYSTEM_PROMPT = (
    "You are an SRE for data pipelines. Given a machine-produced diagnosis and "
    "evidence, write a crisp root cause (1 sentence) and a diagnosis summary "
    "(1-2 sentences). Do NOT change or second-guess the proposed fix. "
    'Return strict JSON: {"root_cause": str, "diagnosis_summary": str}.'
)


def select_provider() -> str | None:
    forced = os.environ.get("HOMEOSTAT_LLM_PROVIDER")
    if forced:
        return forced.lower()
    if os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"):
        return "gemini"
    if os.environ.get("OPENAI_API_KEY"):
        return "openai"
    return None


def enrich_narrative(context: dict) -> dict | None:
    """Return {root_cause, diagnosis_summary, source} or None on any failure."""
    provider = select_provider()
    if provider == "gemini":
        return _gemini(context)
    if provider == "openai":
        return _openai(context)
    return None


def _gemini(context: dict) -> dict | None:
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    model_name = os.environ.get("HOMEOSTAT_LLM_MODEL", "gemini-2.5-flash")
    prompt = f"{SYSTEM_PROMPT}\n\nEvidence:\n{json.dumps(context, default=str)}"

    # Preferred: the modern `google-genai` SDK.
    try:  # pragma: no cover - network/LLM path
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)
        resp = client.models.generate_content(
            model=model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json", temperature=0
            ),
        )
        return _parse_narrative(resp.text)
    except Exception:
        pass

    # Fallback: the legacy `google-generativeai` SDK.
    try:  # pragma: no cover - network/LLM path
        import google.generativeai as genai_legacy

        genai_legacy.configure(api_key=api_key)
        model = genai_legacy.GenerativeModel(model_name, system_instruction=SYSTEM_PROMPT)
        resp = model.generate_content(
            json.dumps(context, default=str),
            generation_config={"response_mime_type": "application/json", "temperature": 0},
        )
        return _parse_narrative(resp.text)
    except Exception:
        return None


def _parse_narrative(text: str) -> dict | None:
    data = json.loads(text)
    return {
        "root_cause": data.get("root_cause"),
        "diagnosis_summary": data.get("diagnosis_summary"),
        "source": "gemini",
    }


def _openai(context: dict) -> dict | None:
    try:  # pragma: no cover - network/LLM path
        from openai import OpenAI

        client = OpenAI()
        resp = client.chat.completions.create(
            model=os.environ.get("HOMEOSTAT_LLM_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(context, default=str)},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        data = json.loads(resp.choices[0].message.content)
        return {
            "root_cause": data.get("root_cause"),
            "diagnosis_summary": data.get("diagnosis_summary"),
            "source": "openai",
        }
    except Exception:
        return None
