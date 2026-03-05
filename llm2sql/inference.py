"""LLM inference backends for SQL generation."""

import re

from openai import OpenAI

from llm2sql.config import (
    OPENAI_API_KEY, MODAL_QWEN_URL, MODAL_GRPO_URL, MODAL_SFT_URL,
    OPENAI_MODEL, QWEN_MODEL, GRPO_MODEL, SFT_MODEL,
)


def extract_sql(text: str) -> str:
    """Extract SQL from model response, stripping thinking tags and markdown fences."""
    if not text:
        return ""
    # Strip <think>...</think> blocks (Qwen3 chain-of-thought)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    # Strip ```sql ... ``` or ``` ... ```
    match = re.search(r"```(?:sql)?\s*\n?(.*?)```", text, re.DOTALL)
    if match:
        text = match.group(1)
    # Take first statement if multiple
    text = text.strip()
    if ";" in text:
        text = text[: text.index(";") + 1]
    return text


def run_openai(system_prompt: str, question: str) -> tuple[str, str]:
    """Run query through OpenAI GPT model. Returns (extracted_sql, raw_response)."""
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
    )
    raw = response.choices[0].message.content or ""
    return extract_sql(raw), raw


def run_modal(system_prompt: str, question: str) -> tuple[str, str]:
    """Run query through Qwen3-4B on Modal via OpenAI-compatible API. Returns (extracted_sql, raw_response)."""
    client = OpenAI(base_url=MODAL_QWEN_URL, api_key="not-needed")
    response = client.chat.completions.create(
        model=QWEN_MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        extra_body={"chat_template_kwargs": {"enable_thinking": False}},
    )
    raw = response.choices[0].message.content or ""
    return extract_sql(raw), raw


def run_grpo(system_prompt: str, question: str) -> tuple[str, str]:
    """Run query through GRPO fine-tuned Qwen3-4B on Modal. Returns (extracted_sql, raw_response)."""
    client = OpenAI(base_url=MODAL_GRPO_URL, api_key="not-needed")
    response = client.chat.completions.create(
        model=GRPO_MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        extra_body={"chat_template_kwargs": {"enable_thinking": False}},
    )
    raw = response.choices[0].message.content or ""
    return extract_sql(raw), raw


def run_sft(system_prompt: str, question: str) -> tuple[str, str]:
    """Run query through SFT fine-tuned Qwen3-4B on Modal. Returns (extracted_sql, raw_response)."""
    client = OpenAI(base_url=MODAL_SFT_URL, api_key="not-needed")
    response = client.chat.completions.create(
        model=SFT_MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        extra_body={"chat_template_kwargs": {"enable_thinking": False}},
    )
    raw = response.choices[0].message.content or ""
    return extract_sql(raw), raw


BACKENDS = {
    "gpt-5.2": run_openai,
    "qwen3-4b": run_modal,
    "qwen3-4b-grpo": run_grpo,
    "qwen3-4b-sft": run_sft,
}
