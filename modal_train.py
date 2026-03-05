"""GRPO and SFT fine-tuning of Qwen3-4B for SQL generation on Modal."""

import subprocess
import modal

MODEL_NAME = "Qwen/Qwen3-4B"
GRPO_MODEL_NAME = "qwen3-4b-grpo"
TRAINED_MODEL_PATH = f"/trained-models/{GRPO_MODEL_NAME}"
SFT_MODEL_NAME = "qwen3-4b-sft"
SFT_MODEL_PATH = f"/trained-models/{SFT_MODEL_NAME}"

# --- Images ---

train_image = (
    modal.Image.from_registry(
        "nvidia/cuda:12.8.0-devel-ubuntu22.04", add_python="3.12"
    )
    .entrypoint([])
    .uv_pip_install(
        "torch>=2.5",
        "transformers<4.52",
        "trl>=0.17",
        "peft>=0.15",
        "datasets",
        "accelerate",
        "huggingface-hub[hf_xet]>=0.28.0",
        "bitsandbytes",
    )
    .add_local_dir("llm2sql", remote_path="/root/llm2sql")
    .add_local_file("data/benchmark.db", remote_path="/root/data/benchmark.db")
)

vllm_image = (
    modal.Image.from_registry(
        "nvidia/cuda:12.8.0-devel-ubuntu22.04", add_python="3.12"
    )
    .entrypoint([])
    .uv_pip_install(
        "vllm>=0.8",
        "huggingface-hub[hf_xet]>=0.28.0",
        "transformers<4.52",
    )
)

# --- App & Volumes ---

app = modal.App("qwen3-4b-grpo")

hf_cache = modal.Volume.from_name("huggingface-cache", create_if_missing=True)
trained_models = modal.Volume.from_name("trained-models", create_if_missing=True)

MINUTES = 60
VLLM_PORT = 8000


# --- Training ---


@app.function(
    image=train_image,
    gpu="H100",
    timeout=90 * MINUTES,
    volumes={
        "/root/.cache/huggingface": hf_cache,
        "/trained-models": trained_models,
    },
)
def train():
    import sys
    sys.path.insert(0, "/root")

    import sqlite3
    import re
    import torch
    from datasets import Dataset
    from transformers import AutoTokenizer, AutoModelForCausalLM
    from peft import LoraConfig
    from trl import GRPOConfig, GRPOTrainer

    from llm2sql.training_queries import TRAINING_QUERIES
    from llm2sql.queries import QUERIES as EVAL_QUERIES
    from llm2sql.schema import DDL, BUSINESS_CONTEXT

    TODAY = "2026-03-04"

    DB_PATH = "/root/data/benchmark.db"

    # -- Build system prompt (same as prompt.py) --
    system_prompt = f"""\
You are an expert SQL analyst. You write SQLite-compatible SQL queries against the following database schema.

## Database Schema

{DDL}

## Business Context Definitions

{BUSINESS_CONTEXT}

## Rules

- Today's date is {TODAY}.
- Use SQLite date functions: DATE(), strftime(), JULIANDAY().
- Return ONLY the SQL query, no explanation or markdown."""

    # -- Build dataset (training + eval queries for overfit experiment) --
    all_queries = TRAINING_QUERIES + EVAL_QUERIES
    prompts = []
    for q in all_queries:
        prompts.append([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": q["question"]},
        ])

    dataset = Dataset.from_dict({
        "prompt": prompts,
        "ground_truth_sql": [q["sql"].strip() for q in all_queries],
    })

    print(f"Training dataset: {len(dataset)} examples")

    # -- Load model --
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    # Disable Qwen3 thinking mode: patch chat template to always set enable_thinking=False
    # so GRPOTrainer generates without <think>...</think> reasoning tokens
    original_apply = tokenizer.apply_chat_template.__func__ if hasattr(tokenizer.apply_chat_template, '__func__') else None
    if original_apply is None:
        _orig_apply = tokenizer.apply_chat_template
        def _patched_apply(*args, **kwargs):
            kwargs.setdefault("enable_thinking", False)
            return _orig_apply(*args, **kwargs)
    else:
        def _patched_apply(self_tok, *args, **kwargs):
            kwargs.setdefault("enable_thinking", False)
            return original_apply(self_tok, *args, **kwargs)
        import types
        _patched_apply = types.MethodType(_patched_apply, tokenizer)
    tokenizer.apply_chat_template = _patched_apply

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.bfloat16,
        attn_implementation="sdpa",
    )

    # -- LoRA config --
    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        target_modules="all-linear",
        task_type="CAUSAL_LM",
    )

    # -- Reward functions --

    def _extract_sql(text: str) -> str:
        """Extract SQL from model response."""
        if not text:
            return ""
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
        match = re.search(r"```(?:sql)?\s*\n?(.*?)```", text, re.DOTALL)
        if match:
            text = match.group(1)
        text = text.strip()
        if ";" in text:
            text = text[: text.index(";") + 1]
        return text

    def _execute_sql(sql: str) -> tuple[bool, list[list], list[str], str | None]:
        """Execute SQL against benchmark DB."""
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.execute(sql)
            columns = [d[0] for d in cursor.description] if cursor.description else []
            rows = [list(row) for row in cursor.fetchall()]
            conn.close()
            return True, rows, columns, None
        except Exception as e:
            return False, [], [], str(e)

    def _normalize_value(v):
        if v is None:
            return None
        if isinstance(v, float):
            return round(v, 2)
        return v

    def _normalize_rows(rows):
        return sorted(tuple(_normalize_value(v) for v in row) for row in rows)

    def _compare_results(gt_rows, gen_rows, gt_cols, gen_cols):
        """Simplified compare_results matching evaluate.py logic."""
        if not gt_rows:
            return 1.0 if not gen_rows else 0.0

        gt_norm = _normalize_rows(gt_rows)
        gen_norm = _normalize_rows(gen_rows)
        if gt_norm == gen_norm:
            return 1.0

        # Column mapping by lowercase name
        mapping = []
        used = set()
        for gi, gc in enumerate(gt_cols):
            for geni, genc in enumerate(gen_cols):
                if geni not in used and gc.lower().strip() == genc.lower().strip():
                    mapping.append((gi, geni))
                    used.add(geni)
                    break

        if not mapping:
            return 0.0

        gt_proj = sorted(
            tuple(_normalize_value(row[i]) for i, _ in mapping) for row in gt_rows
        )
        gen_proj = sorted(
            tuple(_normalize_value(row[j]) for _, j in mapping) for row in gen_rows
        )

        if gt_proj == gen_proj:
            return 1.0

        # Partial match
        gen_set = set(gen_proj)
        matched = sum(1 for r in gt_proj if r in gen_set)
        ratio = matched / len(gt_proj)
        if ratio > 0.5:
            return 0.5
        return 0.0

    def sql_format_reward(completions, **kwargs):
        """Reward for output starting with SELECT."""
        rewards = []
        for completion in completions:
            text = completion[0]["content"] if isinstance(completion, list) else completion
            sql = _extract_sql(text).strip().upper()
            rewards.append(0.25 if sql.startswith("SELECT") else 0.0)
        return rewards

    def sql_execution_reward(completions, **kwargs):
        """Reward for valid executable SQL."""
        rewards = []
        for completion in completions:
            text = completion[0]["content"] if isinstance(completion, list) else completion
            sql = _extract_sql(text)
            if not sql:
                rewards.append(0.0)
                continue
            ok, _, _, _ = _execute_sql(sql)
            rewards.append(0.5 if ok else 0.0)
        return rewards

    def sql_correctness_reward(completions, ground_truth_sql, **kwargs):
        """Reward for matching ground truth results."""
        rewards = []
        for completion, gt_sql in zip(completions, ground_truth_sql):
            text = completion[0]["content"] if isinstance(completion, list) else completion
            gen_sql = _extract_sql(text)
            if not gen_sql:
                rewards.append(0.0)
                continue

            gt_ok, gt_rows, gt_cols, _ = _execute_sql(gt_sql)
            gen_ok, gen_rows, gen_cols, _ = _execute_sql(gen_sql)

            if not gen_ok or not gt_ok:
                rewards.append(0.0)
                continue

            score = _compare_results(gt_rows, gen_rows, gt_cols, gen_cols)
            rewards.append(score)
        return rewards

    # -- GRPO config --
    training_args = GRPOConfig(
        output_dir="/tmp/grpo_output",
        num_generations=8,
        per_device_train_batch_size=1,
        gradient_accumulation_steps=8,
        num_train_epochs=2,
        learning_rate=5e-6,
        beta=0.0,
        bf16=True,
        logging_steps=1,
        save_strategy="no",
        max_completion_length=512,
        max_prompt_length=3072,
        report_to="none",
    )

    # -- Trainer --
    trainer = GRPOTrainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
        reward_funcs=[
            sql_format_reward,
            sql_execution_reward,
            sql_correctness_reward,
        ],
        peft_config=lora_config,
        processing_class=tokenizer,
    )

    print("Starting GRPO training...")
    trainer.train()
    print("Training complete!")

    # -- Merge LoRA and save --
    print("Merging LoRA weights...")
    merged_model = trainer.model.merge_and_unload()
    merged_model.save_pretrained(TRAINED_MODEL_PATH)
    tokenizer.save_pretrained(TRAINED_MODEL_PATH)

    trained_models.commit()
    print(f"Merged model saved to {TRAINED_MODEL_PATH}")


# --- SFT Training ---


@app.function(
    image=train_image,
    gpu="H100",
    timeout=90 * MINUTES,
    volumes={
        "/root/.cache/huggingface": hf_cache,
        "/trained-models": trained_models,
    },
)
def sft_train():
    import sys
    sys.path.insert(0, "/root")

    import torch
    from datasets import Dataset
    from transformers import AutoTokenizer, AutoModelForCausalLM
    from peft import LoraConfig
    from trl import SFTConfig, SFTTrainer

    from llm2sql.training_queries import TRAINING_QUERIES
    from llm2sql.queries import QUERIES as EVAL_QUERIES
    from llm2sql.schema import DDL, BUSINESS_CONTEXT

    TODAY = "2026-03-04"

    # -- Build system prompt --
    system_prompt = f"""\
You are an expert SQL analyst. You write SQLite-compatible SQL queries against the following database schema.

## Database Schema

{DDL}

## Business Context Definitions

{BUSINESS_CONTEXT}

## Rules

- Today's date is {TODAY}.
- Use SQLite date functions: DATE(), strftime(), JULIANDAY().
- Return ONLY the SQL query, no explanation or markdown."""

    # -- Build dataset: conversational format with ground truth as assistant response --
    all_queries = TRAINING_QUERIES + EVAL_QUERIES
    conversations = []
    for q in all_queries:
        conversations.append([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": q["question"]},
            {"role": "assistant", "content": q["sql"].strip()},
        ])

    dataset = Dataset.from_dict({"messages": conversations})
    print(f"SFT dataset: {len(dataset)} examples")

    # -- Load model --
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    # Disable Qwen3 thinking mode
    original_apply = tokenizer.apply_chat_template.__func__ if hasattr(tokenizer.apply_chat_template, '__func__') else None
    if original_apply is None:
        _orig_apply = tokenizer.apply_chat_template
        def _patched_apply(*args, **kwargs):
            kwargs.setdefault("enable_thinking", False)
            return _orig_apply(*args, **kwargs)
    else:
        def _patched_apply(self_tok, *args, **kwargs):
            kwargs.setdefault("enable_thinking", False)
            return original_apply(self_tok, *args, **kwargs)
        import types
        _patched_apply = types.MethodType(_patched_apply, tokenizer)
    tokenizer.apply_chat_template = _patched_apply

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        torch_dtype=torch.bfloat16,
        attn_implementation="sdpa",
    )

    # -- LoRA config --
    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        target_modules="all-linear",
        task_type="CAUSAL_LM",
    )

    # -- SFT config --
    training_args = SFTConfig(
        output_dir="/tmp/sft_output",
        per_device_train_batch_size=4,
        gradient_accumulation_steps=4,
        num_train_epochs=10,
        learning_rate=2e-4,
        bf16=True,
        logging_steps=1,
        save_strategy="no",
        max_seq_length=3072,
        report_to="none",
    )

    # -- Trainer --
    trainer = SFTTrainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
        peft_config=lora_config,
        processing_class=tokenizer,
    )

    print("Starting SFT training...")
    trainer.train()
    print("SFT training complete!")

    # -- Merge LoRA and save --
    print("Merging LoRA weights...")
    merged_model = trainer.model.merge_and_unload()
    merged_model.save_pretrained(SFT_MODEL_PATH)
    tokenizer.save_pretrained(SFT_MODEL_PATH)

    trained_models.commit()
    print(f"Merged model saved to {SFT_MODEL_PATH}")


# --- Serving ---


@app.function(
    image=vllm_image,
    gpu="H100",
    timeout=10 * MINUTES,
    volumes={
        "/root/.cache/huggingface": hf_cache,
        "/trained-models": trained_models,
    },
)
@modal.concurrent(max_inputs=32)
@modal.web_server(port=VLLM_PORT, startup_timeout=10 * MINUTES)
def serve_grpo():
    cmd = [
        "vllm", "serve", TRAINED_MODEL_PATH,
        "--served-model-name", GRPO_MODEL_NAME,
        "--host", "0.0.0.0",
        "--port", str(VLLM_PORT),
        "--dtype", "bfloat16",
        "--max-model-len", "4096",
    ]
    print("Starting vLLM:", " ".join(cmd))
    subprocess.Popen(cmd)


@app.function(
    image=vllm_image,
    gpu="H100",
    timeout=10 * MINUTES,
    volumes={
        "/root/.cache/huggingface": hf_cache,
        "/trained-models": trained_models,
    },
)
@modal.concurrent(max_inputs=32)
@modal.web_server(port=VLLM_PORT, startup_timeout=10 * MINUTES)
def serve_sft():
    cmd = [
        "vllm", "serve", SFT_MODEL_PATH,
        "--served-model-name", SFT_MODEL_NAME,
        "--host", "0.0.0.0",
        "--port", str(VLLM_PORT),
        "--dtype", "bfloat16",
        "--max-model-len", "4096",
    ]
    print("Starting vLLM:", " ".join(cmd))
    subprocess.Popen(cmd)
