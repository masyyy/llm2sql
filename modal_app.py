"""Modal vLLM server for Qwen3-4B — OpenAI-compatible endpoint."""

import subprocess
import modal

MODEL_NAME = "Qwen/Qwen3-4B"

vllm_image = (
    modal.Image.from_registry("nvidia/cuda:12.8.0-devel-ubuntu22.04", add_python="3.12")
    .entrypoint([])
    .uv_pip_install(
        "vllm>=0.8",
        "huggingface-hub[hf_xet]>=0.28.0",
        "transformers<4.52",
    )
)

app = modal.App("qwen3-4b-sql")

hf_cache = modal.Volume.from_name("huggingface-cache", create_if_missing=True)
vllm_cache = modal.Volume.from_name("vllm-cache", create_if_missing=True)

VLLM_PORT = 8000
MINUTES = 60


@app.function(
    image=vllm_image,
    gpu="H100",
    timeout=10 * MINUTES,
    volumes={
        "/root/.cache/huggingface": hf_cache,
        "/root/.cache/vllm": vllm_cache,
    },
)
@modal.concurrent(max_inputs=32)
@modal.web_server(port=VLLM_PORT, startup_timeout=10 * MINUTES)
def serve():
    cmd = [
        "vllm", "serve", MODEL_NAME,
        "--served-model-name", MODEL_NAME,
        "--host", "0.0.0.0",
        "--port", str(VLLM_PORT),
        "--dtype", "bfloat16",
        "--max-model-len", "4096",
    ]
    print("Starting vLLM:", " ".join(cmd))
    subprocess.Popen(cmd)
