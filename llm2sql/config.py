import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "benchmark.db"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
MODAL_QWEN_URL = os.getenv("MODAL_QWEN_URL", "")
MODAL_GRPO_URL = os.getenv("MODAL_GRPO_URL", "")
MODAL_SFT_URL = os.getenv("MODAL_SFT_URL", "")

OPENAI_MODEL = "gpt-5.2"
QWEN_MODEL = "Qwen/Qwen3-4B"
GRPO_MODEL = "qwen3-4b-grpo"
SFT_MODEL = "qwen3-4b-sft"

TODAY = "2026-03-04"
