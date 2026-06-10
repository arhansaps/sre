from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

BEDROCK_BASE_URL = os.getenv("BEDROCK_BASE_URL", "https://bedrock-mantle.eu-north-1.api.aws/v1")
BEDROCK_REGION = os.getenv("BEDROCK_REGION", "eu-north-1")
BEDROCK_PROJECT = os.getenv("BEDROCK_PROJECT") or ""

if not BEDROCK_PROJECT:
    raise RuntimeError("BEDROCK_PROJECT is not set in .env — add BEDROCK_PROJECT=proj_...")
AGENT_MODEL = os.getenv("AGENT_MODEL", "openai.gpt-oss-120b")
AGENT_MAX_ITERATIONS = int(os.getenv("AGENT_MAX_ITERATIONS", "10"))
AGENT_TEMPERATURE = float(os.getenv("AGENT_TEMPERATURE", "0"))
