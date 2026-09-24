"""Shared fixtures and client setup for the pinned Agents API SDK checks."""

import copy
import json
from pathlib import Path

import openai


SDK_VERSION = "3.17.0"
TOKEN = "test-token-for-the-local-agents-api"
FIXTURES = Path(__file__).with_name("fixtures") / "sdk_contract.json"


def require_pinned_sdk():
    assert openai.__version__ == SDK_VERSION, openai.__version__


def fixture(name):
    with FIXTURES.open(encoding="utf-8") as handle:
        return copy.deepcopy(json.load(handle)[name])


def client(base_url):
    require_pinned_sdk()
    return openai.OpenAI(
        base_url=base_url,
        api_key=TOKEN,
        max_retries=0,
        timeout=20,
        _strict_response_validation=True,
    )


def message(text):
    event = fixture("message_event")
    event["input"][0]["content"][0]["text"] = text
    return event
