"""
Shared OpenRouter LLM client with retry, parallel workers, and rate limiting.

Usage:
    from ufosint.llm.client import LLMClient

    client = LLMClient()
    response = client.call(messages)

    # Parallel batch processing:
    results = client.batch_process(
        items=my_items,
        build_prompt=lambda batch: [{"role": "user", "content": "..."}],
        parse_response=lambda text: json.loads(text),
        batch_size=25,
    )
"""

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from ufosint.config import Config

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


class LLMClient:
    """OpenRouter API client with parallel batch processing."""

    def __init__(self, model=None, workers=None, timeout=90):
        self.model = model or Config.llm_model()
        self.workers = workers or Config.llm_workers()
        self.timeout = timeout
        self._api_key = Config.openrouter_api_key()
        if not self._api_key:
            raise RuntimeError(
                "OPENROUTER_API_KEY not set. "
                "Export it before running LLM operations."
            )

    def call(self, messages, temperature=0.0, max_tokens=4096):
        """Single API call. Returns response content string."""
        import requests

        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self.model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]

    def call_and_parse(self, messages, **kwargs):
        """Call API and parse JSON from response."""
        text = self.call(messages, **kwargs)
        return parse_json_response(text)

    def batch_process(self, items, build_prompt, parse_response,
                      batch_size=None, on_batch_done=None):
        """Process items in parallel batches.

        Args:
            items: list of items to process
            build_prompt: fn(batch_items) -> messages list
            parse_response: fn(response_text) -> parsed results (list)
            batch_size: items per API call (default: Config.llm_batch_size())
            on_batch_done: fn(batch_items, parsed_results) called after each batch

        Returns:
            list of (item, result_or_None) tuples in original order
        """
        batch_size = batch_size or Config.llm_batch_size()
        batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

        all_results = [None] * len(items)

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {}
            for idx, batch in enumerate(batches):
                start_i = idx * batch_size
                future = executor.submit(self._process_one_batch,
                                         batch, build_prompt, parse_response)
                futures[future] = (idx, start_i, batch)

            for future in as_completed(futures):
                idx, start_i, batch = futures[future]
                try:
                    batch_results = future.result()
                except Exception:
                    batch_results = [None] * len(batch)

                for j, result in enumerate(batch_results):
                    if start_i + j < len(all_results):
                        all_results[start_i + j] = result

                if on_batch_done:
                    on_batch_done(batch, batch_results)

        return list(zip(items, all_results))

    def _process_one_batch(self, batch_items, build_prompt, parse_response):
        """Process a single batch (called in worker thread)."""
        messages = build_prompt(batch_items)
        try:
            text = self.call(messages)
            results = parse_response(text)
            if isinstance(results, list):
                # Pad if needed
                while len(results) < len(batch_items):
                    results.append(None)
                return results
        except Exception:
            pass
        return [None] * len(batch_items)


def parse_json_response(text):
    """Extract JSON from an LLM response, handling markdown fences."""
    if not text:
        return None
    text = text.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON array or object
        match = re.search(r'[\[{].*[\]}]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return None
