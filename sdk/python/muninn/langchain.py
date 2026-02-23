"""LangChain memory integration for MuninnDB.

Provides MuninnDBMemory, a drop-in replacement for any LangChain memory backend.
Unlike traditional backends (ConversationBufferMemory, etc.), MuninnDB applies
cognitive primitives to retrieved context: relevance decays over time, frequently
recalled memories strengthen, and associations form automatically from co-activation.

Install:
    pip install muninn-python[langchain]

Usage:
    from muninn.langchain import MuninnDBMemory
    from langchain.chains import ConversationChain
    from langchain_anthropic import ChatAnthropic

    memory = MuninnDBMemory(vault="my-agent")
    chain = ConversationChain(llm=ChatAnthropic(model="claude-haiku-4-5-20251001"), memory=memory)
    chain.predict(input="What did we discuss about the payment service?")
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import textwrap
from typing import Any, Dict, List, Optional

try:
    from langchain_core.memory import BaseMemory
except ImportError:
    try:
        from langchain.memory import BaseMemory  # type: ignore[no-redef]
    except ImportError:
        try:
            from pydantic import BaseModel as BaseMemory  # type: ignore[no-redef,assignment]
        except ImportError:
            # Neither LangChain nor Pydantic is installed. Provide a minimal
            # stub so MuninnDBMemory can be imported and used standalone
            # (activation-only workflows, SDK demos).
            # Full LangChain chain integration requires:
            #   pip install muninn-python[langchain]
            class BaseMemory:  # type: ignore[no-redef]
                """Minimal stub: accepts field kwargs and sets them as attributes."""
                def __init__(self, **kwargs: object) -> None:
                    # Apply class-level field defaults (skip properties, methods,
                    # and descriptors which can't be set as instance attributes).
                    _skip = (property, classmethod, staticmethod)
                    for cls in reversed(type(self).__mro__):
                        for name, value in vars(cls).items():
                            if name.startswith("_"):
                                continue
                            if callable(value) or isinstance(value, _skip):
                                continue
                            object.__setattr__(self, name, value)
                    for key, value in kwargs.items():
                        object.__setattr__(self, key, value)

from .client import MuninnClient
from .types import ActivationItem


def _run_sync(coro: Any) -> Any:
    """Run an async coroutine synchronously.

    Handles both contexts where there is no event loop (plain scripts, pytest)
    and contexts where one is already running (FastAPI, Jupyter, async test runners).
    """
    try:
        asyncio.get_running_loop()
        # Already inside an event loop — run in a fresh thread with its own loop.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
    except RuntimeError:
        # No running event loop — safe to call asyncio.run() directly.
        return asyncio.run(coro)


class MuninnDBMemory(BaseMemory):
    """LangChain memory backend powered by MuninnDB.

    Each conversation turn is stored as a single engram (human input + AI output).
    On every load, MuninnDB activates the most relevant memories for the current
    input using semantic similarity, Hebbian association weights, and decay curves —
    returning only what is genuinely relevant right now, not a raw chat buffer.

    Attributes:
        base_url:       MuninnDB server URL (default: http://localhost:8475)
        token:          Optional Bearer token if MCP auth is enabled
        vault:          Vault name to store memories in (default: "default")
        max_results:    Max memories to surface per activation (default: 10)
        memory_key:     Key injected into chain inputs (default: "history")
        input_key:      Input dict key holding the human message. Auto-detected
                        if None (looks for "input", "question", "human_input", etc.)
        human_prefix:   Prefix for human turns in stored engrams (default: "Human")
        ai_prefix:      Prefix for AI turns in stored engrams (default: "AI")
        return_docs:    If True, return ActivationItem objects instead of a string.
                        Useful when you want to inspect scores or metadata.
    """

    base_url: str = "http://localhost:8475"
    token: Optional[str] = None
    vault: str = "default"
    max_results: int = 10
    memory_key: str = "history"
    input_key: Optional[str] = None
    human_prefix: str = "Human"
    ai_prefix: str = "AI"
    return_docs: bool = False

    class Config:
        arbitrary_types_allowed = True

    # ── Public LangChain interface ───────────────────────────────────────────

    @property
    def memory_variables(self) -> List[str]:
        return [self.memory_key]

    def load_memory_variables(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve relevant memories for the current input (synchronous)."""
        return _run_sync(self.aload_memory_variables(inputs))

    async def aload_memory_variables(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve relevant memories for the current input (async)."""
        query = self._extract_input(inputs)
        if not query:
            return {self.memory_key: [] if self.return_docs else ""}

        async with MuninnClient(self.base_url, token=self.token) as client:
            result = await client.activate(
                vault=self.vault,
                context=[query],
                max_results=self.max_results,
                threshold=0.05,
            )

        if self.return_docs:
            return {self.memory_key: result.activations}

        return {self.memory_key: self._format_activations(result.activations)}

    def save_context(self, inputs: Dict[str, Any], outputs: Dict[str, Any]) -> None:
        """Store the current conversation turn (synchronous)."""
        _run_sync(self.asave_context(inputs, outputs))

    async def asave_context(
        self, inputs: Dict[str, Any], outputs: Dict[str, Any]
    ) -> None:
        """Store the current conversation turn (async)."""
        human_input = self._extract_input(inputs) or ""
        ai_output = self._extract_output(outputs) or ""

        # Concept = first 60 chars of the human turn (readable in the Web UI).
        concept = (human_input[:57] + "...") if len(human_input) > 60 else human_input
        content = f"{self.human_prefix}: {human_input}\n{self.ai_prefix}: {ai_output}"

        async with MuninnClient(self.base_url, token=self.token) as client:
            await client.write(vault=self.vault, concept=concept, content=content)

    def clear(self) -> None:
        """No-op: MuninnDB uses natural decay rather than explicit truncation.

        Memories fade on their own as they stop being recalled. If you need
        a hard reset, create a new vault or use a different vault name per session.
        """

    # ── Internal helpers ────────────────────────────────────────────────────

    def _extract_input(self, inputs: Dict[str, Any]) -> Optional[str]:
        """Extract the human message from the chain's input dict."""
        if self.input_key:
            return str(inputs.get(self.input_key, ""))
        for key in ("input", "question", "human_input", "query", "message", "text"):
            if key in inputs:
                return str(inputs[key])
        # Fall back to first string value.
        for v in inputs.values():
            if isinstance(v, str):
                return v
        return None

    def _extract_output(self, outputs: Dict[str, Any]) -> Optional[str]:
        """Extract the AI response from the chain's output dict."""
        for key in ("output", "response", "answer", "text", "result", "generation"):
            if key in outputs:
                return str(outputs[key])
        for v in outputs.values():
            if isinstance(v, str):
                return v
        return None

    def _format_activations(self, activations: List[ActivationItem]) -> str:
        """Format activated memories as a context string for the LLM prompt."""
        if not activations:
            return ""

        lines = ["[Relevant memory context from MuninnDB]"]
        for item in activations:
            # Wrap long content so it's readable in prompts.
            wrapped = textwrap.fill(item.content, width=120, subsequent_indent="  ")
            lines.append(f"- {wrapped}")
        lines.append("[End of memory context]")
        return "\n".join(lines)
