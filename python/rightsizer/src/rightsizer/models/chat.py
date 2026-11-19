"""The base model's tokenizer and chat template without torch or transformers.

``tokenizers`` (``tokenizer.json``) encodes; ``jinja2`` renders the chat
template from ``tokenizer_config.json`` the way ``transformers``'
``apply_chat_template`` does (sandboxed environment, ``trim_blocks`` and
``lstrip_blocks``, ``loopcontrols``, its ``tojson`` filter, ``raise_exception``
and ``strftime_now``, the special-token strings as variables). Parity with
``transformers.AutoTokenizer`` is a test (``tests/models/test_chat.py``).

``transformers`` is used instead when asked (``prefer_transformers=True``).
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from typing import Any

TOKENIZER_FILES = ("tokenizer.json", "tokenizer_config.json")


def resolve_dir(repo_or_path: str, revision: str | None = None, files: Sequence[str] = TOKENIZER_FILES) -> str:
    """A local directory holding ``files``: ``repo_or_path`` itself, or a Hub
    snapshot of only those files."""
    if os.path.isdir(repo_or_path):
        return repo_or_path
    from huggingface_hub import snapshot_download

    return snapshot_download(repo_or_path, revision=revision, allow_patterns=list(files))


def _special(cfg: dict, key: str) -> str | None:
    v = cfg.get(key)
    if isinstance(v, dict):
        return v.get("content")
    return v


class ChatTokenizer:
    """Encode text and render chat messages like the base model's HF tokenizer."""

    def __init__(self, tokenizer: Any, chat_template: str, special: dict[str, str], hf: Any = None):
        self._tok = tokenizer
        self._hf = hf
        self.chat_template = chat_template
        self.special = special
        self._compiled = None

    @classmethod
    def from_pretrained(cls, repo_or_path: str, revision: str | None = None,
                        prefer_transformers: bool = False) -> ChatTokenizer:
        if prefer_transformers:
            from transformers import AutoTokenizer

            hf = AutoTokenizer.from_pretrained(repo_or_path, revision=revision)
            return cls(None, hf.chat_template, {}, hf=hf)
        from tokenizers import Tokenizer

        d = resolve_dir(repo_or_path, revision)
        tok = Tokenizer.from_file(os.path.join(d, "tokenizer.json"))
        with open(os.path.join(d, "tokenizer_config.json")) as f:
            cfg = json.load(f)
        template = cfg.get("chat_template")
        if template is None and os.path.exists(os.path.join(d, "chat_template.jinja")):
            with open(os.path.join(d, "chat_template.jinja")) as f:
                template = f.read()
        if isinstance(template, list):  # named templates: use "default"
            template = next((t["template"] for t in template if t.get("name") == "default"), template[0]["template"])
        if not template:
            raise ValueError(f"{repo_or_path}: no chat template")
        special = {k: v for k in ("bos_token", "eos_token", "unk_token", "pad_token", "sep_token", "cls_token",
                                  "mask_token") if (v := _special(cfg, k)) is not None}
        return cls(tok, template, special)

    # ── chat template ───────────────────────────────────────────────────

    def _template(self):
        if self._compiled is None:
            from datetime import datetime

            import jinja2
            from jinja2.ext import loopcontrols
            from jinja2.sandbox import ImmutableSandboxedEnvironment

            def raise_exception(message):
                raise jinja2.exceptions.TemplateError(message)

            def tojson(x, ensure_ascii=False, indent=None, separators=None, sort_keys=False):
                return json.dumps(x, ensure_ascii=ensure_ascii, indent=indent, separators=separators,
                                  sort_keys=sort_keys)

            def strftime_now(fmt):
                return datetime.now().strftime(fmt)

            env = ImmutableSandboxedEnvironment(trim_blocks=True, lstrip_blocks=True, extensions=[loopcontrols])
            env.filters["tojson"] = tojson
            env.globals["raise_exception"] = raise_exception
            env.globals["strftime_now"] = strftime_now
            self._compiled = env.from_string(self.chat_template)
        return self._compiled

    def apply_chat_template(self, messages: list[dict], add_generation_prompt: bool = True,
                            enable_thinking: bool = False) -> str:
        if self._hf is not None:
            return self._hf.apply_chat_template(messages, tokenize=False, add_generation_prompt=add_generation_prompt,
                                                enable_thinking=enable_thinking)
        return self._template().render(messages=messages, tools=None, documents=None,
                                       add_generation_prompt=add_generation_prompt, enable_thinking=enable_thinking,
                                       **self.special)

    # ── encoding ────────────────────────────────────────────────────────

    def encode(self, text: str) -> tuple[list[int], list[tuple[int, int]]]:
        """Token ids and character offsets, no special tokens added."""
        if self._hf is not None:
            enc = self._hf(text, add_special_tokens=False, return_offsets_mapping=True)
            return list(enc["input_ids"]), [tuple(x) for x in enc["offset_mapping"]]
        enc = self._tok.encode(text, add_special_tokens=False)
        return list(enc.ids), [tuple(x) for x in enc.offsets]

    def ids(self, text: str) -> list[int]:
        return self.encode(text)[0]


def option_offsets(tok: ChatTokenizer, chat_text: str, keys: Sequence[str],
                   options: Sequence[Sequence[str]]) -> list[list[int]]:
    """Offsets, counted back from the last prompt token, of each option's last
    token (``rightsize_pointer.option_offsets``, reimplemented). Every RightSize
    prompt ends with the same options block and answer line, so they are the
    same for every prompt one tokenizer encodes."""
    ids, spans = tok.encode(chat_text)
    n = len(ids)
    base = chat_text.rindex("### Options\n")

    def last_token_of(char_end: int) -> int:
        for i, (a, b) in enumerate(spans):
            if a < char_end <= b:
                return i
        raise ValueError(f"no token covers character {char_end}")

    out = []
    for key, opts in zip(keys, options):
        pos = chat_text.index(f"{key}: ", base) + len(key) + 2
        offs = []
        for opt in opts:
            pos = chat_text.index(opt, pos) + len(opt)
            offs.append(n - 1 - last_token_of(pos))
        out.append(offs)
    return out
