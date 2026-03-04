#!/usr/bin/env python3
"""
update_docs.py
──────────────
Triggered by GitHub Actions when a PR merges to main/master.

What it does:
  1. Fetches the PR diff and changed-files list from the GitHub API.
  2. Passes the current README + PR context to Claude to produce an
     intelligently updated README (strategy chosen by Claude: targeted edits,
     section additions, or full rewrite depending on the scope of changes).
  3. Appends a new entry to CHANGELOG.md following Keep a Changelog format.
  4. Writes both files — the workflow then commits and pushes them.

Required env vars (all injected by the GitHub Actions workflow):
  ANTHROPIC_API_KEY, GITHUB_TOKEN, GITHUB_REPOSITORY,
  PR_NUMBER, PR_TITLE, PR_BODY, PR_AUTHOR, PR_MERGED_AT

Truncation safety
─────────────────
Claude responses are checked for stop_reason. If a response is cut off
mid-output (stop_reason == "max_tokens"), the agent automatically sends a
continuation request ("continue where you left off") and stitches the chunks
together — up to MAX_CONTINUATIONS times.

If the response is *still* incomplete after all continuations, the original
file is left untouched rather than writing a truncated result. A clear
warning is printed to the Actions log.
"""

from __future__ import annotations

import contextlib
import os
import random
import sys
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, TypeVar

import requests
import anthropic

F = TypeVar("F", bound=Callable[..., Any])

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

MODEL             = "claude-sonnet-4-5-20250929"  # Best reasoning for doc work
MAX_DIFF_CHARS    = 18_000                       # ~4-5k tokens; keeps prompts lean
MAX_CONTINUATIONS = 4                            # Retry budget for truncated responses
README_PATH       = "README.md"
CHANGELOG_PATH    = "CHANGELOG.md"

# Retry / resilience
MAX_RETRIES       = 4                            # Max attempts for any network call
RETRY_BASE_DELAY  = 2.0                          # Seconds; doubles each attempt
RETRY_MAX_DELAY   = 60.0                         # Cap on backoff delay
# HTTP status codes that are permanent failures — never retry these
_NO_RETRY_STATUS  = {400, 401, 403, 404, 422}

# ──────────────────────────────────────────────────────────────────────────────
# Retry infrastructure
# ──────────────────────────────────────────────────────────────────────────────

def _jittered_backoff(attempt: int) -> float:
    """Exponential backoff with full jitter: uniform(0, min(cap, base * 2^attempt))."""
    ceiling = min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2 ** attempt))
    return random.uniform(0, ceiling)


def _retry_after_seconds(response: Any) -> float | None:
    """
    Read the Retry-After header (integer seconds or HTTP-date).
    Returns None if absent or unparseable.
    """
    if response is None:
        return None
    header = (
        getattr(response, "headers", {}).get("Retry-After")
        or getattr(response, "headers", {}).get("retry-after")
    )
    if header is None:
        return None
    try:
        return max(0.0, float(header))
    except ValueError:
        return None  # HTTP-date format; just fall back to backoff


def with_retries(fn: F) -> F:
    """
    Decorator that retries a function on transient network / API failures.

    Handles:
      • requests: ConnectionError, Timeout, HTTPError (5xx / 429 only)
      • anthropic: APIConnectionError, APITimeoutError, RateLimitError,
                   APIStatusError (5xx only)

    Permanent errors (4xx except 429) are re-raised immediately without retry.
    Each retry sleeps for jittered exponential backoff, honouring Retry-After
    headers when present.
    """
    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        last_exc: BaseException | None = None

        for attempt in range(MAX_RETRIES + 1):
            try:
                return fn(*args, **kwargs)

            # ── requests errors ───────────────────────────────────────────────
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status in _NO_RETRY_STATUS:
                    raise  # Permanent — don't retry
                delay = _retry_after_seconds(exc.response) or _jittered_backoff(attempt)
                last_exc = exc

            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as exc:
                delay = _jittered_backoff(attempt)
                last_exc = exc

            # ── anthropic errors ──────────────────────────────────────────────
            except anthropic.RateLimitError as exc:
                delay = _retry_after_seconds(exc.response) or _jittered_backoff(attempt)
                last_exc = exc

            except (anthropic.APIConnectionError,
                    anthropic.APITimeoutError) as exc:
                delay = _jittered_backoff(attempt)
                last_exc = exc

            except anthropic.APIStatusError as exc:
                if exc.status_code in _NO_RETRY_STATUS:
                    raise  # Permanent
                delay = _retry_after_seconds(exc.response) or _jittered_backoff(attempt)
                last_exc = exc

            # ── retry or give up ──────────────────────────────────────────────
            if attempt < MAX_RETRIES:
                print(
                    f"    ⟳  {fn.__name__} failed "
                    f"({type(last_exc).__name__}: {last_exc}), "
                    f"retrying in {delay:.1f}s "
                    f"[attempt {attempt + 1}/{MAX_RETRIES}]…"
                )
                time.sleep(delay)
            else:
                print(
                    f"    ❌  {fn.__name__} failed after {MAX_RETRIES} retries. "
                    f"Last error: {last_exc}"
                )
                raise last_exc  # type: ignore[misc]

    return wrapper  # type: ignore[return-value]


# ──────────────────────────────────────────────────────────────────────────────
# GitHub API helpers
# ──────────────────────────────────────────────────────────────────────────────

def _gh_headers(token: str, accept: str = "application/vnd.github.v3+json") -> dict:
    return {"Authorization": f"token {token}", "Accept": accept}


@with_retries
def get_pr_diff(repo: str, pr_number: int, token: str) -> str:
    """Return the unified diff for the PR."""
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"
    resp = requests.get(url, headers=_gh_headers(token, "application/vnd.github.v3.diff"), timeout=30)
    resp.raise_for_status()
    return resp.text


@with_retries
def _get_pr_files_page(repo: str, pr_number: int, token: str, page: int) -> list[dict]:
    """Fetch a single page of PR files (retried individually so pagination restarts cleanly)."""
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files?per_page=100&page={page}"
    resp = requests.get(url, headers=_gh_headers(token), timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_pr_files(repo: str, pr_number: int, token: str) -> list[dict]:
    """Return all files changed in the PR (handles pagination; each page is retried)."""
    files, page = [], 1
    while True:
        batch = _get_pr_files_page(repo, pr_number, token, page)
        if not batch:
            break
        files.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return files


# ──────────────────────────────────────────────────────────────────────────────
# File helpers
# ──────────────────────────────────────────────────────────────────────────────

def read_file(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return ""


def write_file_atomic(path: str, content: str) -> None:
    """
    Write atomically: write to a sibling .tmp file then rename into place.
    os.replace() is atomic on POSIX (same filesystem) so the target is never
    left in a partially-written state even if the process is killed mid-write.
    """
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Claude helpers
# ──────────────────────────────────────────────────────────────────────────────

def _api_create(client: anthropic.Anthropic, **kwargs: Any) -> Any:
    """
    Single Anthropic API call wrapped with the retry decorator.

    Separated so that call_claude can retry *individual* API calls inside the
    continuation loop without resetting accumulated text.
    """
    @with_retries
    def _call() -> Any:
        return client.messages.create(**kwargs)
    return _call()


def call_claude(
    client: anthropic.Anthropic,
    system: str,
    user: str,
    max_tokens: int = 8192,
) -> tuple[str, bool]:
    """
    Call Claude and handle max_tokens truncation via a continuation loop.
    Each individual API call is independently retried on transient failures
    without resetting accumulated text.

    Returns:
        (text, completed)
          text      — the full (possibly stitched) response text
          completed — True if Claude finished naturally, False if we exhausted
                      all continuation attempts and the output may be incomplete
    """
    messages: list[dict] = [{"role": "user", "content": user}]
    accumulated = ""

    for continuation in range(MAX_CONTINUATIONS + 1):
        # Each call here is independently retried by _api_create on transient errors
        msg = _api_create(
            client,
            model=MODEL,
            max_tokens=max_tokens,
            system=system,
            messages=messages,
        )

        chunk = msg.content[0].text
        accumulated += chunk

        if msg.stop_reason == "end_turn":
            return accumulated.strip(), True

        # stop_reason == "max_tokens": output was cut off
        if continuation < MAX_CONTINUATIONS:
            print(
                f"    ⚠️  Response truncated at ~{len(accumulated):,} chars "
                f"(continuation {continuation + 1}/{MAX_CONTINUATIONS}). "
                "Requesting continuation…"
            )
            messages.append({"role": "assistant", "content": chunk})
            messages.append({
                "role": "user",
                "content": "Please continue exactly where you left off. Do not repeat anything.",
            })
        else:
            print(
                f"    ❌  Response still incomplete after {MAX_CONTINUATIONS} "
                "continuation attempts. Output is likely truncated."
            )
            return accumulated.strip(), False

    return accumulated.strip(), False  # satisfy type checker


# ──────────────────────────────────────────────────────────────────────────────
# Build PR context block (shared between both prompts)
# ──────────────────────────────────────────────────────────────────────────────

def build_pr_context(
    pr_number: int,
    pr_title: str,
    pr_body: str,
    pr_author: str,
    pr_merged_at: str,
    pr_files: list[dict],
    pr_diff: str,
) -> str:
    # Human-readable file table
    file_lines = []
    for f in pr_files:
        status    = f.get("status", "modified")
        filename  = f.get("filename", "")
        additions = f.get("additions", 0)
        deletions = f.get("deletions", 0)
        file_lines.append(f"  [{status:9s}] {filename}  (+{additions} / -{deletions})")
    files_summary = "\n".join(file_lines) or "  (no file data)"

    # Truncate diff if necessary
    truncated = len(pr_diff) > MAX_DIFF_CHARS
    diff_excerpt = pr_diff[:MAX_DIFF_CHARS]
    if truncated:
        diff_excerpt += (
            f"\n\n... [diff truncated — showing first {MAX_DIFF_CHARS:,} of "
            f"{len(pr_diff):,} total characters] ..."
        )

    return f"""### PR #{pr_number}: {pr_title}

- **Author:** @{pr_author}
- **Merged:** {pr_merged_at}

**Description:**
{pr_body.strip() or "_No description provided._"}

**Files changed ({len(pr_files)} files):**
```
{files_summary}
```

**Diff:**
```diff
{diff_excerpt}
```"""


# ──────────────────────────────────────────────────────────────────────────────
# README updater
# ──────────────────────────────────────────────────────────────────────────────

README_SYSTEM = """\
You are a senior technical writer responsible for keeping a project's README
accurate and up-to-date whenever code changes are merged.

## Update strategy — choose based on scope of the PR:

| Scope | Strategy |
|---|---|
| Bug fixes, dependency bumps, small refactors, minor tweaks | **Targeted edit** — only change the specific sentences or lines that are now inaccurate. Leave everything else untouched. |
| New feature, new API/endpoint, new config option, new dependency, new script | **Section update** — add or expand the relevant section(s): Features, Usage, Configuration, API Reference, etc. |
| Major architectural change, new tech stack, significant restructure/redesign | **Full rewrite** — rewrite the entire README so it accurately reflects the new state. Preserve the project's existing tone and voice. |

## Hard rules:
1. Never invent information that cannot be inferred from the PR changes.
2. If something is removed in the PR, remove its documentation too.
3. Do NOT add a "Last updated" timestamp, bot watermark, or any meta-commentary.
4. Preserve the README's formatting style (badges, code blocks, headings hierarchy, etc.).
5. If the README needs absolutely no changes, respond with exactly the string: NO_CHANGES_NEEDED
6. Otherwise return ONLY the complete updated README — no preamble, no code fences around the whole thing, no commentary.
"""

def update_readme(
    client: anthropic.Anthropic,
    current_readme: str,
    pr_context: str,
) -> str | None:
    """
    Returns the updated README string, or None if no changes are needed
    OR if the output was truncated (original file is preserved in that case).
    """
    if not current_readme:
        system = README_SYSTEM + (
            "\n\nNote: There is currently NO README in this repository. "
            "Create a comprehensive one based on the PR context and the files changed."
        )
        user = f"No existing README.\n\n## PR Being Merged\n\n{pr_context}"
    else:
        system = README_SYSTEM
        user = (
            f"## Current README\n\n{current_readme}"
            f"\n\n---\n\n## PR Being Merged\n\n{pr_context}"
        )

    result, completed = call_claude(client, system, user, max_tokens=8192)

    if not completed:
        # Truncation guard: do NOT write a partial README — preserve the original.
        print(
            "    🛡️  Truncation guard: README.md was NOT written to disk.\n"
            "           The original file is preserved. Re-run the Action manually\n"
            "           or split this PR into smaller changes to retry."
        )
        return None

    # If Claude decided no changes are needed it returns this sentinel exactly
    if result.strip() == "NO_CHANGES_NEEDED":
        return None

    return result


# ──────────────────────────────────────────────────────────────────────────────
# CHANGELOG updater
# ──────────────────────────────────────────────────────────────────────────────

CHANGELOG_HEADER = """\
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]
"""

CHANGELOG_SYSTEM = """\
You maintain a CHANGELOG.md that follows the Keep a Changelog format
(https://keepachangelog.com/en/1.0.0/).

## Your task
Insert a new dated entry for the PR described below.

## Entry format
```
## [{DATE}] — PR #{NUMBER} by @{AUTHOR}

### Added        ← only include categories that have relevant entries
- ...

### Changed
- ...

### Fixed
- ...

### Removed
- ...

### Security
- ...

### Deprecated
- ...
```

## Rules
1. The entry date, PR number, and author are provided — use them exactly.
2. Only include Keep-a-Changelog categories that actually apply to this PR.
   Omit empty categories entirely.
3. Each bullet should be a clear, specific, human-readable sentence (not commit-speak).
4. Insert the new entry BELOW the `## [Unreleased]` header (create it if missing)
   and ABOVE any previous dated entries, maintaining reverse-chronological order.
5. Return ONLY the complete updated CHANGELOG.md — no commentary, no code fences.
"""

def _safe_changelog_entry(
    current_changelog: str,
    pr_number: int,
    pr_title: str,
    pr_author: str,
    date_str: str,
) -> str:
    """
    Minimal fallback CHANGELOG entry used when the Claude call is truncated.
    Inserts a stub entry so the merge is at least recorded, even without full detail.
    """
    stub = (
        f"\n## [{date_str}] — PR #{pr_number} by @{pr_author}\n\n"
        f"### Changed\n"
        f"- {pr_title} _(auto-summary unavailable; Claude response was truncated)_\n"
    )
    # Insert below [Unreleased] header if present, else prepend
    if "## [Unreleased]" in current_changelog:
        return current_changelog.replace(
            "## [Unreleased]",
            f"## [Unreleased]{stub}",
            1,
        )
    return stub.lstrip("\n") + "\n" + current_changelog


def update_changelog(
    client: anthropic.Anthropic,
    current_changelog: str,
    pr_context: str,
    pr_number: int,
    pr_title: str,
    pr_author: str,
    pr_merged_at: str,
) -> str:
    """
    Returns an updated CHANGELOG string.
    If the Claude call is truncated beyond recovery, writes a safe stub entry
    so the merge is at least recorded with its date and PR number.
    """
    # Parse the merge date
    try:
        dt = datetime.fromisoformat(pr_merged_at.replace("Z", "+00:00"))
        date_str = dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if not current_changelog:
        current_changelog = CHANGELOG_HEADER

    system = CHANGELOG_SYSTEM.replace("{DATE}", date_str)

    user = (
        f"Entry metadata:\n"
        f"  Date:   {date_str}\n"
        f"  PR:     #{pr_number}\n"
        f"  Author: @{pr_author}\n\n"
        f"## Current CHANGELOG.md\n\n{current_changelog}"
        f"\n\n---\n\n## PR Being Merged\n\n{pr_context}"
    )

    result, completed = call_claude(client, system, user, max_tokens=4096)

    if not completed:
        print(
            "    ⚠️  CHANGELOG Claude response was truncated. "
            "Writing a safe stub entry instead so the merge is still recorded."
        )
        return _safe_changelog_entry(current_changelog, pr_number, pr_title, pr_author, date_str)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    # ── Load env vars ─────────────────────────────────────────────────────────
    token       = os.environ["GITHUB_TOKEN"]
    api_key     = os.environ["ANTHROPIC_API_KEY"]
    repo        = os.environ["GITHUB_REPOSITORY"]
    pr_number   = int(os.environ["PR_NUMBER"])
    pr_title    = os.environ["PR_TITLE"]
    pr_body     = os.environ.get("PR_BODY") or ""
    pr_author   = os.environ["PR_AUTHOR"]
    pr_merged_at = os.environ["PR_MERGED_AT"]

    print(f"\n{'═' * 60}")
    print(f" 📝  Doc agent — PR #{pr_number}: {pr_title}")
    print(f"      Repo: {repo}  |  Author: @{pr_author}  |  Merged: {pr_merged_at}")
    print(f"{'═' * 60}\n")

    # Anthropic client with an explicit connect + read timeout
    client = anthropic.Anthropic(api_key=api_key, timeout=120.0)

    # ── Fetch PR data (failures are independent — degrade gracefully) ─────────
    print("📡  Fetching PR diff from GitHub API…")
    pr_diff: str = ""
    try:
        pr_diff = get_pr_diff(repo, pr_number, token)
        print(f"    → diff is {len(pr_diff):,} chars")
    except Exception as exc:
        print(
            f"    ⚠️  Could not fetch PR diff after {MAX_RETRIES} retries "
            f"({type(exc).__name__}: {exc}). Proceeding without diff.",
            file=sys.stderr,
        )

    print("📡  Fetching PR file list from GitHub API…")
    pr_files: list[dict] = []
    try:
        pr_files = get_pr_files(repo, pr_number, token)
        print(f"    → {len(pr_files)} files changed")
    except Exception as exc:
        print(
            f"    ⚠️  Could not fetch PR file list after {MAX_RETRIES} retries "
            f"({type(exc).__name__}: {exc}). Proceeding without file list.",
            file=sys.stderr,
        )

    # If we have neither diff nor file list the context is too thin to be useful
    if not pr_diff and not pr_files:
        print(
            "❌  Both diff and file list unavailable. Cannot produce meaningful "
            "doc updates. Exiting without changes.",
            file=sys.stderr,
        )
        sys.exit(1)

    pr_context = build_pr_context(
        pr_number, pr_title, pr_body, pr_author, pr_merged_at, pr_files, pr_diff
    )

    # ── README ────────────────────────────────────────────────────────────────
    print("\n🤖  Asking Claude to update README.md…")
    current_readme = read_file(README_PATH)
    try:
        updated_readme = update_readme(client, current_readme, pr_context)
        if updated_readme is not None:
            write_file_atomic(README_PATH, updated_readme)
            print("✅  README.md updated.")
        else:
            print("ℹ️   README.md — no changes needed for this PR.")
    except Exception as exc:
        print(
            f"    ❌  README update failed permanently: {exc}\n"
            "           Original README.md is untouched.",
            file=sys.stderr,
        )

    # ── CHANGELOG ────────────────────────────────────────────────────────────
    print("\n🤖  Asking Claude to update CHANGELOG.md…")
    current_changelog = read_file(CHANGELOG_PATH)
    try:
        updated_changelog = update_changelog(
            client, current_changelog, pr_context,
            pr_number, pr_title, pr_author, pr_merged_at,
        )
        write_file_atomic(CHANGELOG_PATH, updated_changelog)
        print("✅  CHANGELOG.md updated.")
    except Exception as exc:
        print(
            f"    ❌  CHANGELOG update failed permanently: {exc}\n"
            "           Writing minimal stub entry to preserve merge record.",
            file=sys.stderr,
        )
        # Last-resort: record the merge with no AI detail at all
        try:
            dt = datetime.fromisoformat(pr_merged_at.replace("Z", "+00:00"))
            date_str = dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        stub = _safe_changelog_entry(
            current_changelog, pr_number, pr_title, pr_author, date_str
        )
        write_file_atomic(CHANGELOG_PATH, stub)
        print("    📝  Minimal stub entry written to CHANGELOG.md.")

    print(f"\n{'═' * 60}")
    print(" 🎉  Done. Workflow will now commit any file changes.")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()
