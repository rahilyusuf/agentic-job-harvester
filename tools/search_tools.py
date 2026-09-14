"""tools/search_tools.py — google_search and fetch_url tools for CompanyResearchAgent.

These are ADK-compatible async tool functions. CompanyResearchAgent (ReAct loop,
max 5 iterations) uses these as its primary web-research tools.

ADK tools are plain async functions annotated with type hints and docstrings.
The ADK framework uses the docstring as the tool description for the model.
"""

from __future__ import annotations

import logging
import re
from urllib.parse import urljoin, urlparse

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 15.0  # seconds
_MAX_CONTENT_BYTES = 50_000  # cap returned content to avoid token bloat

# Simple user-agent to avoid bot blocks on public pages
_USER_AGENT = (
    "Mozilla/5.0 (compatible; AgenticJobHarvester/1.0; Research bot; contact@example.com)"
)


async def google_search(query: str, num_results: int = 5) -> list[dict[str, str]]:
    """Search the web using Google Custom Search API and return top results.

    Use this tool to find public information about a company, recent news,
    Glassdoor reviews, LinkedIn profiles, or job posting patterns.

    Args:
        query: The search query string. Be specific — include company name and context.
        num_results: Number of results to return (1-10). Default 5.

    Returns:
        List of dicts with keys: 'title', 'url', 'snippet'.
        Returns empty list if the search fails.
    """
    from config.settings import get_settings
    import os

    settings = get_settings()

    # Use Google Custom Search JSON API
    # Requires GOOGLE_SEARCH_API_KEY and GOOGLE_SEARCH_CX in environment
    api_key = os.environ.get("GOOGLE_SEARCH_API_KEY", "")
    cx = os.environ.get("GOOGLE_SEARCH_CX", "")

    if not api_key or not cx:
        logger.warning(
            "GOOGLE_SEARCH_API_KEY or GOOGLE_SEARCH_CX not set. Returning empty results."
        )
        return []

    num_results = max(1, min(10, num_results))

    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
        try:
            response = await client.get(
                "https://www.googleapis.com/customsearch/v1",
                params={
                    "key": api_key,
                    "cx": cx,
                    "q": query,
                    "num": num_results,
                },
            )
            response.raise_for_status()
            data = response.json()

            results = []
            for item in data.get("items", []):
                results.append({
                    "title": item.get("title", ""),
                    "url": item.get("link", ""),
                    "snippet": item.get("snippet", ""),
                })
            logger.info("google_search('%s') returned %d results", query, len(results))
            return results

        except httpx.HTTPError as exc:
            logger.error("google_search failed: %s", exc)
            return []


async def fetch_url(url: str, extract_text: bool = True) -> str:
    """Fetch the content of a web page and return its text.

    Use this to read company websites, job postings, Glassdoor pages,
    LinkedIn company profiles, or news articles found via google_search.

    Args:
        url: The full URL to fetch (must start with http:// or https://).
        extract_text: If True, strips HTML tags and returns clean text.
                     If False, returns raw HTML.

    Returns:
        Page content as a string (capped at ~50KB to avoid token bloat).
        Returns an error message string if the fetch fails.
    """
    if not url.startswith(("http://", "https://")):
        return f"ERROR: Invalid URL '{url}'. Must start with http:// or https://"

    async with httpx.AsyncClient(
        timeout=_DEFAULT_TIMEOUT,
        follow_redirects=True,
        headers={"User-Agent": _USER_AGENT},
    ) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            content = response.text[:_MAX_CONTENT_BYTES * 4]  # over-fetch then trim

            if extract_text:
                content = _strip_html(content)

            content = content[:_MAX_CONTENT_BYTES]
            logger.info("fetch_url('%s') → %d chars", url, len(content))
            return content

        except httpx.HTTPStatusError as exc:
            return f"ERROR: HTTP {exc.response.status_code} for {url}"
        except httpx.RequestError as exc:
            return f"ERROR: Request failed for {url}: {exc}"


def _strip_html(html: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    # Remove script and style blocks
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
    # Remove all other tags
    html = re.sub(r"<[^>]+>", " ", html)
    # Collapse whitespace
    html = re.sub(r"\s+", " ", html).strip()
    return html
