"""PubMed research client.

Searches PubMed via E-utilities REST API (free, no auth needed).
Rate-limited to 3 requests/second per NCBI policy (no API key).
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import defusedxml.ElementTree as ET  # noqa: N817
import httpx

from healthbot.config import Config
from healthbot.research.research_packet import build_research_packet
from healthbot.security.phi_firewall import PhiFirewall

# NCBI allows 3 requests/second without API key. We use 0.4s between requests
# to stay well under the limit and avoid IP bans.
_MIN_REQUEST_INTERVAL = 0.4


@dataclass
class PubMedResult:
    pmid: str
    title: str
    abstract: str
    authors: list[str]
    journal: str
    year: str

    @property
    def url(self) -> str:
        return f"https://pubmed.ncbi.nlm.nih.gov/{self.pmid}/"


class PubMedClient:
    """Search PubMed for health research articles."""

    def __init__(self, config: Config, firewall: PhiFirewall) -> None:
        self._config = config
        self._firewall = firewall
        self._base = config.pubmed_base_url
        self._last_request_time: float = 0.0

    async def _rate_limit(self) -> None:
        """Enforce minimum interval between NCBI requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            await asyncio.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    async def search(self, query: str, max_results: int | None = None) -> list[PubMedResult]:
        """Search PubMed. PHI-checks the query first."""
        if not self._config.pubmed_enabled:
            return []
        if max_results is None:
            max_results = self._config.pubmed_max_results
        packet = build_research_packet(query, firewall=self._firewall)
        if packet.blocked:
            return []

        pmids = await self._search_ids(packet.query, max_results)
        if not pmids:
            return []
        return await self._fetch_summaries(pmids)

    async def _search_ids(self, query: str, max_results: int) -> list[str]:
        """Get PMIDs from search query."""
        await self._rate_limit()
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{self._base}/esearch.fcgi",
                params={
                    "db": "pubmed",
                    "term": query,
                    "retmax": max_results,
                    "retmode": "json",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("esearchresult", {}).get("idlist", [])

    async def _fetch_summaries(self, pmids: list[str]) -> list[PubMedResult]:
        """Fetch article details for given PMIDs."""
        await self._rate_limit()
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{self._base}/efetch.fcgi",
                params={
                    "db": "pubmed",
                    "id": ",".join(pmids),
                    "retmode": "xml",
                },
            )
            resp.raise_for_status()

        results = []
        root = ET.fromstring(resp.text)
        for article in root.iter("PubmedArticle"):
            medline = article.find(".//MedlineCitation")
            if medline is None:
                continue

            pmid = medline.findtext("PMID", "")
            art = medline.find(".//Article")
            if art is None:
                continue

            title = art.findtext("ArticleTitle", "")
            abstract = art.findtext(".//AbstractText", "")
            journal = art.findtext(".//Journal/Title", "")
            year = art.findtext(".//Journal/JournalIssue/PubDate/Year", "")

            authors = []
            for author in art.iter("Author"):
                last = author.findtext("LastName", "")
                first = author.findtext("ForeName", "")
                if last:
                    authors.append(f"{last} {first}".strip())

            results.append(PubMedResult(
                pmid=pmid,
                title=title,
                abstract=abstract,
                authors=authors[:5],
                journal=journal,
                year=year,
            ))

        return results
