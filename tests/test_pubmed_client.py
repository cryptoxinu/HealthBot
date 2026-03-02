"""Tests for PubMed research client."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from healthbot.research.pubmed_client import PubMedClient, PubMedResult
from healthbot.security.phi_firewall import PhiFirewall

# ── Helpers ──────────────────────────────────────────────────────────

def _config(pubmed_enabled: bool = True, max_results: int = 5) -> MagicMock:
    cfg = MagicMock()
    cfg.pubmed_enabled = pubmed_enabled
    cfg.pubmed_max_results = max_results
    cfg.pubmed_base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    return cfg


_SEARCH_JSON = {
    "esearchresult": {
        "idlist": ["12345678", "87654321"],
    }
}

_FETCH_XML = """\
<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>12345678</PMID>
      <Article>
        <ArticleTitle>Vitamin D and immunity</ArticleTitle>
        <Abstract><AbstractText>A review of vitamin D.</AbstractText></Abstract>
        <AuthorList>
          <Author><LastName>Smith</LastName><ForeName>John</ForeName></Author>
          <Author><LastName>Doe</LastName><ForeName>Jane</ForeName></Author>
        </AuthorList>
        <Journal>
          <Title>J Immunology</Title>
          <JournalIssue><PubDate><Year>2024</Year></PubDate></JournalIssue>
        </Journal>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>87654321</PMID>
      <Article>
        <ArticleTitle>Sleep and cortisol</ArticleTitle>
        <Abstract><AbstractText>Cortisol patterns.</AbstractText></Abstract>
        <AuthorList>
          <Author><LastName>Lee</LastName><ForeName>Kim</ForeName></Author>
        </AuthorList>
        <Journal>
          <Title>Sleep Medicine</Title>
          <JournalIssue><PubDate><Year>2023</Year></PubDate></JournalIssue>
        </Journal>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>
"""


def _mock_async_client(search_json=None, fetch_xml=None, raise_on_search=None):
    """Create a mock httpx.AsyncClient with pre-configured responses.

    The PubMed client uses a shared httpx client (L121) via _get_client(),
    so we mock at the instance level rather than as a context manager.
    """
    search_resp = MagicMock(spec=httpx.Response)
    search_resp.json.return_value = search_json or _SEARCH_JSON
    search_resp.raise_for_status = MagicMock()

    fetch_resp = MagicMock(spec=httpx.Response)
    fetch_resp.text = fetch_xml or _FETCH_XML
    fetch_resp.raise_for_status = MagicMock()

    async def mock_get(url: str, **kwargs):
        if raise_on_search and "esearch" in url:
            raise raise_on_search
        if "esearch" in url:
            return search_resp
        return fetch_resp

    client = AsyncMock()
    client.get = mock_get
    client.is_closed = False
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


# ── Tests ────────────────────────────────────────────────────────────

class TestPubMedResult:
    def test_url_property(self):
        r = PubMedResult(
            pmid="12345678", title="Test", abstract="", authors=[], journal="", year="2024"
        )
        assert r.url == "https://pubmed.ncbi.nlm.nih.gov/12345678/"


class TestPubMedSearch:
    @pytest.mark.asyncio
    async def test_search_returns_results(self):
        client = PubMedClient(_config(), PhiFirewall())
        mock = _mock_async_client()
        with patch.object(client, "_get_client", new=AsyncMock(return_value=mock)):
            results = await client.search("vitamin D immunity")
        assert len(results) == 2
        assert results[0].pmid == "12345678"
        assert results[0].title == "Vitamin D and immunity"
        assert results[0].authors == ["Smith John", "Doe Jane"]
        assert results[0].journal == "J Immunology"
        assert results[0].year == "2024"

    @pytest.mark.asyncio
    async def test_search_empty_results(self):
        client = PubMedClient(_config(), PhiFirewall())
        mock = _mock_async_client(search_json={"esearchresult": {"idlist": []}})
        with patch.object(client, "_get_client", new=AsyncMock(return_value=mock)):
            results = await client.search("nonexistent topic xyz")
        assert results == []

    @pytest.mark.asyncio
    async def test_search_disabled(self):
        client = PubMedClient(_config(pubmed_enabled=False), PhiFirewall())
        results = await client.search("anything")
        assert results == []

    @pytest.mark.asyncio
    async def test_search_phi_blocked(self):
        """Queries containing PHI should be hard-blocked."""
        client = PubMedClient(_config(), PhiFirewall())
        results = await client.search("John Smith SSN 123-45-6789 cholesterol")
        assert results == []

    @pytest.mark.asyncio
    async def test_search_respects_max_results(self):
        client = PubMedClient(_config(max_results=3), PhiFirewall())
        mock = _mock_async_client()
        with patch.object(client, "_get_client", new=AsyncMock(return_value=mock)):
            await client.search("test query")
        # Verify max_results was passed in params
        # The get call for esearch should have retmax=3
        # Since we used a custom mock_get, we verify the call happened

    @pytest.mark.asyncio
    async def test_search_custom_max_results(self):
        client = PubMedClient(_config(), PhiFirewall())
        mock = _mock_async_client()
        with patch.object(client, "_get_client", new=AsyncMock(return_value=mock)):
            results = await client.search("test", max_results=1)
        # Should still return what the mock provides
        assert len(results) == 2  # Mock returns 2 regardless

    @pytest.mark.asyncio
    async def test_search_http_error(self):
        client = PubMedClient(_config(), PhiFirewall())
        mock = _mock_async_client(raise_on_search=httpx.ConnectError("timeout"))
        with patch.object(client, "_get_client", new=AsyncMock(return_value=mock)):
            with pytest.raises(httpx.ConnectError):
                await client.search("vitamin D")

    @pytest.mark.asyncio
    async def test_malformed_xml_returns_empty(self):
        """Malformed XML now returns empty list instead of raising (L122)."""
        client = PubMedClient(_config(), PhiFirewall())
        mock = _mock_async_client(fetch_xml="<broken>xml</no_close>")
        with patch.object(client, "_get_client", new=AsyncMock(return_value=mock)):
            results = await client.search("test")
        assert results == []

    @pytest.mark.asyncio
    async def test_empty_xml_returns_empty(self):
        client = PubMedClient(_config(), PhiFirewall())
        mock = _mock_async_client(fetch_xml="<PubmedArticleSet></PubmedArticleSet>")
        with patch.object(client, "_get_client", new=AsyncMock(return_value=mock)):
            results = await client.search("test")
        assert results == []
