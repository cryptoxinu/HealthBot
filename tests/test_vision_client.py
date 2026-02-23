"""Tests for two-stage vision analysis client."""
from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

import pytest

from healthbot.llm.vision_client import VisionClient


@pytest.fixture
def client():
    return VisionClient(base_url="http://localhost:11434", timeout=60)


@pytest.fixture
def fake_image():
    return b"\x89PNG\r\n\x1a\n" + b"\x00" * 100


def _mock_httpx_client(post_side_effect=None, post_return=None, get_return=None):
    """Helper to create a properly mocked httpx.Client context manager."""
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    if post_side_effect:
        mock_client.post.side_effect = post_side_effect
    elif post_return:
        mock_client.post.return_value = post_return
    if get_return:
        mock_client.get.return_value = get_return
    return mock_client


class TestAnalyzePhoto:
    def test_two_stage_pipeline(self, client, fake_image):
        """Both describe and interpret stages should be called."""
        describe_resp = MagicMock()
        describe_resp.raise_for_status = MagicMock()
        describe_resp.json.return_value = {
            "message": {"content": "A red, raised area on the forearm."}
        }

        interpret_resp = MagicMock()
        interpret_resp.raise_for_status = MagicMock()
        interpret_resp.json.return_value = {
            "message": {"content": "This could be worth discussing with your doctor."}
        }

        call_count = [0]
        def post_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return describe_resp
            return interpret_resp

        mock_client = _mock_httpx_client(post_side_effect=post_side_effect)

        with patch("httpx.Client", return_value=mock_client):
            result = client.analyze_photo(fake_image, "user has eczema history")

        assert "What I see" in result
        assert "red, raised area" in result
        assert "Health context" in result
        assert "doctor" in result.lower()
        assert call_count[0] == 2

    def test_describe_sends_base64_image(self, client, fake_image):
        """Describe stage should send base64-encoded image."""
        describe_resp = MagicMock()
        describe_resp.raise_for_status = MagicMock()
        describe_resp.json.return_value = {
            "message": {"content": "A photo."}
        }

        interpret_resp = MagicMock()
        interpret_resp.raise_for_status = MagicMock()
        interpret_resp.json.return_value = {
            "message": {"content": "Noted."}
        }

        call_count = [0]
        def post_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # Verify the describe call has images field
                payload = kwargs.get("json", {})
                messages = payload.get("messages", [])
                user_msg = [m for m in messages if m.get("role") == "user"]
                assert len(user_msg) == 1
                assert "images" in user_msg[0]
                expected_b64 = base64.b64encode(fake_image).decode("utf-8")
                assert user_msg[0]["images"][0] == expected_b64
                return describe_resp
            return interpret_resp

        mock_client = _mock_httpx_client(post_side_effect=post_side_effect)

        with patch("httpx.Client", return_value=mock_client):
            client.analyze_photo(fake_image)

    def test_describe_error_returns_early(self, client, fake_image):
        """If describe fails, should return error without calling interpret."""
        import httpx as httpx_mod

        mock_client = _mock_httpx_client(
            post_side_effect=httpx_mod.ConnectError("refused")
        )

        with patch("httpx.Client", return_value=mock_client):
            result = client.analyze_photo(fake_image)

        assert "Error" in result
        assert "Ollama" in result

    def test_empty_describe_response(self, client, fake_image):
        """Empty description should return error."""
        describe_resp = MagicMock()
        describe_resp.raise_for_status = MagicMock()
        describe_resp.json.return_value = {"message": {"content": ""}}

        mock_client = _mock_httpx_client(post_return=describe_resp)

        with patch("httpx.Client", return_value=mock_client):
            result = client.analyze_photo(fake_image)

        assert "Error" in result


class TestIsAvailable:
    def test_available_when_vision_model_present(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "models": [{"name": "gemma3:27b"}]
        }

        mock_client = _mock_httpx_client(get_return=mock_resp)

        with patch("httpx.Client", return_value=mock_client):
            assert client.is_available() is True

    def test_unavailable_when_missing(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "models": [{"name": "llama3:latest"}]
        }

        mock_client = _mock_httpx_client(get_return=mock_resp)

        with patch("httpx.Client", return_value=mock_client):
            assert client.is_available() is False
