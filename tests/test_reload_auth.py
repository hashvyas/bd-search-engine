import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient

from src.serve.app import app


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("RELOAD_TOKEN", "secret")
    return TestClient(app)


def test_non_ascii_token_is_rejected_with_403(client):
    r = client.post("/reload?version=v0", headers={"X-Reload-Token": "sécret".encode("utf-8")})
    assert r.status_code == 403


def test_missing_or_wrong_token_is_rejected(client):
    assert client.post("/reload?version=v0").status_code == 403
    assert client.post("/reload?version=v0", headers={"X-Reload-Token": "nope"}).status_code == 403


def test_correct_token_passes_auth(client):
    assert client.post("/reload?version=v0", headers={"X-Reload-Token": "secret"}).status_code == 409
