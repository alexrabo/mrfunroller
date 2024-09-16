# tests/test_main.py

from fastapi.testclient import TestClient
from mrfunroller.main import app

client = TestClient(app)

def test_upload():
    response = client.post("/upload/", files={"file": ("testfile.json", open("tests/testfile.json", "rb"))})
    assert response.status_code == 200
    assert "filename" in response.json()
