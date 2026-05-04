import sys
import types
from unittest.mock import MagicMock

import pytest

from dags import market_news_embedding


@pytest.mark.unit
def test_market_news_embedding_dag_has_expected_shape():
    assert market_news_embedding.dag.dag_id == "market_news_embedding"
    assert market_news_embedding.dag.schedule is None
    assert [task.task_id for task in market_news_embedding.dag.tasks] == [
        "generate_and_upsert_embeddings",
    ]


@pytest.mark.unit
def test_generate_and_upsert_embeddings_uses_direct_gemini_calls(monkeypatch):
    rows = [
        {
            "news_row_id": 101,
            "title": "Market headline",
            "news_content": "Full article body",
        }
    ]

    fake_cursor = MagicMock()
    fake_cursor.fetchall.return_value = rows

    fake_conn = MagicMock()
    fake_conn.__enter__.return_value = fake_conn
    fake_conn.__exit__.return_value = None
    fake_conn.cursor.return_value.__enter__.return_value = fake_cursor

    fake_execute_values = MagicMock()
    fake_extras = types.ModuleType("psycopg2.extras")
    fake_extras.RealDictCursor = object()
    fake_extras.execute_values = fake_execute_values

    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_psycopg2.connect = MagicMock(return_value=fake_conn)
    fake_psycopg2.extras = fake_extras

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.embeddings = [
        MagicMock(values=[3.0, 4.0]),
        MagicMock(values=[0.0, 5.0]),
    ]
    mock_client.models.embed_content.return_value = mock_response

    fake_genai = MagicMock()
    fake_genai.Client.return_value = mock_client
    fake_google = types.ModuleType("google")
    fake_google.genai = fake_genai

    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)
    monkeypatch.setitem(sys.modules, "psycopg2.extras", fake_extras)
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setattr(
        market_news_embedding,
        "get_gemini_api_key",
        lambda: "test-gemini-key",
    )
    monkeypatch.setattr(
        market_news_embedding,
        "chunk_text",
        lambda text, chunk_size=4000, chunk_overlap=800: ["chunk-a", "chunk-b"],
    )

    result = market_news_embedding.generate_and_upsert_embeddings.function()

    assert result == 2
    assert fake_psycopg2.connect.call_count == 2
    mock_client.models.embed_content.assert_called_once()

    _, kwargs = mock_client.models.embed_content.call_args
    assert kwargs["model"] == market_news_embedding.MODEL
    assert kwargs["contents"] == ["chunk-a", "chunk-b"]
    assert kwargs["config"] == {
        "output_dimensionality": market_news_embedding.TARGET_DIM,
        "task_type": "RETRIEVAL_DOCUMENT",
    }

    fake_execute_values.assert_called_once()
    execute_args = fake_execute_values.call_args.args
    assert execute_args[2][0][0] == "101"
    assert execute_args[2][0][1] == 0
    assert execute_args[2][0][2] == pytest.approx([0.6, 0.8])
    assert execute_args[2][0][3] == "gemini-001-768d"
    assert execute_args[2][1][0] == "101"
    assert execute_args[2][1][1] == 1
    assert execute_args[2][1][2] == pytest.approx([0.0, 1.0])
    assert execute_args[2][1][3] == "gemini-001-768d"


@pytest.mark.unit
def test_generate_and_upsert_embeddings_returns_zero_for_no_rows(monkeypatch):
    fake_cursor = MagicMock()
    fake_cursor.fetchall.return_value = []

    fake_conn = MagicMock()
    fake_conn.__enter__.return_value = fake_conn
    fake_conn.__exit__.return_value = None
    fake_conn.cursor.return_value.__enter__.return_value = fake_cursor

    fake_extras = types.ModuleType("psycopg2.extras")
    fake_extras.RealDictCursor = object()
    fake_extras.execute_values = MagicMock()

    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_psycopg2.connect = MagicMock(return_value=fake_conn)
    fake_psycopg2.extras = fake_extras

    fake_genai = MagicMock()
    fake_google = types.ModuleType("google")
    fake_google.genai = fake_genai

    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)
    monkeypatch.setitem(sys.modules, "psycopg2.extras", fake_extras)
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setattr(
        market_news_embedding,
        "get_gemini_api_key",
        lambda: "test-gemini-key",
    )

    result = market_news_embedding.generate_and_upsert_embeddings.function()

    assert result == 0
    fake_genai.Client.assert_not_called()
    fake_extras.execute_values.assert_not_called()
