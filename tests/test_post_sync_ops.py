"""Unit tests for _run_post_sync_ops() in proxy/prefect_flows.py."""
import json
from unittest.mock import MagicMock, patch

import pytest

from proxy.prefect_flows_runner import _run_post_sync_ops


POSTGRES_CREDS = {
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "user": "myuser",
    "password": "secret",
}

BIGQUERY_CREDS = {
    "type": "service_account",
    "project_id": "my-project",
    "private_key_id": "key-id",
    "private_key": "key",
    "client_email": "svc@my-project.iam.gserviceaccount.com",
    "client_id": "123",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}

ALTER_SQL = "ALTER TABLE dest.orders ALTER COLUMN amount TYPE numeric USING amount::numeric"
BQ_SQL = "CREATE OR REPLACE TABLE `my-project.dest.orders` AS SELECT `id`, CAST(`amount` AS NUMERIC) AS `amount` FROM `my-project.dest.orders`"


def _secret_block(wtype: str, creds: dict):
    block = MagicMock()
    block.get.return_value = json.dumps({"wtype": wtype, "creds": creds})
    return block


# ---------------------------------------------------------------------------
# No-op cases
# ---------------------------------------------------------------------------


def test_noop_when_no_post_sync_ops():
    """Returns immediately when post_sync_ops is absent."""
    with patch("proxy.prefect_flows_runner.Secret") as mock_secret:
        _run_post_sync_ops({})
        mock_secret.load.assert_not_called()


def test_noop_when_post_sync_ops_empty():
    with patch("proxy.prefect_flows_runner.Secret") as mock_secret:
        _run_post_sync_ops({"post_sync_ops": []})
        mock_secret.load.assert_not_called()


def test_noop_when_no_secret_block_name():
    """Logs warning and returns when env has no secret block key."""
    with patch("proxy.prefect_flows_runner.Secret") as mock_secret:
        _run_post_sync_ops({"post_sync_ops": [{"type": "cast", "sql": ALTER_SQL}]})
        mock_secret.load.assert_not_called()


def test_skips_unknown_op_type():
    """Ops with type != 'cast' are silently skipped."""
    with patch("proxy.prefect_flows_runner.Secret") as mock_secret:
        mock_secret.load.return_value = _secret_block("postgres", POSTGRES_CREDS)
        with patch("psycopg2.connect") as mock_connect:
            _run_post_sync_ops(
                {
                    "env": {"dbt-profile-secret-block": "my-block"},
                    "post_sync_ops": [{"type": "unknown", "sql": "SELECT 1"}],
                }
            )
            mock_connect.assert_not_called()


# ---------------------------------------------------------------------------
# Postgres path
# ---------------------------------------------------------------------------


def test_postgres_executes_sql():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("proxy.prefect_flows_runner.Secret") as mock_secret, patch(
        "psycopg2.connect", return_value=mock_conn
    ) as mock_connect:
        mock_secret.load.return_value = _secret_block("postgres", POSTGRES_CREDS)
        _run_post_sync_ops(
            {
                "env": {"dbt-profile-secret-block": "my-block"},
                "post_sync_ops": [{"type": "cast", "sql": ALTER_SQL}],
            }
        )

    mock_connect.assert_called_once_with(
        host="localhost",
        port=5432,
        dbname="mydb",
        user="myuser",
        password="secret",
    )
    mock_cursor.execute.assert_called_once_with(ALTER_SQL)
    mock_conn.close.assert_called_once()


def test_postgres_closes_connection_on_error():
    """Connection is closed even when execute raises."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("syntax error")
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("proxy.prefect_flows_runner.Secret") as mock_secret, patch(
        "psycopg2.connect", return_value=mock_conn
    ):
        mock_secret.load.return_value = _secret_block("postgres", POSTGRES_CREDS)
        with pytest.raises(Exception, match="syntax error"):
            _run_post_sync_ops(
                {
                    "env": {"dbt-profile-secret-block": "my-block"},
                    "post_sync_ops": [{"type": "cast", "sql": ALTER_SQL}],
                }
            )

    mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# BigQuery path
# ---------------------------------------------------------------------------


def test_bigquery_executes_sql():
    mock_client = MagicMock()
    mock_query_job = MagicMock()
    mock_client.query.return_value = mock_query_job

    with patch("proxy.prefect_flows_runner.Secret") as mock_secret, patch(
        "google.oauth2.service_account.Credentials.from_service_account_info"
    ) as mock_creds, patch(
        "google.cloud.bigquery.Client", return_value=mock_client
    ):
        mock_secret.load.return_value = _secret_block("bigquery", BIGQUERY_CREDS)
        _run_post_sync_ops(
            {
                "env": {"dbt-profile-secret-block": "my-block"},
                "post_sync_ops": [{"type": "cast", "sql": BQ_SQL}],
            }
        )

    mock_client.query.assert_called_once_with(BQ_SQL)
    mock_query_job.result.assert_called_once()
    mock_client.close.assert_called_once()


def test_bigquery_closes_client_on_error():
    """Client is closed even when query raises."""
    mock_client = MagicMock()
    mock_client.query.side_effect = Exception("quota exceeded")

    with patch("proxy.prefect_flows_runner.Secret") as mock_secret, patch(
        "google.oauth2.service_account.Credentials.from_service_account_info"
    ), patch("google.cloud.bigquery.Client", return_value=mock_client):
        mock_secret.load.return_value = _secret_block("bigquery", BIGQUERY_CREDS)
        with pytest.raises(Exception, match="quota exceeded"):
            _run_post_sync_ops(
                {
                    "env": {"dbt-profile-secret-block": "my-block"},
                    "post_sync_ops": [{"type": "cast", "sql": BQ_SQL}],
                }
            )

    mock_client.close.assert_called_once()
