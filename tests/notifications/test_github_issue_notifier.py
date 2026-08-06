import unittest
from unittest.mock import MagicMock, patch

from src.modules.notifications.github_issue_notifier import notify_dag_failure


def _make_context(dag_id="test_dag", task_id="test_task"):
    dag = MagicMock()
    dag.dag_id = dag_id
    task_instance = MagicMock()
    task_instance.task_id = task_id
    task_instance.log_url = "http://airflow.example/log"
    return {
        "dag": dag,
        "task_instance": task_instance,
        "logical_date": "2026-07-25T07:05:00",
        "exception": RuntimeError("boom"),
    }


class TestGithubIssueNotifier(unittest.TestCase):
    @patch.dict("os.environ", {}, clear=True)
    @patch("src.modules.notifications.github_issue_notifier.requests")
    def test_skips_silently_without_token(self, mock_requests):
        # No GITHUB_TOKEN set -- must not raise, and must not call the API.
        notify_dag_failure(_make_context())
        mock_requests.get.assert_not_called()
        mock_requests.post.assert_not_called()

    @patch.dict("os.environ", {"GITHUB_TOKEN": "tok"}, clear=True)
    @patch("src.modules.notifications.github_issue_notifier.requests")
    def test_creates_issue_when_none_open(self, mock_requests):
        mock_requests.get.return_value = MagicMock(
            raise_for_status=lambda: None, json=lambda: {"items": []}
        )
        mock_requests.post.return_value = MagicMock(
            raise_for_status=lambda: None, json=lambda: {"number": 42}
        )

        notify_dag_failure(_make_context(dag_id="new_data_pipeline_dag"))

        mock_requests.post.assert_called_once()
        args, kwargs = mock_requests.post.call_args
        self.assertTrue(args[0].endswith("/issues"))
        self.assertIn("new_data_pipeline_dag", kwargs["json"]["title"])
        self.assertEqual(kwargs["json"]["labels"], ["dag-failure"])

    @patch.dict("os.environ", {"GITHUB_TOKEN": "tok"}, clear=True)
    @patch("src.modules.notifications.github_issue_notifier.requests")
    def test_comments_on_existing_open_issue_instead_of_duplicating(
        self, mock_requests
    ):
        mock_requests.get.return_value = MagicMock(
            raise_for_status=lambda: None, json=lambda: {"items": [{"number": 7}]}
        )
        mock_requests.post.return_value = MagicMock(raise_for_status=lambda: None)

        notify_dag_failure(_make_context())

        mock_requests.post.assert_called_once()
        args, _ = mock_requests.post.call_args
        self.assertTrue(args[0].endswith("/issues/7/comments"))

    @patch.dict("os.environ", {"GITHUB_TOKEN": "tok"}, clear=True)
    @patch("src.modules.notifications.github_issue_notifier.requests")
    def test_api_error_does_not_raise(self, mock_requests):
        mock_requests.get.side_effect = Exception("network down")
        # Must swallow the error -- a notifier failure must never mask the
        # DAG's own failure by raising out of the callback.
        notify_dag_failure(_make_context())


if __name__ == "__main__":
    unittest.main()
