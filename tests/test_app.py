"""Streamlit rerun checks with a mocked backend, never real Hadoop execution."""

import io
import unittest
from unittest.mock import patch

try:
    from streamlit.testing.v1 import AppTest
except ImportError:
    AppTest = None

from log_pipeline import BASE, PipelineError
from test_frontend import VALID

RESULT = {"counts": {"page": {"/home": 2, "/about": 1},
                     "ip": {"192.0.2.1": 2, "2001:db8::1": 1},
                     "status": {"200": 2, "404": 1}},
          "total": 3, "run_id": "mocked-ui-check", "hdfs_path": "/mocked-ui-check"}


@unittest.skipIf(AppTest is None, "Install requirements.txt for Streamlit UI tests")
class AppTests(unittest.TestCase):
    def test_results_reruns_failure_change_and_removal(self):
        with patch("streamlit.file_uploader", return_value=io.BytesIO(VALID)) as upload, \
                patch("log_pipeline.analyze", return_value=RESULT) as backend:
            app = AppTest.from_file(str(BASE / "app.py"), default_timeout=30).run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.metric), 0)
            self.assertFalse(app.button[0].disabled)
            app.button[0].click().run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual([m.value for m in app.metric], ["3", "2"])
            self.assertEqual(len(app.dataframe), 4)
            self.assertEqual(len(app.get("download_button")), 3)
            app.run()
            backend.assert_called_once()
            self.assertEqual(len(app.metric), 2)
            backend.side_effect = PipelineError("mocked Hadoop failure")
            app.button[0].click().run()
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.metric), 0)
            self.assertEqual(len(app.success), 0)
            self.assertIn("mocked Hadoop failure", app.error[0].value)
            backend.side_effect = None
            app.button[0].click().run()
            self.assertEqual(len(app.metric), 2)
            upload.return_value = io.BytesIO(b"malformed")
            app.run()
            self.assertEqual(len(app.metric), 0)
            self.assertEqual(len(app.success), 0)
            self.assertTrue(app.button[0].disabled)
            self.assertIn("Line 1", app.error[0].value)
            upload.return_value = io.BytesIO(VALID)
            app.run()
            self.assertEqual(len(app.metric), 0)
            self.assertEqual(len(app.error), 0)
            app.button[0].click().run()
            self.assertEqual(len(app.metric), 2)
            upload.return_value = None
            app.run()
            self.assertEqual(len(app.metric), 0)
            self.assertEqual(len(app.success), 0)
            self.assertTrue(app.button[0].disabled)


if __name__ == "__main__":
    unittest.main()
