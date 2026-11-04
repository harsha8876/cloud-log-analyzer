"""Local checks. Hadoop subprocesses are mocked; no EC2 integration claim."""

from contextlib import ExitStack
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import log_pipeline as pipeline
import mapper
from streaming_mapper import selected_records

VALID = (b"192.0.2.1 2026-11-01T10:00:00Z /home 200\n"
         b"\n2001:db8::1 2026-11-01T10:01:00+05:30 /about 404\n"
         b"192.0.2.1 2026-11-01T10:02:00 /home 200\n")
REPORTS = {"page": "/about\t1\n/home\t2\n", "ip": "192.0.2.1\t2\n2001:db8::1\t1\n", "status": "200\t2\n404\t1\n"}


class ValidationTests(unittest.TestCase):
    def test_valid_ipv4_ipv6_blank_lines_and_timestamps(self):
        self.assertEqual(len(pipeline.validate_upload(VALID)), 3)
        self.assertEqual(len(pipeline.validate_upload((pipeline.BASE / "sample.log").read_bytes())), 6)
        self.assertEqual(len(pipeline.validate_upload((pipeline.BASE / "large.log").read_bytes())), 10000)

    def test_empty(self):
        for data in (b"", b" \n\t\n"):
            with self.subTest(data=data), self.assertRaisesRegex(pipeline.PipelineError, "no nonblank records"):
                pipeline.validate_upload(data)

    def test_malformed_with_line_numbers(self):
        bad_records = (
            "192.0.2.1 timestamp /home", "999.2.3.4 2026-11-01T10:00:00 / 200",
            "192.0.2.1 2026-02-30T00:00:00 / 200", "192.0.2.1 2026-11-01 / 200",
            "192.0.2.1 2026-11-01T00:00:00 home 200",
            "192.0.2.1 2026-11-01T00:00:00 / 600",
            "192.0.2.1 2026-11-01T00:00:00 / 099",
            "192.0.2.1 2026-11-01T00:00:00 / 200 extra",
        )
        for record in bad_records:
            with self.subTest(record=record), self.assertRaisesRegex(pipeline.PipelineError, "Line 2"):
                pipeline.validate_upload(("\n" + record).encode())

    def test_crlf_and_unicode_record_separators(self):
        self.assertEqual(len(pipeline.validate_upload(VALID.replace(b"\n", b"\r\n"))), 3)
        for separator in ("\v", "\x85", "\u2028", "\u2029"):
            with self.subTest(separator=separator), self.assertRaises(pipeline.PipelineError):
                pipeline.validate_upload(VALID.decode().replace("\n", separator).encode())

    def test_all_records_checked_and_errors_capped(self):
        with self.assertRaisesRegex(pipeline.PipelineError, "5 additional malformed lines"):
            pipeline.validate_upload(b"bad\n" * 25)

    def test_encoding_and_size(self):
        with self.assertRaisesRegex(pipeline.PipelineError, "Line 2.*UTF-8"):
            pipeline.validate_upload(b"\n\xff")
        with self.assertRaisesRegex(pipeline.PipelineError, "10 MB"):
            pipeline.validate_upload(b"x" * (pipeline.MAX_BYTES + 1))


class ComparisonTests(unittest.TestCase):
    def test_matching(self):
        actual = pipeline.compare_reports(REPORTS, pipeline.validate_upload(VALID))
        self.assertEqual(actual["page"]["/home"], 2)

    def test_bad_reports(self):
        for replacement in ("/about\t2\n/home\t1\n", "/about\t1\n/home\t3\n",
                            "/about\t1\n/home\t1\n/home\t1\n", "/about\t-1\n",
                            "/about\t1.0\n", "", "/other\t1\n/home\t2\n"):
            for name in pipeline.REPORTS:
                with self.subTest(report=name, replacement=replacement), self.assertRaises(pipeline.PipelineError):
                    pipeline.compare_reports({**REPORTS, name: replacement}, pipeline.validate_upload(VALID))
        with self.assertRaises(pipeline.PipelineError):
            pipeline.compare_reports({"page": REPORTS["page"]}, pipeline.validate_upload(VALID))

    def test_adapter_uses_existing_mapper(self):
        for mode in pipeline.REPORTS:
            output = io.StringIO()
            with patch("sys.stdin", selected_records(io.StringIO(VALID.decode()), mode)), patch("sys.stdout", output):
                mapper.main()
            counts = {}
            for line in output.getvalue().splitlines():
                key, count = line.split("\t")
                counts[key] = counts.get(key, 0) + int(count)
            expected = pipeline.compare_reports(REPORTS, pipeline.validate_upload(VALID))[mode]
            self.assertEqual(counts, expected)


class CommandTests(unittest.TestCase):
    def process(self, returncode=0):
        process = MagicMock()
        process.__enter__.return_value = process
        process.returncode = returncode
        process.pid = 12345
        return process

    def test_success_shell_false_and_timeout(self):
        process = self.process()
        process.communicate.return_value = ("actual output", "")
        with patch("log_pipeline.subprocess.Popen", return_value=process) as launch:
            self.assertEqual(pipeline.command(["hadoop", "version"], {}, "version"), "actual output")
        self.assertFalse(launch.call_args.kwargs["shell"])
        self.assertTrue(launch.call_args.kwargs["start_new_session"])
        process.communicate.assert_called_once_with(timeout=120)

    def test_failure_checked(self):
        process = self.process(1)
        process.communicate.return_value = ("", "Permission denied")
        with patch("log_pipeline.subprocess.Popen", return_value=process), self.assertRaisesRegex(pipeline.PipelineError, "exit 1.*"):
            pipeline.command(["hdfs"], {}, "upload")

    def test_timeout_kills_group_and_reaps(self):
        process = self.process()
        process.communicate.side_effect = [subprocess.TimeoutExpired(["hadoop"], 10), ("", "")]
        with patch("log_pipeline.subprocess.Popen", return_value=process), patch("log_pipeline.os.killpg") as kill:
            with self.assertRaisesRegex(pipeline.PipelineError, "timed out after 10s"):
                pipeline.command(["hadoop"], {}, "job", timeout=10)
        kill.assert_called_once_with(12345, pipeline.signal.SIGKILL)
        self.assertEqual(process.communicate.call_count, 2)

    def test_missing_executable(self):
        with patch("log_pipeline.subprocess.Popen", side_effect=FileNotFoundError("missing")):
            with self.assertRaisesRegex(pipeline.PipelineError, "Cannot execute"):
                pipeline.command(["missing"], {}, "job")


class PipelineTests(unittest.TestCase):
    def fake_output(self, args, env, label, timeout=120):
        self.calls.append((args, label, timeout))
        if label == "Hadoop version check":
            return "Hadoop 3.4.3\n"
        if label == "HDFS configuration check":
            return "hdfs://localhost:9000\n"
        if label == "HDFS safe mode check":
            return "Safe mode is OFF\n"
        if label == "HDFS DataNode check":
            return "Live datanodes (1):\n"
        if label.endswith("report retrieval"):
            return REPORTS[label.split()[0]]
        return ""

    def setUp(self):
        self.calls = []
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.runtime = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.stack.enter_context(patch("log_pipeline.environment", return_value=({}, "/java", "/hadoop", "/hdfs", "/streaming.jar")))
        self.mock_command = self.stack.enter_context(patch("log_pipeline.command", side_effect=self.fake_output))

    def test_mock_pipeline_success_and_isolation(self):
        results = [pipeline.analyze(VALID, runtime=self.runtime) for _ in range(2)]
        self.assertNotEqual(results[0]["run_id"], results[1]["run_id"])
        jobs = [args for args, label, _ in self.calls if label.endswith("Streaming job")]
        self.assertEqual(len(jobs), 6)
        self.assertEqual(len({args[args.index("-output") + 1] for args in jobs}), 6)
        for args in jobs:
            self.assertIn("mapreduce.framework.name=local", args)
            self.assertIn("mapreduce.job.reduces=1", args)
            self.assertEqual(args[args.index("-reducer") + 1], "python3 reducer.py")
        for result in results:
            self.assertEqual((self.runtime / result["run_id"] / "upload.log").read_bytes(), VALID)
            self.assertEqual(result["total"], 3)

    def test_job_failure_and_timeout_stop_processing(self):
        for error in ("page Streaming job failed (exit 1)", "page Streaming job timed out after 600s"):
            def fail(args, env, label, timeout=120):
                if label == "page Streaming job":
                    raise pipeline.PipelineError(error)
                return self.fake_output(args, env, label, timeout)
            self.calls.clear()
            self.mock_command.side_effect = fail
            with self.assertRaises(pipeline.PipelineError):
                pipeline.analyze(VALID, runtime=self.runtime)
            self.assertFalse(any(label == "ip Streaming job" for _, label, _ in self.calls))
        # Lock is released even on failure.
        self.mock_command.side_effect = self.fake_output
        self.assertEqual(pipeline.analyze(VALID, runtime=self.runtime)["total"], 3)

    def test_incorrect_hadoop_output_fails(self):
        def wrong(args, env, label, timeout=120):
            if label == "status report retrieval":
                return "200\t3\n"
            return self.fake_output(args, env, label, timeout)
        self.mock_command.side_effect = wrong
        with self.assertRaisesRegex(pipeline.PipelineError, "status Hadoop report does not match"):
            pipeline.analyze(VALID, runtime=self.runtime)

    def test_preflight_failures_prevent_upload(self):
        for check, output in (("Hadoop version check", "Hadoop 3.4.2"),
                              ("HDFS configuration check", "file:///"),
                              ("HDFS safe mode check", "Safe mode is ON"),
                              ("HDFS DataNode check", "Live datanodes (0)")):
            def bad(args, env, label, timeout=120):
                return output if label == check else self.fake_output(args, env, label, timeout)
            self.calls.clear()
            self.mock_command.side_effect = bad
            with self.subTest(check=check), self.assertRaises(pipeline.PipelineError):
                pipeline.analyze(VALID, runtime=self.runtime)
            self.assertFalse(any(label == "HDFS upload" for _, label, _ in self.calls))

    def test_concurrent_run_rejected(self):
        with pipeline.run_lock(self.runtime):
            with self.assertRaisesRegex(pipeline.PipelineError, "Another analysis"):
                pipeline.analyze(VALID, runtime=self.runtime)
        self.mock_command.assert_not_called()

    def test_invalid_input_does_not_start_subprocesses(self):
        with self.assertRaises(pipeline.PipelineError):
            pipeline.analyze(b"bad", runtime=self.runtime)
        self.mock_command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
