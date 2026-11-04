"""Validation and isolated, checked Hadoop execution for the EC2 frontend."""

from collections import Counter
from contextlib import contextmanager
from datetime import datetime
import fcntl
import ipaddress
import os
from pathlib import Path
import re
import signal
import subprocess
import uuid

BASE = Path(__file__).resolve().parent
MAX_BYTES = 10 * 1024 * 1024
REPORTS = ("page", "ip", "status")
TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?", re.ASCII)


class PipelineError(RuntimeError):
    """An actionable validation, environment, or execution failure."""


def validate_upload(data):
    if len(data) > MAX_BYTES:
        raise PipelineError("File exceeds the 10 MB (10 MiB) limit.")
    try:
        content = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        line = data[:exc.start].count(b"\n") + 1
        raise PipelineError(f"Line {line}: file must be UTF-8 encoded.") from exc
    records, errors = [], []
    error_count = 0
    for number, line in enumerate(content.split("\n"), 1):
        line = line.removesuffix("\r")
        fields = line.split()
        if not fields:
            continue
        problem = None
        if len(fields) != 4:
            problem = f"expected exactly four columns, found {len(fields)}"
        else:
            ip, timestamp, page, status = fields
            try:
                ipaddress.ip_address(ip)
            except ValueError:
                problem = "invalid IPv4 or IPv6 address"
            try:
                if not TIMESTAMP.fullmatch(timestamp):
                    raise ValueError
                datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                problem = problem or "invalid ISO timestamp; use YYYY-MM-DDTHH:MM:SS with optional fraction and timezone"
            if not page.startswith("/"):
                problem = problem or "page must start with /"
            if not re.fullmatch(r"[1-5][0-9]{2}", status):
                problem = problem or "HTTP status must be an integer from 100 to 599"
            if any(ord(char) < 32 or ord(char) == 127 or char in "\x85\u2028\u2029"
                   for char in line if char != "\t"):
                problem = problem or "record contains control characters"
        if problem:
            error_count += 1
            if len(errors) < 20:
                errors.append(f"Line {number}: {problem}.")
        else:
            records.append(tuple(fields))
    if errors:
        suffix = f"\n{error_count - len(errors)} additional malformed lines." if error_count > len(errors) else ""
        raise PipelineError("\n".join(errors) + suffix)
    if not records:
        raise PipelineError("File contains no nonblank records.")
    return records


def compare_reports(reports, records):
    expected = {name: Counter(row[index] for row in records)
                for name, index in (("page", 2), ("ip", 0), ("status", 3))}
    if set(reports) != set(REPORTS):
        raise PipelineError("Missing or unexpected Hadoop reports; all three are required.")
    parsed = {}
    for name in REPORTS:
        actual = {}
        for number, line in enumerate(reports[name].splitlines(), 1):
            fields = line.split("\t")
            if len(fields) != 2 or not fields[0] or not re.fullmatch(r"[0-9]+", fields[1]):
                raise PipelineError(f"{name} report line {number}: expected key<TAB>nonnegative integer.")
            key, value = fields
            if key in actual:
                raise PipelineError(f"{name} report line {number}: duplicate key {key!r}.")
            actual[key] = int(value)
        if actual != dict(expected[name]) or sum(actual.values()) != len(records):
            differing = sorted(k for k in set(actual) | set(expected[name])
                               if actual.get(k) != expected[name].get(k))
            raise PipelineError(f"{name} Hadoop report does not match the upload: "
                                f"total {sum(actual.values())}, expected {len(records)}; "
                                f"differing keys (first 5): {differing[:5]!r}. Inspect the run's HDFS output.")
        parsed[name] = actual
    return parsed


def command(args, env, label, timeout=120):
    """Run without a shell; terminate the whole Hadoop process group on timeout."""
    try:
        with subprocess.Popen(args, shell=False, env=env, cwd=BASE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, encoding="utf-8", errors="replace",
                              start_new_session=True) as process:
            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass  # The process may have exited at the timeout boundary.
                process.communicate()
                raise PipelineError(f"{label} timed out after {timeout}s. Check HDFS services and EC2 resources before retrying.") from exc
            if process.returncode != 0:
                detail = (stderr or stdout)[-4000:]
                raise PipelineError(f"{label} failed (exit {process.returncode}). Check Hadoop/HDFS permissions and service logs.\n{detail}")
            return stdout
    except OSError as exc:
        raise PipelineError(f"Cannot execute {label}: {exc}. Check JAVA_HOME, HADOOP_HOME, and executable permissions.") from exc


@contextmanager
def run_lock(runtime):
    runtime.mkdir(parents=True, exist_ok=True)
    with (runtime / "hadoop.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PipelineError("Another analysis is running on this EC2 instance. Wait for it to finish, then retry.") from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def environment():
    env = os.environ.copy()
    homes = {}
    for name in ("JAVA_HOME", "HADOOP_HOME"):
        value = env.get(name, "")
        if not value or not Path(value).is_absolute():
            raise PipelineError(f"Set {name} to the absolute installation directory before starting Streamlit.")
        homes[name] = Path(value).resolve()
        env[name] = str(homes[name])
    java = homes["JAVA_HOME"] / "bin/java"
    hadoop = homes["HADOOP_HOME"] / "bin/hadoop"
    hdfs = homes["HADOOP_HOME"] / "bin/hdfs"
    for executable in (java, hadoop, hdfs):
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise PipelineError(f"Executable unavailable: {executable}. Check JAVA_HOME and HADOOP_HOME.")
    jar = homes["HADOOP_HOME"] / "share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar"
    if not jar.is_file():
        raise PipelineError(f"Hadoop 3.4.3 Streaming JAR missing: {jar}")
    env["PATH"] = f"{java.parent}:{hadoop.parent}:{env.get('PATH', '')}"
    return env, str(java), str(hadoop), str(hdfs), str(jar)


def analyze(data, progress=lambda message: None, runtime=None):
    records = validate_upload(data)
    runtime = Path(runtime) if runtime is not None else BASE / ".frontend-runs"
    with run_lock(runtime):
        progress("Checking Java, Hadoop 3.4.3, and HDFS readiness")
        env, java, hadoop, hdfs, jar = environment()
        command([java, "-version"], env, "Java check")
        version = command([hadoop, "version"], env, "Hadoop version check")
        if not re.search(r"^Hadoop 3\.4\.3\s*$", version, re.MULTILINE):
            raise PipelineError("This frontend requires Hadoop 3.4.3. Check HADOOP_HOME.")
        uri = command([hdfs, "getconf", "-confKey", "fs.defaultFS"], env, "HDFS configuration check").strip()
        if not uri.startswith("hdfs://"):
            raise PipelineError("fs.defaultFS must point to HDFS (hdfs://...). Configure the existing cluster manually.")
        safe = command([hdfs, "dfsadmin", "-safemode", "get"], env, "HDFS safe mode check")
        if "Safe mode is OFF" not in safe:
            raise PipelineError("HDFS is in safe mode. Check NameNode/DataNode health before retrying.")
        health = command([hdfs, "dfsadmin", "-report"], env, "HDFS DataNode check")
        if not re.search(r"Live datanodes \([1-9][0-9]*\)", health):
            raise PipelineError("No live HDFS DataNode found. Start/check the existing HDFS services.")
        command([hdfs, "dfs", "-test", "-d", "/"], env, "HDFS root access check")
        run_id = uuid.uuid4().hex
        local = runtime / run_id
        local.mkdir()
        (local / "upload.log").write_bytes(data)
        # HDFS ownership is enforced by HDFS; this fixed namespace has UUID isolation.
        remote = f"/tmp/cloud-log-analyzer/{run_id}"
        progress(f"Uploading validated input to HDFS (run {run_id})")
        command([hdfs, "dfs", "-mkdir", "-p", f"{remote}/input"], env, "HDFS input directory creation")
        command([hdfs, "dfs", "-put", str(local / "upload.log"), f"{remote}/input/upload.log"], env, "HDFS upload")
        reports = {}
        for name in REPORTS:
            progress(f"Running {name} Hadoop Streaming job (run {run_id})")
            output = f"{remote}/{name}-{uuid.uuid4().hex}"
            files = ",".join((BASE / filename).as_uri() for filename in
                             ("mapper.py", "reducer.py", "streaming_mapper.py"))
            command([hadoop, "jar", jar, "-D", "mapreduce.framework.name=local",
                     "-D", "mapreduce.job.reduces=1", "-D", "dfs.replication=1",
                     "-files", files, "-input", f"{remote}/input", "-output", output,
                     "-mapper", f"python3 streaming_mapper.py {name}",
                     "-reducer", "python3 reducer.py"], env, f"{name} Streaming job", timeout=600)
            command([hdfs, "dfs", "-test", "-e", f"{output}/_SUCCESS"], env, f"{name} success marker check")
            reports[name] = command([hdfs, "dfs", "-cat", f"{output}/part-*"], env, f"{name} report retrieval")
            (local / f"{name}.tsv").write_text(reports[name], encoding="utf-8")
        progress("Comparing every Hadoop key/count and total against independent Python Counters")
        counts = compare_reports(reports, records)
        return {"counts": counts, "total": sum(counts["page"].values()), "run_id": run_id, "hdfs_path": remote}
