"""Run on the EC2 host containing Hadoop and its existing HDFS configuration."""

import hashlib

import pandas as pd
import streamlit as st

from log_pipeline import PipelineError, analyze, validate_upload

st.set_page_config(page_title="Hadoop Log Analyzer", layout="wide")
st.title("Hadoop Log Analyzer")
st.caption("Runs on this EC2 host using Hadoop 3.4.3 local MapReduce and HDFS. Jobs run one at a time.")
upload = st.file_uploader("Upload a UTF-8 .log file (maximum 10 MB)", type=["log"])
data = upload.getvalue() if upload is not None else None
fingerprint = hashlib.sha256(data).hexdigest() if data is not None else None
if st.session_state.get("upload_hash") != fingerprint:
    st.session_state["upload_hash"] = fingerprint
    st.session_state.pop("result", None)
    st.session_state.pop("error", None)

records = None
if data is not None:
    try:
        records = validate_upload(data)
    except PipelineError as exc:
        st.error(str(exc))
    else:
        st.write(f"Valid records: {len(records):,}")
        st.dataframe(pd.DataFrame(records[:10], columns=["IP", "Timestamp", "Page", "HTTP status"]), hide_index=True)

if st.button("Analyze Logs", disabled=records is None, type="primary"):
    st.session_state.pop("result", None)
    st.session_state.pop("error", None)
    with st.status("Analyzing logs", expanded=True) as status:
        try:
            result = analyze(data, progress=st.write)
        except (PipelineError, OSError) as exc:
            st.session_state["error"] = str(exc)
            status.update(label="Analysis failed", state="error")
        except Exception:
            st.session_state["error"] = "Unexpected analysis failure. Check the Streamlit server traceback before retrying."
            status.update(label="Analysis failed", state="error")
            import logging
            logging.exception("Unexpected Hadoop analysis failure")
        else:
            st.session_state["result"] = result
            status.update(label="All three Hadoop reports independently verified", state="complete")

if st.session_state.get("error"):
    st.error(st.session_state["error"])
if st.session_state.get("result"):
    result = st.session_state["result"]
    st.success("All Hadoop keys/counts and all three report totals match the validated upload.")
    left, right = st.columns(2)
    left.metric("Total requests", result["total"])
    right.metric("Unique IP addresses", len(result["counts"]["ip"]))
    st.caption(f"Run: {result['run_id']} · HDFS: {result['hdfs_path']}")
    for name, label in (("page", "Page"), ("ip", "IP address"), ("status", "HTTP status")):
        st.subheader(f"{label} counts")
        frame = pd.DataFrame(result["counts"][name].items(), columns=[label, "Requests"])
        frame = frame.sort_values(["Requests", label], ascending=[False, True]).reset_index(drop=True)
        chart = frame.head(10) if name == "ip" else frame
        if name == "ip":
            st.caption("Chart: top 10 IP addresses. Table and download: all IP addresses.")
        st.bar_chart(chart.set_index(label))
        st.dataframe(frame, hide_index=True)
        st.download_button(f"Download {label.lower()} CSV", frame.to_csv(index=False).encode("utf-8"),
                           file_name=f"{name}-{result['run_id']}.csv", mime="text/csv", key=f"csv-{name}")
