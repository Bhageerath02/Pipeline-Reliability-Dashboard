# streamlit run pipeline_databricks_style.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ---------- Page Setup ----------
st.set_page_config(page_title="Pipeline Reliability - Code Force 360", layout="wide")

# ---------- Header ----------
st.title("Pipeline Reliability Dashboard")
st.caption("Code Force 360")

# ---------- Mock Data ----------
np.random.seed(42)
pipelines = ["Orders_ETL","Customer_DQ","Sales_Transform","Inventory_Pipeline",
             "Claims_Enrichment","IoT_Load","Employee_HR"]
owners = ["deepika","raj","mike","jeff"]

now = datetime.utcnow()
history_data = []
for p in pipelines:
    for i in range(10):
        start = now - timedelta(hours=np.random.randint(1,200))
        duration = np.random.randint(60, 500)
        status = np.random.choice(["Success","Failed"], p=[0.8,0.2])
        sla = "Met" if (duration < 300 and status=="Success") else "Violated"
        error = None if status=="Success" else np.random.choice([
            "Network Timeout","Source API Error",
            "Transformation Failure","Warehouse Credit Limit Reached"
        ])
        history_data.append([p, start, start+timedelta(seconds=duration), duration, status, sla, error])

df_history = pd.DataFrame(history_data, columns=["Pipeline","Start","End","Duration (s)","Status","SLA","Error"])

# ---------- Pipeline Summary ----------
st.subheader("All Pipelines Overview")
latest = df_history.sort_values("Start").groupby("Pipeline").tail(1)
summary = latest[["Pipeline","Status","Start","SLA"]].merge(
    df_history.groupby("Pipeline")["Duration (s)"].mean().reset_index().rename(columns={"Duration (s)":"Avg Duration (s)"}),
    on="Pipeline"
)
summary["Owner"] = np.random.choice(owners, size=len(summary))
summary["Type"] = "Ingestion Pipeline"
summary["Connection"] = summary["Pipeline"].apply(lambda x: x.split("_")[0].lower()+"_connection")

st.dataframe(summary.sort_values("Pipeline"), use_container_width=True)

# ---------- Select Pipeline ----------
selected_pipe = st.selectbox("Select Pipeline for Details", pipelines)
pipe_df = df_history[df_history["Pipeline"]==selected_pipe].sort_values("Start", ascending=False).head(10)

# ---------- Collapsible Sections ----------
with st.expander("Pipeline Metadata", expanded=True):
    st.write({
        "Pipeline ID": f"{hash(selected_pipe) % 100000}",
        "Pipeline Type": "Ingestion Pipeline",
        "Connection": f"{selected_pipe.lower()}_connection",
        "Owner": np.random.choice(owners),
        "Tags": ["prod","daily"]
    })

with st.expander("Run Details", expanded=True):
    st.dataframe(pipe_df[["Start","End","Duration (s)","Status","SLA"]], use_container_width=True)

with st.expander("Event Log", expanded=False):
    logs = []
    for _,row in pipe_df.iterrows():
        logs.append({
            "Time": row["Start"],
            "Level": "Error" if row["Status"]=="Failed" else "Info",
            "Message": row["Error"] if row["Error"] else "Pipeline step completed"
        })
    st.dataframe(pd.DataFrame(logs), use_container_width=True)

with st.expander("Query History", expanded=False):
    st.text("Sample query history for pipeline tasks...")
    qh = pd.DataFrame({
        "Query": [f"SELECT * FROM {selected_pipe.lower()}_staging LIMIT 10" for _ in range(3)],
        "Execution Time (ms)": np.random.randint(50,500,size=3),
        "Status": np.random.choice(["Completed","Failed"], size=3, p=[0.8,0.2])
    })
    st.dataframe(qh, use_container_width=True)

# ---------- Action ----------
st.subheader("Actions")
if st.button(f"Re-Run {selected_pipe}"):
    if np.random.rand() > 0.2:
        st.success(f"{selected_pipe} re-run executed successfully.")
    else:
        st.error(f"{selected_pipe} re-run failed. Reason: Mocked connection error.")
