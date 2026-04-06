import streamlit as st
import pandas as pd

# -----------------------------------
# CONFIG
# -----------------------------------
BASE_PATH = "data_lake/gold/"

st.set_page_config(page_title="EV Analytics Dashboard", layout="wide")

st.title("🚗 EV Adoption Analytics Dashboard")

# -----------------------------------
# LOAD DATA
# -----------------------------------
@st.cache_data
def load_data():
    model_path = BASE_PATH + "ev_model_popularity"
    trend_path = BASE_PATH + "ev_adoption_trend"
    fact_path = BASE_PATH + "fact_ev_adoption"

    df_model = pd.read_parquet(model_path)
    df_trend = pd.read_parquet(trend_path)
    df_fact = pd.read_parquet(fact_path)

    # Clean column names (important)
    df_model.columns = df_model.columns.str.strip()
    df_trend.columns = df_trend.columns.str.strip()
    df_fact.columns = df_fact.columns.str.strip()

    return df_model, df_trend, df_fact


df_model, df_trend, df_fact = load_data()

# -----------------------------------
# SIDEBAR FILTERS
# -----------------------------------
st.sidebar.header("🔍 Filters")

selected_make = st.sidebar.selectbox(
    "Select Vehicle Make",
    ["All"] + sorted(df_fact["make"].dropna().unique().tolist())
)

if selected_make != "All":
    df_fact = df_fact[df_fact["make"] == selected_make]

# -----------------------------------
# KPI SECTION
# -----------------------------------
st.subheader("📊 Key Metrics")

col1, col2, col3 = st.columns(3)

col1.metric("Total Vehicles", int(df_fact["vehicle_count"].sum()))
col2.metric("Unique Models", df_fact["model"].nunique())
col3.metric("States Covered", df_fact["state"].nunique())

# -----------------------------------
# 📈 EV ADOPTION TREND (FIXED)
# -----------------------------------
st.subheader("📈 EV Adoption Trend Over Time")

trend_chart = df_trend.sort_values(["year", "month"]).copy()

# Create proper date column (FIX)
trend_chart["date"] = pd.to_datetime(
    trend_chart["year"].astype(str) + "-" + trend_chart["month"].astype(str)
)

st.line_chart(
    trend_chart,
    x="date",
    y="vehicle_count"
)

# -----------------------------------
# 🚗 MODEL POPULARITY
# -----------------------------------
st.subheader("🚗 Top EV Models")

top_models = df_model.sort_values("vehicle_count", ascending=False).head(10)

st.bar_chart(
    top_models.set_index("model")["vehicle_count"]
)

# -----------------------------------
# 🌍 STATE DISTRIBUTION
# -----------------------------------
st.subheader("🌍 State-wise EV Distribution")

state_data = (
    df_fact.groupby("state")["vehicle_count"]
    .sum()
    .sort_values(ascending=False)
)

st.bar_chart(state_data)

# -----------------------------------
# 📋 RAW DATA VIEW
# -----------------------------------
st.subheader("📋 Raw Data Preview")

st.dataframe(df_fact.head(100))