import streamlit as st
import duckdb
import pandas as pd

# 0) Set up a cached DuckDB connection
@st.cache_resource
def get_conn():
    # Adjust this path if your notebook folder structure differs
    return duckdb.connect("../data/staging/warehouse.duckdb")

conn = get_conn()

# 1) Page title
st.set_page_config(page_title="Credit & Market Metrics", layout="wide")
st.title("Credit and Market Metrics Dashboard")

# 2) Load and prepare credit mart and market mart data
df_credit = conn.execute("""
    SELECT month, grade, default_rate
    FROM mart_credit_metrics
    ORDER BY month
""").df()

# Pivot so each grade gets its own column
pivot_credit = df_credit.pivot(index="month", columns="grade", values="default_rate")

df_market = conn.execute("""
    SELECT month, ticker, avg_return, volatility
    FROM mart_market_metrics
    ORDER BY month
""").df()

# --------------------------------------------
# 3) Sidebar selectors for dynamic KPIs
# --------------------------------------------
# Grade selector (single-select)
grades = sorted(df_credit.grade.unique())
sel_grade = st.sidebar.selectbox("Choose loan grade for KPI", grades, index=grades.index("G"))

# Ticker selector (single-select)
tickers = sorted(df_market.ticker.unique())
sel_ticker = st.sidebar.selectbox("Choose bank ticker for KPI", tickers, index=tickers.index("BAC"))

# Sidebar: allow the user to pick which grades to display
grades = list(pivot_credit.columns)
sel_grades = st.sidebar.multiselect(
    "Filter Grades", 
    options=grades, 
    default=grades
)

# Pivot average returns and volatility
pivot_return  = df_market.pivot(index="month", columns="ticker", values="avg_return")
pivot_vol     = df_market.pivot(index="month", columns="ticker", values="volatility")

# Sidebar: allow the user to pick which tickers to display
tickers      = list(pivot_return.columns)
sel_return   = st.sidebar.multiselect("Select Tickers for Returns", options=tickers, default=tickers[:3])
sel_volatility = st.sidebar.multiselect("Select Tickers for Volatility", options=tickers, default=tickers[:3], key="vol")

# 4) Compute KPI metrics
# 4a) Rolling 3-mo default for the selected grade
df_sel = df_credit[df_credit.grade == sel_grade].set_index("month")
roll_3m = df_sel.default_rate.rolling(window=3).mean().iloc[-1]

# 4b) Latest month for market metrics
latest_month = df_market.month.max()
df_latest = df_market[
    (df_market.ticker == sel_ticker) &
    (df_market.month == latest_month)
].iloc[0]

# Build dicts of avg_return and volatility by ticker
avg_ret = df_latest.avg_return
vol     = df_latest.volatility

# delta vs. your 20% rule (only meaningful for G, but harmless elsewhere)
delta = roll_3m - 0.20

# 5) Display KPI cards
st.subheader(f"Key Metrics as of {latest_month.date()}")

col1, col2, col3 = st.columns(3)

col1.metric(
    label=f"{sel_grade}-Grade 3-Mo Default Rate",
    value=f"{roll_3m:.1%}",
    delta=f"{delta:.1%}",
    delta_color="inverse"  # show red on positive breach
)

col2.metric(
    label=f"{sel_ticker} Avg Return (30 days)",
    value=f"{avg_ret:.2%}"
)

col3.metric(
    label=f"{sel_ticker} Volatility (30 days)",
    value=f"{vol:.2%}"
)

st.markdown("**Define: Default**")
st.caption(
    "When a loan is not payed back"
)

st.markdown("**What the Rolling Default Rate means:**")
st.caption(
    "- You choose a grade (A–G).  \n"
    "- Each month’s **default_rate** = defaults ÷ total_loans.  \n"
    "- Then we smooth by averaging the last three months:  \n"
    "  roll3ₜ = (rateₜ + rateₜ₋₁ + rateₜ₋₂) ÷ 3  \n"
    "- A higher roll3ₜ shows a sustained uptick in defaults, not just a one-off spike.")

st.markdown("**What Avg Return and Volatilty mean**")
st.caption(
    "- **Average Return:** mean percent change OVER ONE MONTH of daily closing prices for your chosen bank.  \n"
    "- **Volatility:** standard deviation of those daily returns, showing how choppy the stock was."
)

# 6) Credit | Default Rates table & chart

st.subheader("Credit Classes | Default Rates")

st.caption(
    "A: Best Credit Scores | G: Worst Credit Scores"
)
st.dataframe(df_credit.head(100000), use_container_width=True)

# Line chart for selected grades
st.line_chart(pivot_credit[sel_grades], use_container_width=True)

# 7) Market | Average Returns & Volatility
st.subheader("Market | Average Monthly Returns")
st.dataframe(df_market.head(10000)[["month", "ticker", "avg_return"]], use_container_width=True)
st.line_chart(pivot_return[sel_return], use_container_width=True)

st.subheader("Market | Monthly Volatility")
st.dataframe(df_market.head(10000)[["month", "ticker", "volatility"]], use_container_width=True)
st.line_chart(pivot_vol[sel_volatility], use_container_width=True)
