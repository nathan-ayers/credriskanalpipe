import streamlit as st
import duckdb
import pandas as pd

#connect to duckdb ware house
@st.cache_resource
def get_conn():
    return duckdb.connect('../data/staging/warehouse.duckdb')

conn = get_conn()

st.title("Credit and Market Metrics Dashboard")

# Load the credit mart
df_credit = conn.execute("""
    SELECT month, grade, default_rate
    FROM mart_credit_metrics
    """).df()

pivot_credit = df_credit.pivot(index='month', columns='grade', values='default_rate')
grades = pivot_credit.columns.tolist()
sel = st.sidebar.multiselect("Filter grades", grades, default=grades)
st.subheader("Credit | Default Rates")
st.dataframe(df_credit.head(10000))
st.line_chart(pivot_credit[sel], use_container_width=True)

# Load the Market mart
df_market = conn.execute("""
  SELECT month, ticker, avg_return, volatility
  FROM mart_market_metrics
""").df()
st.subheader("Market | Avg Returns & Volatility")
st.dataframe(df_market.head(1000))
pivot_market = df_market.pivot(index='month', columns='ticker', values='avg_return')

# after you load & pivot your market mart:
tickers = pivot_market.columns.tolist()
sel = st.sidebar.multiselect("Pick bank tickers", tickers, default=tickers[:3])

# only chart the selected series
st.subheader("Average Monthly Returns")
st.line_chart(pivot_market[sel], use_container_width=True)

st.subheader("Monthly Volatility by Bank")
pivot_vol = df_market.pivot(index='month', columns='ticker', values='volatility')
sel_vol = st.sidebar.multiselect("Pick tickers for volatility", tickers, default=tickers[:3], key="vol")
st.line_chart(pivot_vol[sel_vol], use_container_width=True)
