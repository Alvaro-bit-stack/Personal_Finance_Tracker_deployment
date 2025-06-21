import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px
import os
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

# Set wide layout
st.set_page_config(page_title="Personal Finance Dashboard", layout="wide")

# Connect to Supabase database
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT")
)

# --- KPI Data ---
df_kpi = pd.read_sql("""
    SELECT *
    FROM
        (SELECT SUM(amount) AS total_monthly_expenses
         FROM transactions
         WHERE "type" = 'expense'),
        (SELECT SUM(amount) AS total_monthly_income
         FROM transactions
         WHERE "type" = 'income');
""", conn)

# Extract values
income = float(df_kpi['total_monthly_income'][0])
expenses = float(df_kpi['total_monthly_expenses'][0])
net_savings = income - expenses

# --- KPI Display ---
st.title("💼 Personal Finance Dashboard")
st.markdown("## 🔢 Monthly Summary")

col1, col2, col3 = st.columns(3)
col1.metric("💰 Total Income", f"${income:,.2f}")
col2.metric("💸 Total Expenses", f"${expenses:,.2f}")
col3.metric("📈 Net Savings", f"${net_savings:,.2f}")

st.markdown("---")

# --- Income vs Expenses Chart ---
st.subheader("📊 Income vs Expenses")
fig = px.bar(df_kpi, x=["total_monthly_income", "total_monthly_expenses"],
             labels={"value": "Amount", "variable": "Type"},
             title="Monthly Income vs Expenses", barmode="group")
st.plotly_chart(fig, use_container_width=True)

# --- Expenses by Category ---
df_expenses_by_category = pd.read_sql("""
    SELECT * 
    FROM
        (SELECT SUM(amount) AS "Groceries"
         FROM transactions WHERE category_id = 1 AND "type" = 'expense'),
        (SELECT SUM(amount) AS "Subscriptions"
         FROM transactions WHERE category_id = 2 AND "type" = 'expense'),
        (SELECT SUM(amount) AS "Miscellaneous"
         FROM transactions WHERE category_id = 3 AND "type" = 'expense'),
        (SELECT SUM(amount) AS "Transportation"
         FROM transactions WHERE category_id = 4 AND "type" = 'expense'),
        (SELECT SUM(amount) AS "Shopping/Going out"
         FROM transactions WHERE category_id = 6 AND "type" = 'expense');
""", conn)

st.markdown("## 🧾 Expenses by Category")
fig_expense = px.bar(df_expenses_by_category,
    x=["Groceries", "Subscriptions", "Miscellaneous", "Transportation", "Shopping/Going out"],
    labels={"value": "Amount", "variable": "Category"},
    title="Expenses by Category",
    barmode="group"
)
st.plotly_chart(fig_expense, use_container_width=True)

# --- Most Expensive Category ---
df_most_expensive = pd.read_sql("""
    SELECT o.total_expense, c.name
    FROM (
        SELECT SUM(amount) AS total_expense, category_id
        FROM transactions
        WHERE "type" = 'expense'
        GROUP BY category_id
        ORDER BY total_expense DESC
        LIMIT 1
    ) AS o
    JOIN categories c USING(category_id);
""", conn)

st.markdown("## 🏆 Most Expensive Category")
st.write(df_most_expensive)

# --- Average Daily Expenses ---
df_avg_daily = pd.read_sql("""
    SELECT 
        date::date AS day,
        SUM(amount) AS average_daily_expense
    FROM transactions
    WHERE 
        type = 'expense' AND
        date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
    GROUP BY day
    ORDER BY day;
""", conn)

st.markdown("## 📅 Daily Spending Trends")
fig_daily_expenses = px.scatter(df_avg_daily, 
    x='day', 
    y='average_daily_expense',
    title='Average Daily Expenses Over the Last Month',
    labels={'day': 'Date', 'average_daily_expense': 'Total Daily Expense'})
st.plotly_chart(fig_daily_expenses, use_container_width=True)
