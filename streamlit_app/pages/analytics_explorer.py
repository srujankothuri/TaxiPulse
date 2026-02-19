"""
TaxiPulse — Analytics Explorer Page
Interactive charts for exploring taxi trip data.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

# Ensure db.py is importable regardless of how page is launched
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from streamlit_app.db import run_query


def render():
    st.title("🗺️ Analytics Explorer")
    st.markdown("Explore NYC taxi trip patterns and trends")

    # ---- Daily Revenue Trend ----
    st.markdown("### 📈 Daily Revenue & Trip Volume")

    daily = run_query("""
        SELECT date, total_trips, total_revenue,
               avg_fare, avg_distance, avg_duration,
               credit_card_pct, cash_pct
        FROM gold.agg_daily_summary
        ORDER BY date
    """)

    tab1, tab2 = st.tabs(["Revenue", "Trip Volume"])

    with tab1:
        fig = px.area(
            daily, x="date", y="total_revenue",
            title="Daily Total Revenue",
            labels={"total_revenue": "Revenue ($)", "date": "Date"},
            color_discrete_sequence=["#2ecc71"],
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        fig = px.bar(
            daily, x="date", y="total_trips",
            title="Daily Trip Volume",
            labels={"total_trips": "Trips", "date": "Date"},
            color_discrete_sequence=["#3498db"],
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Hourly Patterns ----
    st.markdown("### 🕐 Hourly Trip Patterns")

    hourly = run_query("""
        SELECT hour,
               SUM(total_trips) as trips,
               AVG(avg_fare) as avg_fare,
               SUM(total_revenue) as revenue
        FROM gold.agg_hourly_zone_revenue
        GROUP BY hour
        ORDER BY hour
    """)

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            hourly, x="hour", y="trips",
            title="Trips by Hour of Day",
            labels={"hour": "Hour", "trips": "Total Trips"},
            color="trips",
            color_continuous_scale="Blues",
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.line(
            hourly, x="hour", y="avg_fare",
            title="Average Fare by Hour",
            labels={"hour": "Hour", "avg_fare": "Avg Fare ($)"},
            markers=True,
            color_discrete_sequence=["#e74c3c"],
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Top Zones ----
    st.markdown("### 📍 Top Pickup Zones")

    top_zones = run_query("""
        SELECT pickup_location_id as zone_id,
               SUM(total_trips) as trips,
               SUM(total_revenue) as revenue,
               AVG(avg_fare) as avg_fare
        FROM gold.agg_hourly_zone_revenue
        GROUP BY pickup_location_id
        ORDER BY trips DESC
        LIMIT 20
    """)

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            top_zones, x="zone_id", y="trips",
            title="Top 20 Zones by Trip Volume",
            labels={"zone_id": "Zone ID", "trips": "Total Trips"},
            color="revenue",
            color_continuous_scale="Viridis",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.scatter(
            top_zones, x="trips", y="revenue",
            size="avg_fare", color="avg_fare",
            title="Zone Revenue vs Volume",
            labels={
                "trips": "Total Trips",
                "revenue": "Total Revenue ($)",
                "avg_fare": "Avg Fare ($)",
            },
            color_continuous_scale="RdYlGn",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # ---- Payment Distribution ----
    st.markdown("### 💳 Payment Method Trends")

    payment = run_query("""
        SELECT date, credit_card_pct, cash_pct,
               (100 - credit_card_pct - cash_pct) as other_pct
        FROM gold.agg_daily_summary
        ORDER BY date
    """)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=payment["date"], y=payment["credit_card_pct"],
        name="Credit Card", fill="tonexty",
        line=dict(color="#3498db"),
    ))
    fig.add_trace(go.Scatter(
        x=payment["date"], y=payment["cash_pct"],
        name="Cash", fill="tonexty",
        line=dict(color="#2ecc71"),
    ))
    fig.update_layout(
        title="Payment Method Distribution Over Time",
        yaxis_title="Percentage (%)",
        height=350,
    )
    st.plotly_chart(fig, use_container_width=True)

    # ---- Summary Stats ----
    st.markdown("### 📊 Summary Statistics")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Daily Trips", f"{daily['total_trips'].mean():,.0f}")
    col2.metric("Avg Daily Revenue", f"${daily['total_revenue'].mean():,.0f}")
    col3.metric("Avg Fare", f"${daily['avg_fare'].mean():.2f}")
    col4.metric("Avg Trip Distance", f"{daily['avg_distance'].mean():.1f} mi")


# Auto-run when Streamlit launches this page directly
render()