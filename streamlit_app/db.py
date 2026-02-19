"""
TaxiPulse — Streamlit Database Helper
Provides cached database connection for all dashboard pages.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st


@st.cache_resource
def get_engine():
    """Create a cached SQLAlchemy engine."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "taxipulse")
    user = os.getenv("POSTGRES_USER", "taxipulse")
    pw = os.getenv("POSTGRES_PASSWORD", "taxipulse123")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    """Run a SQL query and return a cached DataFrame (5 min TTL)."""
    engine = get_engine()
    return pd.read_sql(query, con=engine)