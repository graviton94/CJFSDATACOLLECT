"""
Streamlit dashboard for food safety risk analysis.
Provides unified visualization of data from all sources.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime, timedelta
import sys

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from utils.storage import load_all_data, load_recent_data


# Page configuration
st.set_page_config(
    page_title="Global Food Safety Intelligence",
    page_icon="🍎",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    </style>
    """, unsafe_allow_html=True)


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data(days: int = 30):
    """Load data from Parquet files."""
    data_dir = Path("data/processed")
    if days == 0:
        return load_all_data(data_dir)
    else:
        return load_recent_data(data_dir, days=days)


def main():
    """Main dashboard application."""
    
    # Header
    st.markdown('<h1 class="main-header">🍎 Global Food Safety Intelligence Platform</h1>', 
                unsafe_allow_html=True)
    
    st.markdown("""
    Real-time food safety risk intelligence aggregating data from:
    - 🇪🇺 **EU RASFF** (Rapid Alert System for Food and Feed)
    - 🇺🇸 **FDA Import Alerts** (US Food and Drug Administration)
    - 🇰🇷 **Korea MFDS** (Ministry of Food and Drug Safety)
    """)
    
    # Sidebar filters
    st.sidebar.header("📊 Filters")
    
    time_range = st.sidebar.selectbox(
        "Time Range",
        options=[7, 14, 30, 90, 0],
        format_func=lambda x: f"Last {x} days" if x > 0 else "All time",
        index=2  # Default to 30 days
    )
    
    # Load data
    with st.spinner("Loading data..."):
        df = load_data(days=time_range)
    
    if df.empty:
        st.warning("⚠️ No data available. Please run the data collection first.")
        st.info("Run: `python src/scheduler.py --mode once` to collect data")
        return
    
    # Apply filters
    sources = st.sidebar.multiselect(
        "Data Sources",
        options=df['source'].unique().tolist(),
        default=df['source'].unique().tolist()
    )
    
    risk_levels = st.sidebar.multiselect(
        "Risk Levels",
        options=df['risk_level'].dropna().unique().tolist(),
        default=df['risk_level'].dropna().unique().tolist()
    )
    
    # Filter dataframe
    df_filtered = df[
        (df['source'].isin(sources)) &
        (df['risk_level'].isin(risk_levels) | df['risk_level'].isna())
    ]
    
    # Key metrics
    st.header("📈 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Alerts", len(df_filtered))
    
    with col2:
        countries = df_filtered['origin_country'].nunique()
        st.metric("Countries Affected", countries)
    
    with col3:
        products = df_filtered['product_category'].nunique()
        st.metric("Product Categories", products)
    
    with col4:
        serious_alerts = len(df_filtered[df_filtered['risk_level'] == 'serious'])
        st.metric("Serious Risk Alerts", serious_alerts)
    
    # Main visualizations
    st.header("📊 Risk Analysis")
    
    # Two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Alerts by source
        st.subheader("Alerts by Source")
        source_counts = df_filtered['source'].value_counts()
        fig_source = px.pie(
            values=source_counts.values,
            names=source_counts.index,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig_source, use_container_width=True)
    
    with col2:
        # Risk level distribution
        st.subheader("Risk Level Distribution")
        risk_counts = df_filtered['risk_level'].value_counts()
        fig_risk = px.bar(
            x=risk_counts.index,
            y=risk_counts.values,
            labels={'x': 'Risk Level', 'y': 'Count'},
            color=risk_counts.index,
            color_discrete_map={
                'serious': '#d62728',
                'high': '#ff7f0e',
                'moderate': '#ffbb78',
                'low': '#98df8a'
            }
        )
        st.plotly_chart(fig_risk, use_container_width=True)
    
    # Timeline
    st.subheader("📅 Alert Timeline")
    df_timeline = df_filtered.copy()
    df_timeline['date'] = pd.to_datetime(df_timeline['notification_date']).dt.date
    timeline_counts = df_timeline.groupby(['date', 'source']).size().reset_index(name='count')
    
    fig_timeline = px.line(
        timeline_counts,
        x='date',
        y='count',
        color='source',
        labels={'date': 'Date', 'count': 'Number of Alerts', 'source': 'Source'},
        title='Daily Alert Trends'
    )
    st.plotly_chart(fig_timeline, use_container_width=True)
    
    # Top hazards and countries
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔬 Top Hazard Categories")
        top_hazards = df_filtered['hazard_category'].value_counts().head(10)
        fig_hazards = px.bar(
            x=top_hazards.values,
            y=top_hazards.index,
            orientation='h',
            labels={'x': 'Count', 'y': 'Hazard Category'}
        )
        st.plotly_chart(fig_hazards, use_container_width=True)
    
    with col2:
        st.subheader("🌍 Top Origin Countries")
        top_countries = df_filtered['origin_country'].value_counts().head(10)
        fig_countries = px.bar(
            x=top_countries.values,
            y=top_countries.index,
            orientation='h',
            labels={'x': 'Count', 'y': 'Country'}
        )
        st.plotly_chart(fig_countries, use_container_width=True)
    
    # Product categories
    st.subheader("🥗 Product Category Analysis")
    product_counts = df_filtered['product_category'].value_counts().head(15)
    fig_products = px.bar(
        x=product_counts.index,
        y=product_counts.values,
        labels={'x': 'Product Category', 'y': 'Alert Count'}
    )
    fig_products.update_xaxis(tickangle=-45)
    st.plotly_chart(fig_products, use_container_width=True)
    
    # Recent alerts table
    st.header("🔔 Recent Alerts")
    
    display_columns = [
        'notification_date', 'source', 'product_name', 'origin_country',
        'hazard_category', 'risk_level', 'risk_decision'
    ]
    
    df_display = df_filtered[display_columns].sort_values(
        'notification_date', ascending=False
    ).head(50)
    
    # Format dates
    df_display['notification_date'] = pd.to_datetime(
        df_display['notification_date']
    ).dt.strftime('%Y-%m-%d')
    
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True
    )
    
    # Download data
    st.header("💾 Export Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        csv = df_filtered.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"food_safety_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    
    with col2:
        # Data summary
        st.info(f"""
        **Data Summary**
        - Total records: {len(df_filtered)}
        - Date range: {df_filtered['notification_date'].min()} to {df_filtered['notification_date'].max()}
        - Last updated: {df_filtered['ingestion_date'].max()}
        """)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666;">
    Global Food Safety Intelligence Platform | Data sources: EU RASFF, FDA, Korea MFDS
    </div>
    """, unsafe_allow_html=True)


if __name__ == '__main__':
    main()
