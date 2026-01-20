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
import os
from dotenv import load_dotenv
import yaml

# Load environment variables
load_dotenv()

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from scheduler import DataIngestionScheduler


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
def load_data():
    """Load data from hub_data.parquet file."""
    hub_path = Path("data/hub/hub_data.parquet")
    if not hub_path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(hub_path, engine='pyarrow')
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


def get_scheduler_instance():
    """
    Get a new scheduler instance (not cached to avoid state issues).
    
    Returns:
        DataIngestionScheduler instance
    """
    return DataIngestionScheduler(data_dir=Path("data/hub"))


def run_collector(collector_name: str, days_back: int = 7):
    """
    Run a specific collector and return results.
    
    Args:
        collector_name: Name of collector to run
        days_back: Number of days to look back
        
    Returns:
        Number of records collected
    """
    scheduler = get_scheduler_instance()
    scheduler.days_back = days_back
    return scheduler.run_single_collector(collector_name)


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
    
    # Sidebar - Control Panel
    st.sidebar.header("🎮 Control Panel")
    
    st.sidebar.subheader("Manual Data Collection")
    st.sidebar.markdown("Trigger specific collectors to fetch new data:")
    
    # Days back selector for manual collection
    days_back = st.sidebar.number_input(
        "Days to look back",
        min_value=1,
        max_value=30,
        value=7,
        help="Number of days to collect data for"
    )
    
    # Collector buttons
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("🇰🇷 Run MFDS", use_container_width=True):
            with st.spinner("Collecting from Korea MFDS..."):
                try:
                    count = run_collector("MFDS", days_back)
                    st.success(f"✓ Collected {count} records from MFDS")
                    st.cache_data.clear()  # Clear cache to reload data
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    with col2:
        if st.button("🇺🇸 Run FDA", use_container_width=True):
            with st.spinner("Collecting from FDA..."):
                try:
                    count = run_collector("FDA", days_back)
                    st.success(f"✓ Collected {count} records from FDA")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    if st.sidebar.button("🇪🇺 Run RASFF", use_container_width=True):
        with st.spinner("Collecting from EU RASFF..."):
            try:
                count = run_collector("RASFF", days_back)
                st.success(f"✓ Collected {count} records from RASFF")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Error: {str(e)}")
    
    if st.sidebar.button("🔄 Run All Collectors", use_container_width=True, type="primary"):
        with st.spinner("Running all collectors..."):
            try:
                scheduler = get_scheduler_instance()
                scheduler.days_back = days_back
                count = scheduler.run_all_collectors()
                st.success(f"✓ Collected {count} total records from all sources")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Error: {str(e)}")
    
    st.sidebar.markdown("---")
    
    # Sidebar filters
    st.sidebar.header("📊 Filters")
    
    # Load data first to get available options
    with st.spinner("Loading data..."):
        df = load_data()
    
    if df.empty:
        st.warning("⚠️ No data available. Please run the data collection first.")
        st.info("""
        **To collect data:**
        - Use the Control Panel buttons above, or
        - Run: `python src/scheduler.py --mode once --days 7`
        """)
        return
    
    # Date Range Slider Filter
    if 'date_registered' in df.columns:
        df['date_registered_parsed'] = pd.to_datetime(df['date_registered'], errors='coerce')
        min_date = df['date_registered_parsed'].min()
        max_date = df['date_registered_parsed'].max()
        
        # Convert to date for slider
        min_date_val = min_date.date() if pd.notna(min_date) else datetime.now().date() - timedelta(days=30)
        max_date_val = max_date.date() if pd.notna(max_date) else datetime.now().date()
        
        date_range = st.sidebar.slider(
            "Date Range",
            min_value=min_date_val,
            max_value=max_date_val,
            value=(min_date_val, max_date_val),
            help="Filter alerts by registration date range"
        )
    else:
        date_range = None
    
    # Source Multiselect Filter
    sources = st.sidebar.multiselect(
        "Source",
        options=['FDA', 'RASFF', 'MFDS'],
        default=['FDA', 'RASFF', 'MFDS'],
        help="Select data sources to include"
    )
    
    # Hazard Category Multiselect Filter
    if 'hazard_category' in df.columns:
        available_hazards = sorted(df['hazard_category'].dropna().unique().tolist())
        hazard_categories = st.sidebar.multiselect(
            "Hazard Category",
            options=available_hazards,
            default=available_hazards,
            help="Filter by hazard categories"
        )
    else:
        hazard_categories = None
    
    # Filter dataframe based on sidebar selections
    df_filtered = df.copy()
    
    # Apply source filter
    if sources:
        df_filtered = df_filtered[df_filtered['source'].isin(sources)]
    
    # Apply hazard category filter
    if hazard_categories and 'hazard_category' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['hazard_category'].isin(hazard_categories)]
    
    # Apply date range filter
    if date_range and 'date_registered_parsed' in df_filtered.columns:
        start_date, end_date = date_range
        df_filtered = df_filtered[
            (df_filtered['date_registered_parsed'].dt.date >= start_date) &
            (df_filtered['date_registered_parsed'].dt.date <= end_date)
        ]
    
    # Main Page Layout
    st.markdown("---")
    
    # Top Row: 3 Metric Cards
    st.header("📊 Key Metrics")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_alerts = len(df_filtered)
        st.metric("📋 Total Alerts", f"{total_alerts:,}")
    
    with col2:
        # High Risk Count - count records with specific high-risk hazard categories
        high_risk_keywords = ['salmonella', 'listeria', 'e.coli', 'heavy metal', 'pesticide']
        if 'hazard_category' in df_filtered.columns:
            high_risk_count = df_filtered['hazard_category'].str.lower().str.contains(
                '|'.join(high_risk_keywords), na=False
            ).sum()
        elif 'hazard_reason' in df_filtered.columns:
            high_risk_count = df_filtered['hazard_reason'].str.lower().str.contains(
                '|'.join(high_risk_keywords), na=False
            ).sum()
        else:
            high_risk_count = 0
        st.metric("⚠️ High Risk Count", f"{high_risk_count:,}")
    
    with col3:
        # Top Hazard Country - country with most alerts
        if 'origin' in df_filtered.columns and not df_filtered.empty:
            top_country = df_filtered['origin'].value_counts().head(1)
            if len(top_country) > 0:
                country_name = top_country.index[0]
                country_count = top_country.iloc[0]
                st.metric("🌍 Top Hazard Country", f"{country_name}", f"{country_count:,} alerts")
            else:
                st.metric("🌍 Top Hazard Country", "N/A")
        else:
            st.metric("🌍 Top Hazard Country", "N/A")
    
    # Middle Row: Two Charts Side-by-Side
    st.header("📈 Risk Analysis")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Alerts by Country (Top 10)")
        if 'origin' in df_filtered.columns and not df_filtered.empty:
            top_countries = df_filtered['origin'].value_counts().head(10)
            fig_countries = px.bar(
                x=top_countries.values,
                y=top_countries.index,
                orientation='h',
                labels={'x': 'Number of Alerts', 'y': 'Country'},
                title='Top 10 Countries by Alert Count'
            )
            fig_countries.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_countries, use_container_width=True)
        else:
            st.info("No country data available")
    
    with col2:
        st.subheader("Daily Alert Trends")
        if 'date_registered_parsed' in df_filtered.columns and not df_filtered.empty:
            df_timeline = df_filtered.copy()
            df_timeline['date'] = df_timeline['date_registered_parsed'].dt.date
            timeline_counts = df_timeline.groupby('date').size().reset_index(name='count')
            timeline_counts = timeline_counts.sort_values('date')
            
            fig_timeline = px.line(
                timeline_counts,
                x='date',
                y='count',
                labels={'date': 'Date', 'count': 'Number of Alerts'},
                title='Daily Alert Trends Over Time'
            )
            fig_timeline.update_traces(mode='lines+markers')
            st.plotly_chart(fig_timeline, use_container_width=True)
        else:
            st.info("No date data available for timeline")
    
    # Bottom Row: Searchable Dataframe
    st.header("🔍 Raw Data Explorer")
    st.caption(f"Displaying {len(df_filtered):,} filtered records from {len(df):,} total")
    
    # Select columns to display
    display_columns = [
        'date_registered', 'source', 'product_name', 'origin',
        'hazard_category', 'category', 'product_type', 'ref_no'
    ]
    available_columns = [col for col in display_columns if col in df_filtered.columns]
    
    if available_columns:
        df_display = df_filtered[available_columns].copy()
        
        # Sort by date if available
        if 'date_registered' in df_display.columns:
            # Add parsed column if not already in display
            if 'date_registered_parsed' not in df_display.columns and 'date_registered_parsed' in df_filtered.columns:
                df_display['date_registered_parsed'] = df_filtered['date_registered_parsed']
            
            if 'date_registered_parsed' in df_display.columns:
                df_display = df_display.sort_values('date_registered_parsed', ascending=False)
                # Format dates for display using already parsed column
                df_display['date_registered'] = df_display['date_registered_parsed'].dt.strftime('%Y-%m-%d')
                # Drop the temporary parsed column
                df_display = df_display.drop(columns=['date_registered_parsed'])
            else:
                df_display = df_display.sort_values('date_registered', ascending=False)
        
        # Display searchable dataframe
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            height=400
        )
    else:
        st.info("No data columns available for display")
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666;">
    Global Food Safety Intelligence Platform | Data sources: EU RASFF, FDA, Korea MFDS
    </div>
    """, unsafe_allow_html=True)


if __name__ == '__main__':
    main()
