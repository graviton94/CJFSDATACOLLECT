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

from utils.storage import load_all_data, load_recent_data
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
def load_data(days: int = 30):
    """Load data from Parquet files."""
    data_dir = Path("data/processed")
    if days == 0:
        return load_all_data(data_dir)
    else:
        return load_recent_data(data_dir, days=days)


@st.cache_resource
def get_scheduler():
    """Get scheduler instance (cached)."""
    return DataIngestionScheduler(data_dir=Path("data/processed"))


def run_collector(collector_name: str, days_back: int = 7):
    """
    Run a specific collector and return results.
    
    Args:
        collector_name: Name of collector to run
        days_back: Number of days to look back
        
    Returns:
        Number of records collected
    """
    scheduler = get_scheduler()
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
                scheduler = get_scheduler()
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
        st.info("""
        **To collect data:**
        - Use the Control Panel buttons above, or
        - Run: `python src/scheduler.py --mode once --days 7`
        """)
        return
    
    # Statistics section
    st.header("📊 Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_records = len(df)
        st.metric("📋 Total Records", f"{total_records:,}")
    
    with col2:
        if 'ingestion_date' in df.columns:
            last_updated = pd.to_datetime(df['ingestion_date']).max()
            st.metric("🕐 Last Updated", last_updated.strftime('%Y-%m-%d %H:%M'))
        else:
            st.metric("🕐 Last Updated", "N/A")
    
    with col3:
        country_count = df['origin_country'].nunique()
        st.metric("🌍 Countries", country_count)
    
    with col4:
        # Show breakdown by country (top 3)
        top_countries = df['origin_country'].value_counts().head(3)
        country_text = ", ".join([f"{country}: {count}" for country, count in top_countries.items()])
        st.metric("🔝 Top Countries", top_countries.index[0] if len(top_countries) > 0 else "N/A")
        if len(top_countries) > 0:
            st.caption(f"{top_countries.iloc[0]:,} records")
    
    # Apply additional filters
    st.sidebar.subheader("Data Filters")
    
    sources = st.sidebar.multiselect(
        "Data Sources",
        options=df['source'].unique().tolist(),
        default=df['source'].unique().tolist()
    )
    
    # Country filter
    countries = st.sidebar.multiselect(
        "Origin Country",
        options=sorted(df['origin_country'].dropna().unique().tolist()),
        default=None,
        help="Filter by country of origin"
    )
    
    # Product category filter (food_type)
    if 'product_category' in df.columns:
        product_categories = st.sidebar.multiselect(
            "Food Type / Product Category",
            options=sorted(df['product_category'].dropna().unique().tolist()),
            default=None,
            help="Filter by product category or food type"
        )
    else:
        product_categories = None
    
    # Date range filter
    if 'notification_date' in df.columns:
        df['notification_date_parsed'] = pd.to_datetime(df['notification_date'])
        min_date = df['notification_date_parsed'].min().date()
        max_date = df['notification_date_parsed'].max().date()
        
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Filter by notification date range"
        )
    else:
        date_range = None
    
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
    
    # Apply country filter
    if countries:
        df_filtered = df_filtered[df_filtered['origin_country'].isin(countries)]
    
    # Apply product category filter
    if product_categories and 'product_category' in df.columns:
        df_filtered = df_filtered[df_filtered['product_category'].isin(product_categories)]
    
    # Apply date range filter
    if date_range and len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df_filtered[
            (df_filtered['notification_date_parsed'].dt.date >= start_date) &
            (df_filtered['notification_date_parsed'].dt.date <= end_date)
        ]
    
    # Display filtered record count
    st.info(f"📊 Showing {len(df_filtered):,} records (filtered from {len(df):,} total)")
    
    # Key metrics
    st.header("📈 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Alerts", f"{len(df_filtered):,}")
    
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
    fig_products.update_xaxes(tickangle=-45)
    st.plotly_chart(fig_products, use_container_width=True)
    
    # Recent alerts table
    st.header("🔔 Recent Alerts")
    
    # Add a tab selector for different views
    tab1, tab2 = st.tabs(["📊 Summary View", "📋 Full Data Table"])
    
    with tab1:
        # Summary view with selected columns
        display_columns = [
            'notification_date', 'source', 'product_name', 'origin_country',
            'hazard_category', 'risk_level', 'risk_decision'
        ]
        
        # Only use columns that exist in the dataframe
        available_columns = [col for col in display_columns if col in df_filtered.columns]
        
        df_display = df_filtered[available_columns].sort_values(
            'notification_date', ascending=False
        ).head(100)
        
        # Format dates
        if 'notification_date' in df_display.columns:
            df_display['notification_date'] = pd.to_datetime(
                df_display['notification_date']
            ).dt.strftime('%Y-%m-%d')
        
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            height=400
        )
    
    with tab2:
        # Full data table with all columns from hub_data.parquet
        st.subheader("Complete Data from hub_data.parquet")
        st.caption(f"Displaying all {len(df_filtered)} filtered records with all available columns")
        
        # Show column selector
        all_columns = df_filtered.columns.tolist()
        
        # Let users select which columns to display
        with st.expander("🔧 Customize Columns", expanded=False):
            selected_columns = st.multiselect(
                "Select columns to display",
                options=all_columns,
                default=all_columns[:10] if len(all_columns) > 10 else all_columns,
                help="Choose which columns to show in the data table"
            )
        
        if selected_columns:
            display_df = df_filtered[selected_columns].sort_values(
                'notification_date' if 'notification_date' in selected_columns else df_filtered.columns[0],
                ascending=False
            )
        else:
            display_df = df_filtered.sort_values('notification_date', ascending=False)
        
        # Use st.data_editor for interactive table
        st.data_editor(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=500,
            disabled=True,  # Read-only mode
            num_rows="fixed"
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
