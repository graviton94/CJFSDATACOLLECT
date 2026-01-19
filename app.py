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
        if 'date_registered' in df.columns:
            last_updated = pd.to_datetime(df['date_registered']).max()
            st.metric("🕐 Last Updated", last_updated.strftime('%Y-%m-%d %H:%M'))
        else:
            st.metric("🕐 Last Updated", "N/A")
    
    with col3:
        country_count = df['origin'].nunique() if 'origin' in df.columns else 0
        st.metric("🌍 Countries", country_count)
    
    with col4:
        # Show breakdown by country (top 3)
        if 'origin' in df.columns:
            top_countries = df['origin'].value_counts().head(3)
            st.metric("🔝 Top Countries", top_countries.index[0] if len(top_countries) > 0 else "N/A")
            if len(top_countries) > 0:
                st.caption(f"{top_countries.iloc[0]:,} records")
        else:
            st.metric("🔝 Top Countries", "N/A")
    
    # Apply additional filters
    st.sidebar.subheader("Data Filters")
    
    sources = st.sidebar.multiselect(
        "Data Sources",
        options=df['source'].unique().tolist(),
        default=df['source'].unique().tolist()
    )
    
    # Country filter
    if 'origin' in df.columns:
        countries = st.sidebar.multiselect(
            "Origin Country",
            options=sorted(df['origin'].dropna().unique().tolist()),
            default=None,
            help="Filter by country of origin"
        )
    else:
        countries = None
    
    # Product type filter (food_type)
    if 'product_type' in df.columns:
        product_types = st.sidebar.multiselect(
            "Food Type / Product Type",
            options=sorted(df['product_type'].dropna().unique().tolist()),
            default=None,
            help="Filter by product type or food category"
        )
    else:
        product_types = None
    
    # Date range filter
    if 'date_registered' in df.columns:
        df['date_registered_parsed'] = pd.to_datetime(df['date_registered'])
        min_date = df['date_registered_parsed'].min().date()
        max_date = df['date_registered_parsed'].max().date()
        
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Filter by registration date range"
        )
    else:
        date_range = None
    
    # Hazard category filter
    if 'hazard_category' in df.columns:
        hazard_categories = st.sidebar.multiselect(
            "Hazard Categories",
            options=sorted(df['hazard_category'].dropna().unique().tolist()),
            default=df['hazard_category'].dropna().unique().tolist()
        )
    else:
        hazard_categories = None
    
    # Filter dataframe
    df_filtered = df[df['source'].isin(sources)]
    
    # Apply country filter
    if countries and 'origin' in df.columns:
        df_filtered = df_filtered[df_filtered['origin'].isin(countries)]
    
    # Apply product type filter
    if product_types and 'product_type' in df.columns:
        df_filtered = df_filtered[df_filtered['product_type'].isin(product_types)]
    
    # Apply hazard category filter
    if hazard_categories and 'hazard_category' in df.columns:
        df_filtered = df_filtered[df_filtered['hazard_category'].isin(hazard_categories)]
    
    # Apply date range filter
    if date_range and len(date_range) == 2 and 'date_registered_parsed' in df_filtered.columns:
        start_date, end_date = date_range
        df_filtered = df_filtered[
            (df_filtered['date_registered_parsed'].dt.date >= start_date) &
            (df_filtered['date_registered_parsed'].dt.date <= end_date)
        ]
    
    # Display filtered record count
    st.info(f"📊 Showing {len(df_filtered):,} records (filtered from {len(df):,} total)")
    
    # Key metrics
    st.header("📈 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Alerts", f"{len(df_filtered):,}")
    
    with col2:
        if 'origin' in df_filtered.columns:
            countries_count = df_filtered['origin'].nunique()
            st.metric("Countries Affected", countries_count)
        else:
            st.metric("Countries Affected", 0)
    
    with col3:
        if 'product_type' in df_filtered.columns:
            products = df_filtered['product_type'].nunique()
            st.metric("Product Types", products)
        else:
            st.metric("Product Types", 0)
    
    with col4:
        if 'hazard_category' in df_filtered.columns:
            hazard_count = df_filtered['hazard_category'].nunique()
            st.metric("Hazard Categories", hazard_count)
        else:
            st.metric("Hazard Categories", 0)
    
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
        # Hazard category distribution
        st.subheader("Hazard Category Distribution")
        if 'hazard_category' in df_filtered.columns:
            hazard_counts = df_filtered['hazard_category'].value_counts().head(8)
            fig_hazard = px.bar(
                x=hazard_counts.index,
                y=hazard_counts.values,
                labels={'x': 'Hazard Category', 'y': 'Count'},
                color=hazard_counts.values,
                color_continuous_scale='Reds'
            )
            fig_hazard.update_xaxes(tickangle=-45)
            st.plotly_chart(fig_hazard, use_container_width=True)
        else:
            st.info("No hazard category data available")
    
    # Timeline
    st.subheader("📅 Alert Timeline")
    if 'date_registered' in df_filtered.columns:
        df_timeline = df_filtered.copy()
        df_timeline['date'] = pd.to_datetime(df_timeline['date_registered']).dt.date
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
    else:
        st.info("No date data available for timeline")
    
    # Top hazards and countries
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔬 Top Hazard Categories")
        if 'hazard_category' in df_filtered.columns:
            top_hazards = df_filtered['hazard_category'].value_counts().head(10)
            fig_hazards = px.bar(
                x=top_hazards.values,
                y=top_hazards.index,
                orientation='h',
                labels={'x': 'Count', 'y': 'Hazard Category'}
            )
            st.plotly_chart(fig_hazards, use_container_width=True)
        else:
            st.info("No hazard category data available")
    
    with col2:
        st.subheader("🌍 Top Origin Countries")
        if 'origin' in df_filtered.columns:
            top_countries = df_filtered['origin'].value_counts().head(10)
            fig_countries = px.bar(
                x=top_countries.values,
                y=top_countries.index,
                orientation='h',
                labels={'x': 'Count', 'y': 'Country'}
            )
            st.plotly_chart(fig_countries, use_container_width=True)
        else:
            st.info("No origin country data available")
    
    # Product types
    st.subheader("🥗 Product Type Analysis")
    if 'product_type' in df_filtered.columns:
        product_counts = df_filtered['product_type'].value_counts().head(15)
        fig_products = px.bar(
            x=product_counts.index,
            y=product_counts.values,
            labels={'x': 'Product Type', 'y': 'Alert Count'}
        )
        fig_products.update_xaxes(tickangle=-45)
        st.plotly_chart(fig_products, use_container_width=True)
    else:
        st.info("No product type data available")
    
    # Recent alerts table
    st.header("🔔 Recent Alerts")
    
    # Add a tab selector for different views
    tab1, tab2 = st.tabs(["📊 Summary View", "📋 Full Data Table"])
    
    with tab1:
        # Summary view with selected columns
        display_columns = [
            'date_registered', 'source', 'product_name', 'origin',
            'hazard_category', 'category', 'product_type'
        ]
        
        # Only use columns that exist in the dataframe
        available_columns = [col for col in display_columns if col in df_filtered.columns]
        
        if available_columns:
            df_display = df_filtered[available_columns].copy()
            
            # Sort by date if available
            if 'date_registered' in df_display.columns:
                df_display = df_display.sort_values('date_registered', ascending=False).head(100)
                # Format dates
                df_display['date_registered'] = pd.to_datetime(
                    df_display['date_registered']
                ).dt.strftime('%Y-%m-%d')
            
            st.dataframe(
                df_display,
                    use_container_width=True,
                hide_index=True,
                height=400
            )
        else:
            st.info("No data columns available for display")
    
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
            display_df = df_filtered[selected_columns].copy()
            
            # Sort by date if available
            if 'date_registered' in selected_columns:
                display_df = display_df.sort_values('date_registered', ascending=False)
        else:
            display_df = df_filtered.copy()
            if 'date_registered' in display_df.columns:
                display_df = display_df.sort_values('date_registered', ascending=False)
        
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
        date_range_text = "N/A"
        if 'date_registered' in df_filtered.columns:
            min_date = pd.to_datetime(df_filtered['date_registered']).min()
            max_date = pd.to_datetime(df_filtered['date_registered']).max()
            date_range_text = f"{min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
        
        st.info(f"""
        **Data Summary**
        - Total records: {len(df_filtered)}
        - Date range: {date_range_text}
        - Sources: {', '.join(df_filtered['source'].unique())}
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
