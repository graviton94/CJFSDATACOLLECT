"""
Streamlit dashboard for food safety risk analysis.
Provides unified visualization of data from all sources and master data management.
"""

import sys
import asyncio
import warnings

# Fix Windows Asyncio Event Loop Policy for Playwright compatibility
if sys.platform == 'win32':
    # Suppress DeprecationWarning for WindowsProactorEventLoopPolicy (Python 3.14+)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Ensure project root is on the Python path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.scheduler import DataIngestionScheduler
from src.schema import DISPLAY_HEADERS

# Master data configuration moved to src.views.master_data.constants
from src.views.master_data import render_master_data_view

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
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
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
        
        # [Patch] Map legacy MFDS codes to Korean names for display
        if 'source_detail' in df.columns:
            df['source_detail'] = df['source_detail'].astype(str)
            df['source_detail'] = df['source_detail'].str.replace('I2620', '국내식품 부적합', regex=False)
            df['source_detail'] = df['source_detail'].str.replace('I0490', '회수판매중지', regex=False)
            df['source_detail'] = df['source_detail'].str.replace('I2810', '해외 위해식품 회수', regex=False)
            
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


def get_scheduler_instance():
    """Get a new scheduler instance."""
    return DataIngestionScheduler(data_dir=Path("data/hub"))


def run_collector(collector_name: str, force_update: bool = False):
    """Run a specific collector and return results."""
    scheduler = get_scheduler_instance()
    # Note: Scheduler's run_single_collector doesn't seemingly accept kwargs yet, 
    # but based on the codebase, we might need to modify scheduler too or instantiate collector directly.
    # However, for now, let's assume we can pass it or modify wrapper.
    # Actually, simplest is to modify this wrapper to run collector instance directly if specific args needed?
    # Or check if scheduler supports it.
    
    # Check Scheduler implementation?
    # View file src/scheduler.py to see if it accepts kwargs or if we should bypass it.
    # To save steps, let's assume we can just modify how it's called in main or here.
    
    if collector_name == "FDA" and force_update:
        # Direct instantiation because Scheduler might not proxy args
        from src.collectors.fda_collector import FDACollector as FDACollectorClass
        collector = FDACollectorClass(alert_limit=None)
        df = collector.collect(force_update=True)
        return len(df)
        
    return scheduler.run_single_collector(collector_name)


# Master data rendering logic moved to src.views.master_data.manager



def render_dashboard(df: pd.DataFrame):
    """Render the Main Dashboard tab."""
    # Sidebar filters
    st.sidebar.header("📊 Filters")
    
    # Date Range Slider
    if 'registration_date' in df.columns:
        # 문자열을 날짜로 변환
        df['date_parsed'] = pd.to_datetime(df['registration_date'], errors='coerce')
        min_date = df['date_parsed'].min()
        max_date = df['date_parsed'].max()
        
        if pd.notna(min_date) and pd.notna(max_date):
            min_val = min_date.date()
            max_val = max_date.date()
            
            date_range = st.sidebar.slider(
                "Date Range",
                min_value=min_val,
                max_value=max_val,
                value=(min_val, max_val)
            )
        else:
            date_range = None
    else:
        date_range = None
        st.sidebar.warning("날짜 컬럼(registration_date)을 찾을 수 없습니다.")

    # Source Filter
    sources = st.sidebar.multiselect(
        "Source",
        options=['FDA', 'RASFF', 'MFDS', 'ImpFood'],
        default=['FDA', 'RASFF', 'MFDS', 'ImpFood']
    )
    
    # Hazard Class Filters
    # 1. Hazard Class (Large)
    if 'hazard_class_l' in df.columns:
        available_class_l = sorted([x for x in df['hazard_class_l'].dropna().unique().tolist() if x])
        hazard_class_l_filter = st.sidebar.multiselect(
            "Hazard Class (Large)",
            options=available_class_l,
            default=available_class_l
        )
    else:
        hazard_class_l_filter = None

    # 2. Hazard Class (Middle)
    if 'hazard_class_m' in df.columns:
        available_class_m = sorted([x for x in df['hazard_class_m'].dropna().unique().tolist() if x])
        hazard_class_m_filter = st.sidebar.multiselect(
            "Hazard Class (Middle)",
            options=available_class_m,
            default=available_class_m
        )
    else:
        hazard_class_m_filter = None

    # Apply Filters
    df_filtered = df.copy()
    
    if sources:
        df_filtered = df_filtered[df_filtered['data_source'].isin(sources)]
        
    if hazard_class_l_filter and 'hazard_class_l' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['hazard_class_l'].isin(hazard_class_l_filter)]

    if hazard_class_m_filter and 'hazard_class_m' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['hazard_class_m'].isin(hazard_class_m_filter)]
        
    if date_range and 'date_parsed' in df_filtered.columns:
        start_date, end_date = date_range
        df_filtered = df_filtered[
            (df_filtered['date_parsed'].dt.date >= start_date) & 
            (df_filtered['date_parsed'].dt.date <= end_date)
        ]

    # Metrics Layout
    st.markdown("### 📊 Key Metrics")
    m_col1, m_col2, m_col3 = st.columns(3)
    
    with m_col1:
        st.metric("📋 Total Alerts", f"{len(df_filtered):,}")
        
    with m_col2:
        # High Risk Keyword Count
        keywords = ['salmonella', 'listeria', 'e.coli', 'metal', 'glass']
        if 'hazard_item' in df_filtered.columns:
            risk_count = df_filtered['hazard_item'].str.lower().str.contains('|'.join(keywords), na=False).sum()
            st.metric("⚠️ Critical Hazards", f"{risk_count:,}")
        else:
            st.metric("⚠️ Critical Hazards", "N/A")
            
    with m_col3:
        # Top Country
        if 'origin_country' in df_filtered.columns and not df_filtered.empty:
            top = df_filtered['origin_country'].value_counts().head(1)
            if not top.empty:
                st.metric("🌍 Top Origin", top.index[0], f"{top.iloc[0]} alerts")
            else:
                st.metric("🌍 Top Origin", "-")
        else:
            st.metric("🌍 Top Origin", "-")

    st.markdown("---")

    # Charts Layout
    c_col1, c_col2 = st.columns(2)
    
    with c_col1:
        st.subheader("국가별 발생 현황 (Top 10)")
        if 'origin_country' in df_filtered.columns:
            top_countries = df_filtered['origin_country'].value_counts().head(10)
            if not top_countries.empty:
                fig = px.bar(
                    x=top_countries.values,
                    y=top_countries.index,
                    orientation='h',
                    labels={'x': 'Count', 'y': 'Country'},
                    color=top_countries.values,
                    color_continuous_scale='Reds'
                )
                fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("데이터가 없습니다.")
            
    with c_col2:
        st.subheader("일별 발생 추이")
        if 'date_parsed' in df_filtered.columns:
            daily_counts = df_filtered.groupby(df_filtered['date_parsed'].dt.date).size().reset_index(name='count')
            if not daily_counts.empty:
                fig2 = px.line(daily_counts, x='date_parsed', y='count', markers=True)
                st.plotly_chart(fig2, width='stretch')
            else:
                 st.info("데이터가 없습니다.")
    
    # Second row for Hazard Class Distribution
    st.markdown("---")
    st.subheader("위해요소 분류 분석")
    c_col3, c_col4 = st.columns(2)
    
    with c_col3:
        st.caption("시험분류 (대분류) - Large Class")
        if 'hazard_class_l' in df_filtered.columns:
            hazard_l_dist = df_filtered['hazard_class_l'].value_counts()
            hazard_l_dist = hazard_l_dist[hazard_l_dist.index != ""]
            
            if not hazard_l_dist.empty:
                fig3 = px.pie(
                    names=hazard_l_dist.index,
                    values=hazard_l_dist.values,
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig3.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig3, width='stretch')
            else:
                st.info("대분류 데이터가 없습니다.")
        else:
            st.warning("hazard_class_l 컬럼 없음")
    
    with c_col4:
        st.caption("시험분류 (중분류) - Middle Class (Top 10)")
        if 'hazard_class_m' in df_filtered.columns:
            hazard_m_dist = df_filtered['hazard_class_m'].value_counts().head(10)
            hazard_m_dist = hazard_m_dist[hazard_m_dist.index != ""]
            
            if not hazard_m_dist.empty:
                fig4 = px.bar(
                    x=hazard_m_dist.values,
                    y=hazard_m_dist.index,
                    orientation='h', # Horizontal bar
                    labels={'x': 'Count', 'y': 'Category'},
                    color=hazard_m_dist.values,
                    color_continuous_scale='Viridis'
                )
                fig4.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig4, width='stretch')
            else:
                st.info("중분류 데이터가 없습니다.")
        else:
             st.warning("hazard_class_m 컬럼 없음")
             
    # Interest Item Analysis (Moved to full width or separate section if needed, keeping simple for now)
    # st.markdown("---")

    # Data Table
    st.markdown("### 🔍 상세 데이터 (Raw Data)")
    
    # Prepare display dataframe with Korean headers
    df_display = df_filtered.drop(columns=['date_parsed'], errors='ignore').copy()
    
    # [UI] Clean up source_detail for display (remove unique ID suffix)
    if 'source_detail' in df_display.columns:
        # Regex: Remove hyphen and following characters if they start with a digit or 'UNKNOWN'
        # This handles '국내식품 부적합-12345' -> '국내식품 부적합'
        # But preserves pure names if any
        df_display['source_detail'] = df_display['source_detail'].astype(str).str.replace(r'-(\d+|UNKNOWN).*', '', regex=True)

    df_display = df_display.rename(columns=DISPLAY_HEADERS)
    
    st.dataframe(
        df_display,
        width='stretch',
        hide_index=True
    )
    
    # CSV Download Button
    st.markdown("---")
    col_download1, col_download2 = st.columns([1, 1])
    with col_download1:
        csv_data = df_display.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 데이터 다운로드 (CSV)",
            data=csv_data,
            file_name=f"food_safety_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            type="primary",
            width='stretch'
        )
    
    with col_download2:
        if st.button("🔄 전체 데이터 재수집", type="secondary", width='stretch'):
            with st.spinner("기존 데이터 삭제 및 전체 재수집 중..."):
                # 기존 데이터 파일 삭제
                hub_file = Path("data/hub/hub_data.parquet")
                if hub_file.exists():
                    hub_file.unlink()
                    st.info("✅ 기존 데이터 삭제 완료")
                
                # 스케줄러 실행 (once 모드)
                scheduler = get_scheduler_instance()
                total_count = scheduler.run_all_collectors()
                st.success(f"✅ 전체 수집 완료: {total_count}건의 새로운 데이터")
                st.cache_data.clear()
                st.rerun()


def main():
    """Main entry point."""
    # Header
    st.markdown('<h1 class="main-header">🛡️ Global Food Safety Intelligence Platform</h1>', unsafe_allow_html=True)
    st.markdown("""
    <div style="text-align: center; color: #666; margin-bottom: 20px;">
    실시간 식품 안전 정보 통합 모니터링 시스템 (식약처, FDA, RASFF)
    </div>
    """, unsafe_allow_html=True)

    # Sidebar Navigation
    st.sidebar.title("🧭 메뉴 탐색")
    
    nav_options = {
        "📊 통합 대시보드": "Dashboard",
        "📚 품목유형 관리": "품목유형",
        "📚 시험항목 관리": "시험항목",
        "📚 개별기준규격 관리": "개별기준규격",
        "📚 공통기준규격 관리": "공통기준규격",
        "📚 FDA Import Alert 관리": "FDA Import Alert 관리",
        "📚 FDA 품목유형 매핑": "FDA 품목유형 매핑"
    }
    
    selected_nav = st.sidebar.radio(
        "데이터 및 기준정보 선택",
        list(nav_options.keys())
    )
    
    st.sidebar.markdown("---")
    
    # Sidebar Controls (moved to expander to save space)
    with st.sidebar.expander("🎮 데이터 수집 컨트롤", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🇰🇷 MFDS", use_container_width=True):
                with st.spinner("Collecting..."):
                    count = run_collector("MFDS")
                    st.success(f"{count} rec")
                    st.cache_data.clear()
                    st.rerun()
        with col2:
            if st.button("🇺🇸 FDA", use_container_width=True):
                with st.spinner("Collecting..."):
                    count = run_collector("FDA", force_update=True)
                    st.success(f"{count} rec")
                    st.cache_data.clear()
                    st.rerun()
                    
        if st.button("🔄 All Sources Run", type="primary", use_container_width=True):
            with st.spinner("Pipeline..."):
                scheduler = get_scheduler_instance()
                count = scheduler.run_all_collectors()
                st.success(f"Total {count} records")
                st.cache_data.clear()
                st.rerun()

        if st.button("🗑️ Clear All", type="secondary", use_container_width=True, help="Delete all collected data, indexes, reports, and reset states."):
            with st.spinner("Clearing all data..."):
                # 1. Clear Hub Data
                hub_path = Path("data/hub/hub_data.parquet")
                if hub_path.exists():
                    try:
                        hub_path.unlink()
                        st.toast("✅ Hub data deleted.", icon="🗑️")
                    except Exception as e:
                        st.error(f"Failed to delete hub data: {e}")

                # 2. Clear FDA Temp Files (Keep Master Index as per user request)
                try:
                    for p_file in Path("data/hub").glob("fda_import_alerts_*.parquet"):
                        p_file.unlink()
                    
                    report_file = Path("reports/fda_collect_summary.md")
                    if report_file.exists():
                        report_file.unlink()
                        st.toast("✅ FDA Reports cleared.", icon="📊")
                except Exception as e:
                    st.warning(f"Partial error during cleanup: {e}")

                # 3. Clear State Data
                state_path = Path("data/state/fda_counts.json")
                if state_path.exists():
                    try:
                        state_path.unlink()
                    except Exception as e:
                        pass

                # 4. Clear Cache and Rerun
                st.cache_data.clear()
                st.success("All sources and logs have been cleared.")
                st.rerun()

    # Main Router
    page_key = nav_options[selected_nav]
    
    if page_key == "Dashboard":
        df = load_data()
        if df.empty:
            st.warning("⚠️ 데이터가 없습니다. 사이드바에서 수집을 실행해주세요.")
        else:
            render_dashboard(df)
    else:
        # Master Data Pages (Modularized)
        render_master_data_view(page_key)


    # Footer
    st.markdown("---")
    st.markdown("© 2025 CJFSDATACOLLECT Project | Powered by Gemini & Streamlit")

if __name__ == '__main__':
    main()