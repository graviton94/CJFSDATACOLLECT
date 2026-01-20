"""
Streamlit dashboard for food safety risk analysis.
Provides unified visualization of data from all sources and master data management.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Ensure project root is on the Python path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.scheduler import DataIngestionScheduler
from src.schema import DISPLAY_HEADERS

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
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


def get_scheduler_instance():
    """Get a new scheduler instance."""
    return DataIngestionScheduler(data_dir=Path("data/hub"))


def run_collector(collector_name: str, days_back: int = 7):
    """Run a specific collector and return results."""
    scheduler = get_scheduler_instance()
    scheduler.days_back = days_back
    return scheduler.run_single_collector(collector_name)


def render_master_data_tab():
    """Render the Master Data Management tab."""
    st.header("📚 기준정보 데이터베이스 관리")
    st.markdown("식약처 기준정보(품목유형, 시험항목 등) Parquet 파일을 조회/수정/저장합니다.")
    
    REF_DIR = Path("data/reference")
    FILES = {
        "품목유형": "product_code_master.parquet",
        "시험항목": "hazard_code_master.parquet",
        "개별기준규격": "individual_spec_master.parquet",
        "공통기준규격": "common_spec_master.parquet"
    }
    
    # 1. File selector
    selected_name = st.selectbox(
        "📂 관리할 백서 선택",
        list(FILES.keys()),
        help="수정하려는 기준정보 파일을 선택하세요"
    )
    file_path = REF_DIR / FILES[selected_name]
    
    # 2. Load data
    if not file_path.exists():
        st.error(f"⚠️ 파일을 찾을 수 없습니다: {file_path}")
        st.info("기준정보를 먼저 생성하려면 `python src/utils/reference_loader.py`를 실행하세요.")
        return
    
    try:
        # Load full dataset
        df_full = pd.read_parquet(file_path, engine='pyarrow')
        st.success(f"✅ 로드 완료: {len(df_full):,}건의 레코드")
        
        # 3. Search filter
        search_term = st.text_input(
            "🔍 데이터 검색",
            placeholder="키워드를 입력하세요 (예: 식품, 검사항목명 등)",
            help="모든 컬럼에서 키워드를 검색합니다"
        )
        
        # Apply search filter
        if search_term:
            mask = df_full.apply(
                lambda x: x.astype(str).str.contains(search_term, case=False, na=False).any(),
                axis=1
            )
            df_display = df_full[mask].copy()
            st.info(f"🔎 검색 결과: {len(df_display):,}건 (전체: {len(df_full):,}건)")
        else:
            df_display = df_full.copy()
        
        # 4. Interactive editor
        st.markdown("---")
        st.subheader("✏️ 데이터 편집기")
        st.caption("행 수정이 가능합니다. 편집 후 반드시 '저장' 버튼을 클릭하세요.")
        
        # Apply Korean headers if columns match UNIFIED_SCHEMA
        display_df = df_display.copy()
        display_df = display_df.rename(columns=DISPLAY_HEADERS)
        
        edited_df = st.data_editor(
            display_df,
            num_rows="dynamic",
            use_container_width=True,
            height=500,
            key=f"editor_{selected_name}"
        )
        
        # Convert back to English column names for saving
        reverse_headers = {v: k for k, v in DISPLAY_HEADERS.items()}
        edited_df = edited_df.rename(columns=reverse_headers)
        
        # 5. Save logic
        st.markdown("---")
        col1, col2 = st.columns([3, 1])
        
        with col2:
            if st.button("💾 변경사항 저장", type="primary", use_container_width=True):
                try:
                    if search_term:
                        # 필터링된 상태에서는 원본 데이터의 해당 인덱스만 업데이트
                        # combine_first나 update를 사용하여 병합
                        st.info("필터링된 데이터를 원본에 병합 중...")
                        # 원본 데이터에 수정본 업데이트 (인덱스 기준)
                        df_full.update(edited_df)
                        # 추가된 행이 있다면 처리 (인덱스가 새로 생성된 경우)
                        new_rows = edited_df.index.difference(df_full.index)
                        if not new_rows.empty:
                            df_full = pd.concat([df_full, edited_df.loc[new_rows]])
                        
                        save_df = df_full
                    else:
                        save_df = edited_df
                    
                    save_df.to_parquet(file_path, engine='pyarrow', compression='snappy')
                    st.success(f"✅ {selected_name} 저장 완료!")
                    st.cache_data.clear() # 캐시 초기화
                    st.rerun()
                except Exception as e:
                    st.error(f"저장 실패: {e}")
                    
    except Exception as e:
        st.error(f"파일 로드 중 오류 발생: {e}")


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
        options=['FDA', 'RASFF', 'MFDS'],
        default=['FDA', 'RASFF', 'MFDS']
    )
    
    # Hazard Category Filter
    if 'hazard_category' in df.columns:
        available_hazards = sorted(df['hazard_category'].dropna().unique().tolist())
        hazard_categories = st.sidebar.multiselect(
            "Hazard Category",
            options=available_hazards,
            default=available_hazards
        )
    else:
        hazard_categories = None

    # Apply Filters
    df_filtered = df.copy()
    
    if sources:
        df_filtered = df_filtered[df_filtered['data_source'].isin(sources)]
        
    if hazard_categories and 'hazard_category' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['hazard_category'].isin(hazard_categories)]
        
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
            fig = px.bar(
                x=top_countries.values,
                y=top_countries.index,
                orientation='h',
                labels={'x': 'Count', 'y': 'Country'},
                color=top_countries.values,
                color_continuous_scale='Reds'
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
            
    with c_col2:
        st.subheader("일별 발생 추이")
        if 'date_parsed' in df_filtered.columns:
            daily_counts = df_filtered.groupby(df_filtered['date_parsed'].dt.date).size().reset_index(name='count')
            fig2 = px.line(daily_counts, x='date_parsed', y='count', markers=True)
            st.plotly_chart(fig2, use_container_width=True)

    # Data Table
    st.markdown("### 🔍 상세 데이터 (Raw Data)")
    
    # Prepare display dataframe with Korean headers
    df_display = df_filtered.drop(columns=['date_parsed'], errors='ignore').copy()
    df_display = df_display.rename(columns=DISPLAY_HEADERS)
    
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True
    )
    
    # CSV Download Button
    st.markdown("---")
    col_download1, col_download2 = st.columns([1, 3])
    with col_download1:
        csv_data = df_display.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 데이터 다운로드 (CSV)",
            data=csv_data,
            file_name=f"food_safety_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            type="primary"
        )


def main():
    """Main entry point."""
    # Header
    st.markdown('<h1 class="main-header">🛡️ Global Food Safety Intelligence Platform</h1>', unsafe_allow_html=True)
    st.markdown("""
    <div style="text-align: center; color: #666; margin-bottom: 20px;">
    실시간 식품 안전 정보 통합 모니터링 시스템 (식약처, FDA, RASFF)
    </div>
    """, unsafe_allow_html=True)

    # Sidebar Controls
    st.sidebar.header("🎮 Data Controls")
    days_back = st.sidebar.number_input("Days to Collect", min_value=1, value=7)
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🇰🇷 Run MFDS"):
            with st.spinner("Collecting MFDS..."):
                count = run_collector("MFDS", days_back)
                st.success(f"{count} records")
                st.cache_data.clear()
                st.rerun()
    with col2:
        if st.button("🇺🇸 Run FDA"):
            with st.spinner("Collecting FDA..."):
                count = run_collector("FDA", days_back)
                st.success(f"{count} records")
                st.cache_data.clear()
                st.rerun()
                
    if st.sidebar.button("🔄 Run All Sources", type="primary"):
        with st.spinner("Running Full Pipeline..."):
            scheduler = get_scheduler_instance()
            scheduler.days_back = days_back
            count = scheduler.run_all_collectors()
            st.success(f"Total {count} records collected.")
            st.cache_data.clear()
            st.rerun()

    st.sidebar.markdown("---")

    # Tabs
    tab1, tab2 = st.tabs(["📊 Dashboard", "📚 기준정보 관리"])
    
    with tab1:
        df = load_data()
        if df.empty:
            st.warning("⚠️ 데이터가 없습니다. 사이드바에서 수집을 실행해주세요.")
        else:
            render_dashboard(df)
            
    with tab2:
        render_master_data_tab()

    # Footer
    st.markdown("---")
    st.markdown("© 2025 CJFSDATACOLLECT Project | Powered by Gemini & Streamlit")

if __name__ == '__main__':
    main()