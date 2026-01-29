import streamlit as st
import pandas as pd
from pathlib import Path

def render_standard_master_view(selected_name: str, file_path: Path, header_map: dict):
    """Render a standard master data management page [v2]."""
    st.header(f"📚 {selected_name} 관리 [v2]")
    st.markdown(f"식약처 기준정보({selected_name}) 데이터를 조회하고 수정합니다.")
    
    if not file_path.exists():
        st.error(f"⚠️ 파일을 찾을 수 없습니다: {file_path}")
        return

    try:
        # Load full dataset
        df_full = pd.read_parquet(file_path, engine='pyarrow')
        
        # Initialize IS_MANUAL_FIXED if missing
        if 'IS_MANUAL_FIXED' not in df_full.columns:
            df_full['IS_MANUAL_FIXED'] = False
            
        st.info(f"📊 로드 완료: {len(df_full):,}건의 레코드 (수동 관리 대상 포함)")
        
        # 1. Search filter
        search_term = st.text_input(
            f"🔍 {selected_name} 키워드 검색",
            placeholder="품목명, 코드 등 아무 키워드나 입력하세요...",
            key=f"search_{selected_name}"
        )
        
        df_filtered = df_full.copy()
        if search_term:
            mask = df_full.apply(
                lambda x: x.astype(str).str.contains(search_term, case=False, na=False).any(),
                axis=1
            )
            df_filtered = df_full[mask].copy()
            st.success(f"🔎 검색 결과: {len(df_filtered):,}건")
        
        # 2. Interactive editor
        st.markdown("---")
        st.subheader("✏️ 데이터 편집기")
        
        display_df = df_filtered.copy()
        display_df = display_df.rename(columns=header_map)
        
        edited_display_df = st.data_editor(
            display_df,
            num_rows="dynamic",
            width='stretch',
            height=600,
            key=f"editor_{selected_name}_v2"
        )
        
        # 3. Save Logic
        st.markdown("---")
        if st.button(f"💾 {selected_name} 변경사항 저장", type="primary", use_container_width=True):
            try:
                changes = st.session_state.get(f"editor_{selected_name}_v2", {})
                
                # A. Handle Deletions
                deleted_rows = changes.get('deleted_rows', [])
                if deleted_rows:
                    target_ids = display_df.index[deleted_rows]
                    df_full = df_full.drop(index=target_ids)
                
                # B. Handle Edits (Auto manual flag)
                edited_rows = changes.get('edited_rows', {})
                manual_col_display = header_map.get('IS_MANUAL_FIXED', '수동고정여부')
                
                for r_idx_str, r_changes in edited_rows.items():
                    r_idx = int(r_idx_str)
                    target_idx = display_df.index[r_idx]
                    if target_idx in df_full.index:
                        if manual_col_display not in r_changes:
                            edited_display_df.loc[target_idx, manual_col_display] = True
                
                # C. Final Merge
                reverse_map = {v: k for k, v in header_map.items()}
                final_df = edited_display_df.rename(columns=reverse_map)
                
                # New rows from editor
                new_idx = final_df.index.difference(df_full.index)
                if not new_idx.empty:
                    final_df.loc[new_idx, 'IS_MANUAL_FIXED'] = True
                
                df_full.update(final_df)
                if not new_idx.empty:
                    df_full = pd.concat([df_full, final_df.loc[new_idx]])
                
                df_full.to_parquet(file_path, engine='pyarrow', compression='snappy')
                st.toast(f"✅ {selected_name} 저장 완료!", icon="💾")
                st.cache_data.clear()
                import time
                time.sleep(1)
                st.rerun()
                
            except Exception as e:
                st.error(f"저장 실패: {e}")
                
    except Exception as e:
        st.error(f"데이터 로드 중 오류 발생: {e}")
