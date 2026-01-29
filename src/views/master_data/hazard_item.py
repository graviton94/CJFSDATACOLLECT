import streamlit as st
import pandas as pd
from pathlib import Path
import time

def render_hazard_item_page(file_path: Path, header_map: dict):
    """Render the Hazard Item (시험항목) Management Page [v2.6]."""
    
    # Custom CSS for Top-Center Toast
    st.markdown("""
        <style>
        div[data-testid="stToast"] {
            position: fixed;
            top: 20px;
            right: 50%;
            transform: translateX(50%);
            z-index: 9999;
            background-color: #2e7d32;
            color: white;
            border-radius: 10px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.header("📚 시험항목 관리 [v2.6]")
    st.markdown("식약처 기준정보(시험항목)를 조회하고 수정합니다.")
    
    if not file_path.exists():
        st.error(f"⚠️ 파일을 찾을 수 없습니다: {file_path}")
        return

    try:
        # Load fresh data
        df_full = pd.read_parquet(file_path, engine='pyarrow')
        
        # Initialize critical columns
        if 'IS_MANUAL_FIXED' not in df_full.columns:
            df_full['IS_MANUAL_FIXED'] = False
            
        # Standardize Columns for easier internal usage
        # L_KOR_NM, M_KOR_NM, KOR_NM, ENG_NM, TESTITM_CD
        
        # --- Section 1: Top Controls (Search & Add) ---
        main_col1, main_col2 = st.columns(2)
        
        with main_col1:
            with st.container(border=True):
                st.subheader("🔍 계층별 데이터 검색")
                df_filtered = df_full.copy()
                
                # Rows 1-2: Hierarchy
                l_opts = ["전체"] + sorted(df_full['L_KOR_NM'].dropna().unique().tolist())
                sel_l = st.selectbox("대분류 선택", l_opts, key="fs_l")
                if sel_l != "전체":
                    df_filtered = df_filtered[df_filtered['L_KOR_NM'] == sel_l]
                    
                m_opts = ["전체"] + sorted(df_filtered['M_KOR_NM'].dropna().unique().tolist())
                sel_m = st.selectbox("중분류 선택", m_opts, key="fs_m")
                if sel_m != "전체":
                    df_filtered = df_filtered[df_filtered['M_KOR_NM'] == sel_m]
                
                # Row 3: Name Search
                nm_search = st.text_input("한글명 검색", placeholder="검색어 입력...", key="fs_nm_haz")
                if nm_search:
                    df_filtered = df_filtered[df_filtered['KOR_NM'].astype(str).str.contains(nm_search, case=False, na=False)]
                
                # Row 4: Info
                st.info(f"🔎 필터 결과: {len(df_filtered):,}건")

        with main_col2:
            with st.container(border=True):
                st.subheader("➕ 신규 시험항목 추가")
                
                # Reset logic for Add Input
                if st.session_state.get("do_reset_haz_name"):
                    st.session_state["new_haz_name"] = ""
                    st.session_state["do_reset_haz_name"] = False

                current_input = st.session_state.get("new_haz_name", "")
                r_l, r_m, r_eng = "", "", ""
                
                # Smart recommendation logic (Fuzzy mapping for KOR -> ENG/Hierarchy)
                if current_input:
                    from rapidfuzz import process, fuzz
                    all_names = [str(n) for n in df_full['KOR_NM'].dropna().unique().tolist()]
                    
                    # Direct substring match first
                    subs = [n for n in all_names if len(n) >= 2 and (current_input in n or n in current_input)]
                    if subs:
                        best_n = min(subs, key=lambda x: abs(len(x) - len(current_input)))
                        m_row = df_full[df_full['KOR_NM'] == best_n].iloc[0]
                        r_l, r_m, r_eng = m_row['L_KOR_NM'], m_row['M_KOR_NM'], m_row.get('ENG_NM', "")
                    else:
                        best_f = process.extractOne(current_input, all_names, scorer=fuzz.token_set_ratio)
                        if best_f and best_f[1] > 60:
                            m_row = df_full[df_full['KOR_NM'] == best_f[0]].iloc[0]
                            r_l, r_m, r_eng = m_row['L_KOR_NM'], m_row['M_KOR_NM'], m_row.get('ENG_NM', "")

                # Sync Recommendation to Session State
                if current_input and st.session_state.get("prev_haz_input") != current_input:
                    if r_l:
                        st.session_state["add_l_k"] = r_l
                        m_list = sorted(df_full[df_full['L_KOR_NM'] == r_l]['M_KOR_NM'].dropna().unique().tolist())
                        st.session_state["add_m_k"] = r_m if r_m in m_list else (m_list[0] if m_list else "")
                        st.session_state["new_haz_eng"] = r_eng
                    st.session_state["prev_haz_input"] = current_input

                # Dropdowns & Inputs
                all_ls = sorted(df_full['L_KOR_NM'].dropna().unique().tolist())
                if st.session_state.get("add_l_k") not in all_ls:
                    st.session_state["add_l_k"] = all_ls[0] if all_ls else ""
                sel_add_l = st.selectbox("대분류 (추천)", all_ls, key="add_l_k")
                
                all_ms = sorted(df_full[df_full['L_KOR_NM'] == sel_add_l]['M_KOR_NM'].dropna().unique().tolist())
                if st.session_state.get("add_m_k") not in all_ms:
                    st.session_state["add_m_k"] = all_ms[0] if all_ms else ""
                sel_add_m = st.selectbox("중분류 (추천)", all_ms, key="add_m_k")
                
                c_add1, c_add2 = st.columns(2)
                with c_add1:
                    new_kor = st.text_input("한글명 입력", key="new_haz_name")
                with c_add2:
                    new_eng = st.text_input("영문명 입력", key="new_haz_eng")
                
                st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
                if st.button("➕ 항목 추가", type="secondary", use_container_width=True):
                    final_name = st.session_state.get("new_haz_name", "").strip()
                    final_eng = st.session_state.get("new_haz_eng", "").strip()
                    if not final_name:
                        st.error("시험항목명을 입력하세요.")
                    else:
                        l_m = df_full[df_full['L_KOR_NM'] == sel_add_l]
                        if l_m.empty:
                            st.error("카테고리 오류")
                        else:
                            new_row = {
                                'KOR_NM': final_name, 'ENG_NM': final_eng,
                                'TESTITM_NM': final_name,
                                'L_KOR_NM': sel_add_l,
                                'M_KOR_NM': sel_add_m,
                                'IS_MANUAL_FIXED': True,
                                'TESTITM_CD': f"HAZ_{int(time.time())}", 'USE_YN': 'Y'
                            }
                            df_full = pd.concat([df_full, pd.DataFrame([new_row])], ignore_index=True)
                            df_full.to_parquet(file_path, engine='pyarrow')
                            st.toast(f"✅ '{final_name}' 항목이 추가되었습니다.", icon="➕")
                            st.cache_data.clear()
                            st.session_state["do_reset_haz_name"] = True
                            time.sleep(1)
                            st.rerun()

        # --- Section 2: Editor & Persistence ---
        st.markdown("---")
        st.subheader("✏️ 데이터 편집기")
        
        # Versioning for total Refresh
        if "edit_haz_v" not in st.session_state: st.session_state["edit_haz_v"] = 0
        ed_key = f"ed_haz_v2_{st.session_state['edit_haz_v']}"
        
        disp_df = df_filtered.copy()
        
        # Row limit for performance
        MAX_DISPLAY_ROWS = 5000
        if len(disp_df) > MAX_DISPLAY_ROWS:
            disp_df = disp_df.head(MAX_DISPLAY_ROWS)
            st.warning(f"⚠️ 데이터가 너무 많아 상위 {MAX_DISPLAY_ROWS:,}건만 표시합니다. 필터를 사용하여 범위를 좁혀주세요.")
            
        cols_to_map = {
            'L_KOR_NM': '대분류', 
            'M_KOR_NM': '중분류', 
            'TESTITM_NM': '표준시험항목명',
            'KOR_NM': '한글명', 
            'ENG_NM': '영문명', 
            'ABRV': '약어',
            'NCKNM': '이명',
            'ANALYZABLE': '분석가능여부',
            'INTEREST_ITEM': '관심물질등록',
            'IS_MANUAL_FIXED': '수동고정여부'
        }
        disp_df = disp_df[list(cols_to_map.keys())].rename(columns=cols_to_map)
        
        edited_raw = st.data_editor(
            disp_df, 
            num_rows="dynamic", 
            width='stretch', 
            height=500, 
            key=ed_key
        )

        def perform_final_save(current_df, changes_map, editor_df):
            try:
                # 1. Deletions
                d_rows = changes_map.get('deleted_rows', [])
                if d_rows:
                    current_df = current_df.drop(index=disp_df.index[d_rows])
                
                # 2. Edits
                e_rows = changes_map.get('edited_rows', {})
                for r_idx_s, r_diff in e_rows.items():
                    target = disp_df.index[int(r_idx_s)]
                    if target in current_df.index and '수동고정여부' not in r_diff:
                        editor_df.loc[target, '수동고정여부'] = True
                
                # 3. Merge
                rev_map = {v: k for k, v in cols_to_map.items()}
                merged = editor_df.rename(columns=rev_map)
                
                # 4. New Rows Integration
                new_idx = merged.index.difference(current_df.index)
                if not new_idx.empty: 
                    merged.loc[new_idx, 'IS_MANUAL_FIXED'] = True
                    # Fill default values for new manual rows if needed
                    for idx in new_idx:
                        if pd.isna(merged.loc[idx, 'KOR_NM']): continue
                        merged.loc[idx, 'TESTITM_NM'] = merged.loc[idx, 'KOR_NM']
                        if pd.isna(merged.loc[idx, 'TESTITM_CD']):
                            merged.loc[idx, 'TESTITM_CD'] = f"HAZ_{int(time.time())}_{idx}"
                
                current_df.update(merged)
                if not new_idx.empty: current_df = pd.concat([current_df, merged.loc[new_idx]])
                
                # Write to disk
                current_df.to_parquet(file_path, engine='pyarrow')
                st.session_state["edit_haz_v"] += 1
                st.cache_data.clear()
                st.toast("✅ 모든 변경사항이 저장되었습니다.", icon="💾")
                time.sleep(1.5)
                st.rerun()
            except Exception as e:
                st.error(f"저장 실패: {e}")

        @st.dialog("⚠️ 삭제 확인")
        def show_delete_confirm(nuke_df, master_df, delta):
            st.warning("정말 아래 항목들을 삭제하시겠습니까?")
            for idx, row in nuke_df.iterrows():
                st.markdown(f"**[{idx}]**")
                for col in nuke_df.columns:
                    if col != '수동고정여부':
                        st.text(f"{col} : {row.get(col, '-')}")
                st.markdown("---")
            
            c1, c2 = st.columns(2)
            if c1.button("🔥 삭제 진행", type="primary", use_container_width=True):
                perform_final_save(master_df, delta, edited_raw)
            if c2.button("취소", use_container_width=True):
                st.rerun()

        if st.button("💾 모든 변경사항 저장", type="primary", use_container_width=True):
            changes = st.session_state.get(ed_key, {})
            d_idx = changes.get('deleted_rows', [])
            if d_idx:
                show_delete_confirm(disp_df.iloc[d_idx], df_full, changes)
            else:
                perform_final_save(df_full, changes, edited_raw)

    except Exception as e:
        st.error(f"시스템 오류: {e}")
