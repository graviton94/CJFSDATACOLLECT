import streamlit as st
import pandas as pd
from pathlib import Path
import time

def render_product_type_page(file_path: Path, header_map: dict):
    """Render the Product Type (품목유형) Management Page [v2.6]."""
    
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
    
    st.header("📚 품목유형 관리 [v2.6]")
    st.markdown("식약처 기준정보(품목유형)를 조회하고 수정합니다.")
    
    if not file_path.exists():
        st.error(f"⚠️ 파일을 찾을 수 없습니다: {file_path}")
        return

    try:
        # Load fresh data
        df_full = pd.read_parquet(file_path, engine='pyarrow')
        
        # Initialize critical columns
        if 'IS_MANUAL_FIXED' not in df_full.columns:
            df_full['IS_MANUAL_FIXED'] = False
            
        # 1. Name Mapping for Hierarchy
        if 'PRDLST_CD' in df_full.columns and 'KOR_NM' in df_full.columns:
            lookup = df_full.set_index('PRDLST_CD')['KOR_NM'].to_dict()
            df_full['최상위품목명'] = df_full['HTRK_PRDLST_CD'].map(lookup).fillna(df_full['HTRK_PRDLST_CD'])
            df_full['상위품목명'] = df_full['HRNK_PRDLST_CD'].map(lookup).fillna(df_full['HRNK_PRDLST_CD'])
            
            # Sort by requested priority: Name > Code > Parent > Top
            df_full = df_full.sort_values(
                by=['KOR_NM', 'PRDLST_CD', '상위품목명', '최상위품목명'],
                ascending=[True, True, True, True]
            )

        # --- Section 1: Top Controls (Search & Add) ---
        main_col1, main_col2 = st.columns(2)
        
        with main_col1:
            with st.container(border=True):
                st.subheader("🔍 계층별 데이터 검색")
                df_filtered = df_full.copy()
                
                # Rows 1-2: Hierarchy
                top_opts = ["전체"] + sorted(df_full['최상위품목명'].dropna().unique().tolist())
                sel_top = st.selectbox("최상위품목 선택", top_opts, key="fs_top")
                if sel_top != "전체":
                    df_filtered = df_filtered[df_filtered['최상위품목명'] == sel_top]
                    
                parent_opts = ["전체"] + sorted(df_filtered['상위품목명'].dropna().unique().tolist())
                sel_parent = st.selectbox("상위품목 선택", parent_opts, key="fs_parent")
                if sel_parent != "전체":
                    df_filtered = df_filtered[df_filtered['상위품목명'] == sel_parent]
                
                # Row 3: Name Search
                nm_search = st.text_input("한글명 검색", placeholder="검색어 입력...", key="fs_nm")
                if nm_search:
                    df_filtered = df_filtered[df_filtered['KOR_NM'].astype(str).str.contains(nm_search, case=False, na=False)]
                
                # Row 4: Info
                st.info(f"🔎 필터 결과: {len(df_filtered):,}건")

        with main_col2:
            with st.container(border=True):
                st.subheader("➕ 신규 인덱스 추가")
                
                # Reset logic for Add Input
                if st.session_state.get("do_reset_name"):
                    st.session_state["new_input_name"] = ""
                    st.session_state["do_reset_name"] = False

                current_input = st.session_state.get("new_input_name", "")
                r_top, r_parent = "", ""
                
                # Smart recommendation logic
                if current_input:
                    from rapidfuzz import process, fuzz
                    all_names = [str(n) for n in df_full['KOR_NM'].dropna().unique().tolist()]
                    subs = [n for n in all_names if len(n) >= 2 and (current_input in n or n in current_input)]
                    if subs:
                        best_n = min(subs, key=lambda x: abs(len(x) - len(current_input)))
                        m_row = df_full[df_full['KOR_NM'] == best_n].iloc[0]
                        r_top, r_parent = m_row['최상위품목명'], m_row['상위품목명']
                    else:
                        best_f = process.extractOne(current_input, all_names, scorer=fuzz.token_set_ratio)
                        if best_f and best_f[1] > 60:
                            m_row = df_full[df_full['KOR_NM'] == best_f[0]].iloc[0]
                            r_top, r_parent = m_row['최상위품목명'], m_row['상위품목명']

                # Sync Recommendation to Session State
                if current_input and st.session_state.get("prev_input") != current_input:
                    if r_top:
                        st.session_state["add_top_k"] = r_top
                        p_list = sorted(df_full[df_full['최상위품목명'] == r_top]['상위품목명'].dropna().unique().tolist())
                        st.session_state["add_parent_k"] = r_parent if r_parent in p_list else (p_list[0] if p_list else "")
                    st.session_state["prev_input"] = current_input

                # Dropdowns
                all_tops = sorted(df_full['최상위품목명'].dropna().unique().tolist())
                if st.session_state.get("add_top_k") not in all_tops:
                    st.session_state["add_top_k"] = all_tops[0] if all_tops else ""
                sel_add_top = st.selectbox("최상위품목 (추천)", all_tops, key="add_top_k")
                
                all_parents = sorted(df_full[df_full['최상위품목명'] == sel_add_top]['상위품목명'].dropna().unique().tolist())
                if st.session_state.get("add_parent_k") not in all_parents:
                    st.session_state["add_parent_k"] = all_parents[0] if all_parents else ""
                sel_add_parent = st.selectbox("상위품목 (추천)", all_parents, key="add_parent_k")
                
                st.text_input("⚠️신규 한글명 입력 (필수)", key="new_input_name")
                
                st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
                if st.button("➕ 항목 추가", type="secondary", use_container_width=True):
                    final_name = st.session_state.get("new_input_name", "").strip()
                    if not final_name:
                        st.error("품목명을 입력하세요.")
                    else:
                        t_m = df_full[df_full['최상위품목명'] == sel_add_top]
                        p_m = df_full[df_full['상위품목명'] == sel_add_parent]
                        if t_m.empty or p_m.empty:
                            st.error("카테고리 오류")
                        else:
                            new_row = {
                                'KOR_NM': final_name, 'PIAM_KOR_NM': "",
                                'HTRK_PRDLST_CD': t_m['HTRK_PRDLST_CD'].iloc[0],
                                'HRNK_PRDLST_CD': p_m['HRNK_PRDLST_CD'].iloc[0],
                                'IS_MANUAL_FIXED': True,
                                'PRDLST_CD': f"MAN_{int(time.time())}", 'USE_YN': 'Y'
                            }
                            df_full = pd.concat([df_full, pd.DataFrame([new_row])], ignore_index=True)
                            df_full.drop(columns=['최상위품목명', '상위품목명'], errors='ignore').to_parquet(file_path, engine='pyarrow')
                            st.toast(f"✅ '{final_name}' 항목이 추가되었습니다.", icon="➕")
                            st.cache_data.clear()
                            st.session_state["do_reset_name"] = True
                            time.sleep(1)
                            st.rerun()

        # --- Section 2: Editor & Persistence ---
        st.markdown("---")
        st.subheader("✏️ 데이터 편집기")
        
        # Versioning for total Refresh
        if "edit_v" not in st.session_state: st.session_state["edit_v"] = 0
        ed_key = f"ed_prod_v2_{st.session_state['edit_v']}"
        
        disp_df = df_filtered.copy()
        
        # Row limit for performance (prevent websocket limit issues)
        MAX_DISPLAY_ROWS = 5000
        if len(disp_df) > MAX_DISPLAY_ROWS:
            disp_df = disp_df.head(MAX_DISPLAY_ROWS)
            st.warning(f"⚠️ 데이터가 너무 많아 상위 {MAX_DISPLAY_ROWS:,}건만 표시합니다. 계층 필터를 사용하여 범위를 좁혀주세요.")
            
        cols_to_map = {
            'KOR_NM': '한글명', 
            'PRDLST_CD': '품목코드',
            '상위품목명': '상위품목명',
            '최상위품목명': '최상위품목명',
            'PIAM_KOR_NM': '속성', 
            'IS_MANUAL_FIXED': '수동고정여부'
        }
        disp_df = disp_df[['KOR_NM', 'PRDLST_CD', '상위품목명', '최상위품목명', 'PIAM_KOR_NM', 'IS_MANUAL_FIXED']].rename(columns=cols_to_map)
        
        edited_raw = st.data_editor(disp_df, num_rows="dynamic", width='stretch', height=500, key=ed_key)

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
                if not new_idx.empty: merged.loc[new_idx, 'IS_MANUAL_FIXED'] = True
                
                current_df.update(merged)
                if not new_idx.empty: current_df = pd.concat([current_df, merged.loc[new_idx]])
                
                # Write to disk
                current_df.drop(columns=['최상위품목명', '상위품목명'], errors='ignore').to_parquet(file_path, engine='pyarrow')
                st.session_state["edit_v"] += 1 # Force key change on rerun
                st.cache_data.clear()
                st.toast("✅ 모든 변경사항이 저장되었습니다.", icon="💾")
                time.sleep(1.5) # Extended pause for better visibility
                st.rerun()
            except Exception as e:
                st.error(f"저장 실패: {e}")

        @st.dialog("⚠️ 삭제 확인")
        def show_delete_confirm(nuke_df, master_df, delta):
            st.warning("정말 아래 항목들을 삭제하시겠습니까?")
            # Format display as requested
            for idx, row in nuke_df.iterrows():
                st.markdown(f"**[{idx+1}]**")
                st.text(f"최상위품목명 : {row.get('최상위품목명', '-')}")
                st.text(f"상위품목명 : {row.get('상위품목명', '-')}")
                st.text(f"한글명 : {row.get('한글명', '-')}")
                st.text(f"속성 : {row.get('속성', '-')}")
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
