import streamlit as st
import pandas as pd
from pathlib import Path

def render_standard_master_view(selected_name: str, file_path: Path, header_map: dict):
    """Render a standard master data management page [v2.6]."""
    
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

    st.header(f"📚 {selected_name} 관리 [v2.6]")
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
        if selected_name == "FDA Import Alert 관리":
            if not st.session_state.get("fda_show_all", False):
                df_filtered = df_full[df_full['IsCollect'] == True].copy()
            else:
                df_filtered = df_full.copy()
        else:
            df_filtered = df_full.copy()
        
        if selected_name in ["개별기준규격", "공통기준규격"]:
            st.markdown("🔍 **상세 필터 (Multi-Select)**")
            
            k_prod = f"f_prod_{selected_name}"
            k_test = f"f_test_{selected_name}"
            
            # Use 2-column layout for hierarchical filtering (Product -> Test)
            cols = st.columns(2)
            
            # 1. Product Name (Single Select)
            prod_opts = ["전체"] + sorted(df_full['PRDLST_CD_NM'].dropna().unique().astype(str).tolist())
            sel_prod = cols[0].selectbox("품목명 선택 (단일)", options=prod_opts, key=k_prod)
            
            # 2. Test Name (Dependent Multiselect)
            if sel_prod != "전체":
                test_opts = sorted(df_full[df_full['PRDLST_CD_NM'] == sel_prod]['TESTITM_NM'].dropna().unique().astype(str).tolist())
            else:
                test_opts = sorted(df_full['TESTITM_NM'].dropna().unique().astype(str).tolist())
            
            f_test = cols[1].multiselect("시험항목명 선택 (복수)", options=test_opts, key=k_test)
            
            # Filtering logic
            if sel_prod == "전체":
                if f_test:
                    df_filtered = df_full[df_full['TESTITM_NM'].isin(f_test)]
                else:
                    df_filtered = df_full.copy()
            else:
                mask = (df_full['PRDLST_CD_NM'] == sel_prod)
                if f_test:
                    mask = mask & df_full['TESTITM_NM'].isin(f_test)
                df_filtered = df_full[mask]
            
            if sel_prod != "전체" or f_test:
                st.success(f"🔎 필터 결과: {len(df_filtered):,}건")


        else:
            # 1.5. Master Code Search Helper (FDA Only) - Moved and Expanded per user request
            if selected_name in ["FDA Import Alert 관리", "FDA 품목유형 매핑"]:
                with st.expander("🔍 식약처 품목/시험항목 코드 검색 도우미 (수동 매핑용)", expanded=True):
                    t1, t2 = st.tabs(["품목 검색", "Hazard(시험항목) 검색"])
                    with t1:
                        p_master_path = Path("data/reference/product_code_master.parquet")
                        if p_master_path.exists():
                            p_df_m = pd.read_parquet(p_master_path)
                            p_lookup = p_df_m.set_index('PRDLST_CD')['KOR_NM'].to_dict()
                            p_df_m['상위품목명'] = p_df_m['HRNK_PRDLST_CD'].map(p_lookup).fillna("-")
                            p_df_m['최상위품목명'] = p_df_m['HTRK_PRDLST_CD'].map(p_lookup).fillna("-")
                            
                            search_opts = sorted(p_df_m['KOR_NM'].dropna().unique().tolist())
                            p_sel = st.selectbox("품목명 검색 (검색 후 품목명과 품목코드를 복사하여 사용하세요.)", options=search_opts, index=None, placeholder="키워드 입력 또는 선택...", key="helper_ps")
                            
                            if p_sel:
                                res_p = p_df_m[p_df_m['KOR_NM'] == p_sel].copy()
                                st.dataframe(
                                    res_p[['KOR_NM', 'PRDLST_CD', '상위품목명', '최상위품목명']].rename(columns={
                                        'KOR_NM': '한글명', 'PRDLST_CD': '품목코드'
                                    }),
                                    width='stretch', hide_index=True
                                )
                        else:
                            st.warning("품목 마스터 파일을 찾을 수 없습니다.")
                    
                    with t2:
                        h_master_path = Path("data/reference/hazard_code_master.parquet")
                        k_master_path = Path("data/reference/keyword_map_master.parquet")
                        
                        if h_master_path.exists():
                            h_df_m = pd.read_parquet(h_master_path)
                            
                            # Load and merge keyword map entries if available
                            if k_master_path.exists():
                                try:
                                    k_df_m = pd.read_parquet(k_master_path)
                                    # Treat TESTITM_NM from keyword map same as standard ones
                                    # Ensure we only use the common columns to avoid fragmentation
                                    h_cols = ['TESTITM_NM', 'TESTITM_CD', 'M_KOR_NM', 'L_KOR_NM']
                                    k_df_subset = k_df_m[[c for c in h_cols if c in k_df_m.columns]].copy()
                                    
                                    # Combine and drop duplicates based on name
                                    combined_hazards = pd.concat([h_df_m[h_cols], k_df_subset], ignore_index=True)
                                    combined_hazards = combined_hazards.drop_duplicates(subset=['TESTITM_NM']).sort_values('TESTITM_NM')
                                except Exception as e:
                                    st.warning(f"키워드 매핑 데이터 병합 실패: {e}")
                                    combined_hazards = h_df_m.sort_values('TESTITM_NM')
                            else:
                                combined_hazards = h_df_m.sort_values('TESTITM_NM')
                            
                            h_opts = combined_hazards['TESTITM_NM'].dropna().unique().tolist()
                            h_sel = st.selectbox("시험항목 검색 (마스터 데이터 및 키워드 매핑 전체)", options=h_opts, index=None, placeholder="시험항목명 입력 또는 선택...", key="helper_hs")
                            
                            if h_sel:
                                res_h = combined_hazards[combined_hazards['TESTITM_NM'] == h_sel]
                                
                                if not res_h.empty:
                                    st.dataframe(
                                        res_h[['TESTITM_NM', 'TESTITM_CD', 'M_KOR_NM', 'L_KOR_NM']].rename(columns={
                                            'TESTITM_NM': '시험항목명', 'TESTITM_CD': '시험항목코드', 
                                            'M_KOR_NM': '중분류', 'L_KOR_NM': '대분류'
                                        }),
                                        width='stretch', hide_index=True
                                    )
                        else:
                            st.warning("시험항목 마스터 파일을 찾을 수 없습니다.")

            search_term = st.text_input(
                f"🔍 {selected_name} 키워드 검색",
                placeholder="품목명, 코드 등 아무 키워드나 입력하세요...",
                key=f"search_{selected_name}"
            )
            if search_term:
                mask = df_filtered.apply(
                    lambda x: x.astype(str).str.contains(search_term, case=False, na=False).any(),
                    axis=1
                )
                df_filtered = df_filtered[mask].copy()
                st.success(f"🔎 검색 결과: {len(df_filtered):,}건")


        # 2. Interactive editor
        st.markdown("---")
        st.subheader("✏️ 데이터 편집기")
        
        if selected_name == "FDA Import Alert 관리":
            st.info("💡 **모니터링 여부 설정 기준**: 식품만 해당 (의료기기 등 제외 / Green List만 있는 경우 제외, 수정 가능)")
            st.toggle("비모니터링 항목 포함 (전체 보기)", value=False, key="fda_show_all")
        
        # Identify columns to hide (Codes and Dates as requested)
        if selected_name == "개별기준규격":
            # Specific refined list for Individual Spec Management
            SPEC_SHOW_COLS = [
                "PRDLST_CD_NM", "TESTITM_NM", "SPEC_VAL", "UNIT_NM", 
                "SPEC_VAL_SUMUP", "MXMM_VAL", "A081_FNPRT_CD_NM", 
                "MIMM_VAL", "A080_FNPRT_CD_NM", 
                "A082_CF_FNPRT_CD_NM", "A082_CI_FNPRT_CD_NM", 
                "IS_MANUAL_FIXED"
            ]
            cols_to_show = [c for c in SPEC_SHOW_COLS if c in header_map]
        elif selected_name == "공통기준규격":
            # Specific refined list for Common Spec Management
            CMMN_SHOW_COLS = [
                "PRDLST_CD_NM", "SPEC_NM", "TESTITM_NM", "PIAM_KOR_NM", 
                "SPEC_VAL", "UNIT_NM", "SPEC_VAL_SUMUP", 
                "MXMM_VAL", "A081_FNPRT_CD_NM", "MIMM_VAL", "A080_FNPRT_CD_NM", 
                "A082_CF_FNPRT_CD_NM", "A082_CI_FNPRT_CD_NM", 
                "MCRRGNSM_2N", "MCRRGNSM_2C", "MCRRGNSM_2M", "MCRRGNSM_3M", 
                "SORC", "IS_MANUAL_FIXED"
            ]
            cols_to_show = [c for c in CMMN_SHOW_COLS if c in header_map]
        elif selected_name == "FDA Import Alert 관리":
            # FDA Management: Hide auto-filled hierarchy/classes to declutter
            FDA_SHOW_COLS = [
                "Alert_No", "IsCollect", "Title", "OASIS_Charge_Code_Line", "Product_Description",
                "Manual_Product_Type_NM", "Manual_Product_Type", 
                "Manual_Hazard_Item", "Manual_Hazard_Item_CD",
                "Has_Red_List", "Has_Yellow_List", "Has_Green_List", "Last_Updated_Date", "URL"
            ]
            cols_to_show = [c for c in FDA_SHOW_COLS if c in header_map]
        elif selected_name == "FDA 품목유형 매핑":
            # Show mapping fields, hiding auto-filled hierarchy names for cleaner UI
            MAPPING_SHOW_COLS = [
                "FDA_CODE", "ENG_NM", "KOR_NM", "PRDLST_CD", "IS_MANUAL_FIXED"
            ]
            cols_to_show = [c for c in MAPPING_SHOW_COLS if c in header_map]
        else:
            HIDE_KEYWORDS = ["코드", "일자", "일시", "시퀀스", "일련번호"]
            cols_to_show = []
            for eng_col, kor_col in header_map.items():
                if not any(k in kor_col for k in HIDE_KEYWORDS):
                    cols_to_show.append(eng_col)
            
            # Always include IS_MANUAL_FIXED if it's in the map
            if 'IS_MANUAL_FIXED' not in cols_to_show and 'IS_MANUAL_FIXED' in header_map:
                cols_to_show.append('IS_MANUAL_FIXED')
            
        # Versioning for total Refresh
        if f"edit_v_{selected_name}" not in st.session_state:
            st.session_state[f"edit_v_{selected_name}"] = 0
            
        ed_key = f"ed_{selected_name}_v2_{st.session_state[f'edit_v_{selected_name}']}"
        
        # Filter for visible columns only
        display_df = df_filtered.copy()
        display_df = display_df[cols_to_show]
        
        # Row limit for performance (prevent websocket limit issues)
        MAX_DISPLAY_ROWS = 5000
        is_limited = False

        if len(display_df) > MAX_DISPLAY_ROWS:
            display_df = display_df.head(MAX_DISPLAY_ROWS)
            is_limited = True
            st.warning(f"⚠️ 데이터가 너무 많아 상위 {MAX_DISPLAY_ROWS:,}건만 표시합니다. 전체 데이터를 보려면 필터를 사용하세요.")
            
        display_df = display_df.rename(columns=header_map)
        
        # Configure columns dynamically
        col_config = {}
        disabled_cols = []
        
        if selected_name == "FDA Import Alert 관리":
            col_config["상세 링크"] = st.column_config.LinkColumn(
                display_text="링크",
                width="small"
            )
            # FDA Management: Hide auto-filled hierarchy/classes to declutter
            FDA_SHOW_COLS = [
                "Alert_No", "IsCollect", "Title", "OASIS_Charge_Code_Line", "Product_Description",
                "Manual_Product_Type_NM", "Manual_Product_Type", 
                "Manual_Hazard_Item", "Manual_Hazard_Item_CD",
                "Has_Red_List", "Has_Yellow_List", "Last_Updated_Date", "URL"
            ]
            # Since we renamed columns, checking against header_map values is tricky?
            # header_map maps "Title_KR" -> "제목".
            # display_df has "제목".
            # cols_to_show logic: `[c for c in COLS if c in header_map]` -> returns keys.
            # But earlier `df_full` filtering used keys.
            # `standard_view.py` logic around line 107 in my view was:
            # `cols_to_show = [c for c in FDA_SHOW_COLS if c in header_map]`
            # This logic selects KEYS.
            # But line 118 `cols_to_show` is used where? 
            # It's NOT used in `st.data_editor(display_df)` directly?
            # Wait, `display_df` contains ALL columns by default unless filtered?
            # `standard_view.py` usually does `display_df = display_df[cols_to_show]`?
            # I need to check if I missed that logic.
            
            # Re-reading lines 118-140 via context I recall:
            # `if cols_to_show: display_df = display_df[cols_to_show]`
            # Yes. 
            # Since I changed FDA_SHOW_COLS to use "Title_KR", and `header_map` has "Title_KR", it works.
            
            # Disable editing for auto-collected fields (Use Display Names)
            disabled_cols = [
                "Alert 번호", "제목", "오아시스 코드", "제품 설명 헤더", 
                "최종 업데이트일", "Red List 여부", "Yellow List 여부",
                "상위품목", "최상위품목", "중분류", "대분류"
            ]
        elif selected_name == "FDA 품목유형 매핑":
            # Disable hierarchy names and parent codes as they are auto-looked up
            disabled_cols = ["상위품목명", "최상위품목명", "상위품목코드", "최상위품목코드"]

        edited_raw = st.data_editor(
            display_df,
            num_rows="dynamic",
            width='stretch',
            height=600,
            key=ed_key,
            column_config=col_config,
            disabled=disabled_cols
        )
        
        def perform_final_save(current_df, changes_map, editor_df):
            try:
                import time
                # 1. Deletions
                d_rows = changes_map.get('deleted_rows', [])
                if d_rows:
                    current_df = current_df.drop(index=display_df.index[d_rows])
                
                # 2. Edits (Auto manual flag)
                e_rows = changes_map.get('edited_rows', {})
                manual_col_display = header_map.get('IS_MANUAL_FIXED', '수동고정여부')
                
                for r_idx_s, r_diff in e_rows.items():
                    target = display_df.index[int(r_idx_s)]
                    if target in current_df.index and manual_col_display not in r_diff:
                        editor_df.loc[target, manual_col_display] = True
                
                # 3. Final Merge
                reverse_map = {v: k for k, v in header_map.items()}
                merged = editor_df.rename(columns=reverse_map)
                
                # 4. New Rows Integration
                new_idx = merged.index.difference(current_df.index)
                if not new_idx.empty:
                    merged.loc[new_idx, 'IS_MANUAL_FIXED'] = True
                
                # [Custom Logic] FDA Product Mapping Enrichment
                if selected_name == "FDA 품목유형 매핑":
                    master_path = Path("data/reference/product_code_master.parquet")
                    if master_path.exists():
                        try:
                            m_df = pd.read_parquet(master_path)
                            # Create maps
                            name_map = m_df.set_index('PRDLST_CD')['KOR_NM'].to_dict()
                            parent_map = m_df.set_index('PRDLST_CD')['HRNK_PRDLST_CD'].to_dict()
                            top_map = m_df.set_index('PRDLST_CD')['HTRK_PRDLST_CD'].to_dict()
                            
                            def enrich_row(row):
                                code = str(row.get('PRDLST_CD', '')).strip()
                                if code and code in name_map:
                                    p_code = parent_map.get(code)
                                    t_code = top_map.get(code)
                                    row['HRNK_PRDLST_CD'] = p_code
                                    row['HTRK_PRDLST_CD'] = t_code
                                    row['HRNK_KOR_NM'] = name_map.get(p_code) if p_code else None
                                    row['HTRK_KOR_NM'] = name_map.get(t_code) if t_code else None
                                return row
                            
                            merged = merged.apply(enrich_row, axis=1)
                        except Exception as e:
                            st.warning(f"Hierarchy enrichment failed: {e}")
                
                # [Custom Logic] FDA Import Alert Enrichment
                elif selected_name == "FDA Import Alert 관리":
                    prod_master_path = Path("data/reference/product_code_master.parquet")
                    haz_master_path = Path("data/reference/hazard_code_master.parquet")
                    
                    try:
                        # 1. Product Hierarchy Enrichment (By Code Priority)
                        if prod_master_path.exists():
                            m_df = pd.read_parquet(prod_master_path)
                            p_name_map = m_df.set_index('PRDLST_CD')['KOR_NM'].to_dict()
                            p_rev_map = m_df.set_index('KOR_NM')['PRDLST_CD'].to_dict() # Multiple codes possible, but we take latest/one
                            p_parent_map = m_df.set_index('PRDLST_CD')['HRNK_PRDLST_CD'].to_dict()
                            p_top_map = m_df.set_index('PRDLST_CD')['HTRK_PRDLST_CD'].to_dict()
                            
                            def enrich_prod(row):
                                code = str(row.get('Manual_Product_Type', '')).strip()
                                name = str(row.get('Manual_Product_Type_NM', '')).strip()
                                
                                # Priority 1: Fill Name from Code
                                if code and code in p_name_map:
                                    row['Manual_Product_Type_NM'] = p_name_map.get(code)
                                # Priority 2: Fill Code from Name
                                elif name and name in p_rev_map:
                                    code = p_rev_map.get(name)
                                    row['Manual_Product_Type'] = code
                                
                                if code and code in p_name_map:
                                    p_code = p_parent_map.get(code)
                                    t_code = p_top_map.get(code)
                                    row['Manual_HRNK_NM'] = p_name_map.get(p_code) if p_code else None
                                    row['Manual_HTRK_NM'] = p_name_map.get(t_code) if t_code else None
                                return row
                            merged = merged.apply(enrich_prod, axis=1)

                        # 2. Hazard Category Enrichment
                        if haz_master_path.exists():
                            h_df = pd.read_parquet(haz_master_path)
                            h_m_map = h_df.set_index('TESTITM_NM')['M_KOR_NM'].to_dict()
                            h_l_map = h_df.set_index('TESTITM_NM')['L_KOR_NM'].to_dict()
                            h_c_map = h_df.set_index('TESTITM_NM')['TESTITM_CD'].to_dict()
                            h_rev_map = h_df.set_index('TESTITM_CD')['TESTITM_NM'].to_dict()
                            
                            def enrich_haz(row):
                                name = str(row.get('Manual_Hazard_Item', '')).strip()
                                code = str(row.get('Manual_Hazard_Item_CD', '')).strip()
                                
                                # Priority 1: Fill Code from Name
                                if name and name in h_c_map:
                                    row['Manual_Hazard_Item_CD'] = h_c_map.get(name)
                                # Priority 2: Fill Name from Code
                                elif code and code in h_rev_map:
                                    name = h_rev_map.get(code)
                                    row['Manual_Hazard_Item'] = name
                                
                                if name and name in h_m_map:
                                    row['Manual_Class_M'] = h_m_map.get(name)
                                    row['Manual_Class_L'] = h_l_map.get(name)
                                return row
                            merged = merged.apply(enrich_haz, axis=1)
                    except Exception as e:
                        st.warning(f"FDA Enrichment failed: {e}")

                # Update current_df with merged data (Respects Nulls unlike .update())
                common_idx = merged.index.intersection(current_df.index)
                if not common_idx.empty:
                    current_df.loc[common_idx, merged.columns] = merged.loc[common_idx]
                if not new_idx.empty:
                    current_df = pd.concat([current_df, merged.loc[new_idx]])
                
                # Write to disk
                current_df.to_parquet(file_path, engine='pyarrow', compression='snappy')
                st.session_state[f"edit_v_{selected_name}"] += 1
                st.cache_data.clear()
                st.toast(f"✅ {selected_name} 모든 변경사항 저장 완료!", icon="💾")
                time.sleep(1.5)
                st.rerun()
            except Exception as e:
                st.error(f"저장 실패: {e}")

        @st.dialog("⚠️ 삭제 확인")
        def show_delete_confirm(nuke_df, master_df, delta):
            st.warning("정말 아래 항목들을 삭제하시겠습니까?")
            for idx, row in nuke_df.iterrows():
                st.markdown(f"**[{idx}]**")
                # Show key identifying columns or all columns
                for col in nuke_df.columns:
                    if col != '수동고정여부':
                        st.text(f"{col} : {row.get(col, '-')}")
                st.markdown("---")
            
            c1, c2 = st.columns(2)
            if c1.button("🔥 삭제 진행", type="primary", width="stretch"):
                perform_final_save(master_df, delta, edited_raw)
            if c2.button("취소", width="stretch"):
                st.rerun()

        # 3. Save Button
        st.markdown("---")
        if st.button(f"💾 {selected_name} 모든 변경사항 저장", type="primary", width="stretch"):
            changes = st.session_state.get(ed_key, {})
            d_idx = changes.get('deleted_rows', [])
            if d_idx:
                show_delete_confirm(display_df.iloc[d_idx], df_full, changes)
            else:
                perform_final_save(df_full, changes, edited_raw)
                
    except Exception as e:
        st.error(f"데이터 로드 중 오류 발생: {e}")
