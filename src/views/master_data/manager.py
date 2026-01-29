import streamlit as st
from .constants import FILES_CONFIG, MASTER_HEADER_MAP, REF_DIR
from .product_type import render_product_type_page
from .hazard_item import render_hazard_item_page
from .standard_view import render_standard_master_view

def render_master_data_view(selected_name: str):
    """
    Main entry point for rendering master data management pages.
    Delegates to specific modules based on selected_name.
    """
    file_name = FILES_CONFIG.get(selected_name)
    if not file_name:
        st.error(f"Unknown master data category: {selected_name}")
        return
        
    file_path = REF_DIR / file_name
    header_map = MASTER_HEADER_MAP.get(selected_name, {})
    
    if selected_name == "품목유형":
        render_product_type_page(file_path, header_map)
    elif selected_name == "시험항목":
        render_hazard_item_page(file_path, header_map)
    else:
        render_standard_master_view(selected_name, file_path, header_map)
