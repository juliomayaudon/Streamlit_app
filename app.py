import streamlit as st

st.set_page_config(page_title="ABM App", page_icon="https://media.licdn.com/dms/image/v2/C4E0BAQEUNQJN0rf-yQ/company-logo_200_200/company-logo_200_200/0/1630648936722/kalungi_inc_logo?e=2147483647&v=beta&t=4vrP50CSK9jEFI7xtF7DzTlSMZdjmq__F0eG8IJwfN8")

# --- PAGE SETUP ---
about_page = st.Page(
    "Scripts/welcome.py",
    title="Welcome",
    icon=":material/account_circle:",
    default=True,
)
project_1_page = st.Page(
    "Scripts/AI_QA.py",
    title="AI QA Enrichemnt",
    icon=":material/list_alt:",
)
project_2_page = st.Page(
    "Scripts/LB_Script.py",
    title="LB Script (Complete)",
    icon=":material/table_chart:",
)

project_3_page = st.Page(
    "Scripts/soon.py",
    title="Coming Soon...",
    icon=":material/people:",
)

project_4_page = st.Page(
    "Scripts/lkdn.py",
    title="Linkedin Scripts...",
    icon=":material/supervisor_account:",
)

project_5_page = st.Page(
    "Scripts/TC.py",
    title="Title Cleaning",
    icon=":material/face:",
)


# --- NAVIGATION SETUP [WITHOUT SECTIONS] ---
# pg = st.navigation(pages=[about_page, project_1_page, project_2_page,project_3_page,project_4_page,project_5_page])

# --- NAVIGATION SETUP [WITH SECTIONS]---
pg = st.navigation(
    {
        "Info": [about_page],
        "LB Scripts": [project_1_page, project_2_page, project_5_page],
        "LinkedIn Scripts": [project_4_page],
        "Outreach Scripts": [project_3_page]
    }
)


# --- SHARED ON ALL PAGES ---
st.logo("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSYXyGNY0mQSyCRUKXrXWI4-O31kspcM0eVLg&s")
st.sidebar.markdown("Kalungi ABM App [V1.0](https://docs.google.com/document/d/1armsOtBlHntK4YUWpPH3tTLYlo53ZkzyY-yDW_Nu1x8/edit)")


# --- RUN NAVIGATION ---
pg.run()
