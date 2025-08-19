

import streamlit as st
import pandas as pd
import sqlite3
import os
from io import BytesIO
from datetime import datetime

st.set_page_config(page_title="Local Food Waste Dashboard", layout="wide")

BASE_PATH = os.path.expanduser(r"C:/Users/Shweta/OneDrive/Desktop/local-food-waste")
DB_PATH = os.path.join(BASE_PATH, "food_waste.db")

# Utility: ensure DB exists and tables loaded from CSVs
@st.cache_resource
def init_db(load_csv=True):
    # Create DB folder if needed
    os.makedirs(BASE_PATH, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    if load_csv:
        # load CSVs if present
        try:
            providers_fp = os.path.join(BASE_PATH, "providers_data.csv")
            receivers_fp = os.path.join(BASE_PATH, "receivers_data.csv")
            listings_fp = os.path.join(BASE_PATH, "food_listings_data.csv")
            claims_fp = os.path.join(BASE_PATH, "claims_data.csv")
            if os.path.exists(providers_fp):
                pd.read_csv(providers_fp).to_sql("providers", conn, if_exists="replace", index=False)
            if os.path.exists(receivers_fp):
                pd.read_csv(receivers_fp).to_sql("receivers", conn, if_exists="replace", index=False)
            if os.path.exists(listings_fp):
                pd.read_csv(listings_fp).to_sql("food_listings", conn, if_exists="replace", index=False)
            if os.path.exists(claims_fp):
                pd.read_csv(claims_fp).to_sql("claims", conn, if_exists="replace", index=False)
        except Exception as e:
            st.error(f"Error loading CSVs: {e}")
    return conn

conn = init_db(load_csv=True)

# Helper to read tables
@st.cache_data
def read_table(table):
    return pd.read_sql_query(f"SELECT * FROM {table}", conn)

# Helper to run query and return df
def run_sql(query):
    return pd.read_sql_query(query, conn)

# CRUD helpers
def insert_row(table, row_dict):
    cols = ", ".join(row_dict.keys())
    placeholders = ", ".join(["?" for _ in row_dict])
    vals = tuple(row_dict.values())
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    cur = conn.cursor()
    cur.execute(sql, vals)
    conn.commit()
    return cur.lastrowid

def update_row(table, pk_col, pk_val, update_dict):
    set_clause = ", ".join([f"{k} = ?" for k in update_dict.keys()])
    vals = tuple(update_dict.values()) + (pk_val,)
    sql = f"UPDATE {table} SET {set_clause} WHERE {pk_col} = ?"
    conn.execute(sql, vals)
    conn.commit()

def delete_row(table, pk_col, pk_val):
    sql = f"DELETE FROM {table} WHERE {pk_col} = ?"
    conn.execute(sql, (pk_val,))
    conn.commit()

# Queries dictionary (15 queries adjusted to your column names)
QUERIES = {
    "Q1_Providers_and_Receivers_per_City": """
        SELECT City,
               (SELECT COUNT(*) FROM providers p2 WHERE p2.City = City) AS Provider_Count,
               (SELECT COUNT(*) FROM receivers r2 WHERE r2.City = City) AS Receiver_Count
        FROM (
            SELECT City FROM providers
            UNION
            SELECT City FROM receivers
        ) as cities
        GROUP BY City
    """,

    "Q2_Most_common_provider_type": """
        SELECT Type AS Provider_Type, COUNT(*) AS Count
        FROM providers
        GROUP BY Type
        ORDER BY Count DESC
        LIMIT 1
    """,

    "Q3_Provider_contacts_in_city": """
        SELECT Name, Contact, Address
        FROM providers
        WHERE City = :city
    """,

    "Q4_Receivers_with_most_claims": """
        SELECT r.Name AS ReceiverName, COUNT(c.Claim_ID) AS ClaimCount
        FROM claims c
        JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
        GROUP BY r.Receiver_ID
        ORDER BY ClaimCount DESC
    """,

    "Q5_Total_quantity_available": """
        SELECT SUM(Quantity) AS Total_Quantity
        FROM food_listings
    """,

    "Q6_City_with_most_listings": """
        SELECT Location AS City, COUNT(*) AS Listings_Count
        FROM food_listings
        GROUP BY Location
        ORDER BY Listings_Count DESC
        LIMIT 1
    """,

    "Q7_Most_common_food_types": """
        SELECT Food_Type, COUNT(*) AS Count
        FROM food_listings
        GROUP BY Food_Type
        ORDER BY Count DESC
    """,

    "Q8_Claims_per_food_item": """
        SELECT f.Food_Name, COUNT(c.Claim_ID) AS Claim_Count
        FROM claims c
        JOIN food_listings f ON c.Food_ID = f.Food_ID
        GROUP BY f.Food_ID
        ORDER BY Claim_Count DESC
    """,

    "Q9_Provider_with_highest_successful_claims": """
        SELECT p.Name AS ProviderName, COUNT(c.Claim_ID) AS Successful_Claims
        FROM claims c
        JOIN food_listings f ON c.Food_ID = f.Food_ID
        JOIN providers p ON f.Provider_ID = p.Provider_ID
        WHERE c.Status = 'Completed'
        GROUP BY p.Provider_ID
        ORDER BY Successful_Claims DESC
        LIMIT 1
    """,

    "Q10_Claim_status_percentages": """
        SELECT Status, COUNT(*) * 100.0 / (SELECT COUNT(*) FROM claims) AS Percentage
        FROM claims
        GROUP BY Status
    """,

    "Q11_Avg_quantity_claimed_per_receiver": """
        SELECT r.Name AS ReceiverName, AVG(f.Quantity) AS Avg_Quantity_Claimed
        FROM claims c
        JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
        JOIN food_listings f ON c.Food_ID = f.Food_ID
        GROUP BY r.Receiver_ID
    """,

    "Q12_Most_claimed_meal_type": """
        SELECT f.Meal_Type, COUNT(c.Claim_ID) AS ClaimCount
        FROM claims c
        JOIN food_listings f ON c.Food_ID = f.Food_ID
        GROUP BY f.Meal_Type
        ORDER BY ClaimCount DESC
        LIMIT 1
    """,

    "Q13_Total_quantity_donated_by_provider": """
        SELECT p.Name AS ProviderName, SUM(f.Quantity) AS Total_Donated
        FROM food_listings f
        JOIN providers p ON f.Provider_ID = p.Provider_ID
        GROUP BY p.Provider_ID
        ORDER BY Total_Donated DESC
    """,

    "Q14_Highest_demand_location_based_on_claims": """
        SELECT f.Location AS City, COUNT(c.Claim_ID) AS Total_Claims
        FROM claims c
        JOIN food_listings f ON c.Food_ID = f.Food_ID
        GROUP BY f.Location
        ORDER BY Total_Claims DESC
        LIMIT 1
    """,

    "Q15_Trends_in_wastage": """
        SELECT Location,
               SUM(Quantity) - IFNULL(SUM(claimed_count), 0) AS Wasted_Quantity
        FROM (
            SELECT f.Food_ID, f.Location, f.Quantity, COUNT(c.Claim_ID) AS claimed_count
            FROM food_listings f
            LEFT JOIN claims c ON f.Food_ID = c.Food_ID AND c.Status = 'Completed'
            GROUP BY f.Food_ID
        ) sub
        GROUP BY Location
        ORDER BY Wasted_Quantity DESC
    """,
}

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Dashboard", "Manage Data", "Queries & Export", "About"])

# Dashboard page
if page == "Dashboard":
    st.title("Local Food Waste Dashboard")
    st.markdown("Use the sidebar to manage data, run queries, and export results.")

    # Filters
    st.sidebar.subheader("Filters for Food Listings")
    providers_df = read_table("providers")
    listings_df = read_table("food_listings")

    city_options = ["All"] + sorted(listings_df['Location'].dropna().unique().tolist())
    sel_city = st.sidebar.selectbox("Location", city_options)

    provider_options = ["All"] + sorted(providers_df['Name'].dropna().unique().tolist())
    sel_provider = st.sidebar.selectbox("Provider (Name)", provider_options)

    food_type_options = ["All"] + sorted(listings_df['Food_Type'].dropna().unique().tolist())
    sel_food_type = st.sidebar.selectbox("Food Type", food_type_options)

    # Apply filters
    df_display = listings_df.copy()
    if sel_city != "All":
        df_display = df_display[df_display['Location'] == sel_city]
    if sel_provider != "All":
        # join to get provider ID for chosen name
        pid = providers_df[providers_df['Name'] == sel_provider]['Provider_ID'].iloc[0]
        df_display = df_display[df_display['Provider_ID'] == pid]
    if sel_food_type != "All":
        df_display = df_display[df_display['Food_Type'] == sel_food_type]

    st.subheader("Filtered Food Listings")
    st.dataframe(df_display)

    # Contact quick actions (provider + receivers)
    st.subheader("Contact Providers in Filter")
    providers_in_view = providers_df[providers_df['Provider_ID'].isin(df_display['Provider_ID'].unique())]
    for _, row in providers_in_view.iterrows():
        contact = row.get('Contact', '')
        st.markdown(f"**{row['Name']}** — {row.get('Address','')} — {contact}  ")
        if contact and '@' in str(contact):
            st.markdown(f"[Email]({ 'mailto:' + contact })")
        else:
            st.markdown(f"Contact: {contact}")

# Manage Data page for CRUD
if page == "Manage Data":
    st.title("Manage Data (CRUD)")
    st.markdown("Add / Update / Delete records for Providers, Receivers, Listings, and Claims.")

    tab = st.tabs(["Providers", "Receivers", "Listings", "Claims"])

    # Providers tab
    with tab[0]:
        st.subheader("Providers")
        df = read_table('providers')
        st.dataframe(df)

        with st.expander("Add Provider"):
            with st.form("add_provider"):
                name = st.text_input("Name")
                ptype = st.text_input("Type")
                address = st.text_input("Address")
                city = st.text_input("City")
                contact = st.text_input("Contact")
                submitted = st.form_submit_button("Add")
                if submitted:
                    # infer new Provider_ID
                    try:
                        new_id = int(df['Provider_ID'].max()) + 1
                    except Exception:
                        new_id = 1
                    insert_row('providers', {
                        'Provider_ID': new_id,
                        'Name': name,
                        'Type': ptype,
                        'Address': address,
                        'City': city,
                        'Contact': contact
                    })
                    st.success("Provider added. Refresh the page to see updates.")

        with st.expander("Delete Provider"):
            prov_options = df['Provider_ID'].astype(str).tolist()
            sel = st.selectbox("Provider_ID to delete", options=prov_options)
            if st.button("Delete Provider"):
                delete_row('providers', 'Provider_ID', int(sel))
                st.success("Deleted. Refresh to see updates.")

    # Receivers tab
    with tab[1]:
        st.subheader("Receivers")
        df = read_table('receivers')
        st.dataframe(df)

        with st.expander("Add Receiver"):
            with st.form("add_receiver"):
                name = st.text_input("Name")
                rtype = st.text_input("Type")
                city = st.text_input("City")
                contact = st.text_input("Contact")
                submitted = st.form_submit_button("Add")
                if submitted:
                    try:
                        new_id = int(df['Receiver_ID'].max()) + 1
                    except Exception:
                        new_id = 1
                    insert_row('receivers', {
                        'Receiver_ID': new_id,
                        'Name': name,
                        'Type': rtype,
                        'City': city,
                        'Contact': contact
                    })
                    st.success("Receiver added. Refresh to see updates.")

        with st.expander("Delete Receiver"):
            rec_options = df['Receiver_ID'].astype(str).tolist()
            sel = st.selectbox("Receiver_ID to delete", options=rec_options)
            if st.button("Delete Receiver"):
                delete_row('receivers', 'Receiver_ID', int(sel))
                st.success("Deleted. Refresh to see updates.")

    # Listings tab
    with tab[2]:
        st.subheader("Food Listings")
        df = read_table('food_listings')
        st.dataframe(df)

        with st.expander("Add Listing"):
            with st.form("add_listing"):
                fname = st.text_input("Food_Name")
                qty = st.number_input("Quantity", min_value=0, value=1)
                expiry = st.date_input("Expiry_Date")
                provider_id = st.selectbox("Provider_ID", options=providers_df['Provider_ID'].tolist())
                provider_type = st.text_input("Provider_Type")
                location = st.text_input("Location")
                food_type = st.text_input("Food_Type")
                meal_type = st.text_input("Meal_Type")
                submitted = st.form_submit_button("Add")
                if submitted:
                    try:
                        new_id = int(df['Food_ID'].max()) + 1
                    except Exception:
                        new_id = 1
                    insert_row('food_listings', {
                        'Food_ID': new_id,
                        'Food_Name': fname,
                        'Quantity': qty,
                        'Expiry_Date': expiry.strftime('%Y-%m-%d'),
                        'Provider_ID': int(provider_id),
                        'Provider_Type': provider_type,
                        'Location': location,
                        'Food_Type': food_type,
                        'Meal_Type': meal_type
                    })
                    st.success("Listing added. Refresh to see updates.")

        with st.expander("Delete Listing"):
            list_options = df['Food_ID'].astype(str).tolist()
            sel = st.selectbox("Food_ID to delete", options=list_options)
            if st.button("Delete Listing"):
                delete_row('food_listings', 'Food_ID', int(sel))
                st.success("Deleted. Refresh to see updates.")

    # Claims tab
    with tab[3]:
        st.subheader("Claims")
        df = read_table('claims')
        st.dataframe(df)

        with st.expander("Add Claim"):
            with st.form("add_claim"):
                food_id = st.selectbox("Food_ID", options=read_table('food_listings')['Food_ID'].tolist())
                receiver_id = st.selectbox("Receiver_ID", options=read_table('receivers')['Receiver_ID'].tolist())
                status = st.selectbox("Status", options=["Pending", "Completed", "Cancelled"]) 
                submitted = st.form_submit_button("Add")
                if submitted:
                    try:
                        new_id = int(df['Claim_ID'].max()) + 1
                    except Exception:
                        new_id = 1
                    insert_row('claims', {
                        'Claim_ID': new_id,
                        'Food_ID': int(food_id),
                        'Receiver_ID': int(receiver_id),
                        'Status': status,
                        'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    st.success("Claim added. Refresh to see updates.")

        with st.expander("Delete Claim"):
            claim_options = df['Claim_ID'].astype(str).tolist()
            sel = st.selectbox("Claim_ID to delete", options=claim_options)
            if st.button("Delete Claim"):
                delete_row('claims', 'Claim_ID', int(sel))
                st.success("Deleted. Refresh to see updates.")

# Queries & Export page
if page == "Queries & Export":
    st.title("Run Analysis Queries & Export Results")
    st.markdown("Run the 15 predefined SQL queries, browse results, and download them as Excel files.")

    # Run all queries and store results in-memory
    results = {}
    for name, sql in QUERIES.items():
        if ':city' in sql:
            # prompt for city when query needs parameter
            city = st.text_input("Enter city for provider contacts (used by Q3)", value="Mumbai")
            df = pd.read_sql_query(sql, conn, params={"city": city})
        else:
            df = pd.read_sql_query(sql, conn)
        results[name] = df

    # Show results with expanders and download buttons
    for name, df in results.items():
        with st.expander(name):
            st.dataframe(df)
            # prepare excel bytes
            towrite = BytesIO()
            df.to_excel(towrite, index=False, engine='openpyxl')
            towrite.seek(0)
            st.download_button(label=f"Download {name} as Excel", data=towrite, file_name=f"{name}.xlsx", mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    # Bulk export all results into single workbook
    if st.button("Download ALL queries as one workbook"):
        combined = BytesIO()
        with pd.ExcelWriter(combined, engine='openpyxl') as writer:
            for name, df in results.items():
                # sanitize sheet name length
                sheet = name[:30]
                df.to_excel(writer, sheet_name=sheet, index=False)
        combined.seek(0)
        st.download_button("Download workbook", data=combined, file_name="all_queries_results.xlsx", mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# About page
if page == "About":
    st.title("About this Project")
    st.markdown("""
    **Local Food Waste Management** — Streamlit dashboard to analyze and manage donations.

    Features implemented:
    - Load CSV data into SQLite and show tables
    - Filter food listings by Location, Provider, Food Type
    - Contact providers (mailto link when contact contains '@')
    - CRUD operations for Providers, Receivers, Food Listings, Claims (Add/Delete/Update via SQL)
    - 15 predefined SQL queries with per-query download
    - Bulk export all queries into one Excel workbook

    Next improvements:
    - Authentication for providers/receivers
    - More advanced charts and time-series analysis
    - Automated scheduled exports
    """)

# Ensure connection closed on exit
# (Streamlit will keep the process alive; you can call conn.close() when needed)
