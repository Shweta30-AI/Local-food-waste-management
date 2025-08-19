import streamlit as st
import pandas as pd
from database import get_connection, create_tables
import matplotlib.pyplot as plt
import os

# DB Setup - Only create tables if database file doesn't exist
if "db_initialized" not in st.session_state:
    if not os.path.exists("food_waste.db"):
        create_tables()
    st.session_state["db_initialized"] = True


st.set_page_config(page_title="Local Food Wastage Management System", layout="wide")
st.title("🍛 Local Food Wastage Management System")





# DB Setup
if "db_initialized" not in st.session_state:
    create_tables()
    st.session_state["db_initialized"] = True

# Sidebar
option = st.sidebar.selectbox(
    "📌 Select Section",
    [
        "🏠 Home",
        "📝 Donor Register",
        "📋 View Donors",
        "🎯 Receiver Register",
        "🍽️ Add Food Listing",
        "🔍 Filter/Search",
        "📊 SQL Query Analysis",
        "📈 Visual Analytics",
        "📞 Contact"
    ]
)

# Footer



if option == "🏠 Home":
    
    st.write("""
     * This platform helps reduce food wastage by connecting providers like restaurants, households, etc.
    with receivers like NGOs or individuals in need.
    """)
    
    st.markdown("""
    ⭐ This system helps connect food providers with those in need.  
    ⭐ 🔹 Filter donations  
    ⭐ 🔹 Analyze food trends  
    ⭐ 🔹 Reduce food waste  
    ⭐ 🔹 Register Donors and Receivers  
    ⭐ 🔹 Add and Track Food Listings  
    ⭐ 🔹 Visualize Data Insights  
    ⭐ 🔹 Run SQL Queries for Advanced Analysis  
    ⭐ 🔹 Contact Providers directly  
    """)
    
    st.markdown("---\n📘 Made with ❤️ by **Youraj Kumar (IIT Patna)**")

# 📝 Donor Register Form
if option == "📝 Donor Register":
    st.subheader("📝 Register a Food Donor")
    with st.form("donor_form"):
        name = st.text_input("Name")
        donor_type = st.selectbox("Type", ["Restaurant", "Household", "Business", "Other"])
        address = st.text_input("Address")
        city = st.text_input("City")
        contact = st.text_input("Contact")
        submitted = st.form_submit_button("Register")
    if submitted:
        conn = get_connection()
        conn.execute(
            "INSERT INTO Providers (Name, Type, Address, City, Contact) VALUES (?, ?, ?, ?, ?)",
            (name, donor_type, address, city, contact),
        )
        conn.commit()
        conn.close()
        st.success("Donor registered successfully!")
        
        
# View donors       
elif option == "📋 View Donors":
    st.subheader("📋 Registered Food Donors")
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM Providers", conn)
    conn.close()
    
    if not df.empty:
        st.dataframe(df)
        st.success(f"{len(df)} donors found.")
    else:
        st.warning("No donor data found.")
      


# 🎯 Receiver Register Form
if option == "🎯 Receiver Register":
    st.subheader("🎯 Register a Food Receiver")
    with st.form("receiver_form"):
        name = st.text_input("Name")
        receiver_type = st.selectbox("Type", ["NGO", "Individual", "Community", "Other"])
        city = st.text_input("City")
        contact = st.text_input("Contact")
        submitted = st.form_submit_button("Register")
    if submitted:
        conn = get_connection()
        conn.execute(
            "INSERT INTO Receivers (Name, Type, City, Contact) VALUES (?, ?, ?, ?)",
            (name, receiver_type, city, contact),
        )
        conn.commit()
        conn.close()
        st.success("Receiver registered successfully!")

# 🍽️ Add Food Listing
if option == "🍽️ Add Food Listing":
    st.subheader("🍽️ Add Food Listing")
    conn = get_connection()
    providers_df = pd.read_sql("SELECT Provider_ID, Name FROM Providers", conn)
    conn.close()
    if not providers_df.empty:
        with st.form("add_food_form"):
            food_name = st.text_input("Food Name")
            quantity = st.number_input("Quantity (in units)", min_value=1)
            expiry_date = st.date_input("Expiry Date")
            provider = st.selectbox("Provider", providers_df["Name"])
            provider_id = providers_df[providers_df["Name"] == provider]["Provider_ID"].values[0]
            provider_type = st.text_input("Provider Type")
            location = st.text_input("Location")
            food_type = st.selectbox("Food Type", ["Veg", "Non-Veg"])
            meal_type = st.selectbox("Meal Type", ["Breakfast", "Lunch", "Dinner", "Snack", "Other"])
            submitted = st.form_submit_button("Add Food")
        if submitted:
            conn = get_connection()
            conn.execute(
                "INSERT INTO Food_Listings (Food_Name, Quantity, Expiry_Date, Provider_ID, Provider_Type, Location, Food_Type, Meal_Type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (food_name, quantity, expiry_date.strftime("%Y-%m-%d"), provider_id, provider_type, location, food_type, meal_type),
            )
            conn.commit()
            conn.close()
            st.success("Food listing added successfully!")
    else:
        st.warning("Please add a Donor first.")

# 🔍 Filter/Search Food
if option == "🔍 Filter/Search":
    st.subheader("🔍 Filter/Search Food Listings")
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM Food_Listings", conn)
    conn.close()
    search = st.text_input("Search by Food Name, Location or Provider Type")
    if search.strip() != "":
        df = df[
            df["Food_Name"].str.contains(search, case=False) |
            df["Location"].str.contains(search, case=False) |
            df["Provider_Type"].str.contains(search, case=False)
        ]
    st.dataframe(df)
    st.success(f"{len(df)} results found." if not df.empty else "No results found.")

# 📊 SQL Query Analysis
if option == "📊 SQL Query Analysis":
    st.subheader("📊 SQL Query Analysis")

    query_map = {
        "1. Providers and Receivers count per city": """
            SELECT City, COUNT(DISTINCT Provider_ID) AS Providers, COUNT(DISTINCT Receiver_ID) AS Receivers
            FROM Providers LEFT JOIN Receivers USING(City)
            GROUP BY City
        """,
        "2. Provider type contributing most food": """
            SELECT Provider_Type, COUNT(*) AS Total_Listings
            FROM Food_Listings
            GROUP BY Provider_Type
            ORDER BY Total_Listings DESC
            LIMIT 1
        """,
        "3. Cities with most food listings": """
            SELECT Location AS City, COUNT(*) AS Total_Listings
            FROM Food_Listings
            GROUP BY Location
            ORDER BY Total_Listings DESC
            LIMIT 5
        """,
        "4. Most common meal type": """
            SELECT Meal_Type, COUNT(*) AS Count
            FROM Food_Listings
            GROUP BY Meal_Type
            ORDER BY Count DESC
            LIMIT 1
        """,
        "5. Top 5 most listed food items": """
            SELECT Food_Name, COUNT(*) AS Times_Listed
            FROM Food_Listings
            GROUP BY Food_Name
            ORDER BY Times_Listed DESC
            LIMIT 5
        """,
        "6. Number of food claims per receiver": """
            SELECT r.Name, COUNT(c.Claim_ID) AS Claims_Made
            FROM Receivers r
            LEFT JOIN Claims c ON r.Receiver_ID = c.Receiver_ID
            GROUP BY r.Name
            ORDER BY Claims_Made DESC
        """,
        "7. Unclaimed food items": """
            SELECT f.Food_ID, f.Food_Name, f.Quantity, f.Expiry_Date
            FROM Food_Listings f
            LEFT JOIN Claims c ON f.Food_ID = c.Food_ID
            WHERE c.Claim_ID IS NULL
        """,
        "8. Providers who contributed most": """
            SELECT p.Name, COUNT(f.Food_ID) AS Foods_Provided
            FROM Providers p
            LEFT JOIN Food_Listings f ON p.Provider_ID = f.Provider_ID
            GROUP BY p.Name
            ORDER BY Foods_Provided DESC
            LIMIT 5
        """,
        "9. List all expired food items": """
            SELECT Food_Name, Expiry_Date
            FROM Food_Listings
            WHERE date(Expiry_Date) < date('now')
        """,
        "10. Food type (Veg/Non-Veg) summary": """
            SELECT Food_Type, COUNT(*) AS Count
            FROM Food_Listings
            GROUP BY Food_Type
        """,
        "11. Meal distribution by city": """
            SELECT Location AS City, Meal_Type, COUNT(*) AS Count
            FROM Food_Listings
            GROUP BY Location, Meal_Type
            ORDER BY City
        """,
        "12. Receivers who claimed most Non-Veg food": """
            SELECT r.Name, COUNT(c.Claim_ID) AS NonVeg_Claims
            FROM Claims c
            JOIN Receivers r ON c.Receiver_ID = r.Receiver_ID
            JOIN Food_Listings f ON c.Food_ID = f.Food_ID
            WHERE f.Food_Type = 'Non-Veg'
            GROUP BY r.Name
            ORDER BY NonVeg_Claims DESC
            LIMIT 5
        """,
        "13. Datewise total food claims": """
            SELECT Claim_Date, COUNT(*) AS Total_Claims
            FROM Claims
            GROUP BY Claim_Date
            ORDER BY Claim_Date DESC
        """,
        "14. Provider-Receiver city wise mapping": """
            SELECT p.City AS Provider_City, r.City AS Receiver_City, COUNT(*) AS Total_Transactions
            FROM Claims c
            JOIN Food_Listings f ON c.Food_ID = f.Food_ID
            JOIN Providers p ON f.Provider_ID = p.Provider_ID
            JOIN Receivers r ON c.Receiver_ID = r.Receiver_ID
            GROUP BY Provider_City, Receiver_City
            ORDER BY Total_Transactions DESC
        """,
        "15. Food claims status summary": """
            SELECT Status, COUNT(*) AS Count
            FROM Claims
            GROUP BY Status
        """
    }

    selected_query = st.selectbox("Select a query to run:", list(query_map.keys()))
    conn = get_connection()
    result_df = pd.read_sql(query_map[selected_query], conn)
    conn.close()
    st.dataframe(result_df)
    st.success(f"Query executed: {selected_query}")

# 📈 Visual Analytics
if option == "📈 Visual Analytics":
    st.subheader("📈 Visual Analytics")
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM Food_Listings", conn)
    conn.close()
    if not df.empty:
        # 1. Food Type Pie Chart
        food_type_count = df["Food_Type"].value_counts()
        fig1, ax1 = plt.subplots()
        ax1.pie(food_type_count, labels=food_type_count.index, autopct='%1.1f%%', startangle=90)
        ax1.set_title("Food Type Distribution")
        st.pyplot(fig1)
        # 2. Meal Type Bar Chart
        meal_count = df["Meal_Type"].value_counts()
        fig2, ax2 = plt.subplots()
        ax2.bar(meal_count.index, meal_count.values)
        ax2.set_title("Meal Type Count")
        st.pyplot(fig2)
    else:
        st.warning("No Food Listings data available for analytics.")

# 📞 Contact Section
if option == "📞 Contact":
    st.subheader("📞 Contact Food Providers")
    conn = get_connection()
    query = "SELECT Provider_ID, Name, Type, City, Contact FROM Providers"
    df = pd.read_sql(query, conn)
    conn.close()
    search = st.text_input("🔎 Search by City or Name")
    if search.strip() != "":
        df = df[df["City"].str.contains(search, case=False) | df["Name"].str.contains(search, case=False)]
    if not df.empty:
        st.dataframe(df)
        st.success(f"{len(df)} providers found.")
    else: