import os
import pandas as pd
import sqlite3
# Set your base path (folder where your files are located)
base_path = r"C:/Users/Shweta/OneDrive/Desktop/local-food-waste"

# Output folder for Excel results
output_folder = os.path.join(base_path, "query_results")
os.makedirs(output_folder, exist_ok=True)

# ====== STEP 1: Remove old database if exists ======
if os.path.exists("food_waste.db"):
    os.remove("food_waste.db")

# ====== STEP 2: Connect to SQLite ======
conn = sqlite3.connect("food_waste.db")

# ====== STEP 3: Load CSV files ======
providers = pd.read_csv("C:/Users/Shweta/OneDrive/Desktop/local-food-waste/providers_data.csv")
receivers = pd.read_csv("C:/Users/Shweta/OneDrive/Desktop/local-food-waste/receivers_data.csv")
claims = pd.read_csv("C:/Users/Shweta/OneDrive/Desktop/local-food-waste/claims_data.csv")
food_listings = pd.read_csv("C:/Users/Shweta/OneDrive/Desktop/local-food-waste/food_listings_data.csv")



# ====== STEP 4: Store in SQLite ======
providers.to_sql("providers", conn, index=False)
receivers.to_sql("receivers", conn, index=False)
claims.to_sql("claims", conn, index=False)
food_listings.to_sql("food_listings", conn, index=False)

# ====== STEP 5: Define & Run Queries ======
queries = {
    # 1. Number of food providers in each city
    "providers_per_city": """
        SELECT City, COUNT(*) AS num_providers
        FROM providers
        GROUP BY City
    """,

    # 2. Number of food receivers in each city
    "receivers_per_city": """
        SELECT City, COUNT(*) AS num_receivers
        FROM receivers
        GROUP BY City
    """,

    # 3. Number of food providers by type
    "providers_by_type": """
        SELECT Type, COUNT(*) AS num_providers
        FROM providers
        GROUP BY Type
    """,

    # 4. Number of food receivers by type
    "receivers_by_type": """
        SELECT Type, COUNT(*) AS num_receivers
        FROM receivers
        GROUP BY Type
    """,

    # 5. Food listings by provider type
    "listings_by_provider_type": """
        SELECT Provider_Type, COUNT(*) AS num_listings
        FROM food_listings
        GROUP BY Provider_Type
    """,

    # 6. Most common food types
    "most_common_food_type": """
        SELECT Food_Type, COUNT(*) AS frequency
        FROM food_listings
        GROUP BY Food_Type
        ORDER BY frequency DESC
    """,

    # 7. Claims count by status
    "claims_by_status": """
        SELECT Status, COUNT(*) AS count
        FROM claims
        GROUP BY Status
    """,

    # 8. Top providers by number of listings
    "top_providers_by_listings": """
        SELECT p.Name AS provider_name, COUNT(f.Food_ID) AS num_listings
        FROM food_listings f
        JOIN providers p ON f.Provider_ID = p.Provider_ID
        GROUP BY p.Name
        ORDER BY num_listings DESC
    """,

    # 9. Top receivers by number of claims
    "top_receivers_by_claims": """
        SELECT r.Name AS receiver_name, COUNT(c.Claim_ID) AS num_claims
        FROM claims c
        JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
        GROUP BY r.Name
        ORDER BY num_claims DESC
    """,

    # 10. Average quantity of food listed by provider type
    "avg_quantity_by_provider_type": """
        SELECT Provider_Type, AVG(Quantity) AS avg_quantity
        FROM food_listings
        GROUP BY Provider_Type
    """,

    # 11. Claims by month
    "claims_by_month": """
        SELECT strftime('%Y-%m', Timestamp) AS claim_month, COUNT(*) AS num_claims
        FROM claims
        GROUP BY claim_month
    """,

    # 12. Expired food listings
    "expired_food_listings": """
        SELECT Food_Name, Expiry_Date
        FROM food_listings
        WHERE date(Expiry_Date) < date('now')
    """,

    # 13. Receivers who claimed most expired items
    "receivers_most_expired_claims": """
        SELECT r.Name AS receiver_name, COUNT(c.Claim_ID) AS expired_claims
        FROM claims c
        JOIN food_listings f ON c.Food_ID = f.Food_ID
        JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
        WHERE date(f.Expiry_Date) < date('now')
        GROUP BY r.Name
        ORDER BY expired_claims DESC
    """,

    # 14. Number of meals by meal type
    "meals_by_meal_type": """
        SELECT Meal_Type, COUNT(*) AS num_meals
        FROM food_listings
        GROUP BY Meal_Type
    """,

    # 15. Providers who listed the highest quantity of food
    "top_providers_by_quantity": """
        SELECT p.Name AS provider_name, SUM(f.Quantity) AS total_quantity
        FROM food_listings f
        JOIN providers p ON f.Provider_ID = p.Provider_ID
        GROUP BY p.Name
        ORDER BY total_quantity DESC
    """
}
# ===== Output folder =====
output_folder = os.path.join(base_path, "query_results")
os.makedirs(output_folder, exist_ok=True)

# ===== Run each query and save to Excel =====
for name, sql in queries.items():
    df = pd.read_sql(sql, conn)
    output_path = os.path.join(output_folder, f"{name}.xlsx")
    df.to_excel(output_path, index=False)
    print(f"✅ Saved: {output_path}")

conn.close()
print("\n🎯 All query results have been saved to:", output_folder)