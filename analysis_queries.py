import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect("food_waste.db")
cursor = conn.cursor()

# List of queries with titles
queries = [
    ("All Providers", "SELECT * FROM providers;"),
    ("All Receivers", "SELECT * FROM receivers;"),
    ("Total Quantity of Food Available", "SELECT SUM(Quantity) AS total_quantity FROM food_listings;"),
    ("Count of Food Items by Type", "SELECT Food_Type, COUNT(*) AS count FROM food_listings GROUP BY Food_Type;"),
    ("Count of Food Items by Meal Type", "SELECT Meal_Type, COUNT(*) AS count FROM food_listings GROUP BY Meal_Type;"),
    ("Food Expiring in Next 3 Days", "SELECT * FROM food_listings WHERE Expiry_Date <= DATE('now', '+3 day');"),
    ("Top 5 Providers by Quantity Donated", """
        SELECT p.Name, SUM(f.Quantity) AS total
        FROM food_listings f
        JOIN providers p ON f.Provider_ID = p.Provider_ID
        GROUP BY p.Name
        ORDER BY total DESC
        LIMIT 5;
    """),
    ("Claims Count by Status", "SELECT Status, COUNT(*) AS count FROM claims GROUP BY Status;"),
    ("Most Claimed Food Items", """
        SELECT f.Food_Name, COUNT(c.Claim_ID) AS claim_count
        FROM claims c
        JOIN food_listings f ON c.Food_ID = f.Food_ID
        GROUP BY f.Food_Name
        ORDER BY claim_count DESC;
    """),
    ("Providers in Delhi", "SELECT * FROM providers WHERE City = 'Delhi';"),
    ("Receivers in Delhi", "SELECT * FROM receivers WHERE City = 'Delhi';"),
    ("Food Availability by City", "SELECT Location, SUM(Quantity) AS total_quantity FROM food_listings GROUP BY Location;"),
    ("Average Quantity per Listing", "SELECT AVG(Quantity) AS avg_quantity FROM food_listings;"),
    ("Receivers Who Claimed Most Items", """
        SELECT r.Name, COUNT(c.Claim_ID) AS total_claims
        FROM claims c
        JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
        GROUP BY r.Name
        ORDER BY total_claims DESC;
    """),
    ("Completed Claims with Provider & Receiver", """
        SELECT c.Claim_ID, p.Name AS Provider, r.Name AS Receiver, c.Status, c.Timestamp
        FROM claims c
        JOIN food_listings f ON c.Food_ID = f.Food_ID
        JOIN providers p ON f.Provider_ID = p.Provider_ID
        JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
        WHERE c.Status = 'Completed';
    """)
    SELECT city, 
       COUNT(DISTINCT CASE WHEN provider_type IS NOT NULL THEN provider_id END) AS food_providers,
       COUNT(DISTINCT CASE WHEN receiver_type IS NOT NULL THEN receiver_id END) AS food_receivers
FROM food_data,
GROUP BY city;

]

# Run and display each query
# Save results to text file and print
with open("sql_results.txt", "w", encoding="utf-8") as f:
    for title, sql in queries:
        print(f"\n=== {title} ===")
        f.write(f"\n=== {title} ===\n")
        try:
            df = pd.read_sql_query(sql, conn)
            if df.empty:
                print("No data found.")
                f.write("No data found.\n")
            else:
                print(df.to_string(index=False))
                f.write(df.to_string(index=False) + "\n")
        except Exception as e:
            print(f"Error running query: {e}")
            f.write(f"Error running query: {e}\n")
            


conn.close()
