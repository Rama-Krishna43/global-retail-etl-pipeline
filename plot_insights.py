import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import os
from config import DB_PATH, PROCESSED_DATA_DIR

def plot_revenue_trends():
    """Generates a bar chart for monthly revenue."""
    print("Generating Revenue Trends...")
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT order_month, SUM(total_price_inr) as revenue
    FROM processed_retail_data
    GROUP BY order_month
    ORDER BY order_month
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    plt.figure(figsize=(10, 6))
    plt.bar(df['order_month'], df['revenue'], color='skyblue')
    plt.title('Monthly Revenue Trends (Normalized to INR)')
    plt.xlabel('Month')
    plt.ylabel('Revenue (INR)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    save_path = os.path.join(PROCESSED_DATA_DIR, 'revenue_trends.png')
    plt.savefig(save_path)
    print(f"Chart saved to {save_path}")

def plot_category_distribution():
    """Generates a pie chart for product categories."""
    print("Generating Category Distribution...")
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT product_category_name, COUNT(*) as count FROM processed_retail_data GROUP BY product_category_name"
    df = pd.read_sql_query(query, conn)
    conn.close()

    plt.figure(figsize=(8, 8))
    plt.pie(df['count'], labels=df['product_category_name'], autopct='%1.1f%%', startangle=140)
    plt.title('Sales Distribution by Product Category')
    
    save_path = os.path.join(PROCESSED_DATA_DIR, 'category_distribution.png')
    plt.savefig(save_path)
    print(f"Chart saved to {save_path}")

if __name__ == "__main__":
    if os.path.exists(DB_PATH):
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
        plot_revenue_trends()
        plot_category_distribution()
    else:
        print("Database not found. Please run main.py first.")
