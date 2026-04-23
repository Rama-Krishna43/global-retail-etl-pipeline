import pandas as pd
import numpy as np
import os
from config import RAW_DATA_DIR

def generate_synthetic_data():
    np.random.seed(42)
    num_orders = 100
    num_customers = 80
    num_products = 20

    print("Generating synthetic data...")

    # 1. Customers
    customers = pd.DataFrame({
        'customer_id': [f'cust_{i}' for i in range(num_customers)],
        'customer_unique_id': [f'uniq_{i}' for i in range(num_customers)],
        'customer_zip_code_prefix': np.random.randint(1000, 9999, num_customers),
        'customer_city': np.random.choice(['Sao Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Curitiba'], num_customers),
        'customer_state': np.random.choice(['SP', 'RJ', 'MG', 'PR'], num_customers)
    })

    # 2. Products
    products = pd.DataFrame({
        'product_id': [f'prod_{i}' for i in range(num_products)],
        'product_category_name': np.random.choice(['electronics', 'health_beauty', 'sports_leisure', 'housewares'], num_products),
        'product_weight_g': np.random.randint(100, 5000, num_products)
    })

    # 3. Orders
    order_ids = [f'order_{i}' for i in range(num_orders)]
    orders = pd.DataFrame({
        'order_id': order_ids,
        'customer_id': np.random.choice(customers['customer_id'], num_orders),
        'order_status': np.random.choice(['delivered', 'shipped', 'canceled'], num_orders, p=[0.9, 0.05, 0.05]),
        'order_purchase_timestamp': pd.date_range(start='2023-01-01', periods=num_orders, freq='D').strftime('%Y-%m-%d %H:%M:%S')
    })

    # 4. Order Items
    order_items_list = []
    for oid in order_ids:
        num_items = np.random.randint(1, 4)
        for i in range(num_items):
            order_items_list.append({
                'order_id': oid,
                'order_item_id': i + 1,
                'product_id': np.random.choice(products['product_id']),
                'price': round(np.random.uniform(50.0, 500.0), 2),
                'freight_value': round(np.random.uniform(5.0, 50.0), 2)
            })
    order_items = pd.DataFrame(order_items_list)

    # Save to CSV
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    customers.to_csv(os.path.join(RAW_DATA_DIR, 'olist_customers_dataset.csv'), index=False)
    products.to_csv(os.path.join(RAW_DATA_DIR, 'olist_products_dataset.csv'), index=False)
    orders.to_csv(os.path.join(RAW_DATA_DIR, 'olist_orders_dataset.csv'), index=False)
    order_items.to_csv(os.path.join(RAW_DATA_DIR, 'olist_order_items_dataset.csv'), index=False)

    print(f"Data generated successfully in {RAW_DATA_DIR}")

if __name__ == "__main__":
    generate_synthetic_data()
