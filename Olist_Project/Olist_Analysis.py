import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def connect_to_sql_server():
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=DESKTOP-BHL20RS\\SQLEXPRESS;'
            'DATABASE=Olist_Project;'
            'Trusted_Connection=yes;'
        )
        print("Connected to SQL Server successfully.")
        return conn
    except pyodbc.Error as e:
        print(f"Connection Error: {e}")
        return None

def read_query(conn, query):
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Query Error: {e}")
        return None

def clean_columns(df):
    df.columns = df.columns.str.replace('"', '').str.strip().str.lower()
    return df

def load_all_data(conn):
    dfs = {}
    tables = [
        "olist_orders_dataset",
        "olist_customers_dataset",
        "olist_order_items_dataset",
        "olist_products_dataset",
        "olist_sellers_dataset",
        "olist_order_reviews_dataset",
        "olist_order_payments_dataset",
        "olist_geolocation_clean"
    ]
    for table in tables:
        query = f"SELECT * FROM dbo.{table}"
        df = read_query(conn, query)
        if df is not None:
            dfs[table] = clean_columns(df)
        else:
            print(f"Could not load {table}")
    return dfs

def prepare_master_df(dfs):
    orders_df = dfs["olist_orders_dataset"]
    order_items_df = dfs["olist_order_items_dataset"]
    products_df = dfs["olist_products_dataset"]
    customers_df = dfs["olist_customers_dataset"]
    sellers_df = dfs["olist_sellers_dataset"]
    reviews_df = dfs["olist_order_reviews_dataset"]
    payments_df = dfs["olist_order_payments_dataset"]

    orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])
    orders_df['order_delivered_customer_date'] = pd.to_datetime(orders_df['order_delivered_customer_date'])

    orders_df['delivery_days'] = (orders_df['order_delivered_customer_date'] - orders_df['order_purchase_timestamp']).dt.days
    orders_df = orders_df[orders_df['delivery_days'] >= 0]  

    master_df = orders_df.merge(order_items_df, on='order_id', how='left') \
                         .merge(products_df, on='product_id', how='left') \
                         .merge(customers_df, on='customer_id', how='left') \
                         .merge(sellers_df, on='seller_id', how='left') \
                         .merge(reviews_df, on='order_id', how='left') \
                         .merge(payments_df, on='order_id', how='left')

    master_df['price'] = pd.to_numeric(master_df['price'], errors='coerce')

    return master_df

def plot_monthly_orders(master_df):
    monthly_orders = master_df.groupby(master_df['order_purchase_timestamp'].dt.to_period('M'))['order_id'].nunique().reset_index()
    monthly_orders['order_purchase_timestamp'] = monthly_orders['order_purchase_timestamp'].dt.to_timestamp()

    plt.figure(figsize=(14,6))
    sns.barplot(x='order_purchase_timestamp', y='order_id', data=monthly_orders, palette="Blues_d")
    plt.title("Monthly Number of Orders", fontsize=16)
    plt.xlabel("Month")
    plt.ylabel("Number of Orders")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_delivery_days(master_df):
    plt.figure(figsize=(12,6))
    sns.histplot(master_df['delivery_days'].dropna(), bins=30, kde=True, color="#ff7f0e")
    plt.title("Delivery Time Distribution", fontsize=16)
    plt.xlabel("Delivery Days")
    plt.ylabel("Number of Orders")
    plt.tight_layout()
    plt.show()

def plot_orders_by_state(master_df):
    plt.figure(figsize=(10,8))
    sns.countplot(y='customer_state', data=master_df,
                  order=master_df['customer_state'].value_counts().index, palette="coolwarm")
    plt.title("Orders by Customer State", fontsize=16)
    plt.xlabel("Number of Orders")
    plt.ylabel("State")
    plt.tight_layout()
    plt.show()

def plot_payment_distribution(master_df):
    payment_counts = master_df.groupby('payment_type')['order_id'].nunique().reset_index()
    plt.figure(figsize=(10,6))
    sns.barplot(x='payment_type', y='order_id', data=payment_counts, palette="magma")
    plt.title("Payment Method Distribution", fontsize=16)
    plt.ylabel("Number of Orders")
    plt.xlabel("Payment Type")
    plt.tight_layout()
    plt.show()

def plot_top_categories(master_df):
    avg_price_category = master_df.groupby('product_category_name')['price'].mean().sort_values(ascending=False).head(10)
    plt.figure(figsize=(12,6))
    sns.barplot(x=avg_price_category.values, y=avg_price_category.index, palette="cubehelix")
    plt.title("Top 10 Categories by Average Order Value", fontsize=16)
    plt.xlabel("Average Price (BRL)")
    plt.ylabel("Product Category")
    plt.tight_layout()
    plt.show()

def plot_price_vs_delivery(master_df):
    plt.figure(figsize=(12,6))
    sns.scatterplot(x='price', y='delivery_days', data=master_df, alpha=0.5)
    plt.title("Order Price vs Delivery Days", fontsize=16)
    plt.xlabel("Order Price (BRL)")
    plt.ylabel("Delivery Days")
    plt.tight_layout()
    plt.show()

def close_connection(conn):
    if conn:
        conn.close()
        print("Connection Closed.")


if __name__ == "__main__":
    conn = connect_to_sql_server()

    if conn:
        dfs = load_all_data(conn)
        master_df = prepare_master_df(dfs)

        plot_monthly_orders(master_df)
        plot_delivery_days(master_df)
        plot_orders_by_state(master_df)
        plot_payment_distribution(master_df)
        plot_top_categories(master_df)
        plot_price_vs_delivery(master_df)

        close_connection(conn)