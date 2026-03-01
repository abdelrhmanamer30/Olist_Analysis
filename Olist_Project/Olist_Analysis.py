import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

engine = create_engine(
    "mssql+pyodbc://@DESKTOP-BHL20RS\\SQLEXPRESS/Olist_Project?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

orders_df = pd.read_sql("SELECT TOP 5 * FROM dbo.olist_orders_dataset", engine)
# print(orders_df)
customers_df = pd.read_sql("SELECT * FROM dbo.olist_customers_dataset", engine)
order_items_df = pd.read_sql("SELECT * FROM dbo.olist_order_items_dataset", engine)
products_df = pd.read_sql("SELECT * FROM dbo.olist_products_dataset", engine)
sellers_df = pd.read_sql("SELECT * FROM dbo.olist_sellers_dataset", engine)
reviews_df = pd.read_sql("SELECT * FROM dbo.olist_order_reviews_dataset", engine)
payments_df = pd.read_sql("SELECT * FROM dbo.olist_order_payments_dataset", engine)
geolocation_df = pd.read_sql("SELECT * FROM dbo.olist_geolocation_clean", engine)
orders_df = pd.read_sql("SELECT * FROM dbo.olist_orders_dataset", engine)

def clean_columns(df):
    df.columns = df.columns.str.replace('"', '').str.strip().str.lower()
    return df

orders_df = clean_columns(orders_df)
customers_df = clean_columns(customers_df)
order_items_df = clean_columns(order_items_df)
products_df = clean_columns(products_df)
sellers_df = clean_columns(sellers_df)
reviews_df = clean_columns(reviews_df)
payments_df = clean_columns(payments_df)
geolocation_df = clean_columns(geolocation_df)

orders_df.columns = orders_df.columns.str.replace('"', '').str.strip()
orders_df.columns = orders_df.columns.str.lower()
print(orders_df.columns.tolist())

orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])
orders_df['order_delivered_customer_date'] = pd.to_datetime(orders_df['order_delivered_customer_date'])

orders_df['delivery_days'] = (orders_df['order_delivered_customer_date'] - orders_df['order_purchase_timestamp']).dt.days
print(orders_df[['order_id','order_purchase_timestamp','order_delivered_customer_date','delivery_days']].head())

master_df = orders_df.merge(order_items_df, on='order_id', how='left') \
                     .merge(products_df, on='product_id', how='left') \
                     .merge(customers_df, on='customer_id', how='left') \
                     .merge(sellers_df, on='seller_id', how='left') \
                     .merge(reviews_df, on='order_id', how='left') \
                     .merge(payments_df, on='order_id', how='left')

print("Master DataFrame shape:", master_df.shape)
print(master_df.head())

monthly_orders = master_df.groupby(master_df['order_purchase_timestamp'].dt.to_period('M'))['order_id'].count().reset_index()
monthly_orders['order_purchase_timestamp'] = monthly_orders['order_purchase_timestamp'].dt.to_timestamp()

print(master_df.columns.tolist())

plt.figure(figsize=(14,6))
sns.barplot(x='order_purchase_timestamp', y='order_id', data=monthly_orders, palette="Blues_d")
plt.title("Monthly Number of Orders", fontsize=16)
plt.xlabel("Month")
plt.ylabel("Number of Orders")
plt.xticks(rotation=45)
plt.show()

plt.figure(figsize=(12,6))
sns.histplot(master_df['delivery_days'].dropna(), bins=30, kde=True, color="#ff7f0e")
plt.title("Delivery Time Distribution", fontsize=16)
plt.xlabel("Delivery Days")
plt.ylabel("Number of Orders")
plt.show()

plt.figure(figsize=(10,8))
sns.countplot(y='customer_state', data=master_df, order=master_df['customer_state'].value_counts().index, palette="coolwarm")
plt.title("Orders by Customer State", fontsize=16)
plt.xlabel("Number of Orders")
plt.ylabel("State")
plt.show()

payment_counts = master_df.groupby('payment_type')['payment_type'].count()

plt.figure(figsize=(10,6))
sns.barplot(x=payment_counts.index, y=payment_counts.values, palette="magma")
plt.title("Payment Method Distribution", fontsize=16)
plt.ylabel("Number of Orders")
plt.xlabel("Payment Type")
plt.show()

master_df['price'] = master_df['price'].astype(str)  
master_df['price'] = master_df['price'].str.replace('.', '', regex=False)  
master_df['price'] = master_df['price'].str.replace(',', '.', regex=False) 
master_df['price'] = pd.to_numeric(master_df['price'], errors='coerce')  

avg_price_category = master_df.groupby('product_category_name')['price'].mean().sort_values(ascending=False).head(10)

plt.figure(figsize=(12,6))
sns.barplot(x=avg_price_category.values, y=avg_price_category.index, palette="cubehelix")
plt.title("Top 10 Categories by Average Order Value", fontsize=16)
plt.xlabel("Average Price (BRL)")
plt.ylabel("Product Category")
plt.show()

plt.figure(figsize=(12,6))
sns.scatterplot(x='price', y='delivery_days', data=master_df, alpha=0.5)
plt.title("Order Price vs Delivery Days", fontsize=16)
plt.xlabel("Order Price (BRL)")
plt.ylabel("Delivery Days")
plt.show()

