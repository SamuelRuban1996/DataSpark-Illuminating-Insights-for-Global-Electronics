# import necessary packages

import streamlit as st
import pandas as pd
import numpy as np
import base64
from sqlalchemy import create_engine, inspect, text

# Database configuration (obfuscated for security)
# These variables hold sensitive information for connecting to a MySQL database.
DB_USER = '***'  # Enter your MySQL username
DB_PASSWORD = '***'  # Enter your MySQL password
DB_HOST = '***'  # Enter your MySQL host
DB_PORT = '3306'  # Default MySQL port number
DB_NAME = '***'  # Enter your MySQL database name

def get_base64_of_bin_file(bin_file):
    """Function to convert a binary file to a base64-encoded string."""
    with open(bin_file, 'rb') as f:
        data = f.read()
    return base64.b64encode(data).decode()

# Background image path (obfuscated for security)
# This image will be used as the background for the Streamlit app.
background_image_path = "***"  # Enter your background image path 
base64_image = get_base64_of_bin_file(background_image_path)

# Styling part of the Streamlit app 
# This block defines custom CSS styles to enhance the visual appearance of the app.
st.markdown(
    f"""
    <style>
    .stApp {{
        background-image: url("data:image/jpg;base64,{base64_image}");
        background-size: cover;
    }}
    .stTitle {{
        color: white;
    }}
    .stSelectbox label {{
        color: white !important;
    }}
    .dataframe-container .dataframe {{
        color: white !important;
    }}
    .stSuccess {{
        background-color: white !important;
        color: green !important;
    }}
    .stWarning {{
        background-color: white !important;
        color: orange !important;
    }}
    .results-header {{
        color: white;
    }}
    .custom-success {{
        padding: 10px;
        border-radius: 5px;
        background-color: white;
        color: green;
    }}
    .custom-warning {{
        padding: 10px;
        border-radius: 5px;
        background-color: white;
        color: orange;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

def create_connection():
    """Create a connection to the MySQL database."""
    # This function initializes a connection to the MySQL database using the credentials provided.
    engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    return engine

def check_table_exists(engine, table_name):
    """Check if a table already exists in the database."""
    # This function checks if a specific table exists in the database.
    inspector = inspect(engine)
    return inspector.has_table(table_name)

def load_and_clean_stores(filepath):
    """Load and clean Stores.csv."""
    # This function reads and processes the 'Stores.csv' file, handling missing data and date formatting.
    df = pd.read_csv(filepath)
    df['Square Meters'] = df['Square Meters'].fillna(df['Square Meters'].median())
    df['Open Date'] = pd.to_datetime(df['Open Date'], dayfirst=False, errors='coerce')
    return df

def load_and_clean_sales(filepath):
    """Load and clean Sales.csv."""
    # This function reads and processes the 'Sales.csv' file, handling missing data and date formatting.
    df = pd.read_csv(filepath)
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].fillna(df[column].mode()[0])
        else:
            df[column] = df[column].fillna(df[column].median())
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    return df

def load_and_clean_products(filepath):
    """Load and clean Products.csv."""
    # This function reads and processes the 'Products.csv' file, handling missing data and converting prices to numeric values.
    df = pd.read_csv(filepath)
    
    for column in ['Unit Price USD', 'Unit Cost USD']:
        if column in df.columns:
            df[column] = df[column].replace('[\$,]', '', regex=True).astype(float)
    
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].fillna(df[column].mode()[0])
        else:
            df[column] = df[column].fillna(df[column].median())
    
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    return df

def load_and_clean_exchange_rates(filepath):
    """Load and clean Exchange_Rates.csv."""
    # This function reads and processes the 'Exchange_Rates.csv' file, handling missing data.
    df = pd.read_csv(filepath)
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].fillna(df[column].mode()[0])
        else:
            df[column] = df[column].fillna(df[column].median())
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    return df

def load_and_clean_customers(filepath):
    """Load and clean Customers.csv with encoding handling."""
    # This function reads and processes the 'Customers.csv' file, handling encoding issues and missing data.
    encodings = ['utf-8', 'iso-8859-1', 'cp1252']
    for encoding in encodings:
        try:
            df = pd.read_csv(filepath, encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        st.error("Failed to read the file with any of the specified encodings.")
        return None
    
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].mean())
    df = df.fillna(df.mode().iloc[0])
    
    if 'date_column' in df.columns:
        df['date_column'] = pd.to_datetime(df['date_column'], errors='coerce')
    if 'price_column' in df.columns:
        df['price_column'] = pd.to_numeric(df['price_column'], errors='coerce')
    
    return df

def load_df_to_sql(df, table_name, engine):
    """Load dataframe into MySQL database if table does not already exist."""
    # This function loads a DataFrame into a MySQL table, creating the table if it doesn't exist.
    if check_table_exists(engine, table_name):
        return False
    else:
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        return True

# SQL queries
# These predefined SQL queries will be available for execution within the app.
queries = {
    "Demographic Distribution by Gender and Age": """
        SELECT Gender, 
               TIMESTAMPDIFF(YEAR, Birthday, CURDATE()) AS Age, 
               COUNT(*) AS CustomerCount 
        FROM Customers 
        GROUP BY Gender, Age
        ORDER BY Gender, Age;
    """,
    "Demographic Distribution by Location": """
        SELECT Country, State, COUNT(*) AS CustomerCount 
        FROM Customers 
        GROUP BY Country, State
        ORDER BY CustomerCount DESC;
    """,
    "Average Order Value and Purchase Frequency": """
        SELECT c.CustomerKey, 
               COUNT(s.`Order Number`) AS PurchaseFrequency, 
               AVG(p.`Unit Price USD` * s.Quantity) AS AverageOrderValue
        FROM Customers c
        JOIN Sales s ON c.CustomerKey = s.CustomerKey
        JOIN Products p ON s.ProductKey = p.ProductKey
        GROUP BY c.CustomerKey
        ORDER BY PurchaseFrequency DESC;
    """,
    "Customer Segmentation by Demographics and Purchasing Behavior": """
        SELECT Gender, Country, 
               COUNT(DISTINCT c.CustomerKey) AS CustomerCount, 
               SUM(p.`Unit Price USD` * s.Quantity) AS TotalSpend
        FROM Customers c
        JOIN Sales s ON c.CustomerKey = s.CustomerKey
        JOIN Products p ON s.ProductKey = p.ProductKey
        GROUP BY Gender, Country
        ORDER BY TotalSpend DESC;
    """,
    "Total Sales Over Time": """
        SELECT DATE(`Order Date`) AS OrderDate, 
               SUM(p.`Unit Price USD` * s.Quantity) AS TotalSales
        FROM Sales s
        JOIN Products p ON s.ProductKey = p.ProductKey
        GROUP BY OrderDate
        ORDER BY OrderDate;
    """,
    "Top Performing Products by Quantity Sold and Revenue": """
        SELECT p.`Product Name`, 
               SUM(s.Quantity) AS TotalQuantitySold, 
               SUM(p.`Unit Price USD` * s.Quantity) AS TotalRevenue
        FROM Sales s
        JOIN Products p ON s.ProductKey = p.ProductKey
        GROUP BY p.`Product Name`
        ORDER BY TotalRevenue DESC, TotalQuantitySold DESC
        LIMIT 10;
    """,
    "Store Sales Performance": """
        SELECT st.StoreKey, st.Country, st.State, 
               SUM(p.`Unit Price USD` * s.Quantity) AS TotalRevenue
        FROM Sales s
        JOIN Stores st ON s.StoreKey = st.StoreKey
        JOIN Products p ON s.ProductKey = p.ProductKey
        GROUP BY st.StoreKey, st.Country, st.State
        ORDER BY TotalRevenue DESC;
    """,
    "Sales by Currency": """
        SELECT s.`Currency Code`, 
           SUM(p.`Unit Price USD` * s.Quantity) * MAX(er.Exchange) AS TotalSales
        FROM Sales s
        JOIN Products p ON s.ProductKey = p.ProductKey
        JOIN Exchange_Rates er ON s.`Currency Code` = er.Currency
        GROUP BY s.`Currency Code`
        ORDER BY TotalSales DESC;
    """,
    "Product Profitability": """
        SELECT p.`Product Name`, 
               SUM((p.`Unit Price USD` - p.`Unit Cost USD`) * s.Quantity) AS TotalProfit
        FROM Sales s
        JOIN Products p ON s.ProductKey = p.ProductKey
        GROUP BY p.`Product Name`
        ORDER BY TotalProfit DESC
        LIMIT 10;
    """,
    "Sales Performance by Product Category": """
        SELECT p.Category, 
               SUM(s.Quantity) AS TotalQuantitySold, 
               SUM(p.`Unit Price USD` * s.Quantity) AS TotalRevenue
        FROM Sales s
        JOIN Products p ON s.ProductKey = p.ProductKey
        GROUP BY p.Category
        ORDER BY TotalRevenue DESC;
    """
}

def main():
    st.markdown('<h1 class="stTitle">Data Processing and Visualization App</h1>', unsafe_allow_html=True)

    # File paths (obfuscated for security)
    # These paths point to the CSV files that will be loaded into the app.
    file_paths = {
        'Stores': '***',  # Enter path of the Stores table
        'Sales': '***',  # Enter path of the Sales table
        'Products': '***',  # Enter path of the Products table
        'Exchange Rates': '***',  # Enter path of the Exchange Rates table
        'Customers': '***'  # Enter path of the Customers table
    }
    
    # Load and clean data
    # Each CSV file is loaded, cleaned, and prepared for further processing.
    stores_df = load_and_clean_stores(file_paths['Stores'])
    sales_df = load_and_clean_sales(file_paths['Sales'])
    products_df = load_and_clean_products(file_paths['Products'])
    exchange_rates_df = load_and_clean_exchange_rates(file_paths['Exchange Rates'])
    customers_df = load_and_clean_customers(file_paths['Customers'])
    
    # Create a database connection
    # The app establishes a connection to the MySQL database.
    engine = create_connection()
    
    # Load data into MySQL
    # Cleaned data is loaded into the MySQL database, creating tables if they don't already exist.
    load_df_to_sql(stores_df, 'Stores', engine)
    load_df_to_sql(sales_df, 'Sales', engine)
    load_df_to_sql(products_df, 'Products', engine)
    load_df_to_sql(exchange_rates_df, 'Exchange_Rates', engine)
    load_df_to_sql(customers_df, 'Customers', engine)

    # Select SQL query to execute
    # Users can select one of the predefined SQL queries to execute.
    st.markdown('<h2 class="stTitle">Select a query to execute:</h2>', unsafe_allow_html=True)
    query_selection = st.selectbox('Choose a query', list(queries.keys()))

    if query_selection:
        query = queries[query_selection]
        st.markdown(f'<h3 class="results-header">{query_selection} Results</h3>', unsafe_allow_html=True)
        
        try:
            with engine.connect() as connection:
                result = connection.execute(text(query))
                df_result = pd.DataFrame(result.fetchall(), columns=result.keys())
                st.dataframe(df_result)
        except Exception as e:
            st.markdown(f'<div class="custom-warning">{str(e)}</div>', unsafe_allow_html=True)

if __name__ == '__main__':
    main()
