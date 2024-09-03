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
    engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    return engine

def check_table_exists(engine, table_name):
    """Check if a table already exists in the database."""
    inspector = inspect(engine)
    return inspector.has_table(table_name)

def load_and_clean_stores(filepath):
    """Load and clean Stores.csv."""
    df = pd.read_csv(filepath)
    df['Square Meters'] = df['Square Meters'].fillna(df['Square Meters'].median())
    df['Open Date'] = pd.to_datetime(df['Open Date'], dayfirst=False, errors='coerce')
    return df

def load_and_clean_sales(filepath):
    """Load and clean Sales.csv."""
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

    # File paths
    # These paths point to the CSV files that will be loaded into the app.
    file_paths = {
        'stores': '../data/Stores.csv',
        'sales': '../data/Sales.csv',
        'products': '../data/Products.csv',
        'exchange_rates': '../data/Exchange_Rates.csv',
        'customers': '../data/Customers.csv'
    }

    # Create database connection
    engine = create_connection()

    # Initialize session state
    if 'loaded_files' not in st.session_state:
        st.session_state.loaded_files = []
    if 'all_files_loaded' not in st.session_state:
        st.session_state.all_files_loaded = False
    if 'data_loaded_to_sql' not in st.session_state:
        st.session_state.data_loaded_to_sql = False
    if 'success_message' not in st.session_state:
        st.session_state.success_message = ""
    if 'sql_success_messages' not in st.session_state:
        st.session_state.sql_success_messages = []
    if 'show_sql_messages' not in st.session_state:
        st.session_state.show_sql_messages = True

    
    # Dropdown for loading and cleaning data
    if not st.session_state.all_files_loaded:
        remaining_files = [file for file in file_paths.keys() if file not in st.session_state.loaded_files]
        if remaining_files:
            selected_file = st.selectbox("Select a file to load and clean:", remaining_files, key="file_selector")
            if st.button("Load and Clean"):
                file_path = file_paths[selected_file]
                if selected_file == 'stores':
                    df = load_and_clean_stores(file_path)
                elif selected_file == 'sales':
                    df = load_and_clean_sales(file_path)
                elif selected_file == 'products':
                    df = load_and_clean_products(file_path)
                elif selected_file == 'exchange_rates':
                    df = load_and_clean_exchange_rates(file_path)
                elif selected_file == 'customers':
                    df = load_and_clean_customers(file_path)
    
                # Display the success message
                st.session_state.success_message = f"Successfully loaded and cleaned {selected_file.capitalize()} data."
                
                if st.session_state.success_message:
                    st.markdown(f'<div class="custom-success">{st.session_state.success_message}</div>', unsafe_allow_html=True)
                    # Clear the success message when the dropdown value changes
                    if st.session_state.get('file_selector') != st.session_state.get('last_selected_file'):
                        st.session_state.success_message = ""
                    st.session_state.last_selected_file = st.session_state.get('file_selector')
                    
                # Display the cleaned dataframe
                st.dataframe(df)
    
                # Store the dataframe in session state
                st.session_state[f'{selected_file}_df'] = df
                st.session_state.loaded_files.append(selected_file)
    
                if len(st.session_state.loaded_files) == len(file_paths):
                    st.session_state.all_files_loaded = True
    
                # No need to rerun immediately, as the dataframe is already displayed
    else:
        st.session_state.all_files_loaded = True
        st.rerun()
   

    # Load to SQL button
    if st.session_state.all_files_loaded and not st.session_state.data_loaded_to_sql:
        if st.button("Load to SQL"):
            all_loaded = True
            st.session_state.sql_success_messages = []
            for file_name in file_paths.keys():
                if f'{file_name}_df' in st.session_state:
                    df = st.session_state[f'{file_name}_df']
                    table_name = file_name.capitalize()
                    if check_table_exists(engine, table_name):
                        
                        st.session_state.sql_success_messages.append(f"Table '{table_name}' already exists in the database.")
                    else:
                        load_df_to_sql(df, table_name, engine)
                        st.session_state.sql_success_messages.append(f"Data loaded into table '{table_name}'.")
                else:
                    all_loaded = False
                    st.error(f"Data for {file_name} is not loaded. Please load all data before proceeding.")
                    break
            
            if all_loaded:
                st.session_state.data_loaded_to_sql = True
                st.session_state.show_sql_messages = True
                st.rerun()

    # Display success messages for SQL loading
    if st.session_state.show_sql_messages and st.session_state.sql_success_messages:
        for message in st.session_state.sql_success_messages:
            if "already exists" in message:
                st.markdown(f'<div class="custom-warning">{message}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="custom-success">{message}</div>', unsafe_allow_html=True)

    # Dropdown for SQL queries
    if st.session_state.data_loaded_to_sql:
        query_names = list(queries.keys())
        selected_query = st.selectbox("Select a query to execute:", query_names, key="query_selector")

        if selected_query:
            # Hide SQL messages when a query is selected
            st.session_state.show_sql_messages = False
            
            query = queries[selected_query]
            try:
                with engine.connect() as connection:
                    df = pd.read_sql_query(text(query), connection)
                st.markdown(f'<h3 class="results-header">Results for {selected_query}:</h3>', unsafe_allow_html=True)
                st.dataframe(df)
            except Exception as e:
                st.error(f"An error occurred while executing the query: {e}")

if __name__ == '__main__':
    main()
