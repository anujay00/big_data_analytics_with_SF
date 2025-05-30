import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(page_title="Enhanced Snowflake Dashboard", layout="wide")

# Dashboard title with improved styling
st.title("Sales Data Dashboard")
st.write("This dashboard provides key insights from the TEST5.STREAMING.RAW_DATA.")

# Add sidebar for interactive filters
st.sidebar.header("Dashboard Controls")

# Snowflake Connection
try:
    session = get_active_session()
    
    # First, let's inspect the schema to find the correct column names
    try:
        schema_query = session.sql("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'STREAMING' 
            AND TABLE_NAME = 'RAW_DATA'
        """).collect()
        
        columns = [row[0] for row in schema_query]
        st.sidebar.markdown("### Available Columns")
        st.sidebar.write(f"Found columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
    except Exception as e:
        st.sidebar.warning(f"Could not fetch schema information: {str(e)}")
        columns = []
    
    # Define default date ranges since we don't have a timestamp column
    today = datetime.now().date()
    default_start_date = today - timedelta(days=30)
    default_end_date = today
    
    # Price range slider - without date filtering
    try:
        price_query = session.sql("""
            SELECT MIN(total_price) AS min_price, 
                   MAX(total_price) AS max_price 
            FROM TEST5.STREAMING.RAW_DATA
        """).collect()
        
        if price_query and price_query[0][0] is not None:
            min_price = float(price_query[0][0])
            max_price = float(price_query[0][1])
        else:
            min_price = 0.0
            max_price = 10000.0
    except Exception as e:
        st.sidebar.warning(f"Could not fetch price range: {str(e)}")
        min_price = 0.0
        max_price = 10000.0
    
    price_range = st.sidebar.slider(
        "Price Range (LKR)",
        min_value=min_price,
        max_value=max_price,
        value=(min_price, max_price),
        step=100.0
    )
    min_selected_price, max_selected_price = price_range
    
    # Get locations for location filter without date filtering
    try:
        locations_query = session.sql("""
            SELECT DISTINCT location FROM TEST5.STREAMING.RAW_DATA
            ORDER BY location
        """).collect()
        locations = [row[0] for row in locations_query]
        
        # Add 'All Locations' option
        all_locations = ['All Locations'] + locations
        selected_location = st.sidebar.selectbox("Filter by Location", all_locations)
    except Exception as e:
        st.sidebar.warning(f"Could not fetch locations: {str(e)}")
        selected_location = "All Locations"
        locations = []
    
    # Build the WHERE clause based on available filters
    where_clauses = [
        f"total_price BETWEEN {min_selected_price} AND {max_selected_price}"
    ]
    
    if selected_location != "All Locations" and selected_location in locations:
        where_clauses.append(f"location = '{selected_location}'")
    
    where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    # Layout with columns for metrics
    col1, col2, col3 = st.columns(3)
    
    # 1. Total Sales Revenue with filter
    try:
        revenue_query = session.sql(f"""
            SELECT SUM(total_price) 
            FROM TEST5.STREAMING.RAW_DATA
            {where_clause}
        """).collect()
        total_revenue = revenue_query[0][0] if revenue_query and revenue_query[0][0] else 0
        col1.metric(label="Total Sales Revenue (LKR)", value=f"{total_revenue:,.2f}")
    except Exception as e:
        col1.error(f"Error fetching total revenue: {str(e)}")
    
    # 2. Total Orders with filter
    try:
        orders_query = session.sql(f"""
            SELECT COUNT(*) 
            FROM TEST5.STREAMING.RAW_DATA
            {where_clause}
        """).collect()
        total_orders = orders_query[0][0] if orders_query else 0
        col2.metric(label="Total Orders", value=f"{total_orders:,}")
    except Exception as e:
        col2.error(f"Error fetching order count: {str(e)}")
    
    # 3. Average Order Value with filter
    try:
        if total_orders and total_orders > 0:
            avg_order_value = total_revenue / total_orders
            col3.metric(label="Average Order Value (LKR)", value=f"{avg_order_value:,.2f}")
        else:
            col3.metric(label="Average Order Value (LKR)", value="0.00")
    except Exception as e:
        col3.error(f"Error calculating average order value: {str(e)}")
    
    # Top-Selling Products section
    st.header("Top-Selling Products")
    
    # Number of products to show slider
    top_n_products = st.slider("Number of top products to display", min_value=1, max_value=5, value=5)
    
    # 4. Top-Selling Products with filter and slider
    try:
        top_products_query = session.sql(f"""
            SELECT product_name, SUM(quantity) AS total_sold, SUM(total_price) AS revenue
            FROM TEST5.STREAMING.RAW_DATA
            {where_clause}
            GROUP BY product_name
            ORDER BY total_sold DESC
            LIMIT {top_n_products}
        """).collect()
        
        if top_products_query:
            products_df = pd.DataFrame(top_products_query, columns=["Product", "Quantity Sold", "Revenue (LKR)"])
            products_df["Revenue (LKR)"] = products_df["Revenue (LKR)"].apply(lambda x: f"{x:,.2f}" if x else "0.00")
            st.dataframe(products_df, use_container_width=True)
            
            # Add a bar chart for visualization
            chart_data = {row[0]: row[1] for row in top_products_query}
            st.bar_chart(chart_data, use_container_width=True)
        else:
            st.warning("No product data available with current filters.")
    except Exception as e:
        st.error(f"Error fetching top products: {str(e)}")
    
    # Sales by Location section
    st.header("Sales Distribution by Location")
    
    # 5. Sales Distribution by Location with filter
    try:
        location_clause = where_clause
        if selected_location != "All Locations" and "location =" in location_clause:
            # Remove location filter for location distribution chart
            location_clause = location_clause.replace(f"location = '{selected_location}'", "1=1")
            if " AND 1=1" in location_clause:
                location_clause = location_clause.replace(" AND 1=1", "")
            if "WHERE 1=1" in location_clause:
                location_clause = location_clause.replace("WHERE 1=1", "")
        
        location_sales_query = session.sql(f"""
            SELECT location, SUM(total_price) AS revenue, COUNT(*) as order_count
            FROM TEST5.STREAMING.RAW_DATA
            {location_clause}
            GROUP BY location
            ORDER BY revenue DESC
        """).collect()
        
        if location_sales_query:
            # Create tabs for different visualizations
            tab1, tab2 = st.tabs(["Revenue by Location", "Order Count by Location"])
            
            with tab1:
                locations = [row[0] for row in location_sales_query]
                revenues = [row[1] for row in location_sales_query]
                st.bar_chart(dict(zip(locations, revenues)), use_container_width=True)
            
            with tab2:
                locations = [row[0] for row in location_sales_query]
                order_counts = [row[2] for row in location_sales_query]
                st.bar_chart(dict(zip(locations, order_counts)), use_container_width=True)
        else:
            st.warning("No location data available with current filters.")
    except Exception as e:
        st.error(f"Error fetching sales by location: {str(e)}")
    
    # Payment and Device Analysis section
    st.header("Payment and Device Analysis")
    
    col1, col2 = st.columns(2)
    
    # 6. Payment Method Distribution with filter
    with col1:
        try:
            payment_query = session.sql(f"""
                SELECT purchasing_type, COUNT(*) AS usage_count
                FROM TEST5.STREAMING.RAW_DATA
                {where_clause}
                GROUP BY purchasing_type
                ORDER BY usage_count DESC
            """).collect()
            
            if payment_query:
                payment_methods = [row[0] for row in payment_query]
                usage_counts = [row[1] for row in payment_query]
                st.subheader("Payment Method Distribution")
                st.bar_chart(dict(zip(payment_methods, usage_counts)), use_container_width=True)
            else:
                st.warning("No payment method data available with current filters.")
        except Exception as e:
            st.error(f"Error fetching payment methods: {str(e)}")
    
    # 7. Device Type Usage with filter
    with col2:
        try:
            device_query = session.sql(f"""
                SELECT device_type, COUNT(*) AS usage_count
                FROM TEST5.STREAMING.RAW_DATA
                {where_clause}
                GROUP BY device_type
                ORDER BY usage_count DESC
            """).collect()
            
            if device_query:
                device_types = [row[0] for row in device_query]
                usage_counts = [row[1] for row in device_query]
                st.subheader("Device Type Distribution")
                st.bar_chart(dict(zip(device_types, usage_counts)), use_container_width=True)
            else:
                st.warning("No device type data available with current filters.")
        except Exception as e:
            st.error(f"Error fetching device types: {str(e)}")
        

except Exception as e:
    st.error(f"Failed to connect to Snowflake: {str(e)}")

# Footer
st.markdown("---")
st.markdown("Sales Data Dashboard | Business Insights")