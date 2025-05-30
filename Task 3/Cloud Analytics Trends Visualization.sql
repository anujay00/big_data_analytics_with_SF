--Create the features table to store aggregated sales metrics per product and location
CREATE OR REPLACE TABLE MLDB.STREAMING.SALES_FEATURES AS
SELECT 
    LOCATION, 
    PRODUCT_ID,
    PRODUCT_NAME,
    COUNT(*) AS transaction_count,
    SUM(QUANTITY) AS total_quantity_sold,
    AVG(QUANTITY) AS avg_quantity_per_transaction,
    DATEDIFF('day', MIN(PURCHASE_DATE), MAX(PURCHASE_DATE)) AS sales_period_days,
    DATEDIFF('day', MAX(PURCHASE_DATE), CURRENT_DATE()) AS days_since_last_sale,
    SUM(QUANTITY) / NULLIF(DATEDIFF('day', MIN(PURCHASE_DATE), MAX(PURCHASE_DATE)), 0) AS daily_sales_rate
FROM MLDB.STREAMING.RAW_DATA
GROUP BY LOCATION, PRODUCT_ID, PRODUCT_NAME;

--Create a table with deterministic random values for train/test split
--Assign unique row numbers to each record for deterministic random sampling
CREATE OR REPLACE TABLE MLDB.STREAMING.SALES_FEATURES_WITH_ROWNUM AS
SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY PRODUCT_ID, LOCATION) AS ROW_NUM --Unique row number for each record
FROM MLDB.STREAMING.SALES_FEATURES;

--Create a table with 50 random numbers from 1 to 50, without repeats
CREATE OR REPLACE TABLE MLDB.STREAMING.RANDOM_NUMBERS AS
WITH NUMBERS AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS NUM
    FROM TABLE(GENERATOR(ROWCOUNT => 50)) --Generate 50 sequential numbers
)
SELECT 
    NUM,
    --Assign a random order to each number
    ROW_NUMBER() OVER (ORDER BY RANDOM(42)) AS RANDOM_ORDER
FROM NUMBERS
ORDER BY RANDOM_ORDER;

--Get the 35 records (70% of 50) for training
CREATE OR REPLACE TABLE MLDB.STREAMING.TRAIN_DATA AS
SELECT sf.*
FROM MLDB.STREAMING.SALES_FEATURES_WITH_ROWNUM sf
JOIN MLDB.STREAMING.RANDOM_NUMBERS rn
  ON sf.ROW_NUM = rn.NUM
WHERE rn.RANDOM_ORDER <= 35;

--Get the 15 records (30% of 50) for testing
CREATE OR REPLACE TABLE MLDB.STREAMING.TEST_DATA AS
SELECT sf.*
FROM MLDB.STREAMING.SALES_FEATURES_WITH_ROWNUM sf
JOIN MLDB.STREAMING.RANDOM_NUMBERS rn
  ON sf.ROW_NUM = rn.NUM
WHERE rn.RANDOM_ORDER > 35;

--Procedure to predict top-selling products using a trained Gradient Boosting model
CREATE OR REPLACE PROCEDURE MLDB.STREAMING.PREDICT_TOP_PRODUCTS(LOCATION_INPUT STRING, TOP_N INTEGER)
RETURNS TABLE (LOCATION STRING, PRODUCT_ID STRING, PRODUCT_NAME STRING, PREDICTED_SALES FLOAT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy')
HANDLER = 'predict_top_products'
AS
$$
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
import snowflake.snowpark.functions as F

def predict_top_products(session, LOCATION_INPUT, TOP_N):
    # Load training data as pandas DataFrame
    train_data = session.table("MLDB.STREAMING.TRAIN_DATA").to_pandas()
    
    # Print column names to debug
    print("Available columns:", train_data.columns.tolist())
    
    # Define features and target (using uppercase to match Snowflake column names)
    features = ['TRANSACTION_COUNT', 'TOTAL_QUANTITY_SOLD', 'AVG_QUANTITY_PER_TRANSACTION', 
                'SALES_PERIOD_DAYS', 'DAYS_SINCE_LAST_SALE']
    target = 'DAILY_SALES_RATE'
    
    # Train a Gradient Boosting model using scikit-learn
    model = GradientBoostingRegressor(n_estimators=200,max_depth=5,learning_rate=0.1,min_samples_split=5,random_state=42) 
        
    model.fit(train_data[features], train_data[target])
    
    # Load prediction data
    prediction_query = session.table("MLDB.STREAMING.SALES_FEATURES")
    if LOCATION_INPUT != 'ALL':
        prediction_query = prediction_query.filter(F.col("LOCATION") == LOCATION_INPUT)
    
    # Convert to pandas for prediction
    prediction_data = prediction_query.to_pandas()
    
    # Make predictions
    prediction_data['PREDICTED_SALES'] = model.predict(prediction_data[features])
    
    # Sort and get top N
    top_products = prediction_data.sort_values('PREDICTED_SALES', ascending=False).head(TOP_N)
    
    # Return results as Snowflake DataFrame
    result = session.create_dataframe(
        top_products[['LOCATION', 'PRODUCT_ID', 'PRODUCT_NAME', 'PREDICTED_SALES']]
    )
    
    return result
$$;

--Procedure to evaluate the trained model's performance
CREATE OR REPLACE PROCEDURE MLDB.STREAMING.EVALUATE_MODEL()
RETURNS TABLE (MAE FLOAT, RMSE FLOAT, R2_SCORE FLOAT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy')
HANDLER = 'evaluate_model'
AS
$$
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

def evaluate_model(session):
    # Load training and test data
    train_data = session.table("MLDB.STREAMING.TRAIN_DATA").to_pandas()
    test_data = session.table("MLDB.STREAMING.TEST_DATA").to_pandas()
    
    # Print column names to debug
    print("Available columns:", train_data.columns.tolist())
    
    # Define features and target (using uppercase as they appear in Snowflake)
    features = ['TRANSACTION_COUNT', 'TOTAL_QUANTITY_SOLD', 'AVG_QUANTITY_PER_TRANSACTION', 
                'SALES_PERIOD_DAYS', 'DAYS_SINCE_LAST_SALE']
    target = 'DAILY_SALES_RATE'
    
    # Train model
    model = GradientBoostingRegressor(n_estimators=200,max_depth=5,learning_rate=0.1,min_samples_split=5,random_state=42)
    model.fit(train_data[features], train_data[target])
    
    # Make predictions on test data
    y_pred = model.predict(test_data[features])
    y_true = test_data[target]
    
    # Calculate metrics
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)
    
    # Return metrics as Snowflake DataFrame
    result = session.create_dataframe([{
        "MAE": float(mae),
        "RMSE": float(rmse),
        "R2_SCORE": float(r2)
    }])
    
    return result
$$;

--Run model evaluation
CALL MLDB.STREAMING.EVALUATE_MODEL();

--Test predictions for a specific location
CALL MLDB.STREAMING.PREDICT_TOP_PRODUCTS('Jaffna', 5);

--Test predictions for all locations
CALL MLDB.STREAMING.PREDICT_TOP_PRODUCTS('ALL', 10);