import snowflake.snowpark as snowpark
import pandas as pd
import matplotlib.pyplot as plt
from snowflake.snowpark.functions import col
from statsmodels.tsa.holtwinters import ExponentialSmoothing

def forecast_total_income(session: snowpark.Session):
    # Load data from Snowflake and select required columns
    df = session.table("TEST5.STREAMING.RAW_DATA").select(
        col("PURCHASE_DATE"), col("TOTAL_PRICE")
    ).to_pandas()

    # Debugging: Print DataFrame columns
    print("Columns in DataFrame:", df.columns)

    # Rename columns for consistency
    df.rename(columns={"PURCHASE_DATE": "date", "TOTAL_PRICE": "total_income"}, inplace=True)

    # Convert date column and handle errors
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # **Fix: Copy the column to make it writable**
    df["total_income"] = df["total_income"].copy().fillna(0)

    # Aggregate: Calculate mean total income per day
    historical_df = df.groupby("date", as_index=False)["total_income"].mean()

    # Check if DataFrame is empty
    if historical_df.empty:
        print("DataFrame is empty, check if the table has data.")
        return session.create_dataframe([])  # Return an empty DataFrame

    # Sort by date
    historical_df = historical_df.sort_values("date")

    # Fit Holt-Winters Exponential Smoothing model
    model = ExponentialSmoothing(historical_df["total_income"], trend="add", seasonal="add", seasonal_periods=7)
    model_fit = model.fit()

    # Forecast for the next 14 days
    future_dates = pd.date_range(start=historical_df["date"].iloc[-1] + pd.Timedelta(days=1), periods=14)
    forecast_values = model_fit.forecast(14)

    # Create DataFrame for forecast
    forecast_df = pd.DataFrame({"date": future_dates, "forecasted_income": forecast_values})

    # Save historical and forecasted data separately in Snowflake
    session.create_dataframe(historical_df).write.mode("overwrite").save_as_table("TEST5.ANALYTICS.HISTORICAL_AVG_INCOME")
    session.create_dataframe(forecast_df).write.mode("overwrite").save_as_table("TEST5.ANALYTICS.FORECASTED_INCOME")

    # Plot historical and forecasted income in two separate grids
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 10))

    # Plot Historical Average Income
    axes[0].plot(historical_df["date"], historical_df["total_income"], color="blue", label="Historical Avg Income")
    axes[0].set_title("Historical Average Total Income Per Day")
    axes[0].set_xlabel("Date")
    axes[0].set_ylabel("Average Income")
    axes[0].legend()
    axes[0].grid()

    # Plot Forecasted Average Income
    axes[1].plot(forecast_df["date"], forecast_df["forecasted_income"], color="red", linestyle="dashed", label="Forecasted Avg Income")
    axes[1].set_title("Forecasted Average Total Income for Next 14 Days")
    axes[1].set_xlabel("Date")
    axes[1].set_ylabel("Forecasted Income")
    axes[1].legend()
    axes[1].grid()

    # Save plot instead of showing (useful for Snowflake)
    #plt.savefig("/tmp/income_forecast.png")

    # **Return Snowpark DataFrame for forecast_df or historical_df
    return session.create_dataframe(forecast_df)  # Return the forecast DataFrame 

# Required main function for Snowflake
def main(session: snowpark.Session):
    return forecast_total_income(session)
