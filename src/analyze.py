# import io
#
# import matplotlib.pyplot as plt
# import pandas as pd
# from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
# from statsmodels.tsa.stattools import adfuller
#
#
# # --- Step 0: Simulate your function ---
# # This function simulates your existing method that returns the CSV data as bytes.
# # Replace the file path with the actual path to your 'clean_elonmusk.csv' file.
# def get_tweet_data_bytes(file_path='clean_elonmusk.csv'):
#     """
#     Reads a CSV file and returns its content as bytes.
#     """
#     with open(file_path, 'rb') as f:
#         return f.read()
#
#
# # --- Step 1: Load and Preprocess the Data ---
# print("--- 1. Loading and Preprocessing Data ---")
#
# # Get the data as bytes from your function
# tweet_data_bytes = get_tweet_data_bytes()
#
# # Use io.BytesIO to treat the bytes as a file and load into a pandas DataFrame
# # The 'parse_dates' argument tells pandas to interpret the 'timestamp' column as dates
# try:
#     df = pd.read_csv(io.BytesIO(tweet_data_bytes), parse_dates=['timestamp'])
#
#     # Set the 'timestamp' column as the DataFrame's index
#     df.set_index('timestamp', inplace=True)
#
#     print("Data loaded successfully. DataFrame info:")
#     df.info()
#     print("\nFirst 5 rows of the DataFrame:")
#     print(df.head())
#
# except Exception as e:
#     print(f"An error occurred: {e}")
#     # Exit if data loading fails
#     exit()
#
# print("\n" + "=" * 50 + "\n")
#
# # --- Step 2: Aggregate the Data by Time Intervals ---
# print("--- 2. Aggregating Tweet Counts ---")
#
# # Resample the DataFrame to get the count of tweets per week ('W')
# # .size() counts the number of occurrences in each interval
# weekly_counts = df.resample('W').size()
#
# # Resample to get daily tweet counts ('D')
# daily_counts = df.resample('D').size()
#
# # Resample to get hourly tweet counts ('H')
# hourly_counts = df.resample('H').size()
#
# print("Weekly Counts (first 5 rows):")
# print(weekly_counts.head())
# print("\nDaily Counts (first 5 rows):")
# print(daily_counts.head())
# print("\nHourly Counts (first 5 rows):")
# print(hourly_counts.head())
#
# print("\n" + "=" * 50 + "\n")
#
# # --- Step 3: Exploratory Data Analysis (EDA) ---
# print("--- 3. Performing Exploratory Data Analysis ---")
# print("Generating plots...")
#
# # Plot 1: Weekly Tweet Counts over time
# plt.figure(figsize=(14, 7))
# weekly_counts.plot()
# plt.title('Weekly Tweet Counts Over Time')
# plt.xlabel('Date')
# plt.ylabel('Number of Tweets')
# plt.grid(True)
# plt.tight_layout()
# plt.show()
#
# # Plot 2: Histogram of Weekly Tweet Counts
# plt.figure(figsize=(14, 7))
# weekly_counts.hist(bins=20, edgecolor='black')
# plt.title('Distribution of Weekly Tweet Counts')
# plt.xlabel('Number of Tweets per Week')
# plt.ylabel('Frequency (Number of Weeks)')
# plt.grid(axis='y')
# plt.tight_layout()
# plt.show()
#
# print("\n" + "=" * 50 + "\n")
#
# # --- Step 4: Check for Stationarity ---
# print("--- 4. Checking for Time Series Stationarity ---")
#
# # Perform the Augmented Dickey-Fuller (ADF) test
# # The null hypothesis of the ADF test is that the time series is non-stationary.
# result = adfuller(weekly_counts.dropna())  # dropna() to handle potential missing values
#
# print(f'ADF Statistic: {result[0]:.4f}')
# print(f'p-value: {result[1]:.4f}')
# print('Critical Values:')
# for key, value in result[4].items():
#     print(f'\t{key}: {value:.4f}')
#
# # Interpret the result
# if result[1] <= 0.05:
#     print("\nConclusion: The p-value is less than or equal to 0.05. We reject the null hypothesis.")
#     print("The time series is likely stationary. ✅")
# else:
#     print("\nConclusion: The p-value is greater than 0.05. We fail to reject the null hypothesis.")
#     print("The time series is likely non-stationary. ❌")
#
# print("\n" + "=" * 50 + "\n")
#
# # --- Step 5: Autocorrelation Analysis ---
# print("--- 5. Performing Autocorrelation Analysis ---")
# print("Generating ACF and PACF plots...")
#
# # Generate ACF and PACF plots
# fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
#
# # Autocorrelation Function (ACF) plot
# plot_acf(weekly_counts.dropna(), ax=ax1, lags=40)
# ax1.set_title('Autocorrelation Function (ACF)')
#
# # Partial Autocorrelation Function (PACF) plot
# plot_pacf(weekly_counts.dropna(), ax=ax2, lags=40)
# ax2.set_title('Partial Autocorrelation Function (PACF)')
#
# plt.tight_layout()
# plt.show()
#
# print("\nAnalysis complete. Next step: Model selection based on these results.")
