from sqlalchemy import create_engine
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

API_url = "https://api.eia.gov/v2/co2-emissions/co2-emissions-aggregates/data/?frequency=annual&data[0]=value&start=2017&end=2021&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000&api_key=7y1Xz10b84R2HouNbFdu4kEsxsrWYbRBPkub5Zj8"
try:
    response = requests.get(API_url, params={})
    response.raise_for_status()  # Raise an HTTPError for bad responses
    json_data = response.json().get('response', [])
    # Process the JSON data as needed
    print(json_data)
except requests.RequestException as e:
    print(f"Error during request: {e}")

    df = pd.DataFrame(json_data["data"])
    df.head()
df.info()

# Transform
# checking null_values


def check_null_values(dataframe):
    """
    Check null values in a pandas DataFrame and return a summary.

    Parameters:
    - dataframe: pandas DataFrame

    Returns:
    - pandas Series: Number of null values in each column
    """
    null_values = dataframe.isnull().sum()
    return null_values


null_summary = check_null_values(df)
print(null_summary)

# loading data to database in snowflake

snowflake_credentials = {
    'user': 'MARIAM',
    'password': 'xxxx',
    'account': 'xxxx',
    'database': 'Carbon_intensity',
    'schema': 'Carbon',
}

table_name = 'carbon_emission_data'

# Create a Snowflake SQLAlchemy engine
engine = create_engine(
    f'snowflake://{snowflake_credentials["user"]}:{snowflake_credentials["password"]}@{snowflake_credentials["account"]}/{snowflake_credentials["database"]}/{snowflake_credentials["schema"]}')

# Write the DataFrame to the Snowflake table using the to_sql method
df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')
