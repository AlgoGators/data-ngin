import databento as db
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()


# Establish connection and authenticate

client = db.Historical()

# Authenticated reqeust
# print(client.metadata.list_publishers())

data = client.timeseries.get_range(
    dataset="GLBX.MDP3",
    symbols="ESM2",
    schema="ohlcv-1d",
    start="2022-06-06T00:00:00",
    end="2022-06-10T00:10:00",
    limit=1,
)

df = data.to_df()

pd.set_option("display.max_columns", None)
print(df.head())

def get_data(dataset, symbols, schema, start, end, limit):
    data = client.timeseries.get_range(
        dataset=dataset,
        symbols=symbols,
        schema=schema,
        start=start,
        end=end,
        limit=limit,
    )


