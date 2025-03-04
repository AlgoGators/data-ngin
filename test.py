import schwabdev
import os
from dotenv import load_dotenv

load_dotenv()

key = os.getenv("SCHWAB_API_KEY")
secret = os.getenv("SCHWAB_SECRET")

client = schwabdev.Client(key,secret)
print(client.price_history("SPY"))