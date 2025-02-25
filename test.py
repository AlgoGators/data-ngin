import schwabdev
import os
from dotenv import load_dotenv
from data.modules.schwab_fetcher import Chain, Option
load_dotenv()

client = schwabdev.Client(os.getenv("SCHWAB_API_KEY"),os.getenv("SCHWAB_SECRET"))
chain = Chain("F","2025-02-28","CALL",client)