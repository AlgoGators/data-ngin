from data.modules.fetcher import Fetcher
import schwabdev
import logging
import os
from typing import List,Dict,Any,Optional
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import numpy as np
from scipy.stats import norm
import matplotlib.pyplot as plt

class Option:
    def __init__(self,strike,premium,dte,vol,type):
        self.call = None
        if type == "CALL":
            self.call = 1
        else:
            self.call = 0
        self.strike = float(strike)
        self.premium = float(premium)
        self.dte = int(dte)
        self.iv = float(vol)
        self.type = type

        self.theo_value = None
        self.nd2 = None
        self.delta = None
        self.gamma = None
        self.theta = None
        self.vega = None

    def calculate_greeks(self,St,r=4):
        """
        Parameters:
        St: Current Stock Price (Ex: 100)
        r: risk free rate of return (Ex: 4%)
        """
        t = self.dte / 365
        v = self.iv / 100
        r = r / 100
        d1 = (np.log(St / self.strike) + ((r + (np.power(v, 2) / 2)) * t)) / (v * np.sqrt(t))
        d2 = d1 - (v * np.sqrt(t))
        if self.type == 'CALL':
            N_d1 = norm.cdf(d1)
            N_d2 = norm.cdf(d2)
            n1 = (St * N_d1)
            n2 = (N_d2 * self.strike)
            n3 = (np.exp(-r * t))
            price = n1 - (n2 * n3)
        else:
            N_d1 = norm.cdf(-d1)
            N_d2 = norm.cdf(-d2)
            n4 = N_d2 * (self.strike * (np.exp(-r * t)))
            n5 = N_d1 * St
            price = n4 - n5
        theta = (-((St * v * np.exp(-np.power(d1, 2) / 2)) / (np.sqrt(8 * np.pi * t))) + (
                    d2 * r * self.strike * np.exp(-r * t))) / 365
        gamma = (np.exp(-np.power(d1, 2) / 2)) / (St * v * np.sqrt(2 * np.pi * t))
        vega = (St * np.sqrt(t) * np.exp(-np.power(d1, 2) / 2)) / (np.sqrt(2 * np.pi) * 100)

        self.theo_value = price
        self.nd2 = N_d2
        self.delta = N_d1
        self.gamma = gamma
        self.theta = theta
        self.vega = vega
        return {'theo_value': price, 'nd2': N_d2, 'delta': N_d1, 'gamma': gamma, 'theta': theta, 'vega': vega}

class Chain:
    def __init__(self,ticker,expiration,typee,client):
        self.chain = []
        self.ticker = ticker
        self.expiration = expiration
        self.type = typee
        self.client = client
        self.c = self.client.option_chains(self.ticker,self.type).json()
        if "errors" in list(self.c["callExpDateMap"].keys()):
            print("Invalid ticker")
        elif "errors" in list(self.c["putExpDateMap"].keys()):
            print("Invali ticker")
        if  expiration in self.get_expirations():
            self._populate_chain()
        else:
            print("Invalid expiration date")
            print(f"Valid expirations are: {self.get_expirations()}")


    def get_expirations(self):
        if self.type == "CALL":
            dates = list(self.c["callExpDateMap"].keys())
        else:
            dates = list(self.c["putExpDateMap"].keys())
        return [x.split(":")[0] for x in dates]
    def _populate_chain(self):
        dte = (datetime.strptime(self.expiration,"%Y-%m-%d").date() - datetime.today().date()).days
        if self.type == "CALL":
            word = 'callExpDateMap'
        else:
            word = 'putExpDateMap'
        for i, j in self.c[word][str(self.expiration)+":"+str(dte)].items():
            j = j[0]
            o = Option(i,j['mark'],j['daysToExpiration'],j['volatility'],j['putCall'])
            self.chain.append(o)
        print(f"Sucessefully populated chain with {self.type}s of ticker {self.ticker} for expiration {self.expiration}")

    def plot_chain(self,x="strike",y="volatility"):
        plt.plot([x.strike for x in self.chain],[x.iv for x in self.chain])
        plt.title(self.ticker+" "+self.expiration+" option chain")
        plt.ylabel(y)
        plt.xlabel(x)
        plt.show()


class SchwabFetcher(Fetcher):
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the Schwab with API connection settings and configurations.

        Args:
            config (Dict[str, Any]): Configuration settings.
        """
        load_dotenv()
        super().__init__(config)
        api_key= os.getenv("SCHWAB_API_KEY")
        api_secret = os.getenv("SCHWAB_SECRET")
        self.client = schwabdev.Client(api_key,api_secret)
        self.logger: logging.Logger = logging.getLogger("SchwabFetcher")
        self.logger.setLevel(logging.INFO)

    def fetch_data(self, symbol,start_date="2010-01-01",end_date=None,option_expiration=None,asset_type="EQUITY"):
        if asset_type == "EQUITY": # for equity assets
            if end_date:
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            if start_date:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            try:
                data_old = self.client.price_history(symbol, startDate=start_date
                                                     ,endDate=end_date,periodType="year",frequencyType="daily").json()["candles"]
                data = []
                for i in data_old:
                    i["datetime"] = datetime.fromtimestamp(i["datetime"] / 1000)
                    data.append(i)
                df = pd.DataFrame(data)
                if df.empty:
                    self.logger.warning(f"No data found for {symbol} between {start_date} and {end_date}")
                    return pd.DataFrame(columns=["open", "high", "low", "close", "volume","datetime"])
                self.logger.info(f"Data fetched successfully for {symbol}.")
                return df
            except Exception as e:
                print(e)
        elif asset_type == "CALLS":  # returns entire call options chain for expiration and ticker (can't include calls and puts in one as sometimes leads to error)
            if option_expiration is None:
                self.logger.warning("Can't have option_expiration parameter none if asset_type == CALL")
            else:
                chain = Chain(symbol,option_expiration,"CALL",self.client)
                current_quote = self.client.quote(symbol).json()[symbol]["quote"]["lastPrice"]
                if chain.chain:
                    for i in chain.chain:
                        i.calculate_greeks(current_quote)
                    chain_data = [vars(x) for x in chain.chain]
                    return pd.DataFrame(chain_data)
        elif asset_type == "PUTS":
            if option_expiration is None:
                self.logger.warning("Can't have option_expiration parameter none if asset_type == CALL")
            else:
                chain = Chain(symbol,option_expiration,"PUT",self.client)
                current_quote = self.client.quote(symbol).json()[symbol]["quote"]["lastPrice"]
                if chain.chain:
                    for i in chain.chain:
                        i.calculate_greeks(current_quote)
                    chain_data = [vars(x) for x in chain.chain]
                    return pd.DataFrame(chain_data)





load_dotenv()
api_key= os.getenv("SCHWAB_API_KEY")
api_secret = os.getenv("SCHWAB_SECRET")
client2 = SchwabFetcher({})
print(client2.fetch_data("F",asset_type="PUTS",option_expiration="2025-02-28"))