import pandas as pd


def compute_returns(prices: pd.Series) -> pd.Series:
    return prices.pct_change().dropna()
