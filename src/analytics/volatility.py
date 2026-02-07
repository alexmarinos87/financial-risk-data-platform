import pandas as pd


def rolling_volatility(returns: pd.Series, window: int) -> pd.Series:
    return returns.rolling(window=window).std().dropna()
