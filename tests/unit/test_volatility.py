import pandas as pd
from src.analytics.volatility import rolling_volatility


def test_rolling_volatility():
    returns = pd.Series([0.01, 0.02, -0.01, 0.03])
    vol = rolling_volatility(returns, window=2)
    assert len(vol) == 3
