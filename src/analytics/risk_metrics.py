import pandas as pd


def value_at_risk(returns: pd.Series, confidence: float = 0.95) -> float:
    if returns.empty:
        return 0.0
    return float(returns.quantile(1 - confidence))
