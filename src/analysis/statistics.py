import warnings
from typing import Optional, Tuple

import numpy as np
import statsmodels.api as sm
from scipy.stats import zscore as scipy_zscore
from statsmodels.stats.proportion import proportion_confint


def calculate_appearance_rate_ci(
    post_hits: int,
    total_posts: int,
    alpha: float = 0.05,
) -> Tuple[Optional[float], Optional[float]]:
    if total_posts == 0:
        return None, None
    
    try:
        ci_lower, ci_upper = proportion_confint(
            count=post_hits,
            nobs=total_posts,
            alpha=alpha,
            method='beta',  # Jeffreys区間
        )
        return float(ci_lower), float(ci_upper)
    except Exception:
        return None, None


def calculate_zscore(
    current_rate: float,
    historical_rates: list[float],
) -> Optional[float]:
    if len(historical_rates) < 7:
        return None
    
    if len(historical_rates) == 0:
        return None
    
    try:
        historical_array = np.array(historical_rates)
        mean = np.mean(historical_array)
        std = np.std(historical_array, ddof=1)  # 標本標準偏差
        
        # 浮動小数点数の誤差を考慮して、非常に小さい値は0として扱う
        if std < 1e-10:
            return 0.0
        
        z = (current_rate - mean) / std
        return float(z)
    except Exception:
        return None


def perform_linear_regression(
    weeks: list[int],
    appearance_rates: list[float],
) -> Optional[dict]:
    if len(weeks) != len(appearance_rates) or len(weeks) < 2:
        return None
    
    try:
        x = np.array(weeks)
        y = np.array(appearance_rates)
        
        # 定数項を追加（intercept用）
        X = sm.add_constant(x)
        
        # 回帰分析実行
        # フラットなトレンド（全て同じ値）の場合、R²計算で0除算警告が出るため抑制
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore',
                category=RuntimeWarning,
            )
            model = sm.OLS(y, X).fit()
            
            # 信頼区間を取得
            conf_int = model.conf_int()
            
            # rsquaredへのアクセス時にも警告が発生する可能性があるため、同じコンテキスト内で取得
            r_squared = float(model.rsquared)
        
        return {
            'intercept': float(model.params[0]),
            'slope': float(model.params[1]),
            'intercept_ci_lower': float(conf_int[0][0]),
            'intercept_ci_upper': float(conf_int[0][1]),
            'slope_ci_lower': float(conf_int[1][0]),
            'slope_ci_upper': float(conf_int[1][1]),
            'p_value': float(model.pvalues[1]),
            'r_squared': r_squared,
        }
    except Exception:
        return None

