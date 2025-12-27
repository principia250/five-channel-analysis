import warnings

import pytest
import numpy as np

from src.analysis.statistics import (
    calculate_appearance_rate_ci,
    calculate_zscore,
    perform_linear_regression,
)


class TestCalculateAppearanceRateCI:
    """calculate_appearance_rate_ciのテスト"""
    
    def test_normal_case(self):
        """正常ケース"""
        ci_lower, ci_upper = calculate_appearance_rate_ci(
            post_hits=100,
            total_posts=1000,
        )
        
        assert ci_lower is not None
        assert ci_upper is not None
        assert 0.0 <= ci_lower <= 1.0
        assert 0.0 <= ci_upper <= 1.0
        assert ci_lower < ci_upper
    
    def test_zero_total_posts(self):
        """total_postsが0の場合"""
        ci_lower, ci_upper = calculate_appearance_rate_ci(
            post_hits=0,
            total_posts=0,
        )
        
        assert ci_lower is None
        assert ci_upper is None
    
    def test_zero_post_hits(self):
        """post_hitsが0の場合"""
        ci_lower, ci_upper = calculate_appearance_rate_ci(
            post_hits=0,
            total_posts=1000,
        )
        
        assert ci_lower is not None
        assert ci_upper is not None
        assert ci_lower == 0.0 or ci_lower > 0.0  # Jeffreys区間は0でも少し上になる可能性がある
    
    def test_all_posts_hit(self):
        """全ての投稿がヒットした場合"""
        ci_lower, ci_upper = calculate_appearance_rate_ci(
            post_hits=1000,
            total_posts=1000,
        )
        
        assert ci_lower is not None
        assert ci_upper is not None
        assert ci_upper == 1.0 or ci_upper < 1.0  # Jeffreys区間は1でも少し下になる可能性がある


class TestCalculateZscore:
    """calculate_zscoreのテスト"""
    
    def test_normal_case(self):
        """正常ケース"""
        historical_rates = [0.1, 0.12, 0.11, 0.13, 0.1, 0.12, 0.11]
        current_rate = 0.15
        
        zscore = calculate_zscore(current_rate, historical_rates)
        
        assert zscore is not None
        assert isinstance(zscore, float)
        # 現在のレートが平均より高いのでz-scoreは正の値になるはず
        assert zscore > 0
    
    def test_insufficient_data(self):
        """データが不足している場合（7未満）"""
        historical_rates = [0.1, 0.12, 0.11]  # 3つしかない
        
        zscore = calculate_zscore(0.15, historical_rates)
        
        assert zscore is None
    
    def test_empty_data(self):
        """データが空の場合"""
        zscore = calculate_zscore(0.15, [])
        
        assert zscore is None
    
    def test_zero_std(self):
        """標準偏差が0の場合（全て同じ値）"""
        historical_rates = [0.1] * 7
        current_rate = 0.1
        
        zscore = calculate_zscore(current_rate, historical_rates)
        
        # 浮動小数点数の誤差を考慮して、非常に小さい値は0として扱う
        assert zscore is not None
        assert abs(zscore) < 1e-10
    
    def test_exact_seven_weeks(self):
        """ちょうど7週間のデータ"""
        historical_rates = [0.1, 0.12, 0.11, 0.13, 0.1, 0.12, 0.11]
        current_rate = 0.1
        
        zscore = calculate_zscore(current_rate, historical_rates)
        
        assert zscore is not None
        assert isinstance(zscore, float)


class TestPerformLinearRegression:
    """perform_linear_regressionのテスト"""
    
    def test_normal_case(self):
        """正常ケース"""
        weeks = [0, 1, 2, 3, 4, 5, 6, 7]
        appearance_rates = [0.1, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18]
        
        result = perform_linear_regression(weeks, appearance_rates)
        
        assert result is not None
        assert 'intercept' in result
        assert 'slope' in result
        assert 'intercept_ci_lower' in result
        assert 'intercept_ci_upper' in result
        assert 'slope_ci_lower' in result
        assert 'slope_ci_upper' in result
        assert 'p_value' in result
        assert 'r_squared' in result
        
        # 上昇トレンドなのでslopeは正の値
        assert result['slope'] > 0
    
    def test_insufficient_data(self):
        """データが不足している場合（2未満）"""
        weeks = [0]
        appearance_rates = [0.1]
        
        result = perform_linear_regression(weeks, appearance_rates)
        
        assert result is None
    
    def test_mismatched_length(self):
        """週と出現率の長さが一致しない場合"""
        weeks = [0, 1, 2]
        appearance_rates = [0.1, 0.12]
        
        result = perform_linear_regression(weeks, appearance_rates)
        
        assert result is None
    
    def test_decreasing_trend(self):
        """下降トレンドの場合"""
        weeks = [0, 1, 2, 3, 4, 5, 6, 7]
        appearance_rates = [0.18, 0.17, 0.16, 0.15, 0.14, 0.13, 0.12, 0.11]
        
        result = perform_linear_regression(weeks, appearance_rates)
        
        assert result is not None
        # 下降トレンドなのでslopeは負の値
        assert result['slope'] < 0
    
    def test_flat_trend(self):
        """フラットなトレンドの場合"""
        weeks = [0, 1, 2, 3, 4, 5, 6, 7]
        appearance_rates = [0.1] * 8
        
        # フラットなトレンドの場合、statsmodelsがR²計算で0除算警告を出すため抑制
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=RuntimeWarning)
            result = perform_linear_regression(weeks, appearance_rates)
        
        assert result is not None
        # フラットなのでslopeは0に近い
        assert abs(result['slope']) < 0.001

