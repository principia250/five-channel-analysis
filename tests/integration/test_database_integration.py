"""データベース統合テスト"""
import pytest
from datetime import date
from sqlalchemy.exc import IntegrityError

from src.database.repositories import (
    TermRepository,
    DailyTermStatsRepository,
    PipelineRunRepository,
)
from src.database.models import Term, DailyTermStats, PipelineRun


@pytest.mark.integration
class TestDatabaseIntegration:
    """データベース統合テスト"""
    
    def test_transaction_rollback(self, test_session):
        """トランザクションのロールバックが正しく動作するか"""
        term_repo = TermRepository(test_session)
        daily_stats_repo = DailyTermStatsRepository(test_session)
        
        # Termを作成してflush（コミットはしない）
        term = term_repo.get_or_create("テスト用名詞")
        test_session.flush()  # flushしてIDを取得
        
        # term_idを保存（ロールバック後にexpiredになるため）
        term_id = term.term_id
        
        # flush後、termがセッションに存在することを確認
        assert term_repo.get_by_id(term_id) is not None
        
        # 無効なデータでIntegrityErrorを発生させる
        invalid_stat = DailyTermStats(
            date=date.today(),
            board_key="prog",
            term_id=999999999,  # 存在しないterm_id
            post_hits=-1,  # 負の値（制約違反）
            thread_hits=0,
        )
        
        with pytest.raises(IntegrityError):
            daily_stats_repo.create(invalid_stat)
            test_session.flush()
        
        # ロールバック後、すべての変更がロールバックされる
        test_session.rollback()
        # ロールバック後、termはセッションから削除される（未コミットのため）
        term_after = term_repo.get_by_id(term_id)
        assert term_after is None  # ロールバック後はNone（未コミットのため）
    
    def test_unique_constraints(self, test_session):
        """ユニーク制約が正しく機能するか"""
        run_repo = PipelineRunRepository(test_session)
        target_date = date.today()
        board_key = "prog"
        
        # 最初のレコードを作成
        run1 = PipelineRun(
            target_date=target_date,
            board_key=board_key,
            status="success",
            config={},
        )
        run_repo.create(run1)
        test_session.commit()
        
        # 同じ日付・板で重複作成を試みる
        run2 = PipelineRun(
            target_date=target_date,
            board_key=board_key,
            status="success",
            config={},
        )
        
        with pytest.raises(IntegrityError):
            run_repo.create(run2)
            test_session.commit()
        
        test_session.rollback()
    
    def test_term_get_or_create(self, test_session):
        """Termのget_or_createが正しく動作するか"""
        term_repo = TermRepository(test_session)
        
        # 最初に作成
        term1 = term_repo.get_or_create("テスト名詞")
        test_session.commit()
        
        term_id = term1.term_id
        
        # 同じ名詞で再度取得（作成されない）
        term2 = term_repo.get_or_create("テスト名詞")
        test_session.commit()
        
        # 同じIDが返される
        assert term1.term_id == term2.term_id == term_id
        
        # 異なる名詞で作成
        term3 = term_repo.get_or_create("別の名詞")
        test_session.commit()
        
        # 異なるIDが返される
        assert term3.term_id != term_id
    
    def test_daily_stats_upsert(self, test_session):
        """DailyTermStatsのupsertが正しく動作するか"""
        term_repo = TermRepository(test_session)
        daily_stats_repo = DailyTermStatsRepository(test_session)
        
        # Termを作成
        term = term_repo.get_or_create("アップサートテスト")
        test_session.commit()
        
        target_date = date.today()
        board_key = "prog"
        
        # 最初に作成
        stats1 = DailyTermStats(
            date=target_date,
            board_key=board_key,
            term_id=term.term_id,
            post_hits=10,
            thread_hits=1,
        )
        daily_stats_repo.upsert(stats1)
        test_session.commit()
        
        # 同じキーでupsert（更新される）
        stats2 = DailyTermStats(
            date=target_date,
            board_key=board_key,
            term_id=term.term_id,
            post_hits=20,  # 値を変更
            thread_hits=2,
        )
        daily_stats_repo.upsert(stats2)
        test_session.commit()
        
        # データベースから取得して確認
        results = daily_stats_repo.get_by_date_and_board(
            target_date,
            board_key,
        )
        
        # post_hitsが更新されているか確認
        assert len(results) == 1
        assert results[0].post_hits == 20
        assert results[0].thread_hits == 2
    
    def test_foreign_key_constraint(self, test_session):
        """外部キー制約が正しく機能するか"""
        daily_stats_repo = DailyTermStatsRepository(test_session)
        
        # 存在しないterm_idでDailyTermStatsを作成しようとする
        invalid_stat = DailyTermStats(
            date=date.today(),
            board_key="prog",
            term_id=999999999,  # 存在しないterm_id
            post_hits=10,
            thread_hits=1,
        )
        
        with pytest.raises(IntegrityError):
            daily_stats_repo.create(invalid_stat)
            test_session.commit()
        
        test_session.rollback()

