"""质量统计与治理看板刷新稳定性测试。

覆盖场景：
1. 同一运行两个数据源不同步：匹配任务统计先到、疑似重复簇迟到，
   两个指标的可用时间必须区分；
2. 口径未齐时不出可处理复核结论，且不覆盖历史已确认结果；
3. 簇到齐后提升快照，分子/分母范围正确；
4. 快照的分子、分母与来源记录引用可按统计编号回查；
5. 不同运行的数据源不会互相串口径。
"""
from __future__ import annotations

import os
import sys

_backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

from app.services.auto_match_job import AutoMatchJobService
from app.services.mdm_dashboard import MdmDashboardService
from app.services.mdm_quality import MdmQualityService
from app.services.quality_snapshot import (
    CALIBER_COMPLETE,
    CALIBER_PENDING,
    SOURCE_DUP_CLUSTERS,
    SOURCE_MATCH_STATS,
    QualitySnapshotCaliberService,
)
from app.store import store


def _records() -> list[dict]:
    return [
        {"name": "张三", "phone": "13800138000", "email": "zhang@test.com"},
        {"name": "张  三", "phone": "13800138000", "email": "zhangsan@test.com"},
        {"name": "李四", "phone": "13900139000", "email": "lisi@test.com"},
        {"name": "王五", "phone": "", "email": "wangwu@test.com"},
    ]


def _reset_runtime_tables() -> None:
    """清空与质量观测相关的运行态表，保留种子里的匹配规则。"""
    for table in ("_quality_run_registry", "mdm_quality", "dup_cluster", "auto_match_job"):
        store.rows(table).clear()


class TestQualitySnapshotStability:

    def setup_method(self) -> None:
        _reset_runtime_tables()

    # ── 场景1：两个数据源不同步，指标可用时间区分 ──
    def test_two_sources_arrive_out_of_sync_metric_availability_differs(self):
        job_svc = AutoMatchJobService()
        records = _records()

        registered = job_svc.register_match_run(1, "customer", records)
        assert registered["ok"] is True
        run_id = registered["run_id"]

        caliber = QualitySnapshotCaliberService()
        # 阶段一：匹配任务统计先到，簇数据迟到
        assert caliber.is_metric_available(run_id, "match_precision") is True
        assert caliber.is_metric_available(run_id, "duplication") is False
        assert caliber.is_run_ready(run_id) is False
        assert caliber.caliber_status(run_id) == CALIBER_PENDING

        status = job_svc.query_run_data_status(run_id)
        assert status["match_stats_available"] is True
        assert status["cluster_data_available"] is False

        # 阶段二：疑似重复簇到达
        published = job_svc.publish_cluster_data(run_id, [[0, 1]])
        assert published["ok"] is True
        assert caliber.is_metric_available(run_id, "duplication") is True
        assert caliber.is_run_ready(run_id) is True

        source_status = caliber.source_status(run_id)
        assert source_status[SOURCE_MATCH_STATS]["available"] is True
        assert source_status[SOURCE_DUP_CLUSTERS]["available"] is True
        # 簇数据的可用时间不早于任务统计
        assert source_status[SOURCE_DUP_CLUSTERS]["available_at"] >= \
            source_status[SOURCE_MATCH_STATS]["available_at"]

    # ── 场景2：簇迟到期间的快照不得凑数，且不能覆盖已确认结果 ──
    def test_pending_snapshot_keeps_metrics_unavailable_and_preserves_confirmed(self):
        job_svc = AutoMatchJobService()
        quality_svc = MdmQualityService()
        records = _records()

        # 先造一份完整口径并确认的历史快照
        first = job_svc.schedule_match_job(1, "customer", records)
        first_run = first["run_id"]
        first_stats = quality_svc.compute_quality_metrics(
            "customer", records, ["name", "phone"], run_id=first_run
        )
        assert first_stats["caliber_status"] == CALIBER_COMPLETE
        first_snapshot = quality_svc._find_snapshot(first_run)
        confirmed = quality_svc.confirm_quality_snapshot(first_snapshot["id"])
        assert confirmed["ok"] is True
        confirmed_dup = first_snapshot["重复率"]

        # 新运行：任务统计先到、簇迟到，此时计算质量快照
        second = job_svc.register_match_run(1, "customer", records)
        second_run = second["run_id"]
        pending_stats = quality_svc.compute_quality_metrics(
            "customer", records, ["name", "phone"], run_id=second_run
        )
        assert pending_stats["caliber_status"] == CALIBER_PENDING
        assert pending_stats["duplication_pct"] is None
        assert pending_stats["match_precision_pct"] is not None
        pending_snapshot = quality_svc._find_snapshot(second_run)
        assert pending_snapshot["重复率"] == "待数据源"
        assert pending_snapshot["status"] == "计算中"

        # 待齐快照不能被确认
        reject = quality_svc.confirm_quality_snapshot(pending_snapshot["id"])
        assert reject["ok"] is False

        # 历史确认结果原封不动
        trace = quality_svc.trace_snapshot(first_snapshot["id"])
        assert trace["confirmed"] is True
        assert first_snapshot["重复率"] == confirmed_dup

        # 看板：口径未齐不得发出可处理复核结论，但保留旧确认结果
        dashboard_svc = MdmDashboardService()
        dashboard = store.rows("mdm_dashboard")[0]
        dashboard["监控实体"] = "customer"
        panel = dashboard_svc.generate_governance_dashboard(dashboard["id"])["dashboard"]["quality"]
        assert panel["review"]["actionable"] is False
        assert panel["fallback_confirmed"]["stat_id"] == first_snapshot["id"]
        review = dashboard_svc.review_dashboard_measurement(dashboard["id"])
        assert review["ok"] is False

    # ── 场景3：簇到齐后提升快照，分子分母范围正确 ──
    def test_promote_after_clusters_arrive_with_correct_numerators(self):
        job_svc = AutoMatchJobService()
        quality_svc = MdmQualityService()
        records = _records()

        registered = job_svc.register_match_run(1, "customer", records)
        run_id = registered["run_id"]
        stats = quality_svc.compute_quality_metrics(
            "customer", records, ["name", "phone"], run_id=run_id
        )
        assert stats["duplication_pct"] is None

        # 簇迟到：一个大小为2的簇
        job_svc.publish_cluster_data(run_id, [[0, 1]])
        promoted = quality_svc.promote_pending_snapshot(run_id, records, ["name", "phone"])
        assert promoted["ok"] is True
        snapshot = quality_svc._find_snapshot(run_id)
        assert snapshot["口径状态"] == CALIBER_COMPLETE
        assert snapshot["status"] == "已完成"

        dup_scope = promoted["stats"]["metric_scopes"]["duplication"]
        assert dup_scope["available"] is True
        assert dup_scope["numerator"] == 2
        assert dup_scope["denominator"] == 4

        # 任务统计输出1个簇，匹配精度 = 100 - 1/4*100 = 75.0
        precision_scope = promoted["stats"]["metric_scopes"]["match_precision"]
        assert precision_scope["numerator"] == 1
        assert precision_scope["denominator"] == 4
        assert precision_scope["value"] == 75.0

        # 完整口径后看板可发出可处理复核结论
        dashboard_svc = MdmDashboardService()
        dashboard = store.rows("mdm_dashboard")[0]
        dashboard["监控实体"] = "customer"
        review = dashboard_svc.review_dashboard_measurement(dashboard["id"])
        assert review["ok"] is True
        assert review["review"]["run_id"] == run_id

    # ── 场景4：快照可按记录回查分子分母与来源引用 ──
    def test_snapshot_trace_exposes_scopes_and_record_refs(self):
        job_svc = AutoMatchJobService()
        quality_svc = MdmQualityService()
        records = _records()

        registered = job_svc.register_match_run(1, "customer", records)
        run_id = registered["run_id"]
        job_svc.publish_cluster_data(run_id, [[0, 1]])
        quality_svc.compute_quality_metrics(
            "customer", records, ["name", "phone"], run_id=run_id
        )
        snapshot = quality_svc._find_snapshot(run_id)
        trace = quality_svc.trace_snapshot(snapshot["id"])

        assert trace["run_id"] == run_id
        assert trace["口径状态"] == CALIBER_COMPLETE
        for metric in ("completeness", "consistency", "duplication", "match_precision"):
            scope = trace["metric_scopes"][metric]
            assert "numerator" in scope and "denominator" in scope
            assert scope["scope"]
        # 簇来源记录引用可回查
        dup_refs = trace["metric_scopes"]["duplication"]["source_refs"]
        assert dup_refs and all(str(ref).startswith("DUP-") for ref in dup_refs)

    # ── 场景5：不同运行的数据源不串口径 ──
    def test_runs_are_scoped_independently(self):
        job_svc = AutoMatchJobService()
        quality_svc = MdmQualityService()
        records = _records()

        run_a = job_svc.register_match_run(1, "customer", records)["run_id"]
        run_b = job_svc.register_match_run(1, "customer", records)["run_id"]
        # 只给运行A发布簇数据（一个大小2的簇）
        job_svc.publish_cluster_data(run_a, [[0, 1]])

        stats_a = quality_svc.compute_quality_metrics(
            "customer", records, ["name", "phone"], run_id=run_a
        )
        stats_b = quality_svc.compute_quality_metrics(
            "customer", records, ["name", "phone"], run_id=run_b
        )
        assert stats_a["caliber_status"] == CALIBER_COMPLETE
        assert stats_b["caliber_status"] == CALIBER_PENDING
        # 运行B的重复率不能引用运行A的簇
        assert stats_b["duplication_pct"] is None
        assert stats_a["duplication_pct"] == 50.0

        # 同一运行簇数据重复发布应被拒绝
        again = job_svc.publish_cluster_data(run_a, [[2, 3]])
        assert again["ok"] is False

    # ── 场景6：看板刷新不覆盖既有可处理复核留痕 ──
    def test_refresh_keeps_last_actionable_review_when_caliber_pending(self):
        job_svc = AutoMatchJobService()
        quality_svc = MdmQualityService()
        dashboard_svc = MdmDashboardService()
        records = _records()

        first = job_svc.schedule_match_job(1, "customer", records)
        quality_svc.compute_quality_metrics(
            "customer", records, ["name", "phone"], run_id=first["run_id"]
        )
        snapshot = quality_svc._find_snapshot(first["run_id"])
        quality_svc.confirm_quality_snapshot(snapshot["id"])

        dashboard = store.rows("mdm_dashboard")[0]
        dashboard["监控实体"] = "customer"
        first_review = dashboard_svc.review_dashboard_measurement(dashboard["id"])
        assert first_review["ok"] is True

        # 第二个运行口径未齐期间刷新看板
        second = job_svc.register_match_run(1, "customer", records)
        quality_svc.compute_quality_metrics(
            "customer", records, ["name", "phone"], run_id=second["run_id"]
        )
        refreshed = dashboard_svc.refresh_dashboard_data(dashboard["id"])
        assert refreshed["ok"] is True
        panel = refreshed["dashboard"]["quality"]
        assert panel["review"]["actionable"] is False
        # 上次完整口径的复核留痕保留
        assert dashboard["last_actionable_review"]["run_id"] == first["run_id"]
        assert panel["retained_review"]["run_id"] == first["run_id"]


if __name__ == "__main__":
    import traceback

    tests = [name for name in dir(TestQualitySnapshotStability) if name.startswith("test_")]
    passed = failed = 0
    for name in tests:
        instance = TestQualitySnapshotStability()
        instance.setup_method()
        try:
            getattr(instance, name)()
            print(f"  PASS: {name}")
            passed += 1
        except Exception:
            print(f"  FAIL: {name}")
            traceback.print_exc()
            failed += 1
    print(f"\n结果: {passed}/{len(tests)} 通过, {failed} 失败")
    sys.exit(1 if failed else 0)
