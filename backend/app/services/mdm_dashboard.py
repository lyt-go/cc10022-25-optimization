"""看板配置领域服务：看板统计、实时指标展示与治理趋势分析。

稳定性改造：
1. 看板不再简单取最后一条「已完成」质量统计，而是按口径状态读取模型
   质量观测快照：两个指标（重复率、匹配精度）分别展示数据源可用时间，
   匹配任务统计先到时只有匹配精度可用，簇数据到齐后重复率才可用。
2. 只有口径完整的测量才会发出「可处理」的复核结论；口径待齐时明确
   提示等待哪个数据源，且不覆盖看板上一次已确认的复核记录。
3. 复核结论记录所依据的统计编号与运行编号，旧结果可回查快照明细。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.mdm_quality import PENDING_DISPLAY, MdmQualityService
from app.services.quality_snapshot import (
    CALIBER_COMPLETE,
    CALIBER_PENDING,
    SOURCE_LABELS,
    QualitySnapshotCaliberService,
)
from app.store import store


class MdmDashboardService(BaseModuleService):
    MODULE = "mdm_dashboard"
    ENTITY = "看板配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["看板编号", "看板名称", "监控实体", "展示指标", "刷新频率", "数据范围", "分享链接", "看板版本"]
    REQUIRED_FIELDS = ["看板编号", "看板名称", "监控实体"]
    STATUS_ORDER = ["草稿", "待校验", "已发布", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认发布": "已发布", "停用看板": "已停用"}
    NEGATIVE_ACTIONS = ["停用看板"]

    def __init__(self) -> None:
        self.quality_svc = MdmQualityService()
        self.caliber = QualitySnapshotCaliberService()

    def generate_governance_dashboard(self, dashboard_id: int) -> dict[str, Any]:
        """生成治理看板：汇总治理实时指标，并按口径状态给出质量复核结论。

        运营类指标（来源系统、黄金记录、同步配置、簇处置量）按监控实体
        实时聚合；质量类指标只引用带完整口径状态的观测快照，并逐个指标
        标注数据源可用时间，避免刷新中途混入不同范围的数据。
        """
        dashboard = self.get_entry(dashboard_id)
        if not dashboard:
            return {"ok": False, "reason": "看板配置不存在"}
        entity = dashboard.get("监控实体", "")
        metrics = [m.strip() for m in dashboard.get("展示指标", "").split(",") if m.strip()]

        panel_data: dict[str, Any] = {
            "dashboard_name": dashboard.get("看板名称"),
            "generated_at": datetime.datetime.utcnow().isoformat(),
        }

        # 聚合来源系统接入统计
        sources = store.rows("source_system")
        panel_data["total_source_systems"] = len(sources)
        panel_data["active_systems"] = sum(1 for s in sources if s.get("status") == "已接入")

        # 聚合黄金记录统计
        golden = store.rows("golden_record")
        panel_data["total_golden_records"] = len(golden)
        panel_data["active_golden"] = sum(1 for g in golden if g.get("status") == "已生效")

        # 聚合重复簇处置统计（运营面板，按监控实体过滤）
        clusters = store.rows("dup_cluster")
        if entity:
            clusters = [c for c in clusters if c.get("实体模型") == entity]
        panel_data["total_clusters"] = len(clusters)
        panel_data["pending_clusters"] = sum(1 for c in clusters if c.get("status") == "待确认")
        panel_data["merged_clusters"] = sum(1 for c in clusters if c.get("status") == "已合并")

        # 质量观测：按口径状态读取，区分两个指标的可用时间
        panel_data["quality"] = self._build_quality_panel(entity)

        # 聚合同步统计
        sync_cfgs = store.rows("mdm_sync")
        panel_data["total_sync_configs"] = len(sync_cfgs)
        panel_data["active_syncs"] = sum(1 for s in sync_cfgs if s.get("status") == "同步中")

        panel_data["metrics"] = metrics
        return {"ok": True, "dashboard": panel_data}

    def _build_quality_panel(self, entity: str) -> dict[str, Any]:
        """构造质量面板：快照口径、逐指标可用时间、复核闸门与旧结果兜底。"""
        quality_rows = store.rows("mdm_quality")
        model_rows = [q for q in quality_rows if q.get("实体模型") == entity]
        model_rows.sort(key=lambda r: r.get("统计时间", ""), reverse=True)

        latest = model_rows[0] if model_rows else None
        confirmed = self.quality_svc.latest_confirmed_snapshot(entity)
        panel: dict[str, Any] = {
            "has_snapshot": latest is not None,
            "caliber_status": latest.get("口径状态") if latest else CALIBER_PENDING,
            "metric_availability": (latest or {}).get("metric_availability", []),
            "missing_sources": (latest or {}).get("missing_sources", []),
            "source_status": (latest or {}).get("source_status", {}),
            "completeness": (latest or {}).get("完备率", PENDING_DISPLAY),
            "consistency": (latest or {}).get("一致率", PENDING_DISPLAY),
            "duplication": (latest or {}).get("重复率", PENDING_DISPLAY),
            "match_precision": (latest or {}).get("匹配精度", PENDING_DISPLAY),
            "run_id": (latest or {}).get("run_id"),
            "stat_id": (latest or {}).get("id"),
            # 旧确认结果保留展示，不被待齐测量覆盖
            "fallback_confirmed": self._confirmed_brief(confirmed),
        }
        panel["review"] = self._review_gate(latest, confirmed)
        return panel

    def _review_gate(self, latest: dict[str, Any] | None,
                       confirmed: dict[str, Any] | None) -> dict[str, Any]:
        """复核闸门：只有完整口径的测量才能发出可处理的复核结论。"""
        if latest is not None and latest.get("口径状态") == CALIBER_COMPLETE:
            return {
                "actionable": True,
                "conclusion": "口径完整，可进入人工复核",
                "stat_id": latest.get("id"),
                "统计编号": latest.get("统计编号"),
                "run_id": latest.get("run_id"),
                "confirmed": bool(latest.get("confirmed")),
            }
        missing = (latest or {}).get("missing_sources", [])
        reason = "、".join(missing) if missing else "质量观测快照尚未生成"
        return {
            "actionable": False,
            "conclusion": f"暂缓人工复核：等待{reason}到达后再核对指标",
            "stat_id": (latest or {}).get("id"),
            "run_id": (latest or {}).get("run_id"),
            # 看板仍可回看上一次已确认结论，但本次刷新不产生新结论
            "last_confirmed_stat_id": (confirmed or {}).get("id"),
        }

    @staticmethod
    def _confirmed_brief(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "stat_id": row.get("id"),
            "统计编号": row.get("统计编号"),
            "run_id": row.get("run_id"),
            "完备率": row.get("完备率"),
            "重复率": row.get("重复率"),
            "一致率": row.get("一致率"),
            "匹配精度": row.get("匹配精度"),
            "confirmed_at": row.get("confirmed_at"),
        }

    def review_dashboard_measurement(self, dashboard_id: int) -> dict[str, Any]:
        """对看板当前监控实体的最新测量发出复核结论并留痕。

        口径不完整时拒绝发出可处理结论，同时返回等待中的数据源；
        已确认的历史结论原样保留，可通过统计编号回查。
        """
        dashboard = self.get_entry(dashboard_id)
        if not dashboard:
            return {"ok": False, "reason": "看板不存在"}
        entity = dashboard.get("监控实体", "")
        panel = self._build_quality_panel(entity)
        gate = panel["review"]
        if not gate["actionable"]:
            return {
                "ok": False,
                "reason": gate["conclusion"],
                "missing_sources": panel["missing_sources"],
                "metric_availability": panel["metric_availability"],
                "last_confirmed": panel["fallback_confirmed"],
            }
        review_record = {
            "reviewed_at": datetime.datetime.utcnow().isoformat(),
            "stat_id": gate["stat_id"],
            "统计编号": gate["统计编号"],
            "run_id": gate["run_id"],
            "conclusion": gate["conclusion"],
        }
        # 仅完整口径的复核会覆盖看板上的复核留痕
        dashboard["last_actionable_review"] = review_record
        return {"ok": True, "review": review_record}

    def query_published_dashboards(self, entity_model: str | None = None) -> list[dict[str, Any]]:
        """查询已发布的治理看板配置，按监控实体筛选。"""
        rows = store.rows(self.MODULE)
        dashboards = [row for row in rows if row.get("status") == "已发布"]
        if entity_model:
            dashboards = [d for d in dashboards if d.get("监控实体") == entity_model]
        return dashboards

    def refresh_dashboard_data(self, dashboard_id: int) -> dict[str, Any]:
        """刷新看板数据：重新聚合指标并记录刷新时间。

        刷新时若最新测量口径未齐，看板返回等待提示且保留上一次完整口径
        的复核留痕，不用待齐数据改写已确认结论。
        """
        dashboard = self.get_entry(dashboard_id)
        if not dashboard:
            return {"ok": False, "reason": "看板不存在"}
        refreshed = self.generate_governance_dashboard(dashboard_id)
        if refreshed["ok"]:
            dashboard["last_refreshed"] = datetime.datetime.utcnow().isoformat()
            dashboard["refresh_count"] = str(int(dashboard.get("refresh_count", "0") or 0) + 1)
            quality_panel = refreshed["dashboard"]["quality"]
            dashboard["last_refresh_caliber"] = quality_panel["caliber_status"]
            if not quality_panel["review"]["actionable"]:
                refreshed["dashboard"]["quality"]["retained_review"] = dashboard.get(
                    "last_actionable_review"
                )
        return refreshed

    def aggregate_governance_summary(self) -> dict[str, Any]:
        """聚合全局治理摘要：质量部分区分完整快照与待齐快照数量。"""
        all_sources = store.rows("source_system")
        all_golden = store.rows("golden_record")
        all_clusters = store.rows("dup_cluster")
        all_quality = store.rows("mdm_quality")
        completed_q = [q for q in all_quality if q.get("口径状态") == CALIBER_COMPLETE]
        pending_q = [q for q in all_quality if q.get("口径状态") == CALIBER_PENDING]
        latest_at = completed_q[-1].get("统计时间") if completed_q else "N/A"
        return {
            "source_systems": {"total": len(all_sources), "active": sum(1 for s in all_sources if s.get("status") == "已接入")},
            "golden_records": {"total": len(all_golden), "active": sum(1 for g in all_golden if g.get("status") == "已生效")},
            "duplicate_clusters": {"total": len(all_clusters), "pending": sum(1 for c in all_clusters if c.get("status") == "待确认")},
            "quality_stats": {
                "total": len(completed_q),
                "pending_caliber": len(pending_q),
                "latest_at": latest_at,
            },
        }

    def export_dashboard_report(self, dashboard_id: int) -> dict[str, Any]:
        """导出治理看板报告：包含口径状态与复核依据，供外部系统消费。"""
        result = self.generate_governance_dashboard(dashboard_id)
        if not result["ok"]:
            return result
        dashboard = self.get_entry(dashboard_id)
        result["dashboard"]["report_id"] = f"RPT-{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        result["dashboard"]["entity"] = dashboard.get("监控实体") if dashboard else "N/A"
        return result
