"""质量统计领域服务：基于确定运行口径生成模型质量观测快照。

稳定性改造要点：
1. 重复率与匹配精度不再各自读取簇表/任务表全表，而是按运行口径
   （QualitySnapshotCaliberService 登记的 run_id）只引用本运行注册过的
   数据源记录，刷新中途不会混入其他范围的数据。
2. 数据源未到达的指标标记为「待数据源」而不是凑数输出；口径未齐的
   快照保持「计算中」，不会覆盖历史已确认结果。
3. 每个指标保存分子、分母、实际数据范围与来源记录引用，旧确认结果
   可按统计编号逐条回查。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.quality_snapshot import (
    CALIBER_COMPLETE,
    CALIBER_PENDING,
    EXPECTED_SOURCES,
    METRIC_DEPENDENCIES,
    SOURCE_DUP_CLUSTERS,
    SOURCE_LABELS,
    SOURCE_MATCH_STATS,
    QualitySnapshotCaliberService,
    input_fingerprint,
    new_run_id,
)
from app.store import store

PENDING_DISPLAY = "待数据源"


class MdmQualityService(BaseModuleService):
    MODULE = "mdm_quality"
    ENTITY = "质量统计"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["统计编号", "实体模型", "完备率", "重复率", "一致率", "匹配精度",
                   "统计时间", "统计区间", "口径状态"]
    REQUIRED_FIELDS = ["统计编号", "实体模型", "完备率"]
    STATUS_ORDER = ["草稿", "计算中", "已完成", "已归档"]
    ACTION_RULES = {"发起统计": "计算中", "确认完成": "已完成", "归档统计": "已归档"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        self.caliber = QualitySnapshotCaliberService()

    # ------------------------------------------------------------------
    # 快照计算
    # ------------------------------------------------------------------
    def compute_quality_metrics(self, entity_model: str,
                                  records: list[dict[str, Any]],
                                  required_fields: list[str],
                                  run_id: str | None = None,
                                  data_scope: str = "全量") -> dict[str, Any]:
        """按确定口径计算模型质量观测快照。

        参加统计的模型、输入记录与执行范围在运行注册时冻结：
        - 完备率/一致率只使用本次传入的输入记录；
        - 重复率只引用运行注册过的疑似重复簇记录；
        - 匹配精度只引用运行注册过的匹配任务统计。
        缺少数据源的指标输出 None 并标记不可用，不做跨范围凑数。
        """
        fingerprint = input_fingerprint(records)
        if run_id is None:
            # 未指定运行时新建口径并登记输入；外部数据源需后续注册才会齐套
            run_id = new_run_id()
            self.caliber.register_run(
                entity_model,
                run_id=run_id,
                data_scope=data_scope,
                record_count=len(records),
                required_fields=required_fields,
                fingerprint=fingerprint,
            )
        run = self.caliber.get_run(run_id)
        if run is None:
            raise KeyError(f"质量观测运行不存在：{run_id}")

        computed_at = datetime.datetime.utcnow().isoformat()
        metric_scopes: dict[str, dict[str, Any]] = {}

        # 完备率：分母为本次输入记录总数，分子为必填字段全部非空的记录
        complete = sum(1 for r in records if all(
            str(r.get(f, "")).strip() for f in required_fields
        ))
        completeness = round(complete / max(len(records), 1) * 100, 2)
        metric_scopes["completeness"] = self._scope_entry(
            available=True,
            numerator=complete,
            denominator=len(records),
            value=completeness,
            scope_desc=f"运行{run_id}输入记录",
            source_refs=[f"{run_id}#input"],
        )

        # 一致率：同样只在本次输入范围内计算
        consistency = self._compute_consistency(records, required_fields)
        metric_scopes["consistency"] = self._scope_entry(
            available=True,
            numerator=metric_scopes["completeness"]["numerator"],
            denominator=len(records),
            value=consistency,
            scope_desc=f"运行{run_id}输入记录",
            source_refs=[f"{run_id}#input"],
        )

        # 重复率：仅引用本运行注册的疑似重复簇数据源，未到达则不可用
        dup_scope = self._compute_duplication_scope(run, len(records))
        metric_scopes["duplication"] = dup_scope

        # 匹配精度：仅引用本运行注册的匹配任务统计，未到达则不可用
        precision_scope = self._compute_precision_scope(run, len(records))
        metric_scopes["match_precision"] = precision_scope

        caliber_ready = self.caliber.is_run_ready(run_id)
        quality_stats = {
            "run_id": run_id,
            "entity_model": entity_model,
            "data_scope": run.get("data_scope", data_scope),
            "total_records": len(records),
            "input_fingerprint": fingerprint,
            "completeness_pct": completeness,
            "consistency_pct": consistency,
            "duplication_pct": dup_scope["value"],
            "match_precision_pct": precision_scope["value"],
            "metric_scopes": metric_scopes,
            "metric_availability": self.caliber.metric_availability(run_id),
            "source_status": self.caliber.source_status(run_id),
            "missing_sources": [
                SOURCE_LABELS[s] for s in self.caliber.missing_sources(run_id)
            ],
            "caliber_status": CALIBER_COMPLETE if caliber_ready else CALIBER_PENDING,
            "computed_at": computed_at,
        }
        self._store_quality_stats(entity_model, quality_stats)
        return quality_stats

    def _compute_duplication_scope(self, run: dict[str, Any],
                                     total_records: int) -> dict[str, Any]:
        """重复率：分子=本运行簇记录覆盖的记录数，分母=本运行输入记录数。"""
        run_id = run["run_id"]
        if not self.caliber.is_metric_available(run_id, "duplication"):
            return self._scope_entry(
                available=False, numerator=None, denominator=total_records,
                value=None, scope_desc=f"运行{run_id}疑似重复簇数据未到达",
                source_refs=[],
            )
        detail = run["sources"][SOURCE_DUP_CLUSTERS]["detail"] or {}
        cluster_ids = detail.get("cluster_row_ids", [])
        cluster_rows = [
            row for row in store.rows("dup_cluster")
            if row.get("id") in cluster_ids and row.get("实体模型") == run["entity_model"]
        ]
        dup_count = 0
        for row in cluster_rows:
            try:
                dup_count += int(row.get("簇大小", 0))
            except (TypeError, ValueError):
                continue
        value = round(dup_count / max(total_records, 1) * 100, 2)
        return self._scope_entry(
            available=True,
            numerator=dup_count,
            denominator=total_records,
            value=value,
            scope_desc=f"运行{run_id}注册的{len(cluster_rows)}个疑似重复簇",
            source_refs=[str(row.get("簇编号", row.get("id"))) for row in cluster_rows],
        )

    def _compute_precision_scope(self, run: dict[str, Any],
                                   total_records: int) -> dict[str, Any]:
        """匹配精度：沿用平台口径 100 - 输出簇数/输入记录数*100，下限0。"""
        run_id = run["run_id"]
        if not self.caliber.is_metric_available(run_id, "match_precision"):
            return self._scope_entry(
                available=False, numerator=None, denominator=total_records,
                value=None, scope_desc=f"运行{run_id}匹配任务统计未到达",
                source_refs=[],
            )
        detail = run["sources"][SOURCE_MATCH_STATS]["detail"] or {}
        job_ids = detail.get("job_row_ids", [])
        job_rows = [
            row for row in store.rows("auto_match_job")
            if row.get("id") in job_ids and row.get("实体模型") == run["entity_model"]
        ]
        output_clusters = 0
        for row in job_rows:
            try:
                output_clusters += int(row.get("输出簇数", 0))
            except (TypeError, ValueError):
                continue
        value = round(max(100 - output_clusters / max(total_records, 1) * 100, 0), 2)
        return self._scope_entry(
            available=True,
            numerator=output_clusters,
            denominator=total_records,
            value=value,
            scope_desc=f"运行{run_id}注册的{len(job_rows)}个匹配任务统计",
            source_refs=[str(row.get("任务编号", row.get("id"))) for row in job_rows],
            formula="100 - 输出簇数 / 输入记录数 * 100（下限0）",
        )

    @staticmethod
    def _scope_entry(*, available: bool, numerator: int | None,
                       denominator: int, value: float | None,
                       scope_desc: str, source_refs: list[str],
                       formula: str = "") -> dict[str, Any]:
        return {
            "available": available,
            "numerator": numerator,
            "denominator": denominator,
            "value": value,
            "scope": scope_desc,
            "source_refs": source_refs,
            "formula": formula,
        }

    def _compute_consistency(self, records: list[dict[str, Any]],
                               fields: list[str]) -> float:
        """计算字段一致性：检查同类记录同字段值的分布集中度。"""
        if not records or len(records) < 2:
            return 100.0
        total_checks = 0
        consistent = 0
        for field in fields:
            values = [str(r.get(field, "")) for r in records if str(r.get(field, "")).strip()]
            if len(values) < 2:
                continue
            most_common = max(set(values), key=values.count)
            total_checks += len(values)
            consistent += values.count(most_common)
        return round(consistent / max(total_checks, 1) * 100, 2)

    # ------------------------------------------------------------------
    # 快照存储与确认
    # ------------------------------------------------------------------
    def _store_quality_stats(self, entity_model: str,
                               stats: dict[str, Any]) -> dict[str, Any]:
        """写入或更新质量观测快照；同运行的待齐快照原地更新不新增。"""
        rows = store.rows(self.MODULE)
        run_id = stats["run_id"]
        existing = next(
            (r for r in rows if r.get("run_id") == run_id and not r.get("confirmed")),
            None,
        )
        caliber_ready = stats["caliber_status"] == CALIBER_COMPLETE
        entry = existing or {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "统计编号": f"QLTY-{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
        }
        entry.update({
            "实体模型": entity_model,
            "完备率": self._display(stats["completeness_pct"]),
            "重复率": self._display(stats["duplication_pct"]),
            "一致率": self._display(stats["consistency_pct"]),
            "匹配精度": self._display(stats["match_precision_pct"]),
            "统计时间": stats["computed_at"],
            "统计区间": stats["data_scope"],
            "口径状态": stats["caliber_status"],
            # 口径未齐保持计算中，避免被当作可确认的完整结果
            "status": "已完成" if caliber_ready else "计算中",
            "pending": not caliber_ready,
            "abnormal": False,
            # —— 追溯与口径字段（不在列表字段中展示，供回查使用）——
            "run_id": run_id,
            "confirmed": bool(entry.get("confirmed", False)),
            "total_records": stats["total_records"],
            "input_fingerprint": stats["input_fingerprint"],
            "metric_scopes": stats["metric_scopes"],
            "metric_availability": stats["metric_availability"],
            "source_status": stats["source_status"],
            "missing_sources": stats["missing_sources"],
        })
        if existing is None:
            rows.append(entry)
        return entry

    @staticmethod
    def _display(value: float | None) -> str:
        return PENDING_DISPLAY if value is None else f"{value:.2f}%"

    def promote_pending_snapshot(self, run_id: str,
                                   records: list[dict[str, Any]],
                                   required_fields: list[str]) -> dict[str, Any]:
        """数据源到齐后重算待齐快照。

        只更新该运行自身、且未人工确认的快照；历史已确认结果一律不动，
        缺少输入的测量因此不可能覆盖过去确认过的结论。
        """
        run = self.caliber.get_run(run_id)
        if run is None:
            return {"ok": False, "reason": f"质量观测运行不存在：{run_id}"}
        if not self.caliber.is_run_ready(run_id):
            return {
                "ok": False,
                "reason": "数据源尚未到齐",
                "missing_sources": [
                    SOURCE_LABELS[s] for s in self.caliber.missing_sources(run_id)
                ],
            }
        target = self._find_snapshot(run_id)
        if target is None:
            return {"ok": False, "reason": "该运行没有待提升的质量快照"}
        if target.get("confirmed"):
            return {"ok": False, "reason": "已确认快照不允许重算覆盖"}
        stats = self.compute_quality_metrics(
            run["entity_model"], records, required_fields,
            run_id=run_id, data_scope=run.get("data_scope", "全量"),
        )
        return {"ok": True, "stats": stats}

    def confirm_quality_snapshot(self, stat_id: int) -> dict[str, Any]:
        """确认一份完整口径的质量快照，作为后续看板复核的权威结果。"""
        entry = store.find(self.MODULE, stat_id)
        if entry is None:
            return {"ok": False, "reason": f"质量统计 {stat_id} 不存在"}
        if entry.get("口径状态") != CALIBER_COMPLETE:
            return {
                "ok": False,
                "reason": "口径不完整，缺少数据源："
                          + "、".join(entry.get("missing_sources", []) or ["未知"]),
            }
        entry["confirmed"] = True
        entry["confirmed_at"] = datetime.datetime.utcnow().isoformat()
        entry["status"] = "已完成"
        entry["pending"] = False
        return {"ok": True, "entry": entry}

    def _find_snapshot(self, run_id: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("run_id") == run_id:
                return row
        return None

    def trace_snapshot(self, stat_id: int) -> dict[str, Any] | None:
        """按统计编号回查快照：每个指标的分子、分母、范围与来源记录引用。"""
        entry = store.find(self.MODULE, stat_id)
        if entry is None:
            return None
        return {
            "统计编号": entry.get("统计编号"),
            "实体模型": entry.get("实体模型"),
            "run_id": entry.get("run_id"),
            "口径状态": entry.get("口径状态", CALIBER_PENDING),
            "统计区间": entry.get("统计区间"),
            "input_fingerprint": entry.get("input_fingerprint"),
            "total_records": entry.get("total_records"),
            "confirmed": bool(entry.get("confirmed")),
            "source_status": entry.get("source_status", {}),
            "metric_scopes": entry.get("metric_scopes", {}),
        }

    # ------------------------------------------------------------------
    # 查询与聚合：只消费口径完整的快照
    # ------------------------------------------------------------------
    def list_pending_snapshots(self, entity_model: str | None = None) -> list[dict[str, Any]]:
        """列出口径未齐、正在等待数据源的快照。"""
        rows = store.rows(self.MODULE)
        return [
            row for row in rows
            if row.get("口径状态") == CALIBER_PENDING
            and (entity_model is None or row.get("实体模型") == entity_model)
        ]

    def query_quality_trend(self, entity_model: str, limit: int = 10) -> list[dict[str, Any]]:
        """查询质量趋势：仅返回口径完整且已完成确认的历史快照。"""
        rows = store.rows(self.MODULE)
        stats = [
            row for row in rows
            if row.get("实体模型") == entity_model
            and row.get("status") == "已完成"
            and row.get("口径状态") == CALIBER_COMPLETE
        ]
        stats.sort(key=lambda r: r.get("统计时间", ""), reverse=True)
        return stats[:limit]

    def latest_confirmed_snapshot(self, entity_model: str) -> dict[str, Any] | None:
        """获取实体模型最近一份已确认的完整口径快照。"""
        candidates = [
            row for row in store.rows(self.MODULE)
            if row.get("实体模型") == entity_model
            and row.get("confirmed")
            and row.get("口径状态") == CALIBER_COMPLETE
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda r: r.get("confirmed_at") or r.get("统计时间", ""))

    def aggregate_global_quality(self) -> dict[str, Any]:
        """聚合全局质量指标：只汇总口径完整的快照，待齐数据不参与。"""
        rows = store.rows(self.MODULE)
        completed = [
            r for r in rows
            if r.get("status") == "已完成" and r.get("口径状态") == CALIBER_COMPLETE
        ]
        pending = [r for r in rows if r.get("口径状态") == CALIBER_PENDING]
        if not completed:
            return {
                "total_quality_stats": 0,
                "pending_snapshots": len(pending),
                "models": [],
            }
        models: dict[str, dict[str, Any]] = {}
        for stat in completed:
            model = stat.get("实体模型", "")
            if model not in models:
                models[model] = {
                    "entity_model": model,
                    "latest": None,
                    "stat_count": 0,
                }
            models[model]["stat_count"] += 1
            if not models[model]["latest"] or stat.get("统计时间", "") > models[model]["latest"].get("统计时间", ""):
                models[model]["latest"] = stat
        return {
            "total_quality_stats": len(completed),
            "pending_snapshots": len(pending),
            "models": list(models.values()),
        }

    def archive_old_stats(self, before_date: str) -> int:
        """归档指定日期之前的旧质量统计记录。"""
        rows = store.rows(self.MODULE)
        archived = 0
        for row in rows:
            if row.get("status") == "已完成" and row.get("统计时间", "") < before_date:
                row["status"] = "已归档"
                archived += 1
        return archived
