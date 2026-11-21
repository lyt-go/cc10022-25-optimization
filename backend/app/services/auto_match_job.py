"""自动匹配任务领域服务：匹配任务调度、并行匹配执行与簇结果输出。

稳定性改造：同一匹配运行的两个数据源拆成两个可独立到达的阶段——
阶段一 register_match_run：匹配任务统计先落库，运行口径登记并标记
    「匹配任务统计」可用，此时匹配精度具备计算条件；
阶段二 publish_cluster_data：疑似重复簇数据迟到后写入 dup_cluster，
    标记「疑似重复簇数据」可用，此后重复率才具备计算条件。
schedule_match_job 保留为同步封装，一次完成两个阶段，兼容既有调用。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.quality_snapshot import (
    SOURCE_DUP_CLUSTERS,
    SOURCE_MATCH_STATS,
    QualitySnapshotCaliberService,
    new_run_id,
)
from app.store import store


class AutoMatchJobService(BaseModuleService):
    MODULE = "auto_match_job"
    ENTITY = "匹配任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "实体模型", "匹配规则", "数据范围", "并行度", "匹配策略", "输出簇数", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "实体模型", "匹配规则"]
    STATUS_ORDER = ["草稿", "等待中", "运行中", "已完成", "已失败"]
    ACTION_RULES = {"提交运行": "草稿", "停止匹配": "运行中", "确认完成": "已完成"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        self.caliber = QualitySnapshotCaliberService()

    # ------------------------------------------------------------------
    # 两阶段运行
    # ------------------------------------------------------------------
    def register_match_run(self, rule_id: int, entity_model: str,
                             records: list[dict[str, Any]], *,
                             run_id: str | None = None,
                             data_scope: str = "全量") -> dict[str, Any]:
        """阶段一：登记匹配运行，任务统计先落库（簇数据允许迟到）。

        此时任务处于运行中、输出簇数为0，并向口径注册表登记运行范围、
        标记匹配任务统计数据源可用。
        """
        from app.services.match_rule import MatchRuleService
        rule = MatchRuleService().get_entry(rule_id)
        if not rule:
            return {"ok": False, "reason": "匹配规则不存在"}

        run_id = run_id or new_run_id()
        if self.caliber.get_run(run_id) is not None:
            return {"ok": False, "reason": f"运行编号已存在：{run_id}"}

        rows = store.rows(self.MODULE)
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "任务编号": f"TASK-{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
            "实体模型": entity_model,
            "匹配规则": str(rule_id),
            "数据范围": data_scope,
            "并行度": "4",
            "匹配策略": rule.get("匹配策略", "全量匹配"),
            "输出簇数": "0",
            "任务状态": "运行中",
            "status": "运行中",
            "pending": True,
            "abnormal": False,
            "run_id": run_id,
            "cluster_published": False,
            "total_records": len(records),
            "clusters": [],
        }
        rows.append(entry)

        self.caliber.register_run(
            entity_model,
            run_id=run_id,
            data_scope=data_scope,
            record_count=len(records),
            job_id=entry["任务编号"],
        )
        self.caliber.mark_source_available(
            run_id,
            SOURCE_MATCH_STATS,
            detail={"job_row_ids": [entry["id"]], "output_clusters": 0},
        )
        return {"ok": True, "run_id": run_id, "job": entry}

    def publish_cluster_data(self, run_id: str,
                               clusters: list[Any]) -> dict[str, Any]:
        """阶段二：疑似重复簇数据到达，落库并更新运行口径。

        clusters 支持两种形态：
        - 记录索引簇列表（如 [[0, 1], [3, 4]]），由本服务转换为簇行；
        - 已成型的簇行字典列表，直接补齐运行归属后落库。
        重复发布被拒绝，保证同一运行的簇数据源只有一个确定版本。
        """
        job = self._find_job_by_run(run_id)
        if job is None:
            return {"ok": False, "reason": f"匹配运行不存在：{run_id}"}
        if job.get("cluster_published"):
            return {"ok": False, "reason": f"运行{run_id}的簇数据已发布，不能重复写入"}

        cluster_rows = self._coerce_cluster_rows(
            clusters, job.get("实体模型", ""), job.get("匹配规则", ""), run_id
        )
        dup_rows = store.rows("dup_cluster")
        dup_rows.extend(cluster_rows)

        job["输出簇数"] = str(len(cluster_rows))
        job["任务状态"] = "已完成"
        job["status"] = "已完成"
        job["pending"] = False
        job["cluster_published"] = True
        job["clusters"] = clusters
        job["completed_at"] = datetime.datetime.utcnow().isoformat()

        self.caliber.mark_source_available(
            run_id,
            SOURCE_DUP_CLUSTERS,
            detail={
                "cluster_row_ids": [row["id"] for row in cluster_rows],
                "cluster_count": len(cluster_rows),
            },
        )
        return {"ok": True, "run_id": run_id, "job": job, "clusters": cluster_rows}

    def _coerce_cluster_rows(self, clusters: list[Any], entity_model: str,
                               rule_id: str, run_id: str) -> list[dict[str, Any]]:
        """把索引簇或外部簇行统一为带运行归属与记录引用的 dup_cluster 行。"""
        dup_rows = store.rows("dup_cluster")
        next_id = max((int(r.get("id", 0)) for r in dup_rows), default=0) + 1
        now = datetime.datetime.utcnow().isoformat()
        normalized: list[dict[str, Any]] = []
        for index, cluster in enumerate(clusters, start=1):
            if isinstance(cluster, dict):
                row = dict(cluster)
                row.setdefault("簇编号", f"DUP-{run_id}-{index:03d}")
                size = int(row.get("簇大小", 0) or 0)
                refs = row.get("record_refs") or [
                    f"{run_id}#input/{i}" for i in range(size)
                ]
            else:
                indexes = list(cluster)
                size = len(indexes)
                row = {
                    "簇编号": f"DUP-{run_id}-{index:03d}",
                    "相似度均值": "0.0",
                    "处置状态": "待确认",
                }
                refs = [f"{run_id}#input/{i}" for i in indexes]
            row["id"] = next_id
            next_id += 1
            row["实体模型"] = entity_model
            row["簇大小"] = str(row.get("簇大小", size) if isinstance(cluster, dict) else size)
            row["匹配规则"] = rule_id
            row["创建时间"] = row.get("创建时间") or now
            row["处置批次"] = run_id
            row["status"] = row.get("status", "待确认")
            row["pending"] = row.get("pending", True)
            row["abnormal"] = row.get("abnormal", False)
            row["run_id"] = run_id
            row["record_refs"] = refs
            normalized.append(row)
        return normalized

    def _find_job_by_run(self, run_id: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("run_id") == run_id:
                return row
        return None

    def schedule_match_job(self, rule_id: int, entity_model: str,
                            records: list[dict[str, Any]]) -> dict[str, Any]:
        """调度匹配任务：同步封装两阶段流程，兼容既有调用方。

        读取匹配规则配置，调度自动匹配任务计算记录对的模糊相似度，
        校验阈值并生成疑似重复簇输出，任务统计与簇数据在同一调用内
        按顺序发布，保证两个数据源口径一致。
        """
        from app.services.match_rule import MatchRuleService
        rule_svc = MatchRuleService()
        rule = rule_svc.get_entry(rule_id)
        if not rule:
            return {"ok": False, "reason": "匹配规则不存在"}

        registered = self.register_match_run(rule_id, entity_model, records)
        if not registered["ok"]:
            return registered
        run_id = registered["run_id"]
        clusters = rule_svc.generate_duplicate_clusters(rule_id, records)
        published = self.publish_cluster_data(run_id, clusters)
        if not published["ok"]:
            return published
        return {
            "ok": True,
            "run_id": run_id,
            "job": published["job"],
            "clusters": clusters,
        }

    # ------------------------------------------------------------------
    # 查询与聚合
    # ------------------------------------------------------------------
    def query_job_status(self, job_id: int) -> dict[str, Any] | None:
        """查询匹配任务的执行状态与输出簇数统计。"""
        job = self.get_entry(job_id)
        if not job:
            return None
        return {
            "task_id": job.get("任务编号"),
            "status": job.get("任务状态"),
            "entity_model": job.get("实体模型"),
            "clusters_output": int(job.get("输出簇数", "0") or 0),
            "parallelism": int(job.get("并行度", "1") or 1),
            "strategy": job.get("匹配策略"),
            "run_id": job.get("run_id"),
            "cluster_published": bool(job.get("cluster_published")),
        }

    def query_run_data_status(self, run_id: str) -> dict[str, Any]:
        """查询同一运行两个数据源的到达状态，供值班判断指标可用时间。"""
        job = self._find_job_by_run(run_id)
        if job is None:
            return {"ok": False, "reason": f"匹配运行不存在：{run_id}"}
        return {
            "ok": True,
            "run_id": run_id,
            "task_status": job.get("任务状态"),
            "match_stats_available": True,
            "cluster_data_available": bool(job.get("cluster_published")),
            "source_status": self.caliber.source_status(run_id),
            "metric_availability": self.caliber.metric_availability(run_id),
        }

    def aggregate_job_statistics(self, entity_model: str | None = None) -> dict[str, Any]:
        """聚合匹配任务统计：汇总已完成/失败任务数、输出簇总数和平均并行度。"""
        rows = store.rows(self.MODULE)
        if entity_model:
            rows = [r for r in rows if r.get("实体模型") == entity_model]
        completed = sum(1 for r in rows if r.get("任务状态") == "已完成")
        failed = sum(1 for r in rows if r.get("任务状态") == "已失败")
        total_clusters = sum(int(r.get("输出簇数", "0") or 0) for r in rows)
        return {
            "total_jobs": len(rows),
            "completed": completed,
            "failed": failed,
            "total_clusters_output": total_clusters,
            "success_rate": round(completed / max(len(rows), 1) * 100, 2),
        }

    def validate_job_parameters(self, job_id: int) -> dict[str, Any]:
        """校验匹配任务的参数配置是否合理：并行度、匹配策略和数据范围。"""
        job = self.get_entry(job_id)
        if not job:
            return {"ok": False, "reason": "任务不存在"}
        parallelism = int(job.get("并行度", "1") or 1)
        checks = {
            "parallelism_valid": 1 <= parallelism <= 16,
            "has_match_rule": bool(job.get("匹配规则")),
            "has_entity_model": bool(job.get("实体模型")),
            "has_strategy": bool(job.get("匹配策略")),
        }
        checks["all_valid"] = all(checks.values())
        return {"ok": checks["all_valid"], "checks": checks}

    def retry_failed_jobs(self, entity_model: str) -> int:
        """重试已失败的匹配任务：重新调度失败任务执行模糊匹配计算。"""
        rows = store.rows(self.MODULE)
        retried = 0
        for job in rows:
            if job.get("任务状态") == "已失败" and job.get("实体模型") == entity_model:
                job["任务状态"] = "等待中"
                job["status"] = "等待中"
                job["retry_count"] = job.get("retry_count", 0) + 1
                retried += 1
        return retried
