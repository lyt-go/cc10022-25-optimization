"""质量观测口径服务：登记同一运行的统计口径与两个数据源的就绪状态。

背景：一次匹配运行会产生两个更新节奏不同的数据源——匹配任务统计
（auto_match_job）先落库，疑似重复簇（dup_cluster）数据可能迟到。
质量统计的重复率依赖簇数据、匹配精度依赖任务统计，若在两个时刻分别
读取全表，治理看板刷新时就会把不同范围的数据混进同一份观测快照。

本服务不直接计算指标，只负责：
1. 登记一次运行确定的参加统计模型、输入范围与执行范围（统计口径）；
2. 记录每个数据源的到达时间与记录引用，区分两个指标的可用时间；
3. 给出口径是否完整的判断，供质量统计与治理看板共享同一结论。

注册表存放在 store 的下划线前缀内部表中，不参与业务模块列表与路由。
"""
from __future__ import annotations

import datetime
import uuid
from typing import Any

from app.store import store

# 两个异步到达的数据源
SOURCE_MATCH_STATS = "match_task_stats"      # 匹配任务统计（先到）
SOURCE_DUP_CLUSTERS = "dup_cluster_data"     # 疑似重复簇数据（可能迟到）
EXPECTED_SOURCES = (SOURCE_MATCH_STATS, SOURCE_DUP_CLUSTERS)

SOURCE_LABELS = {
    SOURCE_MATCH_STATS: "匹配任务统计",
    SOURCE_DUP_CLUSTERS: "疑似重复簇数据",
}

# 指标对数据源的依赖关系：完备率/一致率只依赖本次输入记录本身
METRIC_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "completeness": (),
    "consistency": (),
    "duplication": (SOURCE_DUP_CLUSTERS,),
    "match_precision": (SOURCE_MATCH_STATS,),
}

REGISTRY_TABLE = "_quality_run_registry"

# 口径状态
CALIBER_PENDING = "口径待齐"
CALIBER_COMPLETE = "口径完整"


def _now() -> str:
    return datetime.datetime.utcnow().isoformat()


def new_run_id() -> str:
    """生成运行编号：时间戳加随机后缀，避免同秒多次统计互相碰撞。"""
    return f"RUN-{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"


def input_fingerprint(records: list[dict[str, Any]]) -> str:
    """对参加统计的输入记录计算稳定指纹，用于确认两次统计口径是否一致。"""
    import hashlib
    import json

    payload = json.dumps(records, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


class QualitySnapshotCaliberService:
    """运行口径与数据源就绪注册服务。"""

    # ------------------------------------------------------------------
    # 注册表读写
    # ------------------------------------------------------------------
    def _registry(self) -> list[dict[str, Any]]:
        return store.rows(REGISTRY_TABLE)

    def register_run(
        self,
        entity_model: str,
        *,
        run_id: str | None = None,
        data_scope: str = "全量",
        record_count: int = 0,
        required_fields: list[str] | None = None,
        fingerprint: str = "",
        job_id: str | None = None,
    ) -> dict[str, Any]:
        """登记一次质量观测运行的确定口径。

        口径一旦登记即冻结：参加统计的实体模型、输入记录数、必填字段、
        执行范围和输入指纹都保存在运行记录上，后续指标计算只能引用该
        口径，不允许被迟到数据改写范围。
        """
        run_id = run_id or new_run_id()
        registry = self._registry()
        existing = self.get_run(run_id)
        if existing is not None:
            return existing
        run = {
            "run_id": run_id,
            "entity_model": entity_model,
            "data_scope": data_scope,
            "job_id": job_id,
            "input_scope": {
                "record_count": record_count,
                "required_fields": list(required_fields or []),
                "input_fingerprint": fingerprint,
            },
            "expected_sources": list(EXPECTED_SOURCES),
            "sources": {
                source: {"available": False, "available_at": None, "detail": None}
                for source in EXPECTED_SOURCES
            },
            "created_at": _now(),
            "updated_at": _now(),
        }
        registry.append(run)
        return run

    def mark_source_available(
        self, run_id: str, source: str, detail: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """标记某运行的一个数据源已到达，并记录到达时间与记录引用。

        重复标记不会覆盖首次到达时间，保证可用时间可追溯。
        """
        if source not in EXPECTED_SOURCES:
            raise KeyError(f"未知质量统计数据源：{source}")
        run = self.get_run(run_id)
        if run is None:
            raise KeyError(f"质量观测运行不存在：{run_id}")
        state = run["sources"][source]
        if not state["available"]:
            state["available"] = True
            state["available_at"] = _now()
        state["detail"] = detail or state["detail"] or {}
        run["updated_at"] = _now()
        return run

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        for run in self._registry():
            if run.get("run_id") == run_id:
                return run
        return None

    def list_runs(self, entity_model: str | None = None) -> list[dict[str, Any]]:
        rows = self._registry()
        if entity_model:
            rows = [r for r in rows if r.get("entity_model") == entity_model]
        return sorted(rows, key=lambda r: r.get("created_at", ""))

    # ------------------------------------------------------------------
    # 就绪判断
    # ------------------------------------------------------------------
    def available_sources(self, run_id: str) -> list[str]:
        run = self.get_run(run_id)
        if run is None:
            return []
        return [s for s, state in run["sources"].items() if state["available"]]

    def missing_sources(self, run_id: str) -> list[str]:
        run = self.get_run(run_id)
        if run is None:
            return list(EXPECTED_SOURCES)
        return [s for s, state in run["sources"].items() if not state["available"]]

    def is_run_ready(self, run_id: str) -> bool:
        """两个数据源全部到达才视为口径完整。"""
        return not self.missing_sources(run_id)

    def caliber_status(self, run_id: str) -> str:
        return CALIBER_COMPLETE if self.is_run_ready(run_id) else CALIBER_PENDING

    def is_metric_available(self, run_id: str, metric: str) -> bool:
        """单个指标是否具备计算条件：依赖的数据源均已到达。"""
        run = self.get_run(run_id)
        if run is None:
            return False
        return all(
            run["sources"][source]["available"]
            for source in METRIC_DEPENDENCIES.get(metric, ())
        )

    def source_status(self, run_id: str) -> dict[str, dict[str, Any]]:
        """输出供看板使用的数据源状态：名称、是否可用、可用时间。"""
        run = self.get_run(run_id)
        if run is None:
            return {}
        return {
            source: {
                "label": SOURCE_LABELS[source],
                "available": state["available"],
                "available_at": state["available_at"],
                "detail": state["detail"],
            }
            for source, state in run["sources"].items()
        }

    def metric_availability(self, run_id: str) -> list[dict[str, Any]]:
        """逐个指标给出可用状态与对应数据源的到达时间。

        值班人员据此区分两个指标的可用时间：任务统计先到时只有匹配精度
        可算，疑似重复簇数据到达后重复率才可用。
        """
        run = self.get_run(run_id)
        if run is None:
            return []
        result: list[dict[str, Any]] = []
        for metric, dependencies in METRIC_DEPENDENCIES.items():
            if not dependencies:
                available = True
                available_at = run.get("created_at")
            else:
                states = [run["sources"][s] for s in dependencies]
                available = all(state["available"] for state in states)
                # 多数据源时以最后到达者为准
                available_at = max(
                    (state["available_at"] or "" for state in states), default=""
                ) or None
            result.append({
                "metric": metric,
                "available": available,
                "available_at": available_at,
                "depends_on": [SOURCE_LABELS[s] for s in dependencies],
            })
        return result
