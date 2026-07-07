"""Silverジョブが依存するBronzeジョブ集合と、バッチのSLAを定義するマニフェスト。

B案（DAB置換でIDを注入）に合わせ、ここでは job_id を持たない。
job_id は実行時に --silver-job-ids（DABが ${resources.jobs.<key>.id} を埋めたJSON）
から解決する。依存関係とSLAという「変わりにくい事実」だけをここに置く。
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SilverDependency:
    requires: tuple[str, ...]      # 成功が必要なBronzeテーブル名（この集合のANDで判定）
    sla_hour_jst: int = 9          # 業務日の「翌朝」この時刻(JST)までに揃わなければSLA違反
    sla_minute_jst: int = 0


# silver_job（論理名） -> 依存定義。論理名は --silver-job-ids のキーと一致させる。
SILVER_DEPENDENCIES: dict[str, SilverDependency] = {
    "silver_sales": SilverDependency(
        requires=("bronze_orders", "bronze_order_lines", "bronze_customer"),
        sla_hour_jst=9,
    ),
    "silver_inventory": SilverDependency(
        requires=("bronze_stock", "bronze_warehouse"),
        sla_hour_jst=8,
    ),
}

# 監視/検証用に、全Bronzeジョブ名の集合を導出
ALL_BRONZE_JOBS = sorted({b for d in SILVER_DEPENDENCIES.values() for b in d.requires})
