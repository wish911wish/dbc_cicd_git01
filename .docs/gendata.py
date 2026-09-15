"""
SQL*Loader 性能テスト用データ生成スクリプト

設計方針:
  - 表定義（DDL由来の構造）と分布定義を1つの Table スペックに集約する
  - 同じスペックから CSV と 制御ファイル(.ctl) の両方を生成し、書式の齟齬を構造的に防ぐ
  - 全行をメモリに載せず、1行ずつストリーミング出力する
  - シードを固定し、再現可能なデータを生成する
  - 親子関係は「親の件数」だけを参照する（キー配列を持たないので件数に依存しない）

使い方:
  python gendata.py --table ORDERS --rows 10000000 --files 8 --outdir ./out
  python gendata.py --table ORDERS --rows 10000000 --validate-only
  python gendata.py --all --rows-scale 0.001 --outdir ./sample   # 小規模確認用
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

# --------------------------------------------------------------------------
# 書式定義（CSV側と制御ファイル側をここで対にして持つ）
# --------------------------------------------------------------------------

DATE_PY = "%Y-%m-%d %H:%M:%S"
DATE_ORA = "YYYY-MM-DD HH24:MI:SS"

ENCODING = "utf-8"          # CSVの文字コード
ORA_CHARSET = "AL32UTF8"    # 制御ファイルの CHARACTERSET
DELIM = ","
QUOTE = '"'
NEWLINE = "\n"              # LF。Windowsで生成する場合も明示的にLFで揃える


# --------------------------------------------------------------------------
# スペック定義
# --------------------------------------------------------------------------

@dataclass
class Ctx:
    """列値生成時に渡されるコンテキスト"""
    rng: random.Random
    row: int  # 全体通算の行番号（0起点）。PKの連番に使う


ValueGen = Callable[[Ctx], str]


@dataclass
class Column:
    name: str
    gen: ValueGen
    ctl_type: str = "CHAR"      # 制御ファイルに書く型指定
    nullable: bool = True
    max_bytes: int | None = None  # VARCHAR2(n BYTE) の n。検証に使う


@dataclass
class Table:
    name: str
    columns: list[Column]
    rows: int
    parent_rows: dict[str, int] = field(default_factory=dict)  # 参考情報


# --------------------------------------------------------------------------
# 列値ジェネレータ
#   すべて「CSVに書く文字列」を返す。書式変換をここに閉じ込める。
# --------------------------------------------------------------------------

def seq(start: int = 1) -> ValueGen:
    """連番。PK用。"""
    return lambda c: str(start + c.row)


def const(value: str) -> ValueGen:
    return lambda c: value


def rand_int(lo: int, hi: int) -> ValueGen:
    return lambda c: str(c.rng.randint(lo, hi))


def rand_decimal(lo: float, hi: float, scale: int) -> ValueGen:
    """NUMBER(p,s) 用。floatの丸め誤差を避けるため整数演算で生成する。"""
    factor = 10 ** scale
    lo_i, hi_i = int(lo * factor), int(hi * factor)

    def _gen(c: Ctx) -> str:
        v = c.rng.randint(lo_i, hi_i)
        if scale == 0:
            return str(v)
        sign = "-" if v < 0 else ""
        v = abs(v)
        return f"{sign}{v // factor}.{v % factor:0{scale}d}"

    return _gen


def rand_date(start: datetime, end: datetime, recent_bias: float = 0.0) -> ValueGen:
    """
    start〜end の範囲でランダムな日時。
    recent_bias > 0 で新しい日付側に偏らせる（1.0で一様、2.0でかなり新しい側に寄る）。
    パーティションキーの偏りを再現したい場合に使う。
    """
    span = int((end - start).total_seconds())

    def _gen(c: Ctx) -> str:
        r = c.rng.random()
        if recent_bias > 0:
            r = r ** (1.0 / (1.0 + recent_bias))
        return (start + timedelta(seconds=int(span * r))).strftime(DATE_PY)

    return _gen


def weighted(pairs: list[tuple[str, float]]) -> ValueGen:
    """出現比率つきのコード値。例: [("01", 70), ("02", 25), ("09", 5)]"""
    values = [v for v, _ in pairs]
    weights = [w for _, w in pairs]
    return lambda c: c.rng.choices(values, weights=weights, k=1)[0]


def code_pool(prefix: str, count: int, digits: int = 6, alpha: float = 0.0) -> ValueGen:
    """
    count 種類のコード値から抽選。カーディナリティの制御に使う。
    alpha > 0 で偏りを与える（一部のコードに集中するパレート的分布）。
    """
    def _gen(c: Ctx) -> str:
        if alpha > 0:
            n = min(count, int(count * (c.rng.random() ** (1.0 + alpha))) + 1)
        else:
            n = c.rng.randint(1, count)
        return f"{prefix}{n:0{digits}d}"

    return _gen


def fk_seq(parent_rows: int, match_rate: float = 1.0, orphan_offset: int = 10 ** 9) -> ValueGen:
    """
    親表の連番PKを参照する外部キー。親のキー配列を保持しないので件数に依存しない。
    match_rate < 1.0 で、あえて親に存在しないキーを混ぜる（LEFT JOIN のテスト用）。
    """
    def _gen(c: Ctx) -> str:
        if match_rate >= 1.0 or c.rng.random() < match_rate:
            return str(c.rng.randint(1, parent_rows))
        return str(orphan_offset + c.rng.randint(1, parent_rows))

    return _gen


def jp_text(word_count: tuple[int, int] = (2, 6)) -> ValueGen:
    """日本語を含む可変長テキスト。バイト長の検証対象になる。"""
    words = ["定期", "臨時", "追加", "変更", "取消", "至急", "確認済", "保留", "調整中", "分納"]

    def _gen(c: Ctx) -> str:
        n = c.rng.randint(*word_count)
        return "".join(c.rng.choice(words) for _ in range(n))

    return _gen


def nullable_gen(inner: ValueGen, null_rate: float) -> ValueGen:
    """一定割合でNULL（空フィールド）を返す。"""
    return lambda c: "" if c.rng.random() < null_rate else inner(c)


# --------------------------------------------------------------------------
# 表スペック（ここをDDLに合わせて書き換える）
# --------------------------------------------------------------------------

DT_FROM = datetime(2023, 1, 1)
DT_TO = datetime(2026, 9, 30)

ORDERS_ROWS = 10_000_000
DETAILS_PER_ORDER = 4

TABLES: dict[str, Table] = {
    # CREATE TABLE ORDERS (
    #   ORDER_ID     NUMBER(12)   NOT NULL,
    #   CUSTOMER_CD  VARCHAR2(10) NOT NULL,
    #   ORDER_DT     DATE         NOT NULL,
    #   STATUS_CD    CHAR(2)      NOT NULL,
    #   TOTAL_AMOUNT NUMBER(14,2) NOT NULL,
    #   REMARKS      VARCHAR2(200)
    # )
    "ORDERS": Table(
        name="ORDERS",
        rows=ORDERS_ROWS,
        columns=[
            Column("ORDER_ID", seq(1), "INTEGER EXTERNAL", nullable=False),
            Column("CUSTOMER_CD", code_pool("C", 50_000, 9, alpha=1.5),
                   "CHAR(10)", nullable=False, max_bytes=10),
            Column("ORDER_DT", rand_date(DT_FROM, DT_TO, recent_bias=0.8),
                   f'DATE "{DATE_ORA}"', nullable=False),
            Column("STATUS_CD", weighted([("01", 5), ("02", 10), ("03", 75), ("09", 10)]),
                   "CHAR(2)", nullable=False, max_bytes=2),
            Column("TOTAL_AMOUNT", rand_decimal(100, 9_999_999, 2),
                   "DECIMAL EXTERNAL", nullable=False),
            Column("REMARKS", nullable_gen(jp_text(), 0.85),
                   "CHAR(200)", nullable=True, max_bytes=200),
        ],
    ),
    # CREATE TABLE ORDER_DETAILS (
    #   DETAIL_ID   NUMBER(14)   NOT NULL,
    #   ORDER_ID    NUMBER(12)   NOT NULL,   -- FK -> ORDERS
    #   LINE_NO     NUMBER(4)    NOT NULL,
    #   ITEM_CD     VARCHAR2(16) NOT NULL,
    #   QTY         NUMBER(9,3)  NOT NULL,
    #   UNIT_PRICE  NUMBER(12,2) NOT NULL
    # )
    "ORDER_DETAILS": Table(
        name="ORDER_DETAILS",
        rows=ORDERS_ROWS * DETAILS_PER_ORDER,
        parent_rows={"ORDERS": ORDERS_ROWS},
        columns=[
            Column("DETAIL_ID", seq(1), "INTEGER EXTERNAL", nullable=False),
            Column("ORDER_ID", fk_seq(ORDERS_ROWS), "INTEGER EXTERNAL", nullable=False),
            Column("LINE_NO", rand_int(1, DETAILS_PER_ORDER), "INTEGER EXTERNAL", nullable=False),
            Column("ITEM_CD", code_pool("ITM", 20_000, 8, alpha=2.0),
                   "CHAR(16)", nullable=False, max_bytes=16),
            Column("QTY", rand_decimal(1, 500, 3), "DECIMAL EXTERNAL", nullable=False),
            Column("UNIT_PRICE", rand_decimal(10, 500_000, 2), "DECIMAL EXTERNAL", nullable=False),
        ],
    ),
}


# --------------------------------------------------------------------------
# 検証
# --------------------------------------------------------------------------

def validate(table: Table, seed: str, sample: int = 500) -> list[str]:
    """本番件数を流す前に、スペック違反を検出する。"""
    errors: list[str] = []
    rng = random.Random(f"{seed}:{table.name}:validate")

    for i in range(sample):
        ctx = Ctx(rng=rng, row=i)
        for col in table.columns:
            v = col.gen(ctx)

            if v == "" and not col.nullable:
                errors.append(f"{table.name}.{col.name}: NOT NULL列に空値 (row={i})")

            if col.max_bytes is not None:
                b = len(v.encode(ENCODING))
                if b > col.max_bytes:
                    errors.append(
                        f"{table.name}.{col.name}: {col.max_bytes}バイト超過 "
                        f"({b}バイト, value={v!r}, row={i})"
                    )

            if DELIM in v or "\n" in v or "\r" in v or QUOTE in v:
                errors.append(
                    f"{table.name}.{col.name}: 区切り文字/改行/囲み文字を含む (value={v!r}, row={i})"
                )

    return sorted(set(errors))[:20]


# --------------------------------------------------------------------------
# 出力
# --------------------------------------------------------------------------

def csv_filename(table: Table, idx: int, files: int) -> str:
    return f"{table.name.lower()}.csv" if files == 1 else f"{table.name.lower()}_{idx:02d}.csv"


def write_csv(table: Table, outdir: Path, files: int, seed: str, rows: int) -> list[Path]:
    """
    rows件を files 個のファイルに分割して出力する。
    ファイルごとに独立したシードを持つので、並列生成しても再現性が保たれる。
    """
    outdir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    base, extra = divmod(rows, files)
    offset = 0

    for idx in range(files):
        n = base + (1 if idx < extra else 0)
        path = outdir / csv_filename(table, idx, files)
        rng = random.Random(f"{seed}:{table.name}:{idx}")
        gens = [c.gen for c in table.columns]

        with path.open("w", encoding=ENCODING, newline="") as f:
            w = csv.writer(f, delimiter=DELIM, quotechar=QUOTE,
                           quoting=csv.QUOTE_MINIMAL, lineterminator=NEWLINE)
            ctx = Ctx(rng=rng, row=0)
            for i in range(n):
                ctx.row = offset + i
                w.writerow([g(ctx) for g in gens])
                if (i + 1) % 1_000_000 == 0:
                    print(f"  {path.name}: {i + 1:,} 行", file=sys.stderr)

        offset += n
        paths.append(path)
        print(f"  {path.name}: {n:,} 行 完了", file=sys.stderr)

    return paths


def render_ctl(table: Table, csv_name: str) -> str:
    """CSVと同じスペックから制御ファイルを生成する。"""
    width = max(len(c.name) for c in table.columns)
    cols = ",\n".join(f"  {c.name.ljust(width)} {c.ctl_type}" for c in table.columns)
    stem = Path(csv_name).stem
    return f"""OPTIONS (DIRECT=TRUE, ERRORS=100)
LOAD DATA
CHARACTERSET {ORA_CHARSET}
INFILE '{csv_name}'
BADFILE '{stem}.bad'
DISCARDFILE '{stem}.dsc'
TRUNCATE
INTO TABLE {table.name}
FIELDS TERMINATED BY '{DELIM}' OPTIONALLY ENCLOSED BY '{QUOTE}'
TRAILING NULLCOLS
(
{cols}
)
"""


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def run(table: Table, args) -> int:
    rows = int(table.rows * args.rows_scale) if args.rows is None else args.rows
    rows = max(rows, 1)

    print(f"[{table.name}] 検証中...", file=sys.stderr)
    errors = validate(table, args.seed)
    if errors:
        print(f"[{table.name}] スペック違反:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print(f"[{table.name}] 検証OK", file=sys.stderr)

    if args.validate_only:
        return 0

    outdir = Path(args.outdir)
    print(f"[{table.name}] {rows:,} 行 / {args.files} ファイル を生成", file=sys.stderr)
    paths = write_csv(table, outdir, args.files, args.seed, rows)

    for idx, p in enumerate(paths):
        ctl = outdir / f"{p.stem}.ctl"
        ctl.write_text(render_ctl(table, p.name), encoding="utf-8")
        print(f"  {ctl.name} 生成", file=sys.stderr)

    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", help="対象表名")
    ap.add_argument("--all", action="store_true", help="全表を生成")
    ap.add_argument("--rows", type=int, default=None, help="件数（省略時はスペックの値）")
    ap.add_argument("--rows-scale", type=float, default=1.0, help="件数の倍率。小規模確認用")
    ap.add_argument("--files", type=int, default=1, help="分割ファイル数（並列ロード用）")
    ap.add_argument("--outdir", default="./out")
    ap.add_argument("--seed", default="perf-test-2026")
    ap.add_argument("--validate-only", action="store_true")
    args = ap.parse_args()

    if args.all:
        targets = list(TABLES.values())
    elif args.table:
        if args.table not in TABLES:
            print(f"未定義の表: {args.table} (定義済み: {', '.join(TABLES)})", file=sys.stderr)
            return 2
        targets = [TABLES[args.table]]
    else:
        ap.error("--table か --all を指定してください")

    for t in targets:
        rc = run(t, args)
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
