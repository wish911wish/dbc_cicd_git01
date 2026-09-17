"""
SQL*Loader 性能テスト用データ生成エンジン

表スペックはYAMLファイルで外部管理する。このファイルにはテーブル定義を書かない。

  python gendata.py --spec spec/orders.yaml --all --outdir ./out --files 8
  python gendata.py --spec spec/orders.yaml --all --rows-scale 0.001 --outdir ./sample
  python gendata.py --spec spec/orders.yaml --validate-only

設計:
  - スペック(YAML)とエンジン(このファイル)を分離する
  - 生成器はレジストリで名前解決する。未知の型・未知のパラメータはロード時に弾く
  - 外部キーは参照先の表名で書く。件数は解決時にスペックから引く
  - CSVと制御ファイルは同じスペックから生成するので書式が食い違わない
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

try:
    import yaml
except ImportError:
    print("pyyaml が必要です: pip install pyyaml", file=sys.stderr)
    raise


class SpecError(Exception):
    """スペックファイルの記述誤り。生成を始める前に全部ここで弾く。"""


# --------------------------------------------------------------------------
# 書式設定
# --------------------------------------------------------------------------

# Oracleの日付書式からstrftime書式を導出する。長いトークンから順に置換する。
ORA_TO_PY = [("YYYY", "%Y"), ("HH24", "%H"), ("MM", "%m"),
             ("MI", "%M"), ("DD", "%d"), ("SS", "%S")]


@dataclass(frozen=True)
class Fmt:
    encoding: str = "utf-8"
    oracle_charset: str = "AL32UTF8"
    date_format: str = "YYYY-MM-DD HH24:MI:SS"  # Oracle書式で1回だけ書く
    delimiter: str = ","
    quote: str = '"'
    quote_all: bool = True
    header: bool = True
    newline: str = "\n"
    direct: bool = True
    errors: int = 100

    @property
    def date_py(self) -> str:
        s = self.date_format
        for ora, py in ORA_TO_PY:
            s = s.replace(ora, py)
        return s


# --------------------------------------------------------------------------
# 生成器レジストリ
# --------------------------------------------------------------------------

@dataclass
class Ctx:
    rng: random.Random
    row: int
    scale: float = 1.0


ValueGen = Callable[[Ctx], str]

# name -> (factory, 必須パラメータ, 任意パラメータ)
GENERATORS: dict[str, tuple[Callable, set[str], set[str]]] = {}


def generator(name: str, required: set[str] = frozenset(), optional: set[str] = frozenset()):
    def deco(fn):
        GENERATORS[name] = (fn, set(required), set(optional))
        return fn
    return deco


@generator("seq", optional={"start"})
def _seq(p: dict, env: "Env") -> ValueGen:
    start = int(p.get("start", 1))
    return lambda c: str(start + c.row)


@generator("const", required={"value"})
def _const(p: dict, env: "Env") -> ValueGen:
    v = str(p["value"])
    return lambda c: v


@generator("int", required={"min", "max"})
def _int(p: dict, env: "Env") -> ValueGen:
    lo, hi = int(p["min"]), int(p["max"])
    return lambda c: str(c.rng.randint(lo, hi))


@generator("decimal", required={"min", "max", "scale"})
def _decimal(p: dict, env: "Env") -> ValueGen:
    scale = int(p["scale"])
    factor = 10 ** scale
    lo_i, hi_i = int(float(p["min"]) * factor), int(float(p["max"]) * factor)

    def _gen(c: Ctx) -> str:
        v = c.rng.randint(lo_i, hi_i)
        if scale == 0:
            return str(v)
        sign = "-" if v < 0 else ""
        v = abs(v)
        return f"{sign}{v // factor}.{v % factor:0{scale}d}"

    return _gen


@generator("date", required={"from", "to"}, optional={"recent_bias"})
def _date(p: dict, env: "Env") -> ValueGen:
    start = _parse_dt(p["from"])
    end = _parse_dt(p["to"])
    if end <= start:
        raise SpecError(f"date: from({start}) が to({end}) 以降になっています")
    bias = float(p.get("recent_bias", 0.0))
    span = int((end - start).total_seconds())
    fmt = env.fmt.date_py

    def _gen(c: Ctx) -> str:
        r = c.rng.random()
        if bias > 0:
            r = r ** (1.0 / (1.0 + bias))
        return (start + timedelta(seconds=int(span * r))).strftime(fmt)

    return _gen


@generator("weighted", required={"values"})
def _weighted(p: dict, env: "Env") -> ValueGen:
    items = p["values"]
    if not isinstance(items, dict) or not items:
        raise SpecError("weighted.values は {値: 重み} のマップで指定してください")
    values = [str(k) for k in items]
    weights = [float(v) for v in items.values()]
    return lambda c: c.rng.choices(values, weights=weights, k=1)[0]


@generator("code_pool", required={"count"}, optional={"prefix", "digits", "alpha"})
def _code_pool(p: dict, env: "Env") -> ValueGen:
    count = int(p["count"])
    prefix = str(p.get("prefix", ""))
    digits = int(p.get("digits", 6))
    alpha = float(p.get("alpha", 0.0))

    def _gen(c: Ctx) -> str:
        if alpha > 0:
            n = min(count, int(count * (c.rng.random() ** (1.0 + alpha))) + 1)
        else:
            n = c.rng.randint(1, count)
        return f"{prefix}{n:0{digits}d}"

    return _gen


@generator("jp_text", optional={"min_words", "max_words", "max_bytes"})
def _jp_text(p: dict, env: "Env") -> ValueGen:
    words = ["定期", "臨時", "追加", "変更", "取消", "至急", "確認済", "保留", "調整中", "分納"]
    lo = int(p.get("min_words", 2))
    hi = int(p.get("max_words", 6))
    limit = p.get("max_bytes")
    limit = int(limit) if limit is not None else None
    enc = env.fmt.encoding

    def _gen(c: Ctx) -> str:
        out = ""
        for _ in range(c.rng.randint(lo, hi)):
            w = c.rng.choice(words)
            if limit is not None and len((out + w).encode(enc)) > limit:
                break
            out += w
        return out or words[0]

    return _gen


@generator("fk", required={"table"}, optional={"mode", "match_rate", "orphan_offset"})
def _fk(p: dict, env: "Env") -> ValueGen:
    """
    外部キー。参照先は表名で書き、件数はスペックから解決する。

    mode: n_to_1 … 親を重複ありで参照（INNER JOIN の子明細など）
          subset … 親のPKから重複なしで抜く（LEFT JOIN のオプショナルな子）
    match_rate < 1.0 で親に存在しないキーを混ぜる（FK制約とは両立しない）
    """
    parent_name = str(p["table"])
    parent = env.raw_tables.get(parent_name)
    if parent is None:
        raise SpecError(f"fk.table: 未定義の表 '{parent_name}' "
                        f"(定義済み: {', '.join(env.raw_tables)})")

    parent_rows = int(parent["rows"])
    parent_scaled = bool(parent.get("scalable", True))
    mode = str(p.get("mode", "n_to_1"))
    match_rate = float(p.get("match_rate", 1.0))
    orphan_offset = int(p.get("orphan_offset", 10 ** 9))

    if mode == "n_to_1":
        def _gen(c: Ctx) -> str:
            n = max(1, int(parent_rows * c.scale)) if parent_scaled else parent_rows
            if match_rate >= 1.0 or c.rng.random() < match_rate:
                return str(c.rng.randint(1, n))
            return str(orphan_offset + c.rng.randint(1, n))
        return _gen

    if mode == "subset":
        # 子/親の比率はスペックの件数から導出する。ここを二重に書かせない。
        ratio = env.current_rows / parent_rows
        if ratio > 1.0:
            raise SpecError(f"fk mode=subset: 子({env.current_rows})が親({parent_rows})より多いため"
                            "重複なしで割り当てられません")
        stride = max(1, int(1.0 / ratio))

        def _gen(c: Ctx) -> str:
            n = max(1, int(parent_rows * c.scale)) if parent_scaled else parent_rows
            return str(min(1 + c.row * stride + c.rng.randrange(stride), n))
        return _gen

    raise SpecError(f"fk.mode: 不明な値 '{mode}' (n_to_1 | subset)")


# --------------------------------------------------------------------------
# スペックの読み込み
# --------------------------------------------------------------------------

@dataclass
class Column:
    name: str
    gen: ValueGen
    ctl_type: str
    nullable: bool
    max_bytes: int | None


@dataclass
class Table:
    name: str
    rows: int
    scalable: bool
    columns: list[Column]


@dataclass
class Env:
    fmt: Fmt
    raw_tables: dict[str, dict]
    current_rows: int = 0  # subset の比率計算に使う


COLUMN_KEYS = {"name", "ctl", "gen", "nullable", "max_bytes", "null_rate"}
TABLE_KEYS = {"rows", "columns", "scalable", "comment"}


def _parse_dt(v: Any) -> datetime:
    if isinstance(v, datetime):
        return v
    if hasattr(v, "year"):  # PyYAML が date 型で返す場合
        return datetime(v.year, v.month, v.day)
    return datetime.fromisoformat(str(v))


def _build_column(raw: dict, env: Env, table_name: str) -> Column:
    if not isinstance(raw, dict):
        raise SpecError(f"{table_name}: 列定義はマップで書いてください (got {type(raw).__name__})")

    unknown = set(raw) - COLUMN_KEYS
    if unknown:
        raise SpecError(f"{table_name}: 不明なキー {sorted(unknown)} "
                        f"(使えるキー: {sorted(COLUMN_KEYS)})")
    for k in ("name", "ctl", "gen"):
        if k not in raw:
            raise SpecError(f"{table_name}: 列に '{k}' がありません -> {raw}")

    name = str(raw["name"])
    gspec = raw["gen"]
    if not isinstance(gspec, dict) or "type" not in gspec:
        raise SpecError(f"{table_name}.{name}: gen は type を含むマップにしてください")

    gtype = str(gspec["type"])
    if gtype not in GENERATORS:
        raise SpecError(f"{table_name}.{name}: 不明な生成器 '{gtype}' "
                        f"(利用可能: {', '.join(sorted(GENERATORS))})")

    factory, required, optional = GENERATORS[gtype]
    params = {k: v for k, v in gspec.items() if k != "type"}
    missing = required - set(params)
    if missing:
        raise SpecError(f"{table_name}.{name}: '{gtype}' に必須パラメータ {sorted(missing)} がありません")
    extra = set(params) - required - optional
    if extra:
        raise SpecError(f"{table_name}.{name}: '{gtype}' に不明なパラメータ {sorted(extra)} "
                        f"(使えるもの: {sorted(required | optional)})")

    try:
        gen = factory(params, env)
    except SpecError as e:
        raise SpecError(f"{table_name}.{name}: {e}") from None

    null_rate = float(raw.get("null_rate", 0.0))
    if null_rate > 0:
        inner = gen

        def gen(c: Ctx, _i=inner, _r=null_rate) -> str:  # noqa: F811
            return "" if c.rng.random() < _r else _i(c)

    nullable = bool(raw.get("nullable", null_rate > 0))
    if null_rate > 0 and not nullable:
        raise SpecError(f"{table_name}.{name}: null_rate > 0 なのに nullable: false になっています")

    mb = raw.get("max_bytes")
    return Column(name=name, gen=gen, ctl_type=str(raw["ctl"]),
                  nullable=nullable, max_bytes=int(mb) if mb is not None else None)


def load_spec(path: Path) -> tuple[Fmt, dict[str, Table]]:
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise SpecError(f"YAMLの構文エラー: {e}") from None
    if not isinstance(doc, dict):
        raise SpecError("スペックのトップレベルはマップにしてください")

    unknown = set(doc) - {"version", "format", "tables"}
    if unknown:
        raise SpecError(f"トップレベルに不明なキー {sorted(unknown)}")

    fmt_raw = doc.get("format") or {}
    valid_fmt = set(Fmt.__dataclass_fields__)
    bad = set(fmt_raw) - valid_fmt
    if bad:
        raise SpecError(f"format に不明なキー {sorted(bad)} (使えるキー: {sorted(valid_fmt)})")
    fmt = Fmt(**fmt_raw)

    raw_tables = doc.get("tables") or {}
    if not raw_tables:
        raise SpecError("tables が空です")
    for tname, t in raw_tables.items():
        if not isinstance(t, dict):
            raise SpecError(f"{tname}: 表定義はマップにしてください")
        bad = set(t) - TABLE_KEYS
        if bad:
            raise SpecError(f"{tname}: 不明なキー {sorted(bad)} (使えるキー: {sorted(TABLE_KEYS)})")
        if "rows" not in t or "columns" not in t:
            raise SpecError(f"{tname}: rows と columns は必須です")

    env = Env(fmt=fmt, raw_tables=raw_tables)
    tables: dict[str, Table] = {}
    for tname, t in raw_tables.items():
        env.current_rows = int(t["rows"])
        cols = [_build_column(c, env, tname) for c in t["columns"]]
        if not cols:
            raise SpecError(f"{tname}: columns が空です")
        tables[tname] = Table(name=tname, rows=int(t["rows"]),
                              scalable=bool(t.get("scalable", True)), columns=cols)
    return fmt, tables


# --------------------------------------------------------------------------
# 検証・出力
# --------------------------------------------------------------------------

def validate(table: Table, fmt: Fmt, seed: str, scale: float, sample: int = 500) -> list[str]:
    errors: list[str] = []
    rng = random.Random(f"{seed}:{table.name}:validate")
    for i in range(sample):
        ctx = Ctx(rng=rng, row=i, scale=scale)
        for col in table.columns:
            v = col.gen(ctx)
            if v == "" and not col.nullable:
                errors.append(f"{table.name}.{col.name}: NOT NULL列に空値 (row={i})")
            if col.max_bytes is not None:
                b = len(v.encode(fmt.encoding))
                if b > col.max_bytes:
                    errors.append(f"{table.name}.{col.name}: {col.max_bytes}バイト超過 "
                                  f"({b}バイト, value={v!r})")
            if "\n" in v or "\r" in v or fmt.quote in v:
                errors.append(f"{table.name}.{col.name}: 改行/囲み文字を含む (value={v!r})")
    return sorted(set(errors))[:20]


def csv_filename(table: Table, idx: int, files: int) -> str:
    return f"{table.name.lower()}.csv" if files == 1 else f"{table.name.lower()}_{idx:02d}.csv"


def write_csv(table: Table, fmt: Fmt, outdir: Path, files: int,
              seed: str, rows: int, scale: float) -> list[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    base, extra = divmod(rows, files)
    offset = 0
    quoting = csv.QUOTE_ALL if fmt.quote_all else csv.QUOTE_MINIMAL

    for idx in range(files):
        n = base + (1 if idx < extra else 0)
        path = outdir / csv_filename(table, idx, files)
        rng = random.Random(f"{seed}:{table.name}:{idx}")
        gens = [c.gen for c in table.columns]

        with path.open("w", encoding=fmt.encoding, newline="") as f:
            w = csv.writer(f, delimiter=fmt.delimiter, quotechar=fmt.quote,
                           quoting=quoting, lineterminator=fmt.newline)
            if fmt.header:
                w.writerow([c.name for c in table.columns])
            ctx = Ctx(rng=rng, row=0, scale=scale)
            for i in range(n):
                ctx.row = offset + i
                w.writerow([g(ctx) for g in gens])
                if (i + 1) % 1_000_000 == 0:
                    print(f"  {path.name}: {i + 1:,} 行", file=sys.stderr)

        offset += n
        paths.append(path)
        print(f"  {path.name}: {n:,} 行 完了", file=sys.stderr)
    return paths


def render_ctl(table: Table, fmt: Fmt, csv_name: str) -> str:
    width = max(len(c.name) for c in table.columns)
    cols = ",\n".join(f"  {c.name.ljust(width)} {c.ctl_type}" for c in table.columns)
    stem = Path(csv_name).stem
    opts = [f"SKIP={1 if fmt.header else 0}"]
    if fmt.direct:
        opts.append("DIRECT=TRUE")
    opts.append(f"ERRORS={fmt.errors}")
    return f"""OPTIONS ({', '.join(opts)})
LOAD DATA
CHARACTERSET {fmt.oracle_charset}
INFILE '{csv_name}'
BADFILE '{stem}.bad'
DISCARDFILE '{stem}.dsc'
TRUNCATE
INTO TABLE {table.name}
FIELDS TERMINATED BY '{fmt.delimiter}' OPTIONALLY ENCLOSED BY '{fmt.quote}'
TRAILING NULLCOLS
(
{cols}
)
"""


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def run(table: Table, fmt: Fmt, args) -> int:
    if not table.scalable:
        scale, rows = 1.0, table.rows
    else:
        scale = args.rows_scale if args.rows is None else args.rows / table.rows
        rows = max(int(table.rows * scale), 1)

    errors = validate(table, fmt, args.seed, scale)
    if errors:
        print(f"[{table.name}] スペック違反:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print(f"[{table.name}] 検証OK (scale={scale:g}, rows={rows:,})", file=sys.stderr)

    if args.validate_only:
        return 0

    outdir = Path(args.outdir)
    paths = write_csv(table, fmt, outdir, args.files, args.seed, rows, scale)
    for p in paths:
        ctl = outdir / f"{p.stem}.ctl"
        ctl.write_text(render_ctl(table, fmt, p.name), encoding="utf-8")
        print(f"  {ctl.name} 生成", file=sys.stderr)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spec", required=True, help="表スペックのYAMLファイル")
    ap.add_argument("--table", help="対象表名")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--rows", type=int, default=None)
    ap.add_argument("--rows-scale", type=float, default=1.0)
    ap.add_argument("--files", type=int, default=1)
    ap.add_argument("--outdir", default="./out")
    ap.add_argument("--seed", default="perf-test-2026")
    ap.add_argument("--validate-only", action="store_true")
    args = ap.parse_args()

    try:
        fmt, tables = load_spec(Path(args.spec))
    except SpecError as e:
        print(f"スペックエラー ({args.spec}): {e}", file=sys.stderr)
        return 2
    except OSError as e:
        print(f"スペックを読めません: {e}", file=sys.stderr)
        return 2

    if args.all or (not args.table and args.validate_only):
        targets = list(tables.values())
    elif args.table:
        if args.table not in tables:
            print(f"未定義の表: {args.table} (定義済み: {', '.join(tables)})", file=sys.stderr)
            return 2
        targets = [tables[args.table]]
    else:
        ap.error("--table か --all を指定してください")

    for t in targets:
        if run(t, fmt, args) != 0:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
