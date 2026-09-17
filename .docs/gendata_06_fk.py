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
import math
import random
import re
import sys
from dataclasses import dataclass, field
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

# Oracleの日付書式からstrftime書式を導出する。
# 逐次replaceだと置換結果が次の置換に巻き込まれるので、1パスでトークン置換する。
ORA_TO_PY = {
    "YYYY": "%Y", "RRRR": "%Y", "YY": "%y",
    "MM": "%m", "DD": "%d",
    "HH24": "%H", "HH12": "%I", "HH": "%I",
    "MI": "%M", "SS": "%S", "FF6": "%f", "FF": "%f",
    "AM": "%p", "PM": "%p",
}
_TOKEN_RE = re.compile("|".join(sorted(ORA_TO_PY, key=len, reverse=True)))


def ora_to_strftime(mask: str) -> str:
    """Oracleの日付書式をstrftime書式に変換する。未対応トークンはエラーにする。"""
    out = _TOKEN_RE.sub(lambda m: ORA_TO_PY[m.group(0)], mask)
    # 変換後に残った英大文字は未対応トークン（MON, DAY, TZR など）
    leftover = sorted(set(re.findall(r"(?<!%)[A-Z]+", out)))
    if leftover:
        raise SpecError(f"日付書式 '{mask}': 未対応のトークン {leftover} "
                        f"(対応: {', '.join(sorted(ORA_TO_PY))})")
    return out


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
        return ora_to_strftime(self.date_format)


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


@generator("date", required={"from", "to"}, optional={"recent_bias", "format"})
def _date(p: dict, env: "Env") -> ValueGen:
    start = _parse_dt(p["from"])
    end = _parse_dt(p["to"])
    if end <= start:
        raise SpecError(f"date: from({start}) が to({end}) 以降になっています")
    bias = float(p.get("recent_bias", 0.0))
    span = int((end - start).total_seconds())
    # format を省略すると format.date_format（全体既定）を使う
    mask = str(p.get("format", env.fmt.date_format))
    fmt = ora_to_strftime(mask)

    def _gen(c: Ctx) -> str:
        r = c.rng.random()
        if bias > 0:
            r = r ** (1.0 / (1.0 + bias))
        return (start + timedelta(seconds=int(span * r))).strftime(fmt)

    _gen.date_mask = mask   # ctl のマスクと突き合わせるために持たせる
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


@generator("key_part", required={"period"},
           optional={"count", "table", "start", "prefix", "digits"})
def _key_part(p: dict, env: "Env") -> ValueGen:
    """
    複合主キーの構成列。行番号を混合基数で分解し、組み合わせの一意性を保証する。

        値 = (row // period) % count

    period は「この列の値が1つ進むまでに消費する行数」。
    count の代わりに table を書くと、参照先の件数（スケール追従）を使う。
    例) PK(ORDER_ID, LINE_NO) で 1受注4明細:
        ORDER_ID -> {period: 4, table: ORDERS}
        LINE_NO  -> {period: 1, count: 4, start: 1}
    """
    period = int(p["period"])
    if period < 1:
        raise SpecError("key_part.period は1以上にしてください")
    has_count, has_table = "count" in p, "table" in p
    if has_count == has_table:
        raise SpecError("key_part: count か table のどちらか一方を指定してください")

    start = int(p.get("start", 1))
    prefix = p.get("prefix")
    digits = int(p.get("digits", 0))

    if has_count:
        count, parent_scaled = int(p["count"]), False
    else:
        parent = env.raw_tables.get(str(p["table"]))
        if parent is None:
            raise SpecError(f"key_part.table: 未定義の表 '{p['table']}'")
        count = int(parent["rows"])
        parent_scaled = bool(parent.get("scalable", True))

    def _gen(c: Ctx) -> str:
        n = max(1, int(count * c.scale)) if parent_scaled else count
        v = start + (c.row // period) % n
        return f"{prefix}{v:0{digits}d}" if prefix is not None else str(v)

    return _gen


@generator("date_part", required={"from", "period", "count"}, optional={"unit", "format"})
def _date_part(p: dict, env: "Env") -> ValueGen:
    """
    複合主キーの日付列。key_part と同じ分解を日付で行う。
    例) PK(STOCK_DT, WAREHOUSE_CD, ITEM_CD) の STOCK_DT。
    """
    start = _parse_dt(p["from"])
    period = int(p["period"])
    count = int(p["count"])
    unit = str(p.get("unit", "day"))
    if unit not in ("day", "hour", "month"):
        raise SpecError(f"date_part.unit: 不明な値 '{unit}' (day | hour | month)")
    mask = str(p.get("format", env.fmt.date_format))
    fmt = ora_to_strftime(mask)

    def _gen(c: Ctx) -> str:
        i = (c.row // period) % count
        if unit == "hour":
            d = start + timedelta(hours=i)
        elif unit == "month":
            y, m = divmod(start.month - 1 + i, 12)
            d = start.replace(year=start.year + y, month=m + 1)
        else:
            d = start + timedelta(days=i)
        return d.strftime(fmt)

    _gen.date_mask = mask
    return _gen


@generator("fk", required={"table"},
           optional={"mode", "match_rate", "orphan_offset", "period"})
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

    if mode == "superset":
        # 子のキー空間が親より広い。親の件数を超えた分がそのまま孤児キーになる。
        # 不一致率は件数比から決まるので match_rate は使わない。
        if "match_rate" in p:
            raise SpecError("fk mode=superset: 不一致率は件数比から決まるため "
                            "match_rate は指定できません")
        child_rows = env.current_rows
        child_scaled = env.current_scalable
        period = int(p.get("period", 1))
        if period < 1:
            raise SpecError("fk.period は1以上にしてください")
        if child_rows // period < parent_rows:
            raise SpecError(
                f"fk mode=superset: キー数({child_rows // period})が親({parent_rows})以下です。"
                "孤児が生まれないので mode: subset か n_to_1 を検討してください")

        # 孤児がファイル末尾に固まらないよう、キー空間全体に散らす。
        # 法と互いに素な乗数を使うので全単射になり、重複は生じない。
        mult_cache: dict[int, int] = {}

        def _mult(n: int) -> int:
            m = mult_cache.get(n)
            if m is None:
                m = int(n * 0.6180339887) | 1  # 黄金比近傍の奇数から探す
                while math.gcd(m, n) != 1:
                    m += 2
                mult_cache[n] = m
            return m

        def _gen(c: Ctx) -> str:
            nc = max(1, int(child_rows * c.scale)) if child_scaled else child_rows
            n_keys = max(1, -(-nc // period))
            return str((c.row // period) * _mult(n_keys) % n_keys + 1)

        np_ = max(1, int(parent_rows * 1.0)) if not parent_scaled else parent_rows
        n_keys0 = max(1, -(-child_rows // period))
        _gen.fk_note = (f"mode=superset  キー数 {n_keys0:,} / 親 {np_:,}"
                        f"  想定一致率 {min(1.0, np_ / n_keys0):.1%}")
        return _gen

    raise SpecError(f"fk.mode: 不明な値 '{mode}' (n_to_1 | subset | superset)")


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
    note: str | None = None


@dataclass
class Table:
    name: str
    rows: int
    scalable: bool
    columns: list[Column]
    primary_key: list[str] = field(default_factory=list)


@dataclass
class Env:
    fmt: Fmt
    raw_tables: dict[str, dict]
    current_rows: int = 0       # subset / superset の比率計算に使う
    current_scalable: bool = True


COLUMN_KEYS = {"name", "ctl", "gen", "nullable", "max_bytes", "null_rate"}
TABLE_KEYS = {"rows", "columns", "scalable", "comment", "primary_key"}


def _parse_dt(v: Any) -> datetime:
    if isinstance(v, datetime):
        return v
    if hasattr(v, "year"):  # PyYAML が date 型で返す場合
        return datetime(v.year, v.month, v.day)
    return datetime.fromisoformat(str(v))


_CTL_DATE_RE = re.compile(r'^\s*(DATE|TIMESTAMP)\s*(?:"([^"]*)")?\s*$', re.I)


def _resolve_ctl(ctl: str, mask: str | None, where: str) -> str:
    """
    ctl の日付マスクと生成器の書式を突き合わせる。
      - ctl が 'DATE' だけなら生成器の書式で自動補完する
      - 両方書いてあって食い違っていればエラー
    """
    m = _CTL_DATE_RE.match(ctl)
    if m is None:
        return ctl  # CHAR 等。日付文字列を文字列列に入れる場合もあるので触らない
    kw, ctl_mask = m.group(1).upper(), m.group(2)

    if mask is None:
        if ctl_mask is None:
            raise SpecError(f"{where}: ctl に '{kw}' と書く場合は書式マスクが必要です "
                            f'(例: {kw} "YYYY-MM-DD HH24:MI:SS")。'
                            "省略するとNLS_DATE_FORMAT依存になり環境で結果が変わります")
        return ctl

    if ctl_mask is None:
        return f'{kw} "{mask}"'  # 生成器側の書式で補完
    if ctl_mask != mask:
        raise SpecError(f"{where}: ctl の書式 '{ctl_mask}' と生成器の書式 '{mask}' が一致しません")
    return ctl


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

    ctl = _resolve_ctl(str(raw["ctl"]), getattr(gen, "date_mask", None),
                       f"{table_name}.{name}")
    note = getattr(gen, "fk_note", None)

    null_rate = float(raw.get("null_rate", 0.0))
    if null_rate > 0:
        inner = gen

        def gen(c: Ctx, _i=inner, _r=null_rate) -> str:  # noqa: F811
            return "" if c.rng.random() < _r else _i(c)

    nullable = bool(raw.get("nullable", null_rate > 0))
    if null_rate > 0 and not nullable:
        raise SpecError(f"{table_name}.{name}: null_rate > 0 なのに nullable: false になっています")

    mb = raw.get("max_bytes")
    return Column(name=name, gen=gen, ctl_type=ctl, note=note,
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
        env.current_scalable = bool(t.get("scalable", True))
        cols = [_build_column(c, env, tname) for c in t["columns"]]
        if not cols:
            raise SpecError(f"{tname}: columns が空です")
        pk = t.get("primary_key") or []
        if not isinstance(pk, list):
            raise SpecError(f"{tname}: primary_key は列名のリストにしてください")
        known = {c.name for c in cols}
        missing = [k for k in pk if k not in known]
        if missing:
            raise SpecError(f"{tname}: primary_key に存在しない列 {missing}")
        tables[tname] = Table(name=tname, rows=int(t["rows"]),
                              scalable=bool(t.get("scalable", True)),
                              columns=cols, primary_key=[str(k) for k in pk])
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


def check_pk(table: Table, seed: str, scale: float, rows: int, limit: int) -> list[str]:
    """
    複合主キーの組み合わせが一意かを実際に生成して確認する。
    件数が多い場合は先頭 limit 行までを検査する（分解が周期的なので通常はこれで十分）。
    """
    if not table.primary_key:
        return []
    idx = [i for i, c in enumerate(table.columns) if c.name in table.primary_key]
    n = min(rows, limit)
    rng = random.Random(f"{seed}:{table.name}:0")
    gens = [c.gen for c in table.columns]
    ctx = Ctx(rng=rng, row=0, scale=scale)
    seen: set[tuple[str, ...]] = set()

    for i in range(n):
        ctx.row = i
        vals = [g(ctx) for g in gens]  # 乱数列を実生成と揃えるため全列を評価する
        key = tuple(vals[j] for j in idx)
        if key in seen:
            return [f"{table.name}: 主キー {table.primary_key} が重複 "
                    f"(row={i}, key={key})  検査範囲 {n:,} 行"]
        seen.add(key)
    return []


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
    pk_errors = check_pk(table, args.seed, scale, rows, args.pk_check)
    if pk_errors:
        print(f"[{table.name}] 主キー違反:", file=sys.stderr)
        for e in pk_errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    pk_note = f", PK一意性OK({min(rows, args.pk_check):,}行)" if table.primary_key else ""
    print(f"[{table.name}] 検証OK (scale={scale:g}, rows={rows:,}{pk_note})", file=sys.stderr)
    for c in table.columns:
        if c.note:
            print(f"    {c.name}: {c.note}", file=sys.stderr)

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
    ap.add_argument("--pk-check", type=int, default=200_000,
                    help="主キー一意性を検査する行数の上限")
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
