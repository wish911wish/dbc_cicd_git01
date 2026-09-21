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
import bisect
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
    scale: float = 1.0   # 全表共通の縮小率。FKの親件数の解決に使う
    rows: int = 0        # この表が実際に生成する件数


ValueGen = Callable[[Ctx], str]

# --------------------------------------------------------------------------
# 行番号ハッシュ
#
# 乱数列ではなく行番号から値を決める生成器のための基盤。
#   - fk_multi から親の列を再生できる（乱数列は子側で再現できない）
#   - ファイル分割の影響を受けない
#   - 列ごとに異なるソルトを使うので、列間に相関は出ない
# --------------------------------------------------------------------------

_MASK64 = (1 << 64) - 1


def _salt(text: str) -> int:
    """文字列から64bitのソルトを作る。PYTHONHASHSEEDに依存しない決定的な値。"""
    h = 0xCBF29CE484222325
    for b in text.encode("utf-8"):
        h = ((h ^ b) * 0x100000001B3) & _MASK64
    return h


def _hash01(row: int, salt: int) -> float:
    """行番号とソルトから [0,1) の一様な値を決定的に作る（splitmix64 相当）。"""
    h = (row * 0x9E3779B97F4A7C15 + salt) & _MASK64
    h = ((h ^ (h >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    h = ((h ^ (h >> 27)) * 0x94D049BB133111EB) & _MASK64
    h ^= h >> 31
    return (h >> 11) / float(1 << 53)

# name -> (factory, 必須パラメータ, 任意パラメータ)
GENERATORS: dict[str, tuple[Callable, set[str], set[str]]] = {}


def generator(name: str, required: set[str] = frozenset(), optional: set[str] = frozenset()):
    def deco(fn):
        GENERATORS[name] = (fn, set(required), set(optional))
        return fn
    return deco


@generator("seq", optional={"start", "prefix", "digits"})
def _seq(p: dict, env: "Env") -> ValueGen:
    """連番。prefix / digits を付けると接頭辞つきのコード値になる。"""
    start = int(p.get("start", 1))
    prefix = p.get("prefix")
    digits = int(p.get("digits", 0))

    if digits > 0:
        # 最終行の値が桁数に収まるかを生成前に確認する。
        # 検証は先頭数百行しか見ないので、ここで見ないと末尾で静かに桁が伸びる。
        last = start + max(env.current_rows, 1) - 1
        if len(str(last)) > digits:
            raise SpecError(
                f"seq: digits={digits} では最終行の値 {last} を表現できません "
                f"(必要な桁数 {len(str(last))})")

    if prefix is None:
        def g(c: Ctx) -> str:
            return f"{start + c.row:0{digits}d}" if digits else str(start + c.row)
    else:
        pre = str(prefix)

        def g(c: Ctx) -> str:
            return f"{pre}{start + c.row:0{digits}d}"

    g.row_deterministic = True
    return g


@generator("const", required={"value"})
def _const(p: dict, env: "Env") -> ValueGen:
    v = str(p["value"])
    g = lambda c: v
    g.row_deterministic = True
    return g


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
    """
    出現比率つきのコード値。行番号ハッシュで選ぶので値は行番号だけで決まり、
    fk_multi から参照できる。分布は重みどおりで、列ごとのソルトにより
    他の列との相関は生じない。
    """
    items = p["values"]
    if not isinstance(items, dict) or not items:
        raise SpecError("weighted.values は {値: 重み} のマップで指定してください")
    values = [str(k) for k in items]
    weights = [float(v) for v in items.values()]
    if any(w < 0 for w in weights):
        raise SpecError("weighted.values の重みに負の値は指定できません")
    total = sum(weights)
    if total <= 0:
        raise SpecError("weighted.values の重みの合計が0です")

    # 累積比率。bisect で引く
    cum, acc = [], 0.0
    for w in weights:
        acc += w
        cum.append(acc / total)
    cum[-1] = 1.0
    salt = _salt(f"{env.seed}:{env.current_where}:weighted")
    last = len(values) - 1

    def _gen(c: Ctx) -> str:
        return values[min(bisect.bisect_right(cum, _hash01(c.row, salt)), last)]

    _gen.row_deterministic = True
    return _gen


@generator("code_pool", required={"count"}, optional={"prefix", "digits", "alpha"})
def _code_pool(p: dict, env: "Env") -> ValueGen:
    count = int(p["count"])
    if count < 1:
        raise SpecError("code_pool.count は1以上にしてください")
    prefix = str(p.get("prefix", ""))
    digits = int(p.get("digits", 6))
    alpha = float(p.get("alpha", 0.0))
    if alpha < 0:
        raise SpecError("code_pool.alpha は0以上にしてください")
    salt = _salt(f"{env.seed}:{env.current_where}:code_pool")

    def _gen(c: Ctx) -> str:
        r = _hash01(c.row, salt)
        if alpha > 0:
            n = min(count, int(count * (r ** (1.0 + alpha))) + 1)
        else:
            n = min(count, int(r * count) + 1)
        return f"{prefix}{n:0{digits}d}"

    _gen.row_deterministic = True
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

    _gen.row_deterministic = True
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
    _gen.row_deterministic = True
    return _gen


def _parent_key_gen(env: "Env", parent_name: str, parent_raw: dict, colname: str,
                    caller: str) -> ValueGen:
    """親の指定列の生成器を組み立てて返す。行番号だけで値が決まるものに限る。"""
    pcol = next((c for c in parent_raw["columns"]
                 if isinstance(c, dict) and str(c.get("name")) == colname), None)
    if pcol is None:
        raise SpecError(f"{caller}.column: {parent_name} に列 '{colname}' がありません")

    key = (parent_name, colname)
    if key in env.building:
        raise SpecError(f"{caller}: 参照が循環しています ({parent_name}.{colname})")

    saved = (env.current_rows, env.current_scalable)
    env.building.add(key)
    try:
        env.current_rows = int(parent_raw["rows"])
        env.current_scalable = bool(parent_raw.get("scalable", True))
        inner = _make_gen(pcol["gen"], env, f"{parent_name}.{colname}")
    finally:
        env.building.discard(key)
        env.current_rows, env.current_scalable = saved

    if not getattr(inner, "row_deterministic", False):
        raise SpecError(
            f"{caller}: {parent_name}.{colname} の生成器 '{pcol['gen'].get('type')}' は "
            "行番号だけでは値が決まらないため参照できません "
            "(使えるのは seq / key_part / date_part / const / weighted / code_pool / "
            "fk mode=superset)")

    # 親が null_rate を持つ場合、その NULL の位置まで再現する。
    # 再現しないと「親はNULLなのに子には値がある」行ができる。
    return _apply_null_rate(inner, float(pcol.get("null_rate", 0.0)),
                            env.seed, f"{parent_name}.{colname}")


def _bare_seq_pk(parent_raw: dict) -> tuple[bool, str]:
    """
    親の単一PKが「1始まりの素の連番」かを判定する。
    fk を column なしで使う場合、子は 1〜n の整数を出すのでこの形が前提になる。
    """
    pk = parent_raw.get("primary_key") or []
    if len(pk) != 1:
        return True, ""  # 複合PK/未宣言は判定不能。素通しする
    name = str(pk[0])
    col = next((c for c in parent_raw["columns"]
                if isinstance(c, dict) and str(c.get("name")) == name), None)
    if col is None:
        return True, ""
    g = col.get("gen") or {}
    if g.get("type") != "seq":
        return False, f"{name} の生成器が '{g.get('type')}'"
    if g.get("prefix") is not None or int(g.get("digits", 0)) > 0:
        return False, f"{name} が接頭辞/ゼロ埋めつきの文字列連番"
    if int(g.get("start", 1)) != 1:
        return False, f"{name} の開始値が {g.get('start')}"
    return True, ""


@generator("fk", required={"table"},
           optional={"mode", "match_rate", "orphan_offset", "period", "column"})
def _fk(p: dict, env: "Env") -> ValueGen:
    """
    外部キー。参照先は表名で書き、件数はスペックから解決する。

    mode: n_to_1   … 親を重複ありでランダム参照（既定）
          subset   … 親のPKから重複なしで抜く（子 < 親）
          superset … 子のキー空間が親より広く、超過分が孤児になる（子 > 親）
    """
    parent_name = str(p["table"])
    parent = env.raw_tables.get(parent_name)
    if parent is None:
        raise SpecError(f"fk.table: 未定義の表 '{parent_name}' "
                        f"(定義済み: {', '.join(env.raw_tables)})")

    parent_rows = int(parent["rows"])
    parent_scaled = bool(parent.get("scalable", True))
    child_rows, child_scaled = env.current_rows, env.current_scalable
    mode = str(p.get("mode", "n_to_1"))
    if mode not in ("n_to_1", "subset", "superset"):
        raise SpecError(f"fk.mode: 不明な値 '{mode}' (n_to_1 | subset | superset)")

    # --- パラメータの適用範囲を明示的に検査する ---
    if "match_rate" in p and mode != "n_to_1":
        raise SpecError(f"fk mode={mode}: 不一致率は件数比から決まるため "
                        "match_rate は指定できません")
    if "orphan_offset" in p and mode != "n_to_1":
        raise SpecError(f"fk mode={mode}: orphan_offset は mode=n_to_1 専用です")
    if "period" in p and mode != "superset":
        raise SpecError(f"fk mode={mode}: period は mode=superset 専用です。"
                        "親1行あたりの件数を固定したい場合は fk_multi mode=block を使ってください")

    # 縮小率は全表共通なので、子と親でスケール可否が食い違うと件数比が崩れる
    if mode in ("subset", "superset") and child_scaled != parent_scaled:
        raise SpecError(
            f"fk mode={mode}: 子と親で scalable が食い違っています "
            f"(子={child_scaled}, 親={parent_scaled})。件数比が保てないため "
            "両方を揃えるか mode=n_to_1 を使ってください")

    # column を指定すると、親のその列の生成器を再生して値を作る。
    # 省略した場合、子は 1〜n の整数を出すので親のPKがその形である必要がある。
    colname = p.get("column")
    if colname is not None:
        if "orphan_offset" in p:
            raise SpecError("fk: column 指定時は orphan_offset を使いません "
                            "(孤児キーは親のキー空間の外側として生成されます)")
        inner = _parent_key_gen(env, parent_name, parent, str(colname), "fk")
    else:
        inner = None
        ok, why = _bare_seq_pk(parent)
        if not ok:
            raise SpecError(
                f"fk: {parent_name} の主キーは1始まりの素の連番ではありません({why})。"
                f"fk は column 省略時に 1〜n の整数を出すため値が一致しません。"
                f"column: <親のキー列名> を指定してください")

    def _parent_n(c: Ctx) -> int:
        return max(1, int(parent_rows * c.scale)) if parent_scaled else parent_rows

    dummy = random.Random(0)

    def _emit(c: Ctx, idx: int) -> str:
        """親の行番号(0起点)から実際のキー値を作る。"""
        return inner(Ctx(rng=dummy, row=idx, scale=c.scale)) if inner else str(idx + 1)

    if mode == "n_to_1":
        match_rate = float(p.get("match_rate", 1.0))
        if not 0.0 <= match_rate <= 1.0:
            raise SpecError(f"fk.match_rate: 0.0〜1.0 で指定してください (指定値 {match_rate})")
        orphan_offset = int(p.get("orphan_offset", 10 ** 9))
        if inner is None and orphan_offset <= parent_rows:
            raise SpecError(f"fk.orphan_offset({orphan_offset}) が親の件数({parent_rows})以下です。"
                            "孤児キーが実在のキーと衝突します")

        def _gen(c: Ctx) -> str:
            n = _parent_n(c)
            # match_rate=1.0 でも必ず1回引く。閾値を変えても後続列の乱数列がずれない
            orphan = c.rng.random() >= match_rate
            idx = c.rng.randrange(n)
            if not orphan:
                return _emit(c, idx)
            # 親のキー空間の外側に出す。column 指定時は親の採番規則の続き、
            # 省略時は orphan_offset を加えた整数になる
            return _emit(c, n + idx) if inner else str(orphan_offset + idx + 1)

        _gen.fk_note = (f"mode=n_to_1 -> {parent_name}"
                        + (f"  一致率 {match_rate:.1%}" if match_rate < 1.0 else ""))
        return _gen

    if mode == "subset":
        if child_rows > parent_rows:
            raise SpecError(f"fk mode=subset: 子({child_rows:,})が親({parent_rows:,})より多いため"
                            "重複なしで割り当てられません。mode=superset を検討してください")

        def _gen(c: Ctx) -> str:
            # stride は実行時の件数から求める。縮小時の丸めで
            # キーが親の範囲を超え、クランプで重複するのを防ぐ
            n = _parent_n(c)
            stride = max(1, n // max(1, c.rows))
            return _emit(c, min(c.row * stride + c.rng.randrange(stride), n - 1))

        _gen.fk_note = (f"mode=subset -> {parent_name}  "
                        f"親のうち参照される割合 {child_rows / parent_rows:.1%}")
        return _gen

    # superset
    period = int(p.get("period", 1))
    if period < 1:
        raise SpecError("fk.period は1以上にしてください")
    n_keys0 = -(-child_rows // period)
    if n_keys0 <= parent_rows:
        raise SpecError(
            f"fk mode=superset: キー数({n_keys0:,})が親({parent_rows:,})以下です。"
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
        n_keys = max(1, -(-c.rows // period))
        return _emit(c, (c.row // period) * _mult(n_keys) % n_keys)

    _gen.row_deterministic = inner is None or getattr(inner, "row_deterministic", False)
    _gen.fk_note = (f"mode=superset -> {parent_name}  キー数 {n_keys0:,} / 親 {parent_rows:,}"
                    f"  想定一致率 {parent_rows / n_keys0:.1%}")
    return _gen


@generator("fk_multi", required={"table", "column"}, optional={"mode", "period"})
def _fk_multi(p: dict, env: "Env") -> ValueGen:
    """
    複合外部キーの構成列。親の「同じ1レコード」から複数の列をまとめて取る。

    各列が独立にランダム生成すると、組み合わせが親に存在しない行ができる。
    ここでは子の行番号から親の行番号を決定的に導出し、親のPK列の生成器を
    その行番号で再生する。複合キーの全列が同じ親行番号を算出するので、
    組み合わせの整合が保証される。

        - name: ORDER_ID
          gen: {type: fk_multi, table: ORDER_DETAILS, column: ORDER_ID}
        - name: LINE_NO
          gen: {type: fk_multi, table: ORDER_DETAILS, column: LINE_NO}

    mode: n_to_1 … ハッシュで親行を選ぶ（重複あり。既定）
          block  … 親1行につき period 行を連続で割り当てる
          subset … 親を飛ばし飛ばしに1回ずつ使う（子 < 親）
    親側の対象列は行番号だけで値が決まる生成器（seq / key_part / date_part /
    const / fk mode=superset）である必要がある。
    """
    parent_name = str(p["table"])
    parent_raw = env.raw_tables.get(parent_name)
    if parent_raw is None:
        raise SpecError(f"fk_multi.table: 未定義の表 '{parent_name}' "
                        f"(定義済み: {', '.join(env.raw_tables)})")

    inner = _parent_key_gen(env, parent_name, parent_raw, str(p["column"]), "fk_multi")

    parent_rows = int(parent_raw["rows"])
    parent_scaled = bool(parent_raw.get("scalable", True))
    child_rows, child_scaled = env.current_rows, env.current_scalable
    mode = str(p.get("mode", "n_to_1"))
    period = int(p.get("period", 1))
    if mode not in ("n_to_1", "block", "subset"):
        raise SpecError(f"fk_multi.mode: 不明な値 '{mode}' (n_to_1 | block | subset)")
    if period < 1:
        raise SpecError("fk_multi.period は1以上にしてください")

    MIX = 0x9E3779B97F4A7C15
    MASK = (1 << 64) - 1
    dummy = random.Random(0)  # 親の生成器は乱数を使わない前提

    def _gen(c: Ctx) -> str:
        npar = max(1, int(parent_rows * c.scale)) if parent_scaled else parent_rows
        if mode == "n_to_1":
            # 乱数ではなく行番号のハッシュで選ぶ。複合キーの全列が同じ値を出す。
            h = (c.row ^ (c.row >> 33)) * MIX & MASK
            idx = (h >> 17) % npar
        elif mode == "block":
            idx = (c.row // period) % npar
        else:  # subset
            nch = max(1, int(child_rows * c.scale)) if child_scaled else child_rows
            idx = (c.row * max(1, npar // nch)) % npar
        return inner(Ctx(rng=dummy, row=idx, scale=c.scale))

    _gen.row_deterministic = True
    _gen.fk_note = f"mode={mode} -> {parent_name}.{p['column']}"
    return _gen


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
    building: set = field(default_factory=set)  # fk_multi の循環参照検出
    seed: str = ""              # 行番号ハッシュのソルトに混ぜる
    current_where: str = ""     # 組み立て中の列。列ごとのソルトに使う


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


def _make_gen(gspec: Any, env: Env, where: str) -> ValueGen:
    """gen スペックから生成器を組み立てる。列全体の組み立てとは独立に呼べる。"""
    if not isinstance(gspec, dict) or "type" not in gspec:
        raise SpecError(f"{where}: gen は type を含むマップにしてください")
    gtype = str(gspec["type"])
    if gtype not in GENERATORS:
        raise SpecError(f"{where}: 不明な生成器 '{gtype}' "
                        f"(利用可能: {', '.join(sorted(GENERATORS))})")
    factory, required, optional = GENERATORS[gtype]
    params = {k: v for k, v in gspec.items() if k != "type"}
    missing = required - set(params)
    if missing:
        raise SpecError(f"{where}: '{gtype}' に必須パラメータ {sorted(missing)} がありません")
    extra = set(params) - required - optional
    if extra:
        raise SpecError(f"{where}: '{gtype}' に不明なパラメータ {sorted(extra)} "
                        f"(使えるもの: {sorted(required | optional)})")
    saved_where = env.current_where
    env.current_where = where
    try:
        return factory(params, env)
    except SpecError as e:
        raise SpecError(f"{where}: {e}") from None
    finally:
        env.current_where = saved_where


def _apply_null_rate(gen: ValueGen, null_rate: float, seed: str, where: str) -> ValueGen:
    """
    一定割合を空フィールド（=NULL）にする。判定は行番号ハッシュなので、
    fk_multi から親の列を再生したとき NULL の位置まで一致する。
    """
    if null_rate <= 0:
        return gen
    salt = _salt(f"{seed}:{where}:null_rate")
    deterministic = getattr(gen, "row_deterministic", False)

    def _gen(c: Ctx) -> str:
        return "" if _hash01(c.row, salt) < null_rate else gen(c)

    _gen.row_deterministic = deterministic
    for attr in ("date_mask", "fk_note"):
        if hasattr(gen, attr):
            setattr(_gen, attr, getattr(gen, attr))
    return _gen


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
    gen = _make_gen(raw["gen"], env, f"{table_name}.{name}")

    ctl = _resolve_ctl(str(raw["ctl"]), getattr(gen, "date_mask", None),
                       f"{table_name}.{name}")
    note = getattr(gen, "fk_note", None)

    null_rate = float(raw.get("null_rate", 0.0))
    if not 0.0 <= null_rate <= 1.0:
        raise SpecError(f"{table_name}.{name}: null_rate は 0.0〜1.0 で指定してください "
                        f"(指定値 {null_rate})")
    gen = _apply_null_rate(gen, null_rate, env.seed, f"{table_name}.{name}")

    nullable = bool(raw.get("nullable", null_rate > 0))
    if null_rate > 0 and not nullable:
        raise SpecError(f"{table_name}.{name}: null_rate > 0 なのに nullable: false になっています")

    mb = raw.get("max_bytes")
    return Column(name=name, gen=gen, ctl_type=ctl, note=note,
                  nullable=nullable, max_bytes=int(mb) if mb is not None else None)


def load_spec(path: Path, seed: str = "") -> tuple[Fmt, dict[str, Table]]:
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

    env = Env(fmt=fmt, raw_tables=raw_tables, seed=seed)
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

def _column_rngs(table: Table, seed: str, file_idx) -> list[random.Random]:
    """
    列ごとに独立した乱数列を割り当てる。共有すると、ある列の生成器を
    変えただけで後続の全列の値が変わり、シナリオ間の比較ができなくなる。
    """
    return [random.Random(f"{seed}:{table.name}:{file_idx}:{c.name}") for c in table.columns]


def validate(table: Table, fmt: Fmt, seed: str, scale: float, rows: int,
             sample: int = 500) -> list[str]:
    errors: list[str] = []
    rngs = _column_rngs(table, seed, "validate")
    ctx = Ctx(rng=rngs[0], row=0, scale=scale, rows=rows)
    for i in range(sample):
        ctx.row = i
        for col, rng in zip(table.columns, rngs):
            ctx.rng = rng
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
    rngs = _column_rngs(table, seed, 0)
    gens = [c.gen for c in table.columns]
    ctx = Ctx(rng=rngs[0], row=0, scale=scale, rows=rows)
    seen: set[tuple[str, ...]] = set()

    for i in range(n):
        ctx.row = i
        vals = []
        for g, rng in zip(gens, rngs):
            ctx.rng = rng
            vals.append(g(ctx))
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
        rngs = _column_rngs(table, seed, idx)
        gens = list(zip((c.gen for c in table.columns), rngs))

        with path.open("w", encoding=fmt.encoding, newline="") as f:
            w = csv.writer(f, delimiter=fmt.delimiter, quotechar=fmt.quote,
                           quoting=quoting, lineterminator=fmt.newline)
            if fmt.header:
                w.writerow([c.name for c in table.columns])
            ctx = Ctx(rng=rngs[0], row=0, scale=scale, rows=rows)
            row_buf = [""] * len(gens)
            for i in range(n):
                ctx.row = offset + i
                for j, (g, rng) in enumerate(gens):
                    ctx.rng = rng
                    row_buf[j] = g(ctx)
                w.writerow(row_buf)
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
    # 縮小率は全表共通。scalable: false の表は件数だけ据え置く。
    # ここで scale を 1.0 に戻すと、その表のFKが親の未縮小件数を参照して壊れる。
    scale = args.rows_scale if args.rows is None else args.rows / table.rows
    rows = table.rows if not table.scalable else max(int(table.rows * scale), 1)

    errors = validate(table, fmt, args.seed, scale, rows)
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
        fmt, tables = load_spec(Path(args.spec), args.seed)
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
