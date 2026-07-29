#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, yaml, boto3, psycopg2, urllib.request, logging, argparse, traceback, copy
from typing import Optional, Tuple, List, Dict, Any
from psycopg2 import sql
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

os.environ.setdefault("PGCLIENTENCODING", "utf8")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", force=True)

# 環境変数名を一か所にまとめ、設定値の参照先を明確にする
SLACK_WEBHOOK_ENV_VAR = "SLACK_WEBHOOK_URL"
DB_DSN_TEMPLATE_ENV_VAR = "DB_DSN_TEMPLATE"

# Slack Incoming Webhook にテキストと任意の Block Kit データを送信する
def post_to_slack(webhook_url: str, text: str, blocks=None):
    payload = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10).close()

# 監視エラーを Slack の文字数制限に収まる Block Kit 形式へ整形する
def build_slack_blocks(results_subset, jst_today_str: str):
    # 長いメッセージを改行単位で複数のセクションへ分割する
    def chunk_text(s, limit=2800):
        out, cur, cur_len = [], [], 0
        for line in s.splitlines():
            n = len(line) + 1
            if cur_len + n > limit and cur:
                out.append("\n".join(cur)); cur, cur_len = [line], n
            else:
                cur.append(line); cur_len += n
        if cur: out.append("\n".join(cur))
        return out
    header = f"データ監視アラート（{jst_today_str}）"
    sections = []
    for r in results_subset:
        if not r["errors"]:
            continue
        section = "\n".join([r["property"], *r["errors"], ""])
        for part in chunk_text(section):
            sections.append({"type": "section", "text": {"type": "mrkdwn", "text": part}})
    if not sections:
        return []
    return [{"type": "section", "text": {"type": "mrkdwn", "text": header}}] + sections


# 全チェック正常時に送る Slack メッセージを組み立てる
def build_ok_slack_blocks(jst_today_str: str):
    header = f"データ監視アラート（{jst_today_str}）"
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": header}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "データは正常に取得されています。"}},
    ]

# Slack のブロック数上限を避けるため、指定数ごとに分割して送信する
def send_slack_batches(webhook_url: str, header_text: str, blocks: list, max_blocks=40):
    if not blocks:
        return
    batch, count = [], 0
    for b in blocks:
        batch.append(b); count += 1
        if count >= max_blocks:
            post_to_slack(webhook_url, header_text, blocks=batch)
            batch, count = [], 0
    if batch:
        post_to_slack(webhook_url, header_text, blocks=batch)

# RDS IAM 認証トークンを生成し、SSL を使用して PostgreSQL へ接続する
def connect_db(dsn: str):
    session = boto3.Session()
    client = session.client('rds')
    token = client.generate_db_auth_token(DBHostname=os.environ["DB_HOST"], Port=int(os.environ["DB_PORT"]), DBUsername=os.environ["DB_USER"], Region=os.environ["AWS_REGION"])
    conn = psycopg2.connect(dsn, password=token, sslmode="require", sslrootcert="global-bundle.pem")
    conn.set_client_encoding('UTF8')
    return conn

FACILITY_FILTER_DISABLED = object()
FACILITY_COLUMN_FALLBACKS = {"facility_id": "facility_code", "facility_code": "facility_id"}


def build_facility_template_context(facility: Dict[str, Any]) -> Dict[str, Any]:
    return {**facility, "facility_id": facility["code"]}


# 既定値、宿別の上書き値、旧形式の設定値を一つの施設フィルタ設定へ統合する
def normalize_facility_filter_settings(
    base: Any,
    override: Any,
    fallback_column: Any,
    fallback_operator: Optional[str],
    fallback_template: Optional[str],
) -> Optional[Dict[str, Any]]:
    # 文字列、リスト、辞書、無効値という複数の設定形式を共通形式へ変換する
    def coerce(settings: Any) -> Any:
        if settings is None:
            return None
        if settings is False:
            return FACILITY_FILTER_DISABLED
        if isinstance(settings, str):
            return {"column": settings}
        if isinstance(settings, list):
            return {"column": settings}
        if isinstance(settings, dict):
            if settings.get("enabled") is False:
                return FACILITY_FILTER_DISABLED
            if "column" in settings and not settings.get("column"):
                return FACILITY_FILTER_DISABLED
            return settings
        return None

    merged: Dict[str, Any] = {}

    coerced_base = coerce(base)
    if coerced_base is FACILITY_FILTER_DISABLED:
        return None
    if coerced_base:
        merged.update({k: v for k, v in coerced_base.items() if v is not None})

    coerced_override = coerce(override)
    if coerced_override is FACILITY_FILTER_DISABLED:
        return None
    if coerced_override:
        merged.update({k: v for k, v in coerced_override.items() if v is not None})

    if not merged and fallback_column:
        merged["column"] = fallback_column

    if "column" not in merged or not merged.get("column"):
        return None

    if fallback_operator and "operator" not in merged:
        merged["operator"] = fallback_operator
    if fallback_template and "value_template" not in merged and "value" not in merged:
        merged["value_template"] = fallback_template

    if "operator" not in merged or not merged.get("operator"):
        merged["operator"] = "="

    value_tpl = merged.pop("value", None)
    if value_tpl is not None and "value_template" not in merged:
        merged["value_template"] = value_tpl
    if "value_template" not in merged or not merged.get("value_template"):
        merged["value_template"] = "{facility_id}"

    return merged


# 施設フィルタ設定からプレースホルダ付き SQL 条件句とパラメータを生成する
def render_facility_clause(
    filter_settings: Optional[Dict[str, Any]],
    facility: Optional[Dict[str, Any]],
) -> Tuple[Optional[sql.SQL], List[Any]]:
    if not filter_settings or not facility:
        return None, []

    column = filter_settings.get("column")
    if not column:
        return None, []

    operator_raw = (filter_settings.get("operator") or "=").strip().lower()
    value_template = filter_settings.get("value_template") or "{facility_id}"
    ctx = build_facility_template_context(facility)

    try:
        value = value_template.format(**ctx)
    except Exception as e:
        raise ValueError(f"value_template format error: {e}")

    op_map = {
        "=": "=",
        "==": "=",
        "eq": "=",
        "!=": "!=",
        "ne": "!=",
        "<>": "!=",
        "like": "LIKE",
        "ilike": "ILIKE",
    }

    if operator_raw in ("startswith", "prefix"):
        op_sql = "LIKE"
        value = f"{value}%"
    elif operator_raw in ("endswith", "suffix"):
        op_sql = "LIKE"
        value = f"%{value}"
    elif operator_raw in ("contains", "substring"):
        op_sql = "LIKE"
        value = f"%{value}%"
    else:
        op_sql = op_map.get(operator_raw, operator_raw.upper())

    clause = sql.SQL("{col} {op} %s").format(
        col=sql.Identifier(column),
        op=sql.SQL(op_sql),
    )
    return clause, [value]


# 設定された施設列と互換列から、重複のない検索候補一覧を作成する
def build_facility_column_candidates(column_setting: Any) -> List[str]:
    candidates: List[str] = []

    if isinstance(column_setting, str):
        candidates.extend([c.strip() for c in column_setting.split("|") if c and c.strip()])
    elif isinstance(column_setting, list):
        for col in column_setting:
            if col is None:
                continue
            col_str = str(col).strip()
            if col_str:
                candidates.append(col_str)

    if isinstance(column_setting, str):
        fallback = FACILITY_COLUMN_FALLBACKS.get(column_setting.strip())
        if fallback:
            candidates.append(fallback)

    seen = set()
    unique_candidates = []
    for col in candidates:
        if col not in seen:
            seen.add(col)
            unique_candidates.append(col)

    return unique_candidates


# 対象テーブルの実在列を確認し、使用可能な施設フィルタ列を確定する
def resolve_facility_filter_for_table(
    conn,
    schema: str,
    table: str,
    filter_settings: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not filter_settings:
        return None

    base_column = filter_settings.get("column")
    if not base_column:
        return None

    unique_candidates = build_facility_column_candidates(base_column)
    if not unique_candidates:
        return filter_settings

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            [schema, table],
        )
        existing = {row[0] for row in cur.fetchall()}

    for col in unique_candidates:
        if col in existing:
            resolved = dict(filter_settings)
            resolved["column"] = col
            return resolved

    return None


# 対象テーブルに当日分のレコードが存在するか、必要に応じ施設単位で確認する
def has_rows_for_today(
    conn,
    schema: str,
    table: str,
    date_col: str,
    today_jst: datetime,
    facility: Optional[Dict[str, Any]] = None,
    facility_filter: Optional[Dict[str, Any]] = None,
) -> bool:
    with conn.cursor() as cur:
        q = sql.SQL("SELECT 1 FROM {s}.{t} WHERE {c}::date = %s").format(
            s=sql.Identifier(schema), t=sql.Identifier(table), c=sql.Identifier(date_col)
        )
        params = [today_jst.date()]
        clause, clause_params = render_facility_clause(facility_filter, facility)
        if clause is not None:
            q = q + sql.SQL(" AND ") + clause
            params.extend(clause_params)
        q = q + sql.SQL(" LIMIT 1")
        cur.execute(q, params)
        return cur.fetchone() is not None


# 対象テーブルから最新の作成日時を取得し、必要に応じ施設で絞り込む
def latest_created_at(
    conn,
    schema: str,
    table: str,
    created_col: str,
    facility: Optional[Dict[str, Any]] = None,
    facility_filter: Optional[Dict[str, Any]] = None,
):
    with conn.cursor() as cur:
        q = sql.SQL("SELECT MAX({c}) FROM {s}.{t}").format(
            c=sql.Identifier(created_col), s=sql.Identifier(schema), t=sql.Identifier(table)
        )
        params = []
        clause, clause_params = render_facility_clause(facility_filter, facility)
        if clause is not None:
            q = q + sql.SQL(" WHERE ") + clause
            params.extend(clause_params)
        cur.execute(q, params)
        row = cur.fetchone()
        return row[0] if row else None

# 指定した S3 プレフィックス配下で、JST の当日に更新されたファイル数を数える
def count_s3_files_today(s3_cli, bucket: str, prefix: str, today_jst: datetime) -> int:
    jst_start = datetime(today_jst.year, today_jst.month, today_jst.day, tzinfo=today_jst.tzinfo)
    jst_end = jst_start + timedelta(days=1)
    utc_start, utc_end = jst_start.astimezone(timezone.utc), jst_end.astimezone(timezone.utc)
    paginator = s3_cli.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            lm = obj["LastModified"]
            if utc_start <= lm <= utc_end:
                total += 1
    return total


# 宿別設定、環境変数、共通設定の優先順で Slack Webhook URL を解決する
def resolve_slack_webhook(defaults: dict, prop: Optional[dict] = None) -> Optional[str]:
    prop = prop or {}
    return (
        (prop.get("slack") or {}).get("webhook_url")
        or os.environ.get(SLACK_WEBHOOK_ENV_VAR)
        or (defaults.get("slack") or {}).get("webhook_url")
    )


# 宿の直接指定または共通テンプレートから DB 接続文字列を解決する
def resolve_dsn(defaults: dict, prop: dict) -> Optional[str]:
    db = prop.get("db", {}) or {}
    dsn = db.get("dsn")
    if dsn:
        return dsn

    hotel_key = prop.get("hotel_key")
    dsn_template = (
        os.environ.get(DB_DSN_TEMPLATE_ENV_VAR)
        or ((defaults.get("db") or {}).get("dsn_template"))
    )
    if dsn_template and hotel_key:
        return dsn_template.format(hotel_key=hotel_key)
    return None

# 一つの宿について有効な DB・S3 監視を実行し、正常結果とエラーを集約する
def run_checks_for_property(prop: dict, defaults: dict, tz: ZoneInfo, today: datetime) -> dict:
    name = prop.get("name", "UNKNOWN")
    enabled = prop.get("enabled", True)
    results = {"property": name, "errors": [], "ok": [], "_slack_webhook": None}
    if not enabled:
        results["ok"].append("宿設定が無効です。")
        return results
    slack_webhook = resolve_slack_webhook(defaults, prop)
    results["_slack_webhook"] = slack_webhook
    s3_defaults = defaults.get("s3") or {}
    bucket = s3_defaults.get("bucket")
    prefix_tpl = s3_defaults.get("prefix_template", "{hotel_key}/pms-reservations/")
    require_min_default = int(s3_defaults.get("require_min_files", 1))
    hotel_key = prop.get("hotel_key")
    db = prop.get("db", {}) or {}
    dsn = resolve_dsn(defaults, prop)
    schema = db.get("schema", "public")
    checks_default = defaults.get("checks") or {}
    checks_prop = prop.get("checks") or {}

    # 共通チェック設定を土台に宿別設定を上書きし、実行用設定を作成する
    checks: Dict[str, Any] = {}
    for key in set(checks_default.keys()) | set(checks_prop.keys()):
        default_cfg = checks_default.get(key)
        prop_cfg = checks_prop.get(key)

        if isinstance(default_cfg, dict):
            merged_cfg = copy.deepcopy(default_cfg)
        else:
            merged_cfg = {}

        if isinstance(prop_cfg, dict):
            merged_cfg.update(prop_cfg)
        elif prop_cfg is not None:
            merged_cfg["enabled"] = prop_cfg

        default_disabled = False
        if isinstance(default_cfg, dict):
            default_disabled = default_cfg.get("enabled") is False
        elif default_cfg is False:
            default_disabled = True

        if default_disabled:
            merged_cfg["enabled"] = False
        elif "enabled" not in merged_cfg and isinstance(default_cfg, dict) and "enabled" in default_cfg:
            merged_cfg["enabled"] = default_cfg.get("enabled")

        checks[key] = merged_cfg
    facilities = prop.get("facilities") or []

    # 結果メッセージに表示する施設名または施設識別子を取得する
    def facility_label(fac: Optional[dict]) -> str:
        if not fac:
            return ""
        name = fac.get("name")
        if name:
            return name
        return str(fac["code"])

    # ① インポート対象テーブルに当日分のデータがあるかを確認する
    if (checks.get("import_tables") or {}).get("enabled", False):
        if not dsn:
            results["errors"].append("① DB接続情報(dsn)未設定のためチェック不可。")
        else:
            try:
                with connect_db(dsn) as conn:
                    cfg_import = checks.get("import_tables") or {}
                    tables = cfg_import.get("tables", [])
                    default_col = cfg_import.get("date_column", "import_date")
                    default_facility_filter = normalize_facility_filter_settings(
                        base=cfg_import.get("facility_filter"),
                        override=None,
                        fallback_column=cfg_import.get("facility_column"),
                        fallback_operator=cfg_import.get("facility_operator"),
                        fallback_template=cfg_import.get("facility_value_template"),
                    )
                    default_facility_column = (
                        (default_facility_filter or {}).get("column")
                        or cfg_import.get("facility_column")
                    )
                    default_facility_operator = (
                        (default_facility_filter or {}).get("operator")
                        or cfg_import.get("facility_operator")
                    )
                    default_facility_template = (
                        (default_facility_filter or {}).get("value_template")
                        or cfg_import.get("facility_value_template")
                    )

                    # 一つのテーブル・施設の組み合わせについて当日レコードを検査する
                    def run_import_check(
                        table_name: str,
                        date_col: str,
                        fac: Optional[dict],
                        fac_filter: Optional[Dict[str, Any]],
                    ):
                        fac_label = facility_label(fac)
                        prefix = f"① {table_name}"
                        if fac_label:
                            prefix = f"{prefix} [{fac_label}]"
                        try:
                            resolved_fac_filter = resolve_facility_filter_for_table(
                                conn,
                                schema,
                                table_name,
                                fac_filter,
                            )

                            if fac_filter and not resolved_fac_filter:
                                requested_col = fac_filter.get("column")
                                results["ok"].append(
                                    f"{prefix}: 施設フィルタ列({requested_col})が存在しないためスキップ。"
                                )
                                return

                            filter_candidates: List[Optional[Dict[str, Any]]] = [resolved_fac_filter]
                            if resolved_fac_filter:
                                dynamic_candidates = build_facility_column_candidates(
                                    resolved_fac_filter.get("column")
                                )
                                if len(dynamic_candidates) <= 1:
                                    dynamic_candidates = build_facility_column_candidates(
                                        (fac_filter or {}).get("column")
                                    )
                                for col in dynamic_candidates:
                                    if col == resolved_fac_filter.get("column"):
                                        continue
                                    cand = dict(resolved_fac_filter)
                                    cand["column"] = col
                                    filter_candidates.append(cand)

                            last_exception = None
                            ok = False
                            executed = False
                            for cand_filter in filter_candidates:
                                try:
                                    executed = True
                                    ok = has_rows_for_today(
                                        conn,
                                        schema,
                                        table_name,
                                        date_col,
                                        today,
                                        facility=fac,
                                        facility_filter=cand_filter,
                                    )
                                    break
                                except psycopg2.errors.UndefinedColumn as e:
                                    last_exception = e
                                    continue

                            if last_exception and not ok:
                                results["ok"].append(
                                    f"{prefix}: 施設列不一致のためスキップ（候補: {(fac_filter or {}).get('column')}）"
                                )
                                return

                            if not executed:
                                results["ok"].append(f"{prefix}: 施設フィルタ未設定のためスキップ。")
                                return

                            if ok:
                                results["ok"].append(f"{prefix}: OK")
                            else:
                                results["errors"].append(
                                    f"{prefix}: {date_col} が『今日』のレコード無し"
                                )
                        except Exception as e:
                            results["errors"].append(
                                f"{prefix}: チェック失敗（{e.__class__.__name__}: {e}）"
                            )

                    # テーブルごとの新旧設定形式を解釈し、施設単位または宿単位で検査する
                    for t in tables:
                        tname = None
                        try:
                            table_facility_filter = default_facility_filter
                            tcol = default_col
                            inline_override: Dict[str, Any] = {}
                            has_inline_override = False
                            if isinstance(t, dict):
                                tname = t.get("name")
                                tcol = t.get("date_column", default_col)
                                if "facility_column" in t:
                                    inline_override["column"] = t.get("facility_column")
                                    has_inline_override = True
                                if "facility_operator" in t:
                                    inline_override["operator"] = t.get("facility_operator")
                                    has_inline_override = True
                                if "facility_value_template" in t:
                                    inline_override["value_template"] = t.get(
                                        "facility_value_template"
                                    )
                                    has_inline_override = True

                                table_facility_filter = normalize_facility_filter_settings(
                                    base=default_facility_filter,
                                    override=inline_override if has_inline_override else None,
                                    fallback_column=(
                                        inline_override.get("column")
                                        if has_inline_override
                                        and inline_override.get("column") is not None
                                        else default_facility_column
                                    ),
                                    fallback_operator=(
                                        inline_override.get("operator")
                                        if has_inline_override
                                        and inline_override.get("operator") is not None
                                        else default_facility_operator
                                    ),
                                    fallback_template=(
                                        inline_override.get("value_template")
                                        if has_inline_override
                                        and inline_override.get("value_template") is not None
                                        else default_facility_template
                                    ),
                                )

                                if "facility_filter" in t:
                                    table_facility_filter = normalize_facility_filter_settings(
                                        base=table_facility_filter,
                                        override=t.get("facility_filter"),
                                        fallback_column=(
                                            (table_facility_filter or {}).get("column")
                                            or inline_override.get("column")
                                            if has_inline_override
                                            else default_facility_column
                                        ),
                                        fallback_operator=(
                                            (table_facility_filter or {}).get("operator")
                                            or inline_override.get("operator")
                                            if has_inline_override
                                            else default_facility_operator
                                        ),
                                        fallback_template=(
                                            (table_facility_filter or {}).get("value_template")
                                            or inline_override.get("value_template")
                                            if has_inline_override
                                            else default_facility_template
                                        ),
                                    )
                            else:
                                tname = str(t)
                                tcol = default_col

                            if not tname:
                                results["errors"].append("① テーブル名未設定のエントリが存在します。")
                                continue
                            if facilities and table_facility_filter:
                                for fac in facilities:
                                    if not fac.get("enabled", True):
                                        results["ok"].append(
                                            f"① {tname} [{facility_label(fac)}]: 施設設定が無効のためスキップ。"
                                        )
                                        continue
                                    run_import_check(tname, tcol, fac, table_facility_filter)
                            else:
                                run_import_check(
                                    tname,
                                    tcol,
                                    None,
                                    table_facility_filter if not facilities else None,
                                )
                        except Exception as e:
                            if not tname:
                                if isinstance(t, dict):
                                    tname = t.get("name") or "UNKNOWN"
                                else:
                                    tname = str(t) or "UNKNOWN"
                            results["errors"].append(
                                f"① {tname}: 設定処理失敗（{e.__class__.__name__}: {e}）"
                            )
            except Exception as e:
                results["errors"].append(f"① DB接続失敗（{e.__class__.__name__}: {e}）")
    # ② 当日更新された S3 ファイル数が必要件数を満たすか確認する
    if (checks.get("s3_uploads") or {}).get("enabled", False):
        if not bucket: results["errors"].append("② S3: bucket が未設定です。")
        if not hotel_key: results["errors"].append("② S3: hotel_key が未設定です。")
        if bucket and hotel_key:
            try:
                require_min = int((checks.get("s3_uploads") or {}).get("require_min_files", require_min_default))
                prefix = prefix_tpl.format(hotel_key=hotel_key)
                s3_cli = boto3.client("s3")
                cnt = count_s3_files_today(s3_cli, bucket, prefix, today)
                if cnt < require_min:
                    results["errors"].append(f"② S3: 今日({today.strftime('%Y-%m-%d')})のLastModified件数 {cnt} < 必要 {require_min} (bucket={bucket}, prefix={prefix})")
                else:
                    results["ok"].append(f"② S3: OK（{cnt}件）")
            except Exception as e:
                results["errors"].append(f"② S3チェック失敗（{e.__class__.__name__}: {e}）")
    # ③ repeat_track_tags の最終更新から許容日数以上経過していないか確認する
    if (checks.get("repeat_track_tags_stall") or {}).get("enabled", False):
        if not dsn:
            results["errors"].append("③ DB接続情報(dsn)未設定のためチェック不可。")
        else:
            try:
                with connect_db(dsn) as conn:
                    tcfg = checks.get("repeat_track_tags_stall") or {}
                    table = tcfg.get("table", "repeat_track_tags")
                    col = tcfg.get("created_at_column", "created_at")
                    max_days = int(tcfg.get("max_stall_days", 3))
                    # 最新作成日時を現在時刻と比較し、更新停止の有無を判定する
                    def run_repeat_check():
                        prefix = f"③ {table}"
                        latest = latest_created_at(
                            conn,
                            schema,
                            table,
                            col,
                            facility=None,
                            facility_filter=None,
                        )
                        if latest is None:
                            results["errors"].append(f"{prefix}: データ無し（MAX({col})がNULL）")
                            return
                        if latest.tzinfo is None:
                            latest_local = latest.replace(tzinfo=tz)
                        else:
                            latest_local = latest.astimezone(tz)
                        now_jst = datetime.now(tz)
                        diff_days = (now_jst - latest_local).days
                        if diff_days >= max_days:
                            results["errors"].append(
                                f"{prefix}: 最終作成 {latest_local.strftime('%Y-%m-%d %H:%M:%S %Z')} / {max_days}日以上更新無し"
                            )
                        else:
                            results["ok"].append(
                                f"{prefix}: OK（最終 {latest_local.strftime('%Y-%m-%d %H:%M')}）"
                            )

                    run_repeat_check()
            except Exception as e:
                results["errors"].append(f"③ DB照会失敗（{e.__class__.__name__}: {e}）")
    return results

# YAML 設定を UTF-8 で読み込む
def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8-sig") as f:
        return yaml.safe_load(f) or {}


# 設定読込、各種実行モード、宿ごとの監視、Slack 通知を統括する
def run_monitor(
    config_path: str = "config.yaml",
    dry_run: bool = False,
    raise_on_monitor_error: bool = False,
) -> dict:
    print("START")
    try:
        cfg = load_config(config_path)
    except Exception as e:
        print(f"CONFIG_LOAD_ERROR: {e}")
        print("RESULT: ERROR")
        if raise_on_monitor_error:
            raise
        return {"result": "ERROR", "exit_code": 1, "error": f"CONFIG_LOAD_ERROR: {e}"}

    defaults = cfg.get("defaults") or {}
    props = cfg.get("properties") or []
    tz = ZoneInfo(defaults.get("timezone", "Asia/Tokyo"))
    today = datetime.now(tz)
    jst_today_str = today.strftime("%Y-%m-%d")
    global_slack = resolve_slack_webhook(defaults)
    print(f"PROPERTIES: {len(props)}")

    # 通常監視モードでは全宿のチェック結果とエラー有無を集約する
    all_results, any_error = [], False
    for p in props:
        res = run_checks_for_property(p, defaults, tz, today)
        all_results.append(res)
        if res["errors"]:
            any_error = True

    # ドライランでは通知せず、検査結果と終了コードだけを返す
    if dry_run:
        result = "ERROR" if any_error else "OK"
        print(json.dumps(all_results, ensure_ascii=False, indent=2))
        print(f"RESULT: {result}")
        if any_error and raise_on_monitor_error:
            raise RuntimeError("monitor dry-run detected errors")
        return {"result": result, "exit_code": 1 if any_error else 0, "results": all_results}

    # エラー発生時は Webhook ごとに対象宿をまとめてアラートを送信する
    if any_error:
        url_to_subset = {}
        for r in all_results:
            if not r["errors"]:
                continue
            url = r.get("_slack_webhook") or global_slack
            if not url or not isinstance(url, str) or not url.startswith("https://hooks.slack.com/services/"):
                logging.error(f"Slack Webhook不正のため送信不可: {r['property']}")
                continue
            url_to_subset.setdefault(url, []).append(r)
        sent_any = False
        header_text = f"データ監視アラート（{jst_today_str}）"
        for url, subset in url_to_subset.items():
            blocks = build_slack_blocks(subset, jst_today_str)
            if not blocks:
                continue
            try:
                send_slack_batches(url, header_text, blocks, max_blocks=40)
                sent_any = True
            except Exception as e:
                logging.error(f"Slack送信失敗: {e}")
        result = "ERROR_SENT" if sent_any else "ERROR_NO_SLACK"
        print(json.dumps(all_results, ensure_ascii=False, indent=2))
        print(f"RESULT: {result}")
        if raise_on_monitor_error:
            raise RuntimeError(result)
        return {"result": result, "exit_code": 1, "results": all_results, "slack_sent": sent_any}

    # 全件正常時は重複を除いた各 Webhook へ正常通知を送信する
    ok_urls = set()
    for r in all_results:
        url = r.get("_slack_webhook") or global_slack
        if not url or not isinstance(url, str) or not url.startswith("https://hooks.slack.com/services/"):
            continue
        ok_urls.add(url)
    header_text = f"データ監視アラート（{jst_today_str}）"
    ok_blocks = build_ok_slack_blocks(jst_today_str)
    for url in ok_urls:
        try:
            send_slack_batches(url, header_text, ok_blocks, max_blocks=40)
        except Exception as e:
            logging.error(f"Slack送信失敗(OK通知): {e}")
    print(json.dumps(all_results, ensure_ascii=False, indent=2))
    print("RESULT: OK")
    return {"result": "OK", "exit_code": 0, "results": all_results}


# Lambda のイベントと環境変数から実行条件を組み立て、監視処理を呼び出す
def lambda_handler(event, context):
    event = event or {}
    mode = event.get("mode", os.environ.get("MONITOR_MODE", "monitor"))
    config_path = event.get("config_path") or os.environ.get("CONFIG_PATH") or os.path.join(
        os.path.dirname(__file__),
        "config.yaml",
    )
    response = run_monitor(
        config_path=config_path,
        dry_run=bool(event.get("dry_run") or mode == "dry_run"),
        raise_on_monitor_error=os.environ.get("RAISE_ON_MONITOR_ERROR", "").lower() in ("1", "true", "yes"),
    )
    return response


# コマンドライン引数とログ設定を解釈し、ローカル実行時の終了コードを返す
def main():
    parser = argparse.ArgumentParser(description="宿ごとのデータ監視")
    parser.add_argument("-c", "--config", default="config.yaml")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--log-file", default="")
    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if args.log_file:
        fh = logging.FileHandler(args.log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logging.getLogger().addHandler(fh)
    result = run_monitor(
        config_path=args.config,
        dry_run=args.dry_run,
    )
    return int(result.get("exit_code", 1))

# スクリプトとして起動された場合のみ CLI を実行し、未処理例外を標準出力へ記録する
if __name__ == "__main__":
    try:
        code = main()
        sys.stdout.flush(); sys.stderr.flush()
        raise SystemExit(code)
    except SystemExit as e:
        raise
    except Exception as e:
        print(f"UNCAUGHT: {e}")
        traceback.print_exc()
        print("RESULT: ERROR")
        raise SystemExit(1)
