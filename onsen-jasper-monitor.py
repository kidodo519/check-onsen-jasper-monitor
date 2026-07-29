#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, yaml, boto3, psycopg2, urllib.request
from typing import Optional, Tuple, List, Dict, Any
from psycopg2 import sql
from datetime import datetime
from zoneinfo import ZoneInfo

os.environ.setdefault("PGCLIENTENCODING", "utf8")

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

FACILITY_COLUMN_FALLBACKS = {"facility_id": "facility_code", "facility_code": "facility_id"}


def build_facility_template_context(facility: Dict[str, Any]) -> Dict[str, Any]:
    return {**facility, "facility_id": facility["code"]}


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

    value_template = filter_settings.get("value_template") or "{facility_id}"
    value = value_template.format(**build_facility_template_context(facility))

    clause = sql.SQL("{col} {op} %s").format(
        col=sql.Identifier(column),
        op=sql.SQL("="),
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

# 一つの宿について DB 監視を実行し、Slack 通知用のエラーを返す
def run_checks_for_property(prop: dict, defaults: dict, tz: ZoneInfo, today: datetime) -> dict:
    result = {
        "property": prop.get("name", "UNKNOWN"),
        "errors": [],
        "_slack_webhook": resolve_slack_webhook(defaults, prop),
    }
    if not prop.get("enabled", True):
        return result

    dsn = resolve_dsn(defaults, prop)
    schema = (prop.get("db") or {}).get("schema", "public")
    default_checks = defaults.get("checks") or {}
    property_checks = prop.get("checks") or {}

    import_config = {
        **(default_checks.get("import_tables") or {}),
        **(property_checks.get("import_tables") or {}),
    }
    if import_config.get("enabled", False):
        try:
            check_import_tables(
                dsn, schema, import_config, prop.get("facilities") or [], today, result["errors"]
            )
        except Exception as exc:
            result["errors"].append(f"① DB接続失敗（{exc.__class__.__name__}: {exc}）")

    repeat_config = {
        **(default_checks.get("repeat_track_tags_stall") or {}),
        **(property_checks.get("repeat_track_tags_stall") or {}),
    }
    if repeat_config.get("enabled", False):
        try:
            check_repeat_track_tags(dsn, schema, repeat_config, tz, result["errors"])
        except Exception as exc:
            result["errors"].append(f"② DB照会失敗（{exc.__class__.__name__}: {exc}）")

    return result


def check_import_tables(
    dsn: str,
    schema: str,
    config: dict,
    facilities: list,
    today: datetime,
    errors: list,
):
    if not dsn:
        errors.append("① DB接続情報(dsn)未設定のためチェック不可。")
        return

    facility_filter = {
        "column": config.get("facility_column"),
        "value_template": config.get("facility_value_template", "{facility_id}"),
    }
    with connect_db(dsn) as conn:
        for table_config in config.get("tables", []):
            table = table_config if isinstance(table_config, str) else table_config["name"]
            date_column = config.get("date_column", "import_date")
            use_facility_filter = not (
                isinstance(table_config, dict)
                and table_config.get("facility_filter") is False
            )
            table_filter = (
                resolve_facility_filter_for_table(conn, schema, table, facility_filter)
                if use_facility_filter and facilities
                else None
            )
            if use_facility_filter and facilities and not table_filter:
                continue

            targets = facilities if table_filter else [None]
            for facility in targets:
                if facility and not facility.get("enabled", True):
                    continue
                try:
                    exists = has_rows_for_today(
                        conn,
                        schema,
                        table,
                        date_column,
                        today,
                        facility,
                        table_filter,
                    )
                except Exception as exc:
                    errors.append(
                        f"① {table}{facility_suffix(facility)}: "
                        f"チェック失敗（{exc.__class__.__name__}: {exc}）"
                    )
                    continue
                if not exists:
                    errors.append(
                        f"① {table}{facility_suffix(facility)}: "
                        f"{date_column} が『今日』のレコード無し"
                    )


def facility_suffix(facility: Optional[dict]) -> str:
    if not facility:
        return ""
    return f" [{facility.get('name') or facility['code']}]"


def check_repeat_track_tags(
    dsn: str,
    schema: str,
    config: dict,
    tz: ZoneInfo,
    errors: list,
):
    if not dsn:
        errors.append("② DB接続情報(dsn)未設定のためチェック不可。")
        return

    table = config.get("table", "repeat_track_tags")
    column = config.get("created_at_column", "created_at")
    max_days = int(config.get("max_stall_days", 3))
    with connect_db(dsn) as conn:
        latest = latest_created_at(conn, schema, table, column)

    if latest is None:
        errors.append(f"② {table}: データ無し（MAX({column})がNULL）")
        return
    latest_local = latest.replace(tzinfo=tz) if latest.tzinfo is None else latest.astimezone(tz)
    if (datetime.now(tz) - latest_local).days >= max_days:
        errors.append(
            f"② {table}: 最終作成 {latest_local.strftime('%Y-%m-%d %H:%M:%S %Z')} / "
            f"{max_days}日以上更新無し"
        )

# YAML 設定を UTF-8 で読み込む
def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8-sig") as f:
        return yaml.safe_load(f) or {}


# 全宿の監視を実行し、結果を Slack へ通知する
def run_monitor(config_path: str) -> None:
    cfg = load_config(config_path)
    defaults = cfg.get("defaults") or {}
    properties = cfg.get("properties") or []
    tz = ZoneInfo(defaults.get("timezone", "Asia/Tokyo"))
    today = datetime.now(tz)
    today_text = today.strftime("%Y-%m-%d")
    global_slack = resolve_slack_webhook(defaults)

    results = [
        run_checks_for_property(prop, defaults, tz, today)
        for prop in properties
    ]
    errors = [result for result in results if result["errors"]]
    header_text = f"データ監視アラート（{today_text}）"

    if errors:
        results_by_webhook = {}
        for result in errors:
            webhook = result.get("_slack_webhook") or global_slack
            results_by_webhook.setdefault(webhook, []).append(result)
        for webhook, webhook_results in results_by_webhook.items():
            send_slack_batches(
                webhook,
                header_text,
                build_slack_blocks(webhook_results, today_text),
            )
    else:
        webhooks = {
            result.get("_slack_webhook") or global_slack
            for result in results
        }
        blocks = build_ok_slack_blocks(today_text)
        for webhook in webhooks:
            send_slack_batches(webhook, header_text, blocks)



# Lambda の定期実行エントリーポイント
def lambda_handler(_event, _context) -> None:
    config_path = os.environ.get("CONFIG_PATH") or os.path.join(
        os.path.dirname(__file__),
        "config.yaml",
    )
    run_monitor(config_path)
