#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
import psycopg2
import yaml
from psycopg2 import sql

os.environ.setdefault("PGCLIENTENCODING", "utf8")


# Slack Incoming Webhook へ監視結果を送信する
# Block Kit の分割処理は行わず、通知本文を一つの text として送る
# 送信失敗は Lambda のエラーとしてそのまま呼び出し元へ伝える
def post_to_slack(webhook_url: str, text: str):
    payload = json.dumps({"text": text}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(webhook_url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


# RDS IAM 認証トークンを生成し、SSL を使用して PostgreSQL へ接続する
def connect_db(dsn: str):
    client = boto3.client("rds")
    token = client.generate_db_auth_token(
        DBHostname=os.environ["DB_HOST"],
        Port=int(os.environ["DB_PORT"]),
        DBUsername=os.environ["DB_USER"],
        Region=os.environ["AWS_REGION"],
    )
    connection = psycopg2.connect(
        dsn,
        password=token,
        sslmode="require",
        sslrootcert="global-bundle.pem",
    )
    connection.set_client_encoding("UTF8")
    return connection


# 宿別 DSN がなければ、従来どおり hotel_key を共通テンプレートへ埋め込む
def resolve_dsn(prop: dict, defaults: dict) -> str:
    direct_dsn = (prop.get("db") or {}).get("dsn")
    if direct_dsn:
        return direct_dsn
    template = os.environ.get("DB_DSN_TEMPLATE") or (defaults.get("db") or {}).get("dsn_template")
    if not template or not prop.get("hotel_key"):
        raise ValueError("db.dsn または DB_DSN_TEMPLATE と hotel_key を設定してください")
    return template.format(hotel_key=prop["hotel_key"])


# 対象テーブルに当日分のレコードが存在するか確認する
def has_rows_for_today(conn, schema: str, table: str, date_column: str, today: datetime) -> bool:
    with conn.cursor() as cursor:
        query = sql.SQL("SELECT 1 FROM {schema}.{table} WHERE {column}::date = %s LIMIT 1").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            column=sql.Identifier(date_column),
        )
        cursor.execute(query, [today.date()])
        return cursor.fetchone() is not None


# 対象テーブルから最新の作成日時を取得する
def latest_created_at(conn, schema: str, table: str, created_column: str):
    with conn.cursor() as cursor:
        query = sql.SQL("SELECT MAX({column}) FROM {schema}.{table}").format(
            column=sql.Identifier(created_column),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )
        cursor.execute(query)
        row = cursor.fetchone()
        return row[0] if row else None


# 指定した S3 プレフィックス配下で、JST の当日に更新されたファイル数を数える
def count_s3_files_today(s3_client, bucket: str, prefix: str, today: datetime) -> int:
    day_start = datetime(today.year, today.month, today.day, tzinfo=today.tzinfo)
    utc_start = day_start.astimezone(timezone.utc)
    utc_end = (day_start + timedelta(days=1)).astimezone(timezone.utc)
    paginator = s3_client.get_paginator("list_objects_v2")
    return sum(
        utc_start <= item["LastModified"] < utc_end
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get("Contents", [])
    )


# 一つの宿について、設定で有効になっている監視だけを実行する
# 正常時の詳細は保持せず、Slack 通知に必要なエラーだけを返す
def run_checks_for_property(prop: dict, defaults: dict, today: datetime) -> list[str]:
    if not prop.get("enabled", True):
        return []

    name = prop.get("name", "UNKNOWN")
    db = prop.get("db") or {}
    schema = db.get("schema", "public")
    default_checks = defaults.get("checks") or {}
    property_checks = prop.get("checks") or {}
    checks = {
        key: {**(default_checks.get(key) or {}), **(property_checks.get(key) or {})}
        for key in default_checks.keys() | property_checks.keys()
    }
    errors = []

    # ① 各インポートテーブルに当日分のデータがあるか確認する
    import_config = checks.get("import_tables") or {}
    if import_config.get("enabled", False):
        try:
            dsn = resolve_dsn(prop, defaults)
            with connect_db(dsn) as connection:
                default_column = import_config.get("date_column", "import_date")
                for table_config in import_config.get("tables", []):
                    if isinstance(table_config, dict):
                        table = table_config["name"]
                        date_column = table_config.get("date_column", default_column)
                    else:
                        table = table_config
                        date_column = default_column
                    if not has_rows_for_today(connection, schema, table, date_column, today):
                        errors.append(f"① {name} / {table}: {date_column} が今日のレコード無し")
        except Exception as error:
            errors.append(f"① {name}: DBチェック失敗（{error.__class__.__name__}: {error}）")

    # ② 当日更新された S3 ファイル数が必要件数を満たすか確認する
    s3_config = defaults.get("s3") or {}
    upload_config = checks.get("s3_uploads") or {}
    if upload_config.get("enabled", False):
        try:
            hotel_key = prop["hotel_key"]
            bucket = s3_config["bucket"]
            prefix = s3_config.get("prefix_template", "{hotel_key}/pms-reservations/").format(
                hotel_key=hotel_key
            )
            required = int(upload_config.get("require_min_files", s3_config.get("require_min_files", 1)))
            count = count_s3_files_today(boto3.client("s3"), bucket, prefix, today)
            if count < required:
                errors.append(f"② {name} / S3: 今日のファイル数 {count} < 必要 {required}")
        except Exception as error:
            errors.append(f"② {name}: S3チェック失敗（{error.__class__.__name__}: {error}）")

    # ③ repeat_track_tags の最終更新から許容日数以上経過していないか確認する
    repeat_config = checks.get("repeat_track_tags_stall") or {}
    if repeat_config.get("enabled", False):
        try:
            dsn = resolve_dsn(prop, defaults)
            with connect_db(dsn) as connection:
                table = repeat_config.get("table", "repeat_track_tags")
                column = repeat_config.get("created_at_column", "created_at")
                max_days = int(repeat_config.get("max_stall_days", 3))
                latest = latest_created_at(connection, schema, table, column)
                if latest is None:
                    errors.append(f"③ {name} / {table}: データ無し")
                else:
                    latest_local = latest.replace(tzinfo=today.tzinfo) if latest.tzinfo is None else latest.astimezone(today.tzinfo)
                    if (today - latest_local).days >= max_days:
                        errors.append(f"③ {name} / {table}: {max_days}日以上更新無し")
        except Exception as error:
            errors.append(f"③ {name}: DBチェック失敗（{error.__class__.__name__}: {error}）")

    return errors


# YAML 設定を UTF-8 で読み込む
def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file) or {}


# 全宿の通常監視を実行し、エラー一覧または正常メッセージを Slack へ一度だけ送る
def run_monitor(config_path: str = "config.yaml") -> dict:
    config = load_config(config_path)
    defaults = config.get("defaults") or {}
    today = datetime.now(ZoneInfo(defaults.get("timezone", "Asia/Tokyo")))
    errors = [
        error
        for prop in config.get("properties", [])
        for error in run_checks_for_property(prop, defaults, today)
    ]

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL") or (defaults.get("slack") or {}).get("webhook_url")
    date = today.strftime("%Y-%m-%d")
    message = f"データ監視アラート（{date}）\n" + ("\n".join(errors) if errors else "データは正常に取得されています。")
    post_to_slack(webhook_url, message)
    return {"result": "ERROR" if errors else "OK", "errors": errors}


# Lambda の設定ファイル位置を解決し、通常監視を実行する
def lambda_handler(event, context):
    event = event or {}
    config_path = event.get("config_path") or os.environ.get("CONFIG_PATH") or os.path.join(
        os.path.dirname(__file__),
        "config.yaml",
    )
    return run_monitor(config_path)
