"""
Archive Lambda: exports DynamoDB recommendation history to S3 in human-readable form.

DynamoDB: Same table as get_history (TABLE_NAME, default T2D).
  Partition key: userID (String), Sort key: timestamp (String, ISO 8601).
  Items: userID, timestamp, request, response (optional: feedback, patientSummary, ttl).

S3: Writes to bucket ARCHIVE_S3_BUCKET (e.g. user-log-archive).
  Prefix per user: {userID}/archive/  (each user has their own folder).
  One file per record: {userID}/archive/{timestamp}.json (recommendationTimestamp when present, else timestamp; sanitized for S3 key). Human-readable pretty JSON.
  Duplicate prevention: one LIST per user to load existing key suffixes; skips writing if key already exists (dedup by recommendationTimestamp/timestamp). No per-file HEAD calls.

Event (optional). Supports EventBridge (payload in event.detail), API Gateway (event.body), or direct invoke.
  Always archives ALL users; userID in event is ignored. Each user's records go to {userID}/archive/.
  - limit: max items per user (default 500). Omit for all.
  - archiveOlderThanDays: if set (e.g. 14), only archive records older than N days (per user).

Env:
  - TABLE_NAME: DynamoDB table (default T2D).
  - ARCHIVE_S3_BUCKET: S3 bucket name (e.g. user-log-archive).
"""
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = Exception


def _log(msg):
    print(msg)
    sys.stdout.flush()


def _to_native(obj):
    """Convert DynamoDB types (Decimal) to native JSON-serializable types."""
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        f = float(obj)
        return int(f) if f == int(f) else f
    if isinstance(obj, dict):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_native(v) for v in obj]
    return obj


def _parse_response_body(body):
    """Parse response body if stored as JSON string."""
    if body is None:
        return None
    if isinstance(body, str):
        try:
            return json.loads(body) if body else None
        except Exception:
            return body
    return body


def _format_item(item):
    """One record: id, timestamp, request, response (native types, response.body parsed)."""
    user_id = item.get("userID", "")
    timestamp = item.get("timestamp", "")
    request_data = _to_native(item.get("request") or {})
    response_data = _to_native(item.get("response") or {})
    body = response_data.get("body")
    if body is not None:
        response_data["body"] = _parse_response_body(body) if isinstance(body, str) else body
    ts_safe = (timestamp or "")[:19].replace(":", "-").replace(".", "-")
    record_id = f"{user_id}_{ts_safe}" if ts_safe else f"{user_id}_{len(request_data)}"
    return {
        "id": record_id,
        "timestamp": timestamp,
        "request": request_data,
        "response": response_data,
    }


def _timestamp_to_s3_key(timestamp):
    """Safe S3 key segment from ISO timestamp (e.g. 2026-02-12T13-47-49)."""
    if not timestamp or not isinstance(timestamp, str):
        return "record"
    return timestamp[:19].replace(":", "-").replace(".", "-")


def _is_older_than_days(iso_timestamp, days):
    """True if iso_timestamp is older than `days` ago (UTC)."""
    if not iso_timestamp or not isinstance(iso_timestamp, str):
        return False
    try:
        # Parse ISO format (with or without Z / timezone)
        ts = iso_timestamp.strip().replace("Z", "+00:00")
        if "+" not in ts and "-" in ts[-6:]:
            pass
        elif "+" not in ts:
            ts = ts + "+00:00"
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        return dt < cutoff
    except Exception:
        return False


def _sanitize_folder_name(name):
    """Safe for S3 key prefix: alphanumeric, hyphen, underscore only."""
    if not name or not isinstance(name, str):
        return "export"
    s = re.sub(r"[^\w\-]", "_", name.strip())[:64]
    return s or "export"


def _get_recommendation_timestamp(item):
    """Get recommendationTimestamp from item (response.body or response), fallback to None."""
    try:
        resp = item.get("response") or {}
        body = resp.get("body")
        if isinstance(body, str):
            body = json.loads(body) if body else {}
        if isinstance(body, dict):
            ts = body.get("recommendationTimestamp") or body.get("recommendation_timestamp")
            if ts and isinstance(ts, str):
                return ts.strip()
        ts = resp.get("recommendationTimestamp") or resp.get("recommendation_timestamp")
        if ts and isinstance(ts, str):
            return ts.strip()
    except Exception:
        pass
    return None


def _list_existing_s3_suffixes(s3_client, bucket, prefix):
    """
    List all object keys under prefix (paginated). Returns set of key suffixes (the part after prefix).
    One LIST per user instead of many HEADs; used to skip records already in S3 (dedup by key).
    """
    out = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            k = obj.get("Key") or ""
            if k.startswith(prefix):
                out.add(k[len(prefix) :])
    return out


def _get_user_ids_from_table(table):
    """Scan table and return distinct userIDs (for full archive)."""
    user_ids = set()
    scan_kw = {}
    while True:
        resp = table.scan(ProjectionExpression="userID", **scan_kw)
        for item in resp.get("Items", []):
            uid = item.get("userID")
            if uid:
                user_ids.add(str(uid))
        next_key = resp.get("LastEvaluatedKey")
        if not next_key:
            break
        scan_kw["ExclusiveStartKey"] = next_key
    return sorted(user_ids)


def handler(event, context):
    """
    Archive DynamoDB history to S3 for ALL users. One file per record: {userID}/archive/{timestamp}.json (human-readable JSON).
    Event: { limit?: number, archiveOlderThanDays?: number }. userID is ignored; always archives every user.
    """
    if not boto3:
        return _response(500, {"error": "boto3 not available"})

    table_name = os.environ.get("TABLE_NAME", "T2D")
    bucket = (os.environ.get("ARCHIVE_S3_BUCKET") or "").strip()
    if not bucket:
        return _response(500, {"error": "ARCHIVE_S3_BUCKET not set"})

    # Parse event: EventBridge puts payload in "detail"; API Gateway in "body"; direct invoke at top level
    ev = event.get("detail", event.get("body", event))
    if isinstance(ev, str):
        try:
            ev = json.loads(ev) if ev else {}
        except Exception:
            ev = {}
    if not isinstance(ev, dict):
        ev = {}

    limit = ev.get("limit")
    if limit is not None:
        try:
            limit = max(1, min(1000, int(limit)))
        except (TypeError, ValueError):
            limit = 500
    else:
        limit = 500
    archive_older_than_days = ev.get("archiveOlderThanDays")
    if archive_older_than_days is not None:
        try:
            archive_older_than_days = max(1, min(365, int(archive_older_than_days)))
        except (TypeError, ValueError):
            archive_older_than_days = None

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    s3 = boto3.client("s3")

    _log(f"Archive: all users from {table_name} -> {bucket}")
    user_ids = _get_user_ids_from_table(table)
    _log(f"Archive: found {len(user_ids)} userIDs")

    archived = []
    errors = []

    for user_id in user_ids:
        prefix = f"{user_id}/archive/"

        try:
            result = table.query(
                KeyConditionExpression="userID = :uid",
                ExpressionAttributeValues={":uid": str(user_id)},
                ScanIndexForward=False,
                Limit=limit,
            )
            items = result.get("Items", [])
            if archive_older_than_days:
                items = [it for it in items if _is_older_than_days(it.get("timestamp"), archive_older_than_days)]
                _log(f"Archive: user {user_id} records older than {archive_older_than_days} days: {len(items)} to archive")
            # One LIST per user to get existing keys (no HEAD per record); dedup by recommendationTimestamp when present
            existing_suffixes = _list_existing_s3_suffixes(s3, bucket, prefix)
            seen = {}
            file_count = 0
            skipped_count = 0
            for it in items:
                record = _format_item(it)
                # Prefer recommendationTimestamp so we dedup by recommendation identity, not just DB timestamp
                rec_ts = _get_recommendation_timestamp(it)
                ts = rec_ts or it.get("timestamp") or ""
                base = _timestamp_to_s3_key(ts)
                n = seen.get(base, 0)
                seen[base] = n + 1
                key_suffix = f"{base}.json" if n == 0 else f"{base}_{n}.json"
                if key_suffix in existing_suffixes:
                    skipped_count += 1
                    continue
                key = f"{prefix}{key_suffix}"
                body = json.dumps(record, indent=2, default=str)
                s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
                existing_suffixes.add(key_suffix)
                file_count += 1
            archived.append({
                "userID": user_id,
                "prefix": prefix,
                "fileCount": file_count,
                "skipped": skipped_count,
            })
            _log(f"Archived {user_id} -> s3://{bucket}/{prefix}* (wrote {file_count}, skipped {skipped_count} existing)")
        except Exception as e:
            _log(f"Archive failed for {user_id}: {e}")
            errors.append({"userID": user_id, "error": str(e)})

    return _response(200, {
        "archived": archived,
        "errors": errors,
        "bucket": bucket,
    })


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }
#this is a test
