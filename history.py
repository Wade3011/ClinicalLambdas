"""
Get History Lambda: fetches recommendation history from DynamoDB for a user.
Table: T2D (or TABLE_NAME env var).
Partition key: userID (String), Sort key: timestamp (String, ISO 8601).

Returns the 15 most recent logs:
  { history: [ { id, timestamp, request, response }, ... ] }
"""
import json
import os
import re

from decimal import Decimal

try:
    import boto3
except ImportError:
    boto3 = None


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


def _truncate_timestamp_for_id(ts):
    """Truncate ISO timestamp to YYYY-MM-DDTHH:MM:SSZ (no fractional seconds)."""
    if not ts or not isinstance(ts, str):
        return ts or ""
    m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", ts)
    return f"{m.group(1)}Z" if m else ts


def _extract_glucose_averages(request_data):
    """Extract fasting and post-prandial averages for display."""
    gr = (request_data or {}).get("glucoseReadings") or {}
    fp = gr.get("fingerPokeData") or {}
    cgm = gr.get("cgmData") or gr
    fasting = fp.get("fastingAverage") or cgm.get("wakeUpAverage")
    post_pp = fp.get("postPrandialAverage") or cgm.get("bedtimeAverage")
    if fasting is not None or post_pp is not None:
        return {"fastingAverage": fasting, "postPrandialAverage": post_pp}
    fasting_obj = gr.get("fasting") or {}
    post_pp_obj = gr.get("postPrandial") or gr.get("post_prandial") or {}
    fasting = fasting_obj.get("average") if isinstance(fasting_obj, dict) else None
    post_pp = post_pp_obj.get("average") if isinstance(post_pp_obj, dict) else None
    if fasting is not None or post_pp is not None:
        return {"fastingAverage": fasting, "postPrandialAverage": post_pp}
    return None


def _form_data_with_glucose_averages(request_data):
    """Pass through request and add glucoseAverages."""
    if not request_data or not isinstance(request_data, dict):
        return {}
    out = dict(request_data)
    avgs = _extract_glucose_averages(request_data)
    if avgs:
        out["glucoseAverages"] = _to_native(avgs)
    return _to_native(out)


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


def _format_history_item(item):
    """Format: { id, timestamp, request, response }"""
    user_id = item.get("userID", "")
    timestamp = item.get("timestamp", "")
    request_data = item.get("request") or {}
    response_data = item.get("response") or {}

    request_out = _form_data_with_glucose_averages(request_data)

    response_out = dict(response_data)
    body = response_out.get("body")
    if body is not None:
        response_out["body"] = _parse_response_body(body)
    response_out = _to_native(response_out)

    ts_for_id = _truncate_timestamp_for_id(timestamp)
    item_id = f"{user_id}_{ts_for_id}"

    return _to_native({
        "id": item_id,
        "timestamp": timestamp,
        "request": request_out,
        "response": response_out,
    })


def _get_user_id(event):
    """Get userID: prefer Cognito authorizer claims (REST or HTTP API), else body/event.
    GET requests have no body; userID must come from authorizer.claims.sub or authorizer.jwt.claims.sub."""
    try:
        authorizer = (event.get("requestContext") or {}).get("authorizer") or {}
        if not isinstance(authorizer, dict):
            authorizer = {}
        # REST API: authorizer.claims.sub | HTTP API: authorizer.jwt.claims.sub | some setups: authorizer.sub
        sub = authorizer.get("sub")
        if sub:
            return str(sub)
        claims = authorizer.get("claims") or authorizer.get("jwt", {}).get("claims") or {}
        if isinstance(claims, dict):
            sub = claims.get("sub")
            if sub:
                return str(sub)
    except Exception:
        pass
    body = event.get("body")  # GET has no body; avoid using event as body fallback
    if isinstance(body, str):
        try:
            body = json.loads(body) if body else {}
        except Exception:
            body = {}
    if isinstance(body, dict):
        uid = body.get("userID") or body.get("userId")
        if uid:
            return str(uid)
    return event.get("userID") or event.get("userId")


def handler(event, context):
    """
    Returns the 15 most recent history items.
    Payload: { history: [ { id, timestamp, request, response }, ... ] }
    Always returns a valid API Gateway proxy response so the client gets a response.
    """
    try:
        http_method = (event.get("requestContext") or {}).get("http", {}).get("method") or event.get("httpMethod") or "GET"
        has_authorizer = bool((event.get("requestContext") or {}).get("authorizer"))
        print(f"[get_history] method={http_method} has_authorizer={has_authorizer}", flush=True)

        user_id = _get_user_id(event)
        print(f"[get_history] userID={user_id}", flush=True)
        if not user_id:
            out = _response(400, {"error": "Missing userID or userId"})
            print("[get_history] returning 400 (no userID)", flush=True)
            return out

        table_name = os.environ.get("TABLE_NAME", "T2D")
        if not boto3:
            return _response(500, {"error": "boto3 not available (install in Lambda runtime)"})

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)

        result = table.query(
            KeyConditionExpression="userID = :uid",
            ExpressionAttributeValues={":uid": str(user_id)},
            ScanIndexForward=False,
            Limit=15,
        )

        raw_items = result.get("Items", [])
        history = [_format_history_item(it) for it in raw_items]
        print(f"[get_history] returning {len(history)} items for userID={user_id}", flush=True)
        out = _response(200, {"history": history})
        print("[get_history] response built, returning 200", flush=True)
        return out
    except json.JSONDecodeError as e:
        return _response(400, {"error": f"Invalid JSON: {e!s}"})
    except Exception as e:
        print(f"[get_history] error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return _response(500, {"error": str(e)})
    finally:
        # Ensure any buffered stdout is sent before Lambda freezes (helps with "no return" debugging)
        try:
            import sys
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass


def _response(status_code, body):
    """Return API Gateway Lambda proxy response. Ensures body is always a JSON string."""
    try:
        body_str = json.dumps(body, default=str)
    except Exception as e:
        body_str = json.dumps({"error": "Response serialization failed", "detail": str(e)})
        status_code = 500
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": body_str,
    }
