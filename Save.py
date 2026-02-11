"""
Save History Lambda: stores recommendation request and response in DynamoDB.
Table: T2D (or TABLE_NAME env var).
Partition key: userID (String), Sort key: timestamp (String, ISO 8601).

Saves: userID, timestamp, request, response. Does NOT save bestChoiceMed
(redundant; derivable from response.body.bestChoice.medication).
"""
import json
import os
from decimal import Decimal
from datetime import datetime, timezone

try:
    import boto3
except ImportError:
    boto3 = None


def _to_dynamodb(obj):
    """Convert floats to Decimal for DynamoDB compatibility."""
    if obj is None:
        return None
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _to_dynamodb(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_dynamodb(v) for v in obj]
    return obj


def _parse_event(event):
    """Parse event: API Gateway wraps in body, or direct invoke has body at top level."""
    body = event.get("body", event)
    if isinstance(body, str):
        try:
            return json.loads(body) if body else {}
        except json.JSONDecodeError:
            return {}
    return body if isinstance(body, dict) else {}


def _get_user_id(event):
    """Get userID: prefer Cognito JWT claims (requestContext.authorizer.claims.sub), else body."""
    try:
        claims = (event.get("requestContext") or {}).get("authorizer") or {}
        if isinstance(claims, dict) and "claims" in claims:
            sub = (claims["claims"] or {}).get("sub")
            if sub:
                return str(sub)
        sub = claims.get("sub") if isinstance(claims, dict) else None
        if sub:
            return str(sub)
    except Exception:
        pass
    data = _parse_event(event)
    return data.get("userID") or data.get("userId")


def handler(event, context):
    """
    Saves request + response to DynamoDB.
    userID: from Cognito JWT (requestContext.authorizer.claims.sub) when invoked via API Gateway,
            else from body (when invoked by clinical Lambda).
    Returns 200 { saved: true, userID, timestamp } or error.
    """
    try:
        data = _parse_event(event)
        user_id = _get_user_id(event)
        if not user_id:
            return _response(400, {"error": "Missing userID or userId"})

        request_payload = data.get("request")
        response_payload = data.get("response")
        if request_payload is None or response_payload is None:
            return _response(400, {"error": "Missing request or response"})

        table_name = os.environ.get("TABLE_NAME", "T2D")
        if not boto3:
            return _response(500, {"error": "boto3 not available"})

        # Use timestamp from clinical Lambda if provided (for feedback matching), else generate
        timestamp = data.get("timestamp") or datetime.now(timezone.utc).isoformat()

        # DynamoDB requires Decimal for numbers, not float
        item = {
            "userID": str(user_id),
            "timestamp": timestamp,
            "request": _to_dynamodb(request_payload),
            "response": _to_dynamodb(response_payload),
        }

        # Optional fields if provided
        if data.get("patientSummary") is not None:
            item["patientSummary"] = str(data["patientSummary"])
        if data.get("feedback") is not None:
            item["feedback"] = _to_dynamodb(data["feedback"])
        if data.get("ttl") is not None:
            item["ttl"] = int(data["ttl"])

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)
        table.put_item(Item=item)

        print(f"[save_history] saved userID={user_id} timestamp={timestamp}")
        return _response(200, {"saved": True, "userID": str(user_id), "timestamp": timestamp})
    except Exception as e:
        print(f"[save_history] error: {e}")
        return _response(500, {"error": str(e)})


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps(body, default=str),
    }
