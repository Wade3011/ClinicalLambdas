"""
Save History Lambda: stores recommendation request and response in DynamoDB.
Table: T2D (or TABLE_NAME env var).
Partition key: userID (String), Sort key: timestamp (String, ISO 8601 Eastern, same format as ClinicalCalcs e.g. 2026-02-19T23:28:42.692526-05:00).

Item shape (add nothing, remove nothing):
  userID, timestamp, recommendationTimestamp, request, response [, patientSummary ]
  (feedback is written by feedback.py, not here.)

  request: full request (additionalContext, allergies, comorbidities, currentMedications,
            glucoseReadings, patientInfo, etc.)

  response: { statusCode, body, requestId }
    response.body MUST include: assessment, original_assessment, rationale, alternatives,
    futureConsiderations, allDrugWeights, top3BestOptions, recommendationTimestamp,
    requestId, warning-eGFR (and any other keys the clinical Lambda sends).
"""
import json
import os
from decimal import Decimal
from datetime import datetime
from zoneinfo import ZoneInfo

# Match ClinicalCalcs: store timestamps in Eastern (America/New_York), e.g. 2026-02-19T23:28:42.692526-05:00.
# Frontend sends this same format for feedback lookup; history returns it as stored.
EASTERN = ZoneInfo("America/New_York")

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
    """Parse event: API Gateway wraps in body (string), or Lambda invoke has body (dict) or payload at top level."""
    body = event.get("body", event)
    if isinstance(body, str):
        try:
            return json.loads(body) if body else {}
        except json.JSONDecodeError:
            return {}
    if isinstance(body, dict):
        return body
    return {}


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

        # Normalize response: invoker may send { statusCode, body: {...}, requestId } or the body object directly.
        # Ensure we store the full response with body containing assessment, futureConsiderations, etc.
        if isinstance(response_payload, dict) and "body" not in response_payload:
            # Response was sent as the body object directly (assessment, futureConsiderations, ...)
            response_payload = {"statusCode": 200, "body": response_payload}

        table_name = os.environ.get("TABLE_NAME", "T2D")
        if not boto3:
            return _response(500, {"error": "boto3 not available"})

        # Timestamp format: Eastern, same as ClinicalCalcs (e.g. 2026-02-19T23:28:42.692526-05:00). Pass through as-is for feedback/history match.
        response_body = response_payload.get("body") if isinstance(response_payload, dict) else {}
        ts_from_payload = data.get("timestamp")
        rec_ts_from_body = response_body.get("recommendationTimestamp") if isinstance(response_body, dict) else None
        timestamp = ts_from_payload if ts_from_payload is not None and str(ts_from_payload).strip() else datetime.now(EASTERN).isoformat()
        recommendation_timestamp = (
            data.get("recommendationTimestamp")
            or rec_ts_from_body
            or timestamp
        )
        if isinstance(timestamp, str):
            timestamp = timestamp.strip()
        if isinstance(recommendation_timestamp, str):
            recommendation_timestamp = recommendation_timestamp.strip()

        # DynamoDB requires Decimal for numbers, not float. Store full request and full response (no trimming).
        item = {
            "userID": str(user_id),
            "timestamp": timestamp,
            "recommendationTimestamp": recommendation_timestamp,
            "request": _to_dynamodb(request_payload),
            "response": _to_dynamodb(response_payload),
        }

        # Optional fields if provided (TTL not set for now). feedback is written by feedback.py.
        if data.get("patientSummary") is not None:
            item["patientSummary"] = str(data["patientSummary"])

        # Log keys we're storing (for debugging missing fields e.g. futureConsiderations)
        response_body = response_payload.get("body") if isinstance(response_payload, dict) else {}
        body_keys = list(response_body.keys()) if isinstance(response_body, dict) else []
        print(f"[save_history] saved userID={user_id} timestamp={timestamp} response.body keys={body_keys}")

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)
        table.put_item(Item=item)

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
