"""
Feedback Lambda: updates a recommendation history item with user feedback.
Table: T2D (or TABLE_NAME env var).
Partition key: userID (String), Sort key: timestamp (String, ISO 8601).

Receives feedback (thumbs up/down + text). userID from Cognito JWT.
recommendationTimestamp (required) identifies the exact recommendation to attach feedback to.
"""
import json
import os
from decimal import Decimal

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = Exception


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
    Updates the recommendation history item with feedback.
    Expects body: { type, feedbackText, recommendationTimestamp }
    userID from Cognito JWT. recommendationTimestamp required to identify the recommendation.
    Returns 200 { updated: true, save: "success" } or error.
    """
    try:
        data = _parse_event(event)
        user_id = _get_user_id(event)

        if not user_id:
            return _response(400, {"error": "Missing userID (Cognito auth required)", "save": "fail"})

        feedback_type = data.get("type")
        feedback_text = data.get("feedbackText", "")

        if not feedback_type or feedback_type not in ("thumbs_up", "thumbs_down"):
            return _response(400, {"error": "Missing or invalid feedback type (thumbs_up or thumbs_down)", "save": "fail"})

        rec_timestamp = data.get("recommendationTimestamp") or data.get("recommendation_timestamp")
        if not rec_timestamp:
            return _response(400, {"error": "Missing recommendationTimestamp", "save": "fail"})

        table_name = os.environ.get("TABLE_NAME", "T2D")
        if not boto3:
            return _response(500, {"error": "boto3 not available", "save": "fail"})

        # Build feedback object: type, feedbackText, submittedAt (when user submitted)
        feedback_obj = {
            "type": feedback_type,
            "feedbackText": feedback_text,
            "submittedAt": data.get("timestamp") or "",
        }
        feedback_dynamo = _to_dynamodb(feedback_obj)

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)
        region = os.environ.get("AWS_REGION", "")
        print(f"[feedback] table={table_name} region={region} key userID={user_id!r} timestamp={rec_timestamp!r}")

        try:
            # Get current item to append (handle legacy single-object or list format)
            resp = table.get_item(
                Key={"userID": str(user_id), "timestamp": str(rec_timestamp)},
                ProjectionExpression="#req, feedback",
                ExpressionAttributeNames={"#req": "request"},
            )
            item = resp.get("Item")
            if not item or "request" not in item:
                print(f"[feedback] item not found for userID={user_id!r} timestamp={rec_timestamp!r}")
                return _response(404, {
                    "error": "Recommendation not found. The timestamp may not match any saved recommendation.",
                    "save": "fail",
                })

            current = item.get("feedback")
            if current is None:
                new_list = [feedback_dynamo]
            elif isinstance(current, list):
                new_list = current + [feedback_dynamo]
            else:
                # Legacy: was single map, convert to list and append
                new_list = [current, feedback_dynamo]

            new_list_dynamo = _to_dynamodb(new_list)

            table.update_item(
                Key={"userID": str(user_id), "timestamp": str(rec_timestamp)},
                UpdateExpression="SET feedback = :fb",
                ExpressionAttributeValues={":fb": new_list_dynamo},
            )
            print(f"[feedback] appended userID={user_id} timestamp={rec_timestamp} (check attribute 'feedback' on this item in table {table_name})")
            return _response(200, {
                "updated": True,
                "save": "success",
            })
        except ClientError as e:
            print(f"[feedback] DynamoDB ClientError: {e}")
            return _response(500, {"error": str(e), "save": "fail"})

    except Exception as e:
        print(f"[feedback] error: {e}")
        return _response(500, {"error": str(e), "save": "fail"})


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps(body, default=str),
    }


# Alias for Lambda config: use feedback_lambda.lambda_handler if handler is set to lambda_handler
lambda_handler = handler
