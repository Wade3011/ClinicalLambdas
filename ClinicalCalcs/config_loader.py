"""
Load drug_classes.json (includes Goal 1: current_therapy_boost), goal2.json, goal3.json from S3 or local.
Caches in Lambda execution context. Used by lambda_handler.
"""
import json
import os
import sys
from botocore.exceptions import ClientError

try:
    import boto3
    from botocore.config import Config
except ImportError:
    boto3 = None
    Config = None

# S3 client timeouts so we fail fast instead of hanging until Lambda timeout
S3_CONFIG = Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 1}) if Config else None
CONFIG_LOADER_VERSION = "2026-01-30-flush"  # log this to confirm deployed code

_drug_classes_raw_cache = None
_goal1_cache = None
_goal2_cache = None
_goal3_cache = None


def _log(msg):
    """Print and flush so CloudWatch shows logs before timeout."""
    print(msg)
    sys.stdout.flush()

def load_drug_classes_from_s3(bucket_name, object_key):
    """Load drug_classes JSON from S3. Returns full data (drug_classes + goal1 keys)."""
    if boto3 is None:
        raise RuntimeError("boto3 required for S3 load")
    try:
        _log(f"S3 get_object starting: s3://{bucket_name}/{object_key}")
        s3_client = boto3.client("s3", config=S3_CONFIG)
        _log("S3 client created, calling get_object...")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        _log("S3 get_object returned, reading body...")
        raw = response["Body"].read()
        _log(f"S3 body read ({len(raw)} bytes), parsing JSON...")
        data = json.loads(raw.decode("utf-8"))
        _log("drug_classes S3 load complete")
        return data
    except ClientError as e:
        _log(f"Error loading from S3: {e}")
        raise
    except Exception as e:
        _log(f"S3 load_drug_classes_from_s3 error: {type(e).__name__}: {e}")
        raise


def _drug_classes_local_path():
    """Return path to drug_classes.json if it exists in package, else None."""
    base = os.path.dirname(os.path.abspath(__file__))
    for path in [os.path.join(base, "drug_classes.json"), "/var/task/drug_classes.json", "drug_classes.json"]:
        if os.path.exists(path):
            return path
    return None


def load_drug_classes_from_local():
    """Load drug_classes from JSON file bundled with Lambda (fallback). Returns full data."""
    path = _drug_classes_local_path()
    if path is None:
        base = os.path.dirname(os.path.abspath(__file__))
        raise FileNotFoundError(f"drug_classes.json not found. Tried: {base}, /var/task, cwd")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_json_local(filename):
    """Load a JSON file from same dir as this module or /var/task. Returns None if not found."""
    base = os.path.dirname(os.path.abspath(__file__))
    for path in [os.path.join(base, filename), os.path.join("/var/task", filename), filename]:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    return None


def load_goal1():
    """Load Goal 1 data (current_therapy_boost) from drug_classes.json. Cached."""
    global _goal1_cache
    if _goal1_cache is not None:
        return _goal1_cache
    # Ensure drug_classes.json is loaded (populates _drug_classes_raw_cache)
    load_drug_classes()
    _goal1_cache = {
        "current_therapy_boost": _drug_classes_raw_cache.get("current_therapy_boost", 0.20),
        "description": _drug_classes_raw_cache.get("description", ""),
    }
    return _goal1_cache


def _load_json_from_s3(bucket_name, object_key):
    """Load JSON from S3. Returns None on failure."""
    if not bucket_name or not object_key or boto3 is None:
        return None
    try:
        s3_client = boto3.client("s3", config=S3_CONFIG)
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"Failed to load s3://{bucket_name}/{object_key}: {e}")
        return None


def load_goal2():
    """Load goal2.json: try local first, then S3. Cached."""
    global _goal2_cache
    if _goal2_cache is not None:
        return _goal2_cache
    _goal2_cache = _load_json_local("goal2.json")
    if _goal2_cache is not None:
        _log("Goal2 loaded from local")
        return _goal2_cache
    bucket = os.environ.get("DRUG_CLASSES_S3_BUCKET")
    key = os.environ.get("GOAL2_S3_KEY", "goal2.json")
    if bucket and boto3:
        _log(f"Loading goal2 from S3: s3://{bucket}/{key}")
        _goal2_cache = _load_json_from_s3(bucket, key)
    _log("Goal2 loaded" if _goal2_cache else "Goal2 missing (using fallbacks)")
    return _goal2_cache or {}


def load_goal3():
    """Load goal3.json: try local first, then S3. Cached."""
    global _goal3_cache
    if _goal3_cache is not None:
        return _goal3_cache
    _goal3_cache = _load_json_local("goal3.json")
    if _goal3_cache is not None:
        _log("Goal3 loaded from local")
        return _goal3_cache
    bucket = os.environ.get("DRUG_CLASSES_S3_BUCKET")
    key = os.environ.get("GOAL3_S3_KEY", "goal3.json")
    if bucket and boto3:
        _log(f"Loading goal3 from S3: s3://{bucket}/{key}")
        _goal3_cache = _load_json_from_s3(bucket, key)
    _log("Goal3 loaded" if _goal3_cache else "Goal3 missing (using fallbacks)")
    return _goal3_cache or {}


def load_drug_classes(s3_bucket=None, s3_key=None):
    """Load drug config: try LOCAL first, then S3. Cached. Returns {classes, drugs}.
    classes = insurance-only (cost, tier, va_pdf_exists, pa_required, base_access_score, allergy_labels).
    drugs = rule set per drug (class, clinical_base, deny_if, caution_if, clinical_boost)."""
    global _drug_classes_raw_cache
    if _drug_classes_raw_cache is not None:
        return _normalize_drug_config(_drug_classes_raw_cache)
    # Prefer local package so Lambda works without S3 / network (no 502 from S3 hang)
    if _drug_classes_local_path():
        _log("Loading drug classes from local package")
        _drug_classes_raw_cache = load_drug_classes_from_local()
        return _normalize_drug_config(_drug_classes_raw_cache)
    bucket = s3_bucket or os.environ.get("DRUG_CLASSES_S3_BUCKET")
    key = s3_key or os.environ.get("DRUG_CLASSES_S3_KEY", "drug_classes.json")
    if bucket and boto3:
        try:
            _log(f"Loading drug classes from S3: s3://{bucket}/{key}")
            _drug_classes_raw_cache = load_drug_classes_from_s3(bucket, key)
            return _normalize_drug_config(_drug_classes_raw_cache)
        except Exception as e:
            _log(f"Failed to load from S3: {e}")
            raise
    _log("Loading drug classes from local package (no S3 env)")
    _drug_classes_raw_cache = load_drug_classes_from_local()
    return _normalize_drug_config(_drug_classes_raw_cache)


def _normalize_drug_config(raw):
    """Return {classes, drugs}. Drugs get class-level insurance + allergy merged so everything is at drug level."""
    if raw.get("classes") and raw.get("drugs"):
        classes = raw["classes"]
        drugs_out = {}
        for drug_id, data in raw["drugs"].items():
            cls_name = data.get("class", drug_id)
            class_data = classes.get(cls_name, {})
            # Merge class (insurance + allergy) into drug. Union class + drug allergy_labels (hybrid: class-level and drug-level).
            class_allergy = class_data.get("allergy_labels") or []
            drug_allergy = data.get("allergy_labels") or []
            merged_allergy = list(dict.fromkeys(class_allergy + drug_allergy))  # order preserved, deduped
            merged = {
                "cost": class_data.get("cost", "medium"),
                "tier": class_data.get("tier", 2),
                "va_pdf_exists": class_data.get("va_pdf_exists", False),
                "pa_required": class_data.get("pa_required", False),
                "base_access_score": class_data.get("base_access_score", 0.6),
                "allergy_labels": merged_allergy,
            }
            merged.update(data)  # drug overrides (cost, rules, etc.)
            merged["allergy_labels"] = merged_allergy  # keep union; update() may have overwritten with drug-only
            drugs_out[drug_id] = merged
        return {"classes": classes, "drugs": drugs_out}
    # Legacy: single drug_classes dict (class -> full data); split into classes (insurance) and drugs (rules).
    legacy = raw.get("drug_classes", raw)
    if not isinstance(legacy, dict):
        return {"classes": {}, "drugs": {}}
    classes = {}
    drugs = {}
    for cls_name, data in legacy.items():
        if not isinstance(data, dict):
            continue
        classes[cls_name] = {
            "cost": data.get("cost", "medium"),
            "tier": data.get("tier", 2),
            "va_pdf_exists": data.get("va_pdf_exists", False),
            "pa_required": data.get("pa_required", False),
            "base_access_score": data.get("base_access_score", 0.6),
            "allergy_labels": data.get("allergy_labels", []),
        }
        drugs[cls_name] = {
            "class": cls_name,
            "clinical_base": data.get("clinical_base", 0.5),
            "deny_if": data.get("deny_if", []),
            "caution_if": data.get("caution_if", []),
            "clinical_boost": data.get("clinical_boost", []),
        }
    return {"classes": classes, "drugs": drugs}
