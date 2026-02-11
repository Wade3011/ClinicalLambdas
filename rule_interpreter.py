"""
Structured JSON rule interpreter (Option 1).
Evaluates rules like {"field": "eGFR", "op": "lt", "value": 30}
or {"and": [...]} / {"or": [...]} against a context dict.
No eval(); fixed set of fields and operators.
"""

# Allowed fields (context keys or special names)
NUMERIC_FIELDS = {"eGFR", "a1c", "age", "goal", "fasting_above_goal", "post_prandial_above_goal", "fasting_avg", "lows_detected"}
SET_FIELDS = {"comorbidity", "comorbidities", "allergy", "allergy_labels"}

# Allowed ops for numeric comparisons
NUMERIC_OPS = {"lt", "le", "gt", "ge", "eq", "ne"}

# Ops for set membership
SET_OPS = {"in", "not_in"}


def _get_value(context, field):
    """Resolve field to a value from context."""
    if field in ("eGFR", "a1c", "age", "goal", "fasting_above_goal", "post_prandial_above_goal", "fasting_avg", "lows_detected"):
        v = context.get(field)
        if v is None and field == "goal":
            return 7.0
        if v is None and field in ("fasting_above_goal", "post_prandial_above_goal", "fasting_avg"):
            return None  # no glucose data: rule does not apply (no penalty)
        if v is None and field == "lows_detected":
            return 0.0
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0
    if field in ("comorbidity", "comorbidities"):
        s = context.get("comorbidities") or context.get("comorbidity")
        return set() if s is None else set(str(x).strip().upper() for x in s) if hasattr(s, "__iter__") and not isinstance(s, str) else {str(s).strip().upper()}
    if field in ("allergy", "allergy_labels"):
        s = context.get("allergy_labels_set") or context.get("allergy_labels") or context.get("allergies")
        if s is None:
            return set()
        if hasattr(s, "__iter__") and not isinstance(s, str):
            return set(str(x).strip().lower() for x in s)
        return {str(s).strip().lower()}
    return None


def _apply_numeric(op, left, right):
    """Compare two numbers. right can be int or float."""
    try:
        r = float(right) if not isinstance(right, (int, float)) else right
    except (TypeError, ValueError):
        return False
    if op == "lt":
        return left < r
    if op == "le":
        return left <= r
    if op == "gt":
        return left > r
    if op == "ge":
        return left >= r
    if op == "eq":
        return abs(left - r) < 1e-9
    if op == "ne":
        return abs(left - r) >= 1e-9
    return False


def _apply_set(op, context_val, rule_value):
    """Set membership. rule_value can be string or list."""
    if context_val is None:
        context_val = set()
    if not isinstance(context_val, set):
        context_val = {str(context_val).strip()}
    if isinstance(rule_value, list):
        check_set = set(str(x).strip().upper() for x in rule_value)
    else:
        check_set = {str(rule_value).strip().upper()}
    # For comorbidities we compare uppercase
    if op == "in":
        return bool(check_set & context_val) if context_val else False
    if op == "not_in":
        return not (check_set & context_val) if context_val else True
    return False


def _apply_allergy_in(context_val, rule_value):
    """Check if context allergy set intersects rule value (string or list). Case-insensitive."""
    if context_val is None:
        context_val = set()
    if not isinstance(context_val, set):
        context_val = {str(context_val).strip().lower()}
    if isinstance(rule_value, list):
        check_set = set(str(x).strip().lower() for x in rule_value)
    else:
        check_set = {str(rule_value).strip().lower()}
    return bool(check_set & context_val)


def evaluate_structured_rule(rule, context):
    """
    Evaluate a single structured rule against context.
    rule: dict e.g. {"field": "eGFR", "op": "lt", "value": 30}
          or {"and": [rule1, rule2]} / {"or": [rule1, rule2]}
    context: dict with eGFR, a1c, age, goal, comorbidities (set), allergy_labels_set (set)
    Returns bool.
    """
    if rule is None:
        return False
    if not isinstance(rule, dict):
        return False

    if "and" in rule:
        sub = rule["and"]
        if not isinstance(sub, list):
            return False
        return all(evaluate_structured_rule(r, context) for r in sub)

    if "or" in rule:
        sub = rule["or"]
        if not isinstance(sub, list):
            return False
        return any(evaluate_structured_rule(r, context) for r in sub)

    field = rule.get("field")
    op = rule.get("op")
    value = rule.get("value")
    if field is None or op is None:
        return False

    if field in NUMERIC_FIELDS:
        left = _get_value(context, field)
        if left is None:
            return False  # e.g. no glucose data: fasting_above_goal/post_prandial_above_goal rule does not apply
        if op not in NUMERIC_OPS:
            return False
        return _apply_numeric(op, left, value)

    if field in ("comorbidity", "comorbidities"):
        context_val = _get_value(context, field)
        if op not in SET_OPS:
            return False
        return _apply_set(op, context_val, value)

    if field in ("allergy", "allergy_labels"):
        context_val = _get_value(context, field)
        if op == "in":
            return _apply_allergy_in(context_val, value)
        if op == "not_in":
            return not _apply_allergy_in(context_val, value)
        return False

    return False
