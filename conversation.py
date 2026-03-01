"""
Conversation Lambda: follow-up Q&A about a stored recommendation.

- Loads the recommendation from DynamoDB (same table as Save/feedback: userID + timestamp).
- Loads and persists conversation on the same item (conversation attribute).
- Uses conversation + new question to derive intent and include only relevant recommendation
  sections in the prompt (not the full recommendation every time).
- Calls Bedrock to generate an answer, then appends the turn to conversation in DB.

Event body: { "question": "...", "recommendationTimestamp": "..." }.
Optional: responseMode (e.g. "clinical_note") — when set, skips concise-answer guardrail and allows structured note format from the user message.
userID from JWT (requestContext.authorizer.claims.sub or authorizer.sub).
Env: TABLE_NAME (default T2D), BEDROCK_MODEL_ID (required), BEDROCK_REGION (optional),
     BEDROCK_KNOWLEDGE_BASE_ID (optional; when set, retrieves from KB and adds to prompt).
"""

import json
import os
from datetime import datetime
from decimal import Decimal

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = Exception


def _log(msg):
    print(f"[conversation] {msg}", flush=True)


def _parse_event(event):
    """Parse body from API Gateway or direct invoke."""
    body = event.get("body", event)
    if isinstance(body, str):
        try:
            return json.loads(body) if body else {}
        except json.JSONDecodeError:
            return {}
    return body if isinstance(body, dict) else {}


def _get_user_id(event):
    """userID from Cognito JWT (same pattern as feedback.py)."""
    try:
        authorizer = (event.get("requestContext") or {}).get("authorizer") or {}
        if isinstance(authorizer, dict) and "claims" in authorizer:
            sub = (authorizer["claims"] or {}).get("sub")
            if sub:
                return str(sub)
        sub = authorizer.get("sub") if isinstance(authorizer, dict) else None
        if sub:
            return str(sub)
    except Exception:
        pass
    data = _parse_event(event)
    return data.get("userID") or data.get("userId")


def _to_native(obj):
    """Convert DynamoDB types (Decimal) to native JSON-serializable."""
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


def _to_dynamodb(obj):
    """Convert floats to Decimal for DynamoDB."""
    if obj is None:
        return None
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _to_dynamodb(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_dynamodb(v) for v in obj]
    return obj


def _get_relevant_sections(question, conversation_turns, response_body):
    """
    Derive intent from conversation + new question and return only relevant
    parts of the recommendation (assessment, rationale, alternatives, top3, etc.).
    Returns a dict of section_name -> text for the prompt.
    """
    question_lower = (question or "").lower()
    recent_text = " ".join(
        (t.get("content") or "").lower()
        for t in (conversation_turns or [])[-4:]
    )
    combined = f"{recent_text} {question_lower}"

    # Always include assessment (short summary)
    sections = {}
    body = response_body or {}

    assessment = body.get("assessment") or body.get("original_assessment") or ""
    if assessment:
        sections["assessment"] = str(assessment).strip()

    # Always include future considerations
    future = body.get("futureConsiderations") or []
    if isinstance(future, list):
        future_text = "\n".join(str(x).strip() for x in future if x)
    else:
        future_text = str(future).strip() if future else ""
    if future_text:
        sections["future_considerations"] = future_text

    # Include eGFR/therapy warning only if present in recommendation (not tied to user question)
    warning = body.get("warning-eGFR") or body.get("warningEGFR")
    if warning:
        sections["egfr_warning"] = "eGFR/therapy warning applies; see assessment and rationale."

    # Intent keywords: include rationale and alternatives for "why not", cost, comparison
    if any(k in combined for k in ("why", "why not", "prefer", "instead", "alternative", "other option", "rationale", "reason")):
        rationale = body.get("rationale") or []
        if isinstance(rationale, list):
            sections["rationale"] = "\n".join(str(x).strip() for x in rationale if x)
        else:
            sections["rationale"] = str(rationale).strip()
        alts = body.get("alternatives") or []
        if isinstance(alts, list):
            sections["alternatives"] = "\n".join(str(x).strip() for x in alts if x)
        else:
            sections["alternatives"] = str(alts).strip()

    # Cost / cheapest / afford
    if any(k in combined for k in ("cost", "cheap", "afford", "price", "tier")):
        top3 = body.get("top3BestOptions") or []
        if isinstance(top3, list):
            parts = []
            for i, opt in enumerate(top3[:3], 1):
                name = opt.get("medication") or opt.get("displayName") or opt.get("drug") or opt.get("class") or "Option"
                tier = opt.get("costTier") or opt.get("tier") or ""
                parts.append(f"  {i}. {name}" + (f" ({tier})" if tier else ""))
            if parts:
                sections["top3_and_cost"] = "\n".join(parts)
        all_weights = body.get("allDrugWeights") or []
        if all_weights and isinstance(all_weights, list):
            cost_bits = []
            for w in all_weights[:10]:
                name = w.get("drug") or w.get("drugName") or w.get("class") or ""
                tier = w.get("costTier") or w.get("tier") or ""
                if name:
                    cost_bits.append(f"  - {name}" + (f" ({tier})" if tier else ""))
            if cost_bits:
                sections["drug_weights_cost"] = "\n".join(cost_bits)

    # Kidney / renal / eGFR keywords: ensure rationale is included when user asks about kidney/renal
    if any(k in combined for k in ("kidney", "renal", "egfr", "ckd", "glomerular")):
        if not sections.get("rationale"):
            rationale = body.get("rationale") or []
            if isinstance(rationale, list):
                sections["rationale"] = "\n".join(str(x).strip() for x in rationale if x)
            else:
                sections["rationale"] = str(rationale).strip()

    # Default: include top3 and rationale so the model has something to work with
    if "top3_and_cost" not in sections:
        top3 = body.get("top3BestOptions") or []
        if isinstance(top3, list):
            parts = []
            for i, opt in enumerate(top3[:3], 1):
                name = opt.get("medication") or opt.get("displayName") or opt.get("drug") or opt.get("class") or "Option"
                parts.append(f"  {i}. {name}")
            if parts:
                sections["top3"] = "\n".join(parts)
    if "rationale" not in sections:
        rationale = body.get("rationale") or []
        if isinstance(rationale, list):
            sections["rationale"] = "\n".join(str(x).strip() for x in rationale if x)
        else:
            sections["rationale"] = str(rationale).strip()

    return sections


# ---------- Lightweight guardrails (no Bedrock product; minimal tokens) ----------
# Max question length (chars) to avoid token blow-up
QUESTION_MAX_CHARS = 500
CLINICAL_NOTE_MAX_CHARS = 8000  # when responseMode is clinical_note, allow longer prompt

# Off-topic: block if question has no relevance to recommendation/diabetes/medication
_OFF_TOPIC_REJECT_PHRASES = (
    "unrelated", "sports", "recipe", "weather", "politics", "legal advice",
    "another patient", "someone else", "my friend's", "my wife's", "my husband's",
)
# Safety: block harmful / inappropriate intents (no Bedrock call)
_SAFETY_BLOCK_PHRASES = (
    "overdose", "kill myself", "harm myself", "suicide", "end my life",
    "how to hurt", "how much to overdose", "deadly dose", "lethal dose",
)
# PII: don't invite model to store or repeat SSN, etc.
_PII_PROBE_PHRASES = ("my ssn is", "social security", "my dob is", "remember my", "store my")

def _input_guardrails(question, response_mode=None):
    """
    Run before Bedrock. Returns (allowed: bool, canned_response: str | None).
    If not allowed, return canned_response and skip Bedrock (0 tokens).
    When response_mode == 'clinical_note', skip off-topic check so structured note prompts pass.
    """
    if not question or not question.strip():
        return False, None
    q = question.strip().lower()
    # Safety first
    for p in _SAFETY_BLOCK_PHRASES:
        if p in q:
            return False, "I can only answer questions about your diabetes medication recommendation. If you're in crisis, please contact a crisis line or 911."
    # PII: don't send to model
    for p in _PII_PROBE_PHRASES:
        if p in q:
            return False, "I don't collect or store personal information. Please ask about your medication recommendation."
    # Off-topic: skip when clinical_note (structured note prompt may not contain med keywords)
    if response_mode != "clinical_note" and len(q) > 20:
        has_med = any(x in q for x in ("medication", "drug", "insulin", "metformin", "recommend", "diabetes", "blood sugar", "a1c", "dose", "why", "cost", "alternative", "option", "kidney", "renal", "weight", "glp", "sglt", "oral", "injection"))
        has_followup = any(x in q for x in ("why ", "what about", "how about", "when ", "can i", "should i", "?"))
        if not has_med and not has_followup:
            for p in _OFF_TOPIC_REJECT_PHRASES:
                if p in q:
                    return False, "Please ask a question about your diabetes medication recommendation."
    return True, None


def _retrieve_from_bedrock_kb(knowledge_base_id, query, region=None, number_of_results=5, score_threshold=0.3):
    """Retrieve relevant chunks from a Bedrock Knowledge Base. Returns (content_string, chunk_count).
    content_string is XML-wrapped for the prompt; only chunks with score > score_threshold are kept."""
    if not boto3 or not (knowledge_base_id or "").strip():
        return "", 0
    kb_id = (knowledge_base_id or "").strip()
    try:
        kwargs = {"service_name": "bedrock-agent-runtime"}
        if region:
            kwargs["region_name"] = region
        client = boto3.client(**kwargs)
        request_params = {
            "knowledgeBaseId": kb_id,
            "retrievalQuery": {"text": (query or "")[:8000]},
        }
        if number_of_results and number_of_results > 0:
            request_params["retrievalConfiguration"] = {
                "vectorSearchConfiguration": {"numberOfResults": min(number_of_results, 25)}
            }
        response = client.retrieve(**request_params)
        results = response.get("retrievalResults") or []
        chunks = []
        for r in results:
            score = r.get("score", 1.0)
            if score is not None and float(score) <= score_threshold:
                continue
            content = r.get("content") or {}
            if content.get("type") == "TEXT" and content.get("text"):
                chunks.append(content["text"].strip())
        if not chunks:
            return "", 0
        combined = "\n\n".join(chunks)
        content_str = f'<reference name="retrieved">\n{combined}\n</reference>'
        return content_str, len(chunks)
    except Exception as e:
        _log(f"Bedrock KB retrieve failed: {e}")
        return "", 0


def _build_prompt(question, conversation_turns, relevant_sections, kb_references_section=None, response_mode=None):
    """Build system and user message for Bedrock.
    kb_references_section: optional string from Knowledge Base retrieval to include in the prompt.
    When response_mode == 'clinical_note', system omits brevity and user message omits the concise-answer line so format instructions in the question take precedence."""
    question = (question or "").strip()
    if response_mode == "clinical_note":
        system = """You are a Type 2 Diabetes medication expert. Use only the recommendation context and KB references below. Write a clinical note with exactly two sections:

ASSESSMENT: Lead with one or two data-rich paragraphs that set the clinical picture: include specific labs (A1C, fasting glucose, eGFR if relevant), glycemic targets, and weave in any additional context from the user request (preferences, allergies, recent surgery, other reasoning). Do not repeat those same numbers in bullets. Then add at most one or two bullet sections—e.g. "Rationale and cautions:" only, or "Rationale:" and "Cautions:" if you need both. Use bullets for rationale for therapy choice and any cautions (eGFR/therapy warning, monitoring); do not add a separate "Key labs" list that duplicates the paragraph.

PLAN: Do not write narrative or expand. Output only a simple bulleted list, in this order, using only what the user request provides: (1) New medication starts (drug and dose as stated), (2) Continue/adjust/discontinue any current medications, (3) Monitoring metrics selected, (4) Future considerations. One line per bullet; no explanations, no patient education section, no extra content.

Never ask for PHI/PII. If the answer is not in the context or KB, respond exactly: "Sorry, we do not have the exact answer for your question and do not want to steer you in the wrong direction. I would recommend referring to either the American Diabetes Association page, respective medical calculators or drug specific package insert material for more specifics on this inquiry." Do not invent clinical details."""
    else:
        system = """You are a Type 2 Diabetes medication expert answering clinician questions (recommendations, results, other T2D questions). Use only the recommendation context and KB references below. Answer professionally and concisely (1–3 sentences). You may: answer beyond the recommendation; support communication/health literacy; add note context; counsel on new-therapy factors; give nutrition guidance; explain T2 management. Never ask for PHI/PII. If the answer is not in the context or KB, respond exactly: "Sorry, we do not have the exact answer for your question and do not want to steer you in the wrong direction. I would recommend referring to either the American Diabetes Association page, respective medical calculators or drug specific package insert material for more specifics on this inquiry." Do not invent clinical details."""

    context_parts = []
    for label, text in relevant_sections.items():
        if text:
            context_parts.append(f"## {label}\n{text}")
    context_block = "\n\n".join(context_parts) if context_parts else "(No sections selected)"

    kb_block = ""
    if kb_references_section and (kb_references_section or "").strip():
        kb_block = f"""## Knowledge base references (use to support your answer)

{kb_references_section.strip()}

"""

    conv_block = ""
    if conversation_turns:
        last_n = conversation_turns[-6:]
        lines = []
        for t in last_n:
            role = t.get("role", "user")
            content = (t.get("content") or "").strip()
            if content:
                lines.append(f"{role.upper()}: {content}")
        if lines:
            conv_block = "Recent conversation:\n" + "\n".join(lines) + "\n\n"

    if response_mode == "clinical_note":
        user_content = f"""{kb_block}## Recommendation context (relevant sections only)

{context_block}

{conv_block}User request: {question}"""
    else:
        user_content = f"""{kb_block}## Recommendation context (relevant sections only)

{context_block}

{conv_block}User question: {question}

Provide a concise answer in 1–3 sentences based on the context and knowledge base references above."""

    return system, user_content


def _call_bedrock(system_message, user_message, model_id, region=None, max_tokens=1024, temperature=0.2):
    """Call Bedrock Converse API; return (answer_text, input_tokens, output_tokens)."""
    if not boto3:
        raise RuntimeError("boto3 not available")
    kwargs = {"service_name": "bedrock-runtime"}
    if region:
        kwargs["region_name"] = region
    client = boto3.client(**kwargs)
    model_id = (model_id or "").strip()
    if not model_id:
        raise ValueError("BEDROCK_MODEL_ID is not set")
    messages = [{"role": "user", "content": [{"text": user_message}]}]
    request_kw = {
        "modelId": model_id,
        "messages": messages,
        "inferenceConfig": {"maxTokens": max_tokens, "temperature": temperature},
    }
    if system_message:
        request_kw["system"] = [{"text": system_message}]
    response = client.converse(**request_kw)
    usage = response.get("usage") or {}
    input_tokens = int(usage.get("inputTokens", 0))
    output_tokens = int(usage.get("outputTokens", 0))
    output = response.get("output") or {}
    msg = output.get("message") or {}
    content_list = msg.get("content") or []
    text_parts = []
    for block in content_list:
        if isinstance(block, dict) and block.get("text"):
            text_parts.append(block["text"])
    answer = "".join(text_parts).strip()
    return answer, input_tokens, output_tokens


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps(body, default=str),
    }


def handler(event, context):
    """
    Body: { question, recommendationTimestamp }.
    Optional: responseMode (e.g. "clinical_note") for structured note generation (no concise-answer guardrail).
    Returns 200 { answer, inputTokens, outputTokens, totalTokens } or error. Persists conversation turn to DynamoDB.
    """
    try:
        data = _parse_event(event)
        user_id = _get_user_id(event)
        if not user_id:
            _log("Missing userID (auth required)")
            return _response(400, {"error": "Missing userID (Cognito auth required)"})

        response_mode = (data.get("responseMode") or data.get("response_mode") or "").strip() or None
        raw_question = (data.get("question") or "").strip()
        # Fallback: if frontend didn't send responseMode but question asks to draft/generate a clinical note, use note mode
        if response_mode is None and raw_question:
            _head = raw_question[:400].lower()
            if "draft a clinical note" in _head or "generate a clinical note" in _head:
                response_mode = "clinical_note"
                _log("responseMode inferred as clinical_note from question text")
        max_chars = CLINICAL_NOTE_MAX_CHARS if response_mode == "clinical_note" else QUESTION_MAX_CHARS
        question = raw_question[:max_chars]
        rec_ts = data.get("recommendationTimestamp") or data.get("recommendation_timestamp") or ""
        rec_ts = rec_ts.strip() if isinstance(rec_ts, str) else ""
        if not question:
            return _response(400, {"error": "Missing question"})
        if not rec_ts:
            return _response(400, {"error": "Missing recommendationTimestamp"})

        # Log request summary for debugging (visible in CloudWatch)
        _log(f"request: responseMode={response_mode!r} question_len={len(question)} rec_ts={rec_ts[:36] + '...' if len(rec_ts) > 36 else rec_ts!r}")
        _question_preview = question[:500].replace("\n", " ").strip()
        if len(question) > 500:
            _question_preview += " ..."
        _log(f"question_preview: {_question_preview!r}")

        # In-code guardrails: block unsafe/off-topic/PII without calling Bedrock (0 tokens)
        allowed, canned = _input_guardrails(question, response_mode=response_mode)
        if not allowed and canned:
            return _response(200, {
                "answer": canned,
                "inputTokens": 0,
                "outputTokens": 0,
                "totalTokens": 0,
            })

        table_name = (os.environ.get("TABLE_NAME") or "T2D").strip()
        if not boto3:
            return _response(500, {"error": "boto3 not available"})

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)

        # Load recommendation item (request, response, conversation)
        try:
            resp = table.get_item(
                Key={"userID": str(user_id), "timestamp": str(rec_ts)},
            )
        except ClientError as e:
            _log(f"DynamoDB get_item error: {e}")
            return _response(500, {"error": str(e)})

        item = resp.get("Item")
        if not item:
            _log(f"Item not found userID={user_id!r} timestamp={rec_ts!r}")
            return _response(404, {"error": "Recommendation not found for this timestamp."})

        request_data = _to_native(item.get("request") or {})
        response_payload = item.get("response") or {}
        response_body = response_payload.get("body")
        if isinstance(response_body, str):
            try:
                response_body = json.loads(response_body)
            except json.JSONDecodeError:
                response_body = {}
        response_body = _to_native(response_body or {})

        conversation = item.get("conversation")
        if conversation is None:
            conversation = []
        elif not isinstance(conversation, list):
            conversation = [conversation]
        conversation_native = [_to_native(t) for t in conversation]

        # Relevant sections from intent (conversation + question)
        relevant = _get_relevant_sections(question, conversation_native, response_body)
        # Note mode: two-section format. Assessment = assessment + why this choice (rationale) + eGFR if applicable; Plan = from user request (future considerations, drug+dose, monitoring). Fold rationale and eGFR into one assessment block; drop medication/future (frontend sends those).
        if response_mode == "clinical_note":
            _assess = (relevant.get("assessment") or "").strip()
            _rationale = (relevant.get("rationale") or "").strip()
            _egfr = (relevant.get("egfr_warning") or "").strip()
            if _rationale:
                _assess += "\n\nWhy this choice: " + _rationale
            if _egfr:
                _assess += "\n\n" + _egfr
            relevant = {"assessment": _assess} if _assess else {}

        # Optional: retrieve from Bedrock Knowledge Base (files you sync to the KB)
        kb_id = (os.environ.get("BEDROCK_KNOWLEDGE_BASE_ID") or "").strip()
        bedrock_region = (os.environ.get("BEDROCK_REGION") or "").strip() or None
        kb_section = ""
        if kb_id:
            retrieval_query = f"Diabetes medication recommendation follow-up: {question}"
            kb_section, kb_chunk_count = _retrieve_from_bedrock_kb(
                kb_id, retrieval_query, region=bedrock_region, number_of_results=3
            )
            if kb_chunk_count:
                _log(f"Knowledge base: retrieved {kb_chunk_count} chunks")

        system_msg, user_msg = _build_prompt(
            question, conversation_native, relevant, kb_references_section=kb_section or None, response_mode=response_mode
        )

        # Log prompt sizes and mode (CloudWatch)
        max_tokens = 4096 if response_mode == "clinical_note" else 1024
        _log(f"prompt: system_len={len(system_msg)} user_len={len(user_msg)} max_tokens={max_tokens}")

        # Same env vars as ClinicalCalcs: no default model; require BEDROCK_MODEL_ID
        model_id = (os.environ.get("BEDROCK_MODEL_ID") or "").strip()
        if not model_id:
            _log("BEDROCK_MODEL_ID not set")
            return _response(503, {"error": "BEDROCK_MODEL_ID is not configured. Set it to the same value as your ClinicalCalcs Lambda."})

        answer, input_tokens, output_tokens = _call_bedrock(
            system_msg, user_msg, model_id, region=bedrock_region, max_tokens=max_tokens
        )
        _log(
            f"tokens: input={input_tokens} output={output_tokens} total={input_tokens + output_tokens}"
        )

        # Append turn to conversation and persist
        now = datetime.utcnow().isoformat() + "Z"
        new_turn = [
            {"role": "user", "content": question, "timestamp": now},
            {"role": "assistant", "content": answer, "timestamp": now},
        ]
        new_turn_dynamo = _to_dynamodb(new_turn)
        existing = conversation if isinstance(conversation, list) else []
        updated_list = list(existing) + new_turn_dynamo
        updated_list_dynamo = _to_dynamodb(updated_list)

        try:
            table.update_item(
                Key={"userID": str(user_id), "timestamp": str(rec_ts)},
                UpdateExpression="SET conversation = :conv",
                ExpressionAttributeValues={":conv": updated_list_dynamo},
            )
        except ClientError as e:
            _log(f"Failed to save conversation: {e}")
            # Still return the answer; persistence is best-effort

        return _response(200, {
            "answer": answer,
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "totalTokens": input_tokens + output_tokens,
        })

    except Exception as e:
        _log(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return _response(500, {"error": str(e)})


lambda_handler = handler
