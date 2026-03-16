import json
import os
from typing import Any, Dict, List

from anthropic import Anthropic


DEFAULT_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")


SYSTEM_PROMPT = """
You are a supply chain risk analyst supporting a Global Supply Manager.

You will receive multiple public-source items for one supplier.
Use the supplier category context to decide whether the signal matters.

Return ONLY valid JSON with this exact schema:
{
  "supplier_summary": "One concise sentence summarizing the supplier's overall risk posture today.",
  "signals": [
    {
      "item_index": 1,
      "is_relevant": true,
      "risk_type": "financial|logistics|esg_compliance|supply_capacity|general_news|not_relevant",
      "severity": "Low|Medium|High",
      "reason": "One concise sentence explaining why this matters.",
      "recommended_action": "One concise action recommendation.",
      "evidence_summary": "One concise sentence summarizing the evidence."
    }
  ]
}

Rules:
- Include only materially relevant items.
- Use category context when assessing importance.
- A capacity expansion can be positive or risky depending on category and context.
- Exclude irrelevant/noise items from signals.
- Keep all text concise and practical.
- Never include markdown or commentary outside JSON.
""".strip()


def _safe_json_parse(text: str) -> Dict[str, Any]:
    text = text.strip()

    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {
            "supplier_summary": "Model response could not be parsed cleanly.",
            "signals": []
        }


def extract_signals_for_supplier(
    supplier_name: str,
    category_id: str,
    category_name: str,
    category_context: Dict[str, Any],
    items: List[Dict[str, str]],
    model: str = DEFAULT_MODEL,
) -> Dict[str, Any]:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY is not set.")

    client = Anthropic(api_key=api_key)

    formatted_items = []
    for i, item in enumerate(items, 1):
        formatted_items.append(
            f"""
Item {i}
Title: {item.get("source_title", "")}
Snippet: {item.get("source_snippet", "")}
URL: {item.get("source_url", "")}
Source Type: {item.get("source_type", "")}
""".strip()
        )

    user_prompt = f"""
Supplier Name: {supplier_name}
Category ID: {category_id}
Category Name: {category_name}
Category Context: {json.dumps(category_context)}

Analyze these public-source items for supplier risk relevance.

{chr(10).join(formatted_items)}

Return only valid JSON.
""".strip()

    response = client.messages.create(
        model=model,
        max_tokens=800,
        temperature=0,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    content_text = ""
    for block in response.content:
        if getattr(block, "type", None) == "text":
            content_text += block.text

    parsed = _safe_json_parse(content_text)

    signals = []
    for signal in parsed.get("signals", []):
        signals.append(
            {
                "item_index": signal.get("item_index"),
                "is_relevant": bool(signal.get("is_relevant", False)),
                "risk_type": signal.get("risk_type", "not_relevant"),
                "severity": signal.get("severity", "Low"),
                "reason": signal.get("reason", "No reason provided."),
                "recommended_action": signal.get("recommended_action", "Monitor for further updates."),
                "evidence_summary": signal.get("evidence_summary", "No evidence summary provided."),
            }
        )

    return {
        "supplier_summary": parsed.get(
            "supplier_summary",
            "No supplier-level summary generated."
        ),
        "signals": signals,
    }