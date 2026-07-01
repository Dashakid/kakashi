"""
Oil signal engine — feeds market context to an LLM and returns a probability estimate.
Also provides Kelly Criterion position sizing.
"""

import json
import os

from groq import Groq
from dotenv import load_dotenv

load_dotenv()

GROQ_MODEL = "llama-3.3-70b-versatile"


# ---------------------------------------------------------------------------
# Claude probability estimator
# ---------------------------------------------------------------------------

async def get_oil_probability(context: dict) -> dict:
    """
    Feed oil market context to Groq (llama-3.3-70b) and get probability estimate.

    Returns:
        {
            "prob_up": 0.67,
            "prob_down": 0.33,
            "confidence": "medium",
            "key_factors": [...],
            "reasoning": "..."
        }
    """
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    prompt = f"""You are an oil market analyst. Based on the following real-time data, \
estimate the probability that WTI crude oil price will be HIGHER in 7 days than today.

CURRENT DATA:
{json.dumps(context, indent=2)}

Analyze:
1. EIA inventory change (negative = bullish, positive = bearish)
2. Geopolitical headlines and their likely impact
3. Price momentum (5-day trend)
4. OPEC/sanctions news sentiment

Respond ONLY with valid JSON (no markdown, no code fences):
{{
  "prob_up": <float 0-1>,
  "prob_down": <float 0-1>,
  "confidence": "<low|medium|high>",
  "key_factors": ["factor1", "factor2", "factor3"],
  "reasoning": "<2-3 sentence summary>"
}}"""

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500,
    )

    raw = response.choices[0].message.content.strip()

    # Strip accidental markdown code fences if Claude wraps them
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    result = json.loads(raw)

    # Normalise: ensure prob_up + prob_down = 1
    total = result.get("prob_up", 0) + result.get("prob_down", 0)
    if total > 0 and abs(total - 1.0) > 0.01:
        result["prob_up"] = round(result["prob_up"] / total, 4)
        result["prob_down"] = round(result["prob_down"] / total, 4)

    return result


# ---------------------------------------------------------------------------
# Kelly Criterion position sizing
# ---------------------------------------------------------------------------

def kelly_position_size(
    prob_win: float,
    odds: float,
    bankroll: float,
    kelly_fraction: float = 0.25,
) -> float:
    """
    Calculate fractional Kelly bet size.

    Args:
        prob_win:       Claude's probability of winning (e.g. 0.67)
        odds:           Polymarket price / implied probability (e.g. 0.55)
        bankroll:       Current USDC balance
        kelly_fraction: Safety multiplier — 0.25 = quarter Kelly

    Returns:
        Dollar amount to bet (0 if no edge).
    """
    edge = prob_win - odds
    if edge <= 0:
        return 0.0

    # Kelly %: (p * b - q) / b  where b = net odds (1/odds - 1), q = 1 - p
    b = (1.0 / odds) - 1.0
    if b <= 0:
        return 0.0

    kelly_pct = (prob_win * b - (1.0 - prob_win)) / b
    fractional_kelly = kelly_pct * kelly_fraction

    # Hard cap: 5% of bankroll per trade
    max_bet = bankroll * 0.05
    bet = min(bankroll * fractional_kelly, max_bet)
    return round(max(bet, 0.0), 2)
