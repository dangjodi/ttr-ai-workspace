"""
TTR AI Workspace - AI Chat Module
Provides intelligent Q&A about TTR data
"""

import os

# Try to import anthropic, but make it optional
try:
    from anthropic import Anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False


def get_ai_client():
    """Get Anthropic client."""
    if not ANTHROPIC_AVAILABLE:
        return None
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        return None
    return Anthropic(api_key=api_key)


def get_ai_response(question, data_context, conversation_history=None):
    """
    Get AI response to a question about TTR data.
    """
    client = get_ai_client()
    
    if client is None:
        return fallback_response(question, data_context)
    
    system_prompt = f"""You are an AI assistant for the TTR AI Workspace — a tool for analyzing Time To Resolve (TTR) performance for AHA Operations.

Your role:
1. Answer questions about DART P90 performance, trends, and root causes
2. Provide actionable insights and recommendations
3. Be concise and data-driven

Key Definitions:
- DART P90: Days to Action Resolution Target — 90th percentile TTR for Internal Ops cases
- Internal Ops: Cases resolved within Operations control (no external policy dependency)
- Stretch Target: ≤6.0 days (Green)
- Ceiling Target: ≤7.5 days (Yellow/acceptable)
- At Risk: >7.5 days (Red)

Current Data Context:
{data_context}
"""

    messages = []
    if conversation_history:
        messages.extend(conversation_history)
    messages.append({"role": "user", "content": question})
    
    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=system_prompt,
            messages=messages
        )
        return response.content[0].text
    except Exception as e:
        return f"AI Error: {str(e)}\n\n{fallback_response(question, data_context)}"


def fallback_response(question, data_context):
    """Provide a basic response when AI is not available."""
    question_lower = question.lower()
    
    lines = data_context.split('\n')
    
    if 'current' in question_lower or 'latest' in question_lower or 'status' in question_lower:
        for line in lines:
            if 'Latest DART P90' in line:
                return f"📊 **Current Status:**\n{line}\n\n💡 *For deeper AI analysis, install anthropic: `py -m pip install anthropic`*"
    
    if 'trend' in question_lower or 'weekly' in question_lower:
        weekly_lines = []
        capture = False
        for line in lines:
            if 'Weekly DART P90 Trend' in line:
                capture = True
                continue
            if capture and line.startswith('- 20'):
                weekly_lines.append(line)
        if weekly_lines:
            return "📈 **Weekly DART P90 Trend (Last 5 weeks):**\n" + '\n'.join(weekly_lines[-5:])
    
    if 'spike' in question_lower or 'red' in question_lower or 'why' in question_lower:
        red_weeks = []
        for line in lines:
            if 'Red' in line and '20' in line:
                red_weeks.append(line)
        if red_weeks:
            return f"🔴 **Red Weeks Identified:**\n" + '\n'.join(red_weeks) + "\n\n💡 *Check Manager Scorecard and Vertical Analysis for root cause patterns.*"
    
    if 'recommend' in question_lower or 'suggestion' in question_lower:
        return """💡 **Recommendations:**

1. **Focus on Red Weeks** — Drill into Manager Scorecard during spike weeks to identify outliers
2. **Monitor High-Risk Verticals** — Check Vertical Analysis for dev_reasons consistently above 6.0 days
3. **Early Intervention** — Use Case Explorer to identify cases aging >7 days before they become tail cases

*For AI-powered recommendations, install: `py -m pip install anthropic`*"""

    # Default response with data summary
    summary_lines = []
    for line in lines:
        if any(x in line for x in ['Total Internal', 'Overall DART', 'Latest Week', 'Green Weeks']):
            summary_lines.append(line)
    
    return f"""📊 **Data Summary:**
{chr(10).join(summary_lines[:6])}

💡 *Ask about: "current status", "weekly trend", "red weeks", or "recommendations"*

*For full AI chat, install: `py -m pip install anthropic`*"""


def generate_weekly_summary(data_context):
    """Generate a weekly summary."""
    return get_ai_response(
        "Generate a brief weekly summary for leadership (under 100 words).",
        data_context
    )


def get_recommendations(data_context):
    """Get recommendations based on current data."""
    return get_ai_response(
        "Provide 3 specific, actionable recommendations to improve DART P90.",
        data_context
    )
