"""
Score each occupation's AI exposure using an LLM.

This script sends each occupation's details to an LLM with a Pakistan-specific
scoring rubric and collects structured scores. Results are cached incrementally
to output/scores.json so the script can be resumed if interrupted.

The scoring considers Pakistan's specific economic context: infrastructure,
digitization level, informal economy, and technology adoption patterns.

Supported LLM providers:
  - OpenRouter (default): Requires OPENROUTER_API_KEY in .env
  - Any OpenAI-compatible API: Set API_URL and API_KEY in .env

Usage:
    python score.py                                    # score all occupations
    python score.py --start 0 --end 10                 # score first 10 only
    python score.py --model google/gemini-2.5-flash     # use specific model
    python score.py --force                            # re-score all (ignore cache)
    python score.py --dry-run                          # show what would be scored

The model used for scoring is automatically recorded in output/scores.json
metadata. This script allows the community to re-score using different
models, update scores for new occupations, or refine the scoring rubric.
"""

import argparse
import json
import os
import time
from datetime import datetime

import httpx
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_MODEL = "google/gemini-2.5-flash"
OUTPUT_FILE = "output/scores.json"
OCCUPATIONS_FILE = "output/occupations.json"

# Default to OpenRouter; override with API_URL env var for other providers
API_URL = os.environ.get("API_URL", "https://openrouter.ai/api/v1/chat/completions")
API_KEY = os.environ.get("OPENROUTER_API_KEY", os.environ.get("API_KEY", ""))

# ---------------------------------------------------------------------------
# Scoring Rubric (Pakistan-specific)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are an expert analyst evaluating how exposed different occupations are to \
AI, specifically in the context of Pakistan's economy and labor market.

You will be given the title and details of a Pakistani occupation.

Rate the occupation's overall **AI Exposure** on a scale from 0 to 10.

AI Exposure measures: how much will AI reshape this occupation in Pakistan? \
Consider both direct effects (AI automating tasks currently done by humans) \
and indirect effects (AI making each worker so productive that fewer are \
needed). Also consider Pakistan-specific factors: level of digitization, \
infrastructure availability, informal economy prevalence, and technology \
adoption patterns.

A key signal is whether the job's work product is fundamentally digital. If \
the job can be done entirely from a computer — writing, coding, analyzing, \
communicating — then AI exposure is inherently high (7+), because AI \
capabilities in digital domains are advancing rapidly. Conversely, jobs \
requiring physical presence, manual skill, or real-time human interaction \
in the physical world have a natural barrier to AI exposure.

Use these anchors to calibrate your score:

- **0-1: Minimal exposure.** The work is almost entirely physical, hands-on, \
or requires real-time human presence in unpredictable environments. AI has \
essentially no impact on daily work. \
Examples: Rickshaw driver, mason/bricklayer, agricultural laborer, sweeper.

- **2-3: Low exposure.** Mostly physical or interpersonal work. AI might help \
with minor peripheral tasks (scheduling, paperwork) but doesn't touch the \
core job. \
Examples: Electrician, plumber, nurse, firefighter, Lady Health Worker.

- **4-5: Moderate exposure.** A mix of physical/interpersonal work and \
knowledge work. AI can meaningfully assist with the information-processing \
parts but a substantial share of the job still requires human presence. \
Examples: Doctor (MBBS), police officer, veterinarian, pharmacist.

- **6-7: High exposure.** Predominantly knowledge work with some need for \
human judgment, relationships, or physical presence. AI tools are already \
useful and workers using AI may be substantially more productive. \
Examples: Accountant, civil engineer, journalist, HR manager, librarian.

- **8-9: Very high exposure.** The job is almost entirely done on a computer. \
All core tasks — writing, coding, analyzing, designing, communicating — are \
in domains where AI is rapidly improving. The occupation faces major \
restructuring. Pakistan's large freelance workforce is particularly exposed. \
Examples: Software developer, graphic designer, translator, data analyst, \
SEO specialist, content writer.

- **10: Maximum exposure.** Routine information processing, fully digital, \
with no physical component. AI can already do most of it today. \
Examples: Data entry operator, medical transcriptionist.

Respond with ONLY a JSON object in this exact format, no other text:
{
  "exposure": <0-10>,
  "rationale": "<2-3 sentences explaining the key factors, referencing Pakistan's specific context>"
}\
"""


# ---------------------------------------------------------------------------
# Scoring function
# ---------------------------------------------------------------------------

def _save_scores(scores, model):
    """Save scores with metadata (model name, timestamp)."""
    output = {
        "metadata": {
            "model": model,
            "scored_at": datetime.now().strftime("%Y-%m-%d"),
            "total": len(scores),
            "api": API_URL,
        },
        "scores": list(scores.values()),
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)


def is_anthropic_api():
    """Check if we're using the Anthropic API (vs OpenAI-compatible)."""
    return "anthropic.com" in API_URL


def score_occupation(client, occupation, model):
    """Send one occupation to the LLM and parse the structured response."""
    user_content = (
        f"Occupation: {occupation['title']}\n"
        f"Sector: {occupation.get('sector', 'N/A')}\n"
        f"Type: {occupation.get('type', 'N/A')}\n"
        f"Education required: {occupation.get('education', 'N/A')}\n"
        f"Category: {occupation.get('category', 'N/A')}\n"
    )

    if is_anthropic_api():
        # Anthropic Messages API format
        response = client.post(
            API_URL,
            headers={
                "x-api-key": API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 1024,
                "system": SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": user_content},
                ],
                "temperature": 0.2,
            },
            timeout=60,
        )
        response.raise_for_status()
        content = response.json()["content"][0]["text"]
    else:
        # OpenAI-compatible format (OpenRouter, OpenAI, etc.)
        response = client.post(
            API_URL,
            headers={
                "Authorization": f"Bearer {API_KEY}",
            },
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                "temperature": 0.2,
            },
            timeout=60,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]

    # Strip markdown code fences if present
    content = content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()

    return json.loads(content)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Score Pakistani occupations for AI exposure using an LLM"
    )
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"LLM model to use (default: {DEFAULT_MODEL})")
    parser.add_argument("--start", type=int, default=0,
                        help="Start index (inclusive)")
    parser.add_argument("--end", type=int, default=None,
                        help="End index (exclusive)")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Seconds between API calls (default: 0.5)")
    parser.add_argument("--force", action="store_true",
                        help="Re-score even if already cached")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be scored without calling the API")
    args = parser.parse_args()

    # Auto-detect: if using Anthropic API but model is OpenRouter default, suggest change
    if is_anthropic_api() and args.model == DEFAULT_MODEL:
        args.model = "claude-sonnet-4-20250514"
        print(f"(Anthropic API detected, using model: {args.model})")
        print(f"(Override with: python score.py --model your-model-id)")
        print()

    if not API_KEY and not args.dry_run:
        print("ERROR: No API key found.")
        print("Set one of these in pakistan-pipeline/.env:")
        print()
        print("  For Anthropic (Claude):")
        print("    API_URL=https://api.anthropic.com/v1/messages")
        print("    API_KEY=sk-ant-...")
        print()
        print("  For OpenRouter (Gemini, Claude, GPT, etc.):")
        print("    OPENROUTER_API_KEY=sk-or-...")
        print()
        print("Or score manually: edit output/scores.json directly")
        print("Or use Claude Code interactively (no API key needed)")
        return

    # Load occupations
    with open(OCCUPATIONS_FILE, encoding="utf-8") as f:
        occupations = json.load(f)

    subset = occupations[args.start:args.end]

    # Load existing scores (supports both old flat array and new metadata format)
    scores = {}
    existing_metadata = {}
    if os.path.exists(OUTPUT_FILE) and not args.force:
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, dict) and "scores" in raw:
            existing_metadata = raw.get("metadata", {})
            for entry in raw["scores"]:
                scores[entry["slug"]] = entry
        elif isinstance(raw, list):
            for entry in raw:
                scores[entry["slug"]] = entry

    print(f"Pakistan AI Exposure Scorer")
    print(f"Model: {args.model}")
    print(f"Occupations: {len(subset)} (indices {args.start}-{args.start + len(subset)})")
    print(f"Already cached: {len(scores)}")
    print(f"API: {API_URL}")
    print()

    if args.dry_run:
        to_score = [o for o in subset if o["slug"] not in scores]
        print(f"Would score {len(to_score)} occupations:")
        for o in to_score[:20]:
            print(f"  - {o['title']}")
        if len(to_score) > 20:
            print(f"  ... and {len(to_score) - 20} more")
        return

    errors = []
    client = httpx.Client()

    for i, occ in enumerate(subset):
        slug = occ["slug"]

        if slug in scores:
            continue

        print(f"  [{i+1}/{len(subset)}] {occ['title']}...", end=" ", flush=True)

        try:
            result = score_occupation(client, occ, args.model)
            scores[slug] = {
                "slug": slug,
                "title": occ["title"],
                **result,
            }
            print(f"exposure={result['exposure']}")
        except Exception as e:
            print(f"ERROR: {e}")
            errors.append(slug)

        # Save after each one (incremental checkpoint) with metadata
        _save_scores(scores, args.model)

        if i < len(subset) - 1:
            time.sleep(args.delay)

    client.close()

    print(f"\nDone. Scored {len(scores)} occupations, {len(errors)} errors.")
    if errors:
        print(f"Errors: {errors}")

    # Summary stats
    vals = [s for s in scores.values() if "exposure" in s]
    if vals:
        avg = sum(s["exposure"] for s in vals) / len(vals)
        by_score = {}
        for s in vals:
            bucket = s["exposure"]
            by_score[bucket] = by_score.get(bucket, 0) + 1
        print(f"\nAverage exposure: {avg:.1f}")
        print("Distribution:")
        for k in sorted(by_score):
            print(f"  {k}: {'#' * by_score[k]} ({by_score[k]})")


if __name__ == "__main__":
    main()
