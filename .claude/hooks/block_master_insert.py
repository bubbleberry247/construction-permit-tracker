"""PreToolUse hook: Block INSERT INTO companies/company_emails.
Reads JSON from stdin, checks tool_input.command for prohibited patterns.
Exit 0 = allow, prints deny JSON = block.
"""
import json
import sys
import re

try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)

command = data.get("tool_input", {}).get("command", "")

# Check for prohibited patterns (case-insensitive)
prohibited = [
    r"INSERT\s+INTO\s+companies",
    r"INSERT\s+INTO\s+company_emails",
    r"INSERT\s+INTO\s+\[?companies\]?",
    r"INSERT\s+INTO\s+\[?company_emails\]?",
]

for pattern in prohibited:
    if re.search(pattern, command, re.IGNORECASE):
        result = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (
                    "MDI-001/002違反: マスタデータ(companies/company_emails)への自動INSERT禁止。"
                    "policy/master_data_invariants.yaml参照。"
                    "マスタにない会社は隔離・報告のみ。"
                ),
            }
        }
        print(json.dumps(result, ensure_ascii=False))
        sys.exit(0)

sys.exit(0)
