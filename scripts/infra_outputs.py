"""Turn Bicep deployment outputs into KEY=VALUE lines the Makefile can include.

    az deployment sub show -n <name> --query properties.outputs -o json \\
        | python scripts/infra_outputs.py > build/infra.env

workspaceUrl becomes WORKSPACE_URL, accessConnectorId becomes ACCESS_CONNECTOR_ID.
"""

from __future__ import annotations

import json
import re
import sys


def env_name(output_name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", output_name).upper()


def to_env_lines(outputs: dict) -> list[str]:
    return [f"{env_name(name)}={item['value']}" for name, item in sorted(outputs.items())]


def main() -> None:
    outputs = json.load(sys.stdin)
    if not outputs:
        raise SystemExit("The deployment has no outputs. Run make infra-up first.")
    print("\n".join(to_env_lines(outputs)))


if __name__ == "__main__":
    main()
