#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

HEADER_RE = re.compile(r"^(={1,6})\s+(.+?)\s*$")


def extract_first_header(path: Path) -> Optional[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("//"):
                    continue
                match = HEADER_RE.match(line)
                if match is None:
                    continue
                return {"header": match.group(2).strip()}
    except OSError:
        return None
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract the first AsciiDoc header as JSON")
    parser.add_argument("path")
    parser.add_argument("--mime")
    args = parser.parse_args()

    extracted = extract_first_header(Path(args.path))
    if extracted is None:
        return 0
    json.dump(extracted, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
