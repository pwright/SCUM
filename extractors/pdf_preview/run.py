#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import json
import mimetypes
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional


EXTENSIONS_FILE = ".sysextensions.json"
DEFAULT_OUTPUT = ".sysstore/pdf_preview"


def log(message: str) -> None:
    print(f"[pdf_preview] {message}", file=sys.stderr)


def normalize_path(value: str | Path) -> Path:
    return Path(value).expanduser()


def load_extension_settings(repo_root: Optional[Path]) -> dict[str, object]:
    if repo_root is None:
        return {}
    config_path = repo_root / EXTENSIONS_FILE
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    extensions = payload.get("extensions", {})
    if not isinstance(extensions, dict):
        return {}
    raw_settings = extensions.get("pdf_preview", {})
    if isinstance(raw_settings, dict):
        return raw_settings
    return {}


def resolve_value(cli_value, settings: dict[str, object], key: str, default=None):
    if cli_value is not None:
        return cli_value
    return settings.get(key, default)


def parse_after(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(normalized).timestamp()
    except ValueError as exc:
        raise SystemExit(
            f"Invalid --after value: {value!r}. Use ISO date/datetime like 2026-04-18 or 2026-04-18T09:30:00."
        ) from exc


def is_after_cutoff(path: Path, after_ts: Optional[float]) -> bool:
    if after_ts is None:
        return True
    return path.stat().st_mtime > after_ts


def detect_mime(path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_relpath(path: Path, repo_root: Optional[Path]) -> str:
    if repo_root is None:
        return path.name
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def resolve_output_root(raw_output: object, repo_root: Optional[Path]) -> Path:
    output_path = normalize_path(str(raw_output))
    if output_path.is_absolute() or repo_root is None:
        return output_path
    return repo_root / output_path


def output_relpath(path: Path, repo_root: Optional[Path]) -> str:
    if repo_root is None:
        return str(path)
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def output_extensions(fmt: str) -> tuple[str, ...]:
    if fmt == "jpeg":
        return ("jpg", "jpeg")
    return (fmt,)


def generated_images(base: Path, fmt: str, repo_root: Optional[Path]) -> list[str]:
    candidates: list[tuple[int, str]] = []
    for extension in output_extensions(fmt):
        for candidate in base.parent.glob(f"{base.name}-*.{extension}"):
            suffix = candidate.stem.removeprefix(base.name + "-")
            try:
                page = int(suffix)
            except ValueError:
                page = sys.maxsize
            candidates.append((page, output_relpath(candidate, repo_root)))
    return [item[1] for item in sorted(candidates, key=lambda item: (item[0], item[1]))]


def convert_pdf(pdf_path: Path, output_root: Path, fmt: str, dpi: int, repo_root: Optional[Path]) -> Optional[dict[str, object]]:
    pdftoppm_path = shutil.which("pdftoppm")
    if pdftoppm_path is None:
        raise SystemExit("Required tool not found: pdftoppm")

    relative_source = Path(source_relpath(pdf_path, repo_root))
    source_hash = sha256_file(pdf_path)[:16]
    base_dir = output_root / relative_source.parent
    base_dir.mkdir(parents=True, exist_ok=True)
    base = base_dir / f"{pdf_path.stem}-{source_hash}"

    cmd = [
        pdftoppm_path,
        "-r",
        str(dpi),
        f"-{fmt}",
        str(pdf_path),
        str(base),
    ]
    log("Running: " + " ".join(cmd))
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip()
        if detail:
            raise SystemExit(detail)
        raise SystemExit("pdftoppm failed")

    images = generated_images(base, fmt, repo_root)
    if not images:
        raise SystemExit("pdftoppm completed without generating page images")

    return {
        "dpi": dpi,
        "format": fmt,
        "images": images,
        "output_dir": output_relpath(base_dir, repo_root),
        "page_count": len(images),
        "source": source_relpath(pdf_path, repo_root),
        "tool": "pdftoppm",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Render PDF pages to images and emit JSON")
    parser.add_argument("path")
    parser.add_argument("--mime")
    parser.add_argument("--repo-root")
    parser.add_argument("-o", "--output")
    parser.add_argument("-f", "--format", choices=["png", "jpeg"])
    parser.add_argument("--dpi", type=int)
    parser.add_argument("--after")
    args = parser.parse_args()

    pdf_path = normalize_path(args.path)
    if not pdf_path.exists():
        return 0

    repo_root_arg = args.repo_root or os.environ.get("SYSMVP_REPO_ROOT")
    repo_root = normalize_path(repo_root_arg).resolve() if repo_root_arg else None
    settings = load_extension_settings(repo_root)

    fmt = resolve_value(args.format, settings, "format", "png")
    if fmt not in {"png", "jpeg"}:
        raise SystemExit(f"Invalid format: {fmt!r}. Use 'png' or 'jpeg'.")

    dpi = resolve_value(args.dpi, settings, "dpi", 300)
    try:
        dpi = int(dpi)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"Invalid dpi: {dpi!r}. Use an integer.") from exc

    after = parse_after(resolve_value(args.after, settings, "after"))
    mime = args.mime or detect_mime(pdf_path)
    if mime != "application/pdf" and pdf_path.suffix.lower() != ".pdf":
        return 0
    if not is_after_cutoff(pdf_path, after):
        return 0

    output_root = resolve_output_root(resolve_value(args.output, settings, "output", DEFAULT_OUTPUT), repo_root)
    rendered = convert_pdf(pdf_path, output_root, fmt, dpi, repo_root)
    if rendered is None:
        return 0
    json.dump(rendered, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
