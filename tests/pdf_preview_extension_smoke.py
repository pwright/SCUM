#!/usr/bin/env python3
import json
import os
import sqlite3
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


def write_extensions_config(path: Path, enabled: bool) -> None:
    settings: dict[str, object] = {
        "enabled": enabled,
        "dpi": 144,
        "format": "png",
        "output": ".sysstore/custom_pdf_preview",
    }
    path.write_text(
        json.dumps({"extensions": {"pdf_preview": settings}}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_stub_pdftoppm(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env python3
import pathlib
import sys

args = sys.argv[1:]
fmt_flag = args[2]
base = pathlib.Path(args[4])
suffix = "png" if fmt_flag == "-png" else "jpg"
base.parent.mkdir(parents=True, exist_ok=True)
(base.parent / f"{base.name}-1.{suffix}").write_bytes(b"page-1")
(base.parent / f"{base.name}-2.{suffix}").write_bytes(b"page-2")
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        repo_dir = Path(tmp) / "repo"
        repo_dir.mkdir()

        shutil.copy2(ROOT_DIR / "sysmvp.py", repo_dir / "sysmvp.py")
        shutil.copy2(ROOT_DIR / "schema.sql", repo_dir / "schema.sql")
        shutil.copy2(ROOT_DIR / ".sysignore", repo_dir / ".sysignore")
        shutil.copytree(ROOT_DIR / "extractors", repo_dir / "extractors")

        stub_bin = repo_dir / "stub-bin"
        stub_bin.mkdir()
        write_stub_pdftoppm(stub_bin / "pdftoppm")
        env = os.environ.copy()
        env["PATH"] = str(stub_bin) + os.pathsep + env.get("PATH", "")

        subprocess.run(["python3", "sysmvp.py", "init"], cwd=repo_dir, check=True)
        config_path = repo_dir / ".sysextensions.json"
        assert config_path.exists()
        default_config = json.loads(config_path.read_text(encoding="utf-8"))
        assert default_config["extensions"]["pdf_preview"]["enabled"] is False
        write_extensions_config(config_path, enabled=True)

        docs_dir = repo_dir / "docs"
        docs_dir.mkdir()
        pdf_name = "guide.pdf"
        (docs_dir / pdf_name).write_bytes(b"%PDF-1.4\n%stub pdf\n")

        subprocess.run(
            ["python3", "sysmvp.py", "scan", "--root", "docs"],
            cwd=repo_dir,
            check=True,
            env=env,
        )

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        conn.row_factory = sqlite3.Row
        try:
            pdf_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = ?
                    """,
                    (f"docs/{pdf_name}",),
                ).fetchone()[0]
            )
            pdf_preview_row = conn.execute(
                """
                SELECT f.value_json, COUNT(*) OVER () AS fact_count
                FROM fact f
                JOIN attribute a ON a.attr_id = f.attr_id
                WHERE f.entity_id = ?
                  AND a.ident = 'pdf/preview'
                  AND f.added = 1
                ORDER BY f.tx_id DESC, f.fact_id DESC
                LIMIT 1
                """,
                (pdf_file_id,),
            ).fetchone()
        finally:
            conn.close()

        assert pdf_preview_row is not None
        assert pdf_preview_row["fact_count"] == 1
        payload = json.loads(pdf_preview_row["value_json"])
        assert payload["dpi"] == 144
        assert payload["format"] == "png"
        assert payload["page_count"] == 2
        assert payload["source"] == "docs/guide.pdf"
        assert payload["output_dir"] == ".sysstore/custom_pdf_preview/docs"
        assert len(payload["images"]) == 2
        for image in payload["images"]:
            assert image.startswith(".sysstore/custom_pdf_preview/docs/guide-")
            assert image.endswith(".png")
            assert (repo_dir / image).exists()

    print("[pdf-preview-extension-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
