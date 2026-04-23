#!/usr/bin/env python3
import json
import sqlite3
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


def write_extensions_config(path: Path, enabled: bool, file_patterns: list[str] | None = None) -> None:
    settings: dict[str, object] = {"enabled": enabled}
    if file_patterns is not None:
        settings["file_patterns"] = file_patterns
    path.write_text(
        json.dumps({"extensions": {"asciidoc_header": settings}}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        repo_dir = Path(tmp) / "repo"
        repo_dir.mkdir()

        shutil.copy2(ROOT_DIR / "sysmvp.py", repo_dir / "sysmvp.py")
        shutil.copy2(ROOT_DIR / "schema.sql", repo_dir / "schema.sql")
        shutil.copy2(ROOT_DIR / ".sysignore", repo_dir / ".sysignore")
        shutil.copytree(ROOT_DIR / "extractors", repo_dir / "extractors")

        subprocess.run(["python3", "sysmvp.py", "init"], cwd=repo_dir, check=True)
        config_path = repo_dir / ".sysextensions.json"
        assert config_path.exists()
        default_config = json.loads(config_path.read_text(encoding="utf-8"))
        assert default_config["extensions"]["asciidoc_header"]["enabled"] is False
        write_extensions_config(config_path, enabled=True, file_patterns=["*.adoc"])

        docs_dir = repo_dir / "docs"
        docs_dir.mkdir()
        adoc_name = "guide.adoc"
        txt_name = "guide.txt"
        (docs_dir / adoc_name).write_text(
            "// comment before title\n\n= Sample Guide\n\nBody text.\n",
            encoding="utf-8",
        )
        (docs_dir / txt_name).write_text("= Should Not Match\n", encoding="utf-8")

        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "docs"], cwd=repo_dir, check=True)

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        conn.row_factory = sqlite3.Row
        try:
            adoc_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = ?
                    """,
                    (f"docs/{adoc_name}",),
                ).fetchone()[0]
            )
            txt_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = ?
                    """,
                    (f"docs/{txt_name}",),
                ).fetchone()[0]
            )
            adoc_header_row = conn.execute(
                """
                SELECT f.value_json, COUNT(*) OVER () AS fact_count
                FROM fact f
                JOIN attribute a ON a.attr_id = f.attr_id
                WHERE f.entity_id = ?
                  AND a.ident = 'asciidoc/header'
                  AND f.added = 1
                ORDER BY f.tx_id DESC, f.fact_id DESC
                LIMIT 1
                """,
                (adoc_file_id,),
            ).fetchone()
            txt_header_row = conn.execute(
                """
                SELECT f.value_json
                FROM fact f
                JOIN attribute a ON a.attr_id = f.attr_id
                WHERE f.entity_id = ?
                  AND a.ident = 'asciidoc/header'
                  AND f.added = 1
                ORDER BY f.tx_id DESC, f.fact_id DESC
                LIMIT 1
                """,
                (txt_file_id,),
            ).fetchone()
        finally:
            conn.close()

        assert adoc_header_row is not None
        assert adoc_header_row["fact_count"] == 1
        assert json.loads(adoc_header_row["value_json"]) == {"header": "Sample Guide"}
        assert txt_header_row is None

    print("[asciidoc-header-extension-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
