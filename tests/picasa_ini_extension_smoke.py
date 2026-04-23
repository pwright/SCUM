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
        json.dumps({"extensions": {"picasa_ini": settings}}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    sample = """[Picasa]
name=Desktop
category=Folders on Disk
date=39655.566111

[.album:album-1]
name=Ireland and Scotland
date=2016-05-16T19:02:32+01:00
token=album-1

[photo-one.tif]
caption=photo one caption
star=yes
albums=album-1,missing-album

[photo-two.tif]
albums=album-1
"""
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
        assert default_config["extensions"]["picasa_ini"]["enabled"] is False
        write_extensions_config(config_path, enabled=True, file_patterns=["*.picasa.ini"])

        docs_dir = repo_dir / "photos"
        docs_dir.mkdir()
        picasa_name = ".picasa.ini"
        skipped_name = "ignored.ini"
        (docs_dir / picasa_name).write_text(sample, encoding="utf-8")
        (docs_dir / skipped_name).write_text(sample, encoding="utf-8")

        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "photos"], cwd=repo_dir, check=True)

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        conn.row_factory = sqlite3.Row
        try:
            picasa_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = ?
                    """,
                    (f"photos/{picasa_name}",),
                ).fetchone()[0]
            )
            skipped_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = ?
                    """,
                    (f"photos/{skipped_name}",),
                ).fetchone()[0]
            )
            picasa_row = conn.execute(
                """
                SELECT f.value_json, COUNT(*) OVER () AS fact_count
                FROM fact f
                JOIN attribute a ON a.attr_id = f.attr_id
                WHERE f.entity_id = ?
                  AND a.ident = 'picasa/ini'
                  AND f.added = 1
                ORDER BY f.tx_id DESC, f.fact_id DESC
                LIMIT 1
                """,
                (picasa_file_id,),
            ).fetchone()
            skipped_row = conn.execute(
                """
                SELECT f.value_json
                FROM fact f
                JOIN attribute a ON a.attr_id = f.attr_id
                WHERE f.entity_id = ?
                  AND a.ident = 'picasa/ini'
                  AND f.added = 1
                ORDER BY f.tx_id DESC, f.fact_id DESC
                LIMIT 1
                """,
                (skipped_file_id,),
            ).fetchone()
        finally:
            conn.close()

        assert picasa_row is not None
        assert picasa_row["fact_count"] == 1
        payload = json.loads(picasa_row["value_json"])
        assert payload["folder"] == {
            "category": "Folders on Disk",
            "date": "39655.566111",
            "name": "Desktop",
        }
        assert payload["albums"]["album-1"]["images"] == ["photo-one.tif", "photo-two.tif"]
        assert payload["albums"]["album-1"]["name"] == "Ireland and Scotland"
        assert payload["albums"]["missing-album"] == {}
        assert payload["images"][0]["albums"] == ["Ireland and Scotland", "[missing:missing-album]"]
        assert payload["images"][0]["caption"] == "photo one caption"
        assert payload["images"][0]["star"] is True
        assert skipped_row is None

    print("[picasa-ini-extension-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
