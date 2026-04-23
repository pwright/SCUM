#!/usr/bin/env python3
import json
import sqlite3
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent


def create_xmp_image(path: Path, description: str) -> None:
    payload = f"""not-a-real-jpeg
<x:xmpmeta xmlns:x="adobe:ns:meta/">
  <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
    <rdf:Description xmlns:dc="http://purl.org/dc/elements/1.1/">
      <dc:description>
        <rdf:Alt>
          <rdf:li xml:lang="x-default">{description}</rdf:li>
        </rdf:Alt>
      </dc:description>
    </rdf:Description>
  </rdf:RDF>
</x:xmpmeta>
""".encode("utf-8")
    path.write_bytes(payload)


def write_extensions_config(path: Path, enabled: bool, file_patterns: list[str] | None = None) -> None:
    settings: dict[str, object] = {"enabled": enabled}
    if file_patterns is not None:
        settings["file_patterns"] = file_patterns
    path.write_text(
        json.dumps({"extensions": {"image_metadata": settings}}, indent=2, sort_keys=True) + "\n",
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
        assert default_config["extensions"]["image_metadata"]["enabled"] is False
        write_extensions_config(config_path, enabled=True, file_patterns=["*.png"])

        image_demo_dir = repo_dir / "image_demo"
        image_demo_dir.mkdir()
        image_name = "external-script.png"
        create_xmp_image(image_demo_dir / image_name, "embedded fallback text that should be ignored")
        (repo_dir / "extractors" / "image_metadata" / "run.py").write_text(
            """#!/usr/bin/env python3
import json
import sys

json.dump({"caption": "external script caption", "source": "script"}, sys.stdout, sort_keys=True)
sys.stdout.write("\\n")
""",
            encoding="utf-8",
        )
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "image_demo"], cwd=repo_dir, check=True)
        skipped_dir = repo_dir / "image_skipped"
        skipped_dir.mkdir()
        skipped_name = "should-skip.jpg"
        skipped_description = "jpg should not run because repo config narrows patterns to png only"
        create_xmp_image(skipped_dir / skipped_name, skipped_description)
        subprocess.run(["python3", "sysmvp.py", "scan", "--root", "image_skipped"], cwd=repo_dir, check=True)

        image_fallback_dir = repo_dir / "image_fallback"
        image_fallback_dir.mkdir()
        fallback_image_name = "fallback.png"
        fallback_description = "fallback xmp metadata"
        create_xmp_image(image_fallback_dir / fallback_image_name, fallback_description)
        (repo_dir / "extractors" / "image_metadata" / "run.py").unlink()
        subprocess.run(
            ["python3", "sysmvp.py", "scan", "--root", "image_fallback"],
            cwd=repo_dir,
            check=True,
        )

        conn = sqlite3.connect(repo_dir / ".sysmvp.db")
        conn.row_factory = sqlite3.Row
        try:
            image_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = ?
                    """,
                    (f"image_demo/{image_name}",),
                ).fetchone()[0]
            )
            fallback_image_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = ?
                    """,
                    (f"image_fallback/{fallback_image_name}",),
                ).fetchone()[0]
            )
            skipped_image_file_id = int(
                conn.execute(
                    """
                    SELECT file_id
                    FROM file_entry
                    WHERE current_path = ?
                    """,
                    (f"image_skipped/{skipped_name}",),
                ).fetchone()[0]
            )
            image_metadata_row = conn.execute(
                """
                SELECT f.value_json, COUNT(*) OVER () AS fact_count
                FROM fact f
                JOIN attribute a ON a.attr_id = f.attr_id
                WHERE f.entity_id = ?
                  AND a.ident = 'image/metadata'
                  AND f.added = 1
                ORDER BY f.tx_id DESC, f.fact_id DESC
                LIMIT 1
                """,
                (image_file_id,),
            ).fetchone()
            fallback_metadata_row = conn.execute(
                """
                SELECT f.value_json, COUNT(*) OVER () AS fact_count
                FROM fact f
                JOIN attribute a ON a.attr_id = f.attr_id
                WHERE f.entity_id = ?
                  AND a.ident = 'image/metadata'
                  AND f.added = 1
                ORDER BY f.tx_id DESC, f.fact_id DESC
                LIMIT 1
                """,
                (fallback_image_file_id,),
            ).fetchone()
            skipped_metadata_row = conn.execute(
                """
                SELECT f.value_json
                FROM fact f
                JOIN attribute a ON a.attr_id = f.attr_id
                WHERE f.entity_id = ?
                  AND a.ident = 'image/metadata'
                  AND f.added = 1
                ORDER BY f.tx_id DESC, f.fact_id DESC
                LIMIT 1
                """,
                (skipped_image_file_id,),
            ).fetchone()
        finally:
            conn.close()

        assert image_metadata_row is not None
        assert image_metadata_row["fact_count"] == 1
        image_metadata = json.loads(image_metadata_row[0])
        assert image_metadata == {"caption": "external script caption", "source": "script"}

        assert fallback_metadata_row is not None
        assert fallback_metadata_row["fact_count"] == 1
        fallback_metadata = json.loads(fallback_metadata_row[0])
        assert fallback_metadata["XMP-dc:Description"] == fallback_description
        assert skipped_metadata_row is None

    print("[image-metadata-extension-smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
