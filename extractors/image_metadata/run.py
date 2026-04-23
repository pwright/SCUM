#!/usr/bin/env python3
import argparse
import json
import mimetypes
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

XMP_NS = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
}


def normalize_extracted_text(value: str) -> Optional[str]:
    normalized = " ".join(value.split())
    if not normalized:
        return None
    return normalized


def extract_xmp_description(payload: bytes) -> Optional[str]:
    start = 0
    closing_tag = b"</x:xmpmeta>"
    while True:
        open_index = payload.find(b"<x:xmpmeta", start)
        if open_index == -1:
            return None
        close_index = payload.find(closing_tag, open_index)
        if close_index == -1:
            return None
        xml_bytes = payload[open_index : close_index + len(closing_tag)]
        start = close_index + len(closing_tag)
        try:
            root = ET.fromstring(xml_bytes.decode("utf-8", errors="ignore"))
        except ET.ParseError:
            continue

        fallback: Optional[str] = None
        for description in root.findall(".//dc:description", XMP_NS):
            for entry in description.findall("rdf:Alt/rdf:li", XMP_NS):
                text = normalize_extracted_text("".join(entry.itertext()))
                if text is None:
                    continue
                if entry.attrib.get("{http://www.w3.org/XML/1998/namespace}lang") == "x-default":
                    return text
                if fallback is None:
                    fallback = text
            text = normalize_extracted_text("".join(description.itertext()))
            if text is not None and fallback is None:
                fallback = text
        if fallback is not None:
            return fallback


def extract_image_metadata_from_exiftool(path: Path) -> Optional[dict[str, object]]:
    exiftool_path = shutil.which("exiftool")
    if exiftool_path is None:
        return None
    probe = subprocess.run(
        [
            exiftool_path,
            "-json",
            "-G1",
            "-a",
            "-s",
            "-XMP:all",
            "-IPTC:all",
            "-EXIF:all",
            "-JFIF:all",
            "-Photoshop:all",
            str(path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if probe.returncode != 0:
        return None
    try:
        payload = json.loads(probe.stdout)
    except json.JSONDecodeError:
        return None
    if not payload:
        return None
    raw_metadata = payload[0]
    metadata = {
        key: value
        for key, value in raw_metadata.items()
        if key != "SourceFile" and value not in (None, "")
    }
    if not metadata:
        return None
    return metadata


def detect_mime(path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"


def extract_image_metadata(path: Path, mime: str) -> Optional[dict[str, object]]:
    if not mime.startswith("image/"):
        return None
    metadata = extract_image_metadata_from_exiftool(path)
    if metadata is not None:
        return metadata
    try:
        payload = path.read_bytes()
    except OSError:
        return None
    description = extract_xmp_description(payload)
    if description is None:
        return None
    return {"XMP-dc:Description": description}


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract image metadata as JSON")
    parser.add_argument("path")
    parser.add_argument("--mime")
    args = parser.parse_args()

    path = Path(args.path)
    mime = args.mime or detect_mime(path)
    metadata = extract_image_metadata(path, mime)
    if metadata is None:
        return 0
    json.dump(metadata, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
