#!/usr/bin/env python3
import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


SECTION_RE = re.compile(r"^\[(.*)\]\s*$")
ALBUM_SECTION_PREFIX = ".album:"


@dataclass
class Album:
    album_id: str
    name: Optional[str] = None
    date: Optional[str] = None
    token: Optional[str] = None
    images: list[str] = field(default_factory=list)


@dataclass
class ImageRecord:
    file: str
    caption: Optional[str] = None
    star: Optional[bool] = None
    album_ids: list[str] = field(default_factory=list)
    albums: list[str] = field(default_factory=list)


@dataclass
class FolderMeta:
    name: Optional[str] = None
    category: Optional[str] = None
    date: Optional[str] = None


class PicasaParser:
    def __init__(self) -> None:
        self.folder = FolderMeta()
        self.albums: dict[str, Album] = {}
        self.images: dict[str, ImageRecord] = {}

    def parse(self, text: str) -> "PicasaParser":
        current_kind: Optional[str] = None
        current_album_id: Optional[str] = None
        current_image: Optional[str] = None

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            section_match = SECTION_RE.match(line)
            if section_match:
                current_section = section_match.group(1)
                current_album_id = None
                current_image = None

                if current_section == "Picasa":
                    current_kind = "picasa"
                elif current_section.startswith(ALBUM_SECTION_PREFIX):
                    current_kind = "album"
                    current_album_id = current_section[len(ALBUM_SECTION_PREFIX) :]
                    self.albums.setdefault(current_album_id, Album(album_id=current_album_id))
                elif current_section == "encoding":
                    current_kind = "encoding"
                else:
                    current_kind = "image"
                    current_image = current_section
                    self.images.setdefault(current_image, ImageRecord(file=current_image))
                continue

            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()

            if key.startswith("BKTag "):
                continue

            if current_kind == "picasa":
                if key == "name":
                    self.folder.name = value
                elif key == "category":
                    self.folder.category = value
                elif key == "date":
                    self.folder.date = value
                continue

            if current_kind == "album" and current_album_id is not None:
                album = self.albums[current_album_id]
                if key == "name":
                    album.name = value
                elif key == "date":
                    album.date = value
                elif key == "token":
                    album.token = value
                continue

            if current_kind == "image" and current_image is not None:
                image = self.images[current_image]
                if key == "caption":
                    image.caption = value
                elif key == "star":
                    image.star = value.lower() == "yes"
                elif key == "albums":
                    image.album_ids = [item.strip() for item in value.split(",") if item.strip()]

        self._resolve_album_names()
        return self

    def _resolve_album_names(self) -> None:
        for image in self.images.values():
            resolved_names: list[str] = []
            for album_id in image.album_ids:
                album = self.albums.get(album_id)
                if album is None:
                    resolved_names.append(f"[missing:{album_id}]")
                    self.albums[album_id] = Album(album_id=album_id)
                    continue
                resolved_names.append(album.name if album.name else f"[unnamed:{album_id}]")
                if image.file not in album.images:
                    album.images.append(image.file)
            image.albums = resolved_names

    def to_dict(self) -> dict[str, object]:
        folder = {
            key: value
            for key, value in {
                "name": self.folder.name,
                "category": self.folder.category,
                "date": self.folder.date,
            }.items()
            if value not in (None, "", [])
        }
        albums = {
            album_id: {
                key: value
                for key, value in {
                    "name": album.name,
                    "date": album.date,
                    "token": album.token,
                    "images": sorted(album.images),
                }.items()
                if value not in (None, "", [])
            }
            for album_id, album in sorted(self.albums.items())
        }
        images = [
            {
                key: value
                for key, value in {
                    "file": image.file,
                    "caption": image.caption,
                    "star": image.star,
                    "album_ids": image.album_ids,
                    "albums": image.albums,
                }.items()
                if value not in (None, "", [])
            }
            for _, image in sorted(self.images.items())
        ]
        return {"folder": folder, "albums": albums, "images": images}


def extract_picasa_metadata(path: Path) -> Optional[dict[str, object]]:
    try:
        raw_text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None

    parsed = PicasaParser().parse(raw_text).to_dict()
    if not parsed["albums"] and not parsed["images"] and not parsed["folder"]:
        return None
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract Picasa .picasa.ini metadata as JSON")
    parser.add_argument("path")
    parser.add_argument("--mime")
    args = parser.parse_args()

    extracted = extract_picasa_metadata(Path(args.path))
    if extracted is None:
        return 0
    json.dump(extracted, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
