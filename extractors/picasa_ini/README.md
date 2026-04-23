# picasa_ini

Extract Picasa `.picasa.ini` metadata as JSON focused on:

- image filename
- caption
- album membership
- album definitions

This extension intentionally ignores most edit metadata such as filters, crop,
rotate, backup hashes, and `BKTag` lines.

## Why this shape

The output keeps both:

- an album index keyed by album id
- a per-image list with resolved album names

That makes it easy to:

- find an album by partial name
- list all images in an album
- inspect captions
- import into SQLite later

## Usage

Run the extension entrypoint directly against one `.picasa.ini` file:

```bash
python3 run.py /path/to/.picasa.ini
```

Or enable `picasa_ini` in repo-root `.sysextensions.json` and let
`python3 sysmvp.py scan` run it automatically for matching `*.picasa.ini`
files.

For the extension contract and scan behavior, see `picasa_ini.md`.

## Example output

```json
{
  "folder": {
    "name": "Desktop",
    "category": "Folders on Disk",
    "date": "39655.566111"
  },
  "albums": {
    "006641b1ce37d0daf2aa648ff3eae450": {
      "name": "ireland and scotland",
      "date": "2016-05-16T19:02:32+01:00",
      "token": "006641b1ce37d0daf2aa648ff3eae450",
      "images": [
        "alexander kennedy 1851 census slains.tif",
        "isabella kennedy black death 1899 savoch ellon.tif"
      ]
    }
  },
  "images": [
    {
      "file": "isabella kennedy black death 1899 savoch ellon.tif",
      "caption": "isabella kennedy black death 1899 savoch ellon",
      "star": true,
      "album_ids": [
        "006641b1ce37d0daf2aa648ff3eae450"
      ],
      "albums": [
        "ireland and scotland"
      ]
    }
  ]
}
```

## Notes

- `albums=` is parsed as a comma-separated list.
- Missing album definitions are preserved as `[missing:<id>]` in resolved album names.
- Some captions in old Picasa data may be wrong. This tool extracts them faithfully; it does not try to guess corrections.

## Query ideas

Find albums with `recently` in the name:

```bash
jq '.albums | to_entries[] | select(.value.name | test("recently"; "i"))' output.json
```

List all images in matching albums:

```bash
jq -r '
  .albums
  | to_entries[]
  | select(.value.name | test("recently"; "i"))
  | .value.images[]
' output.json
```
