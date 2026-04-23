# Picasa INI Extension

This extension extracts structured metadata from Picasa `.picasa.ini` files and
emits JSON on stdout.

## Entry Point

- `run.py`
- default applicability lives in `extension.json`

## Prerequisites

- enable the extension in repo-root `.sysextensions.json` under `extensions.picasa_ini.enabled`
- run scans through `python3 sysmvp.py scan`
- the scanned file path must match the default `*.picasa.ini` pattern, unless the repo overrides it in `.sysextensions.json`
- `python3` must be available so `sysmvp.py` can execute `run.py`

## When It Runs

`sysmvp.py` decides whether to run this extension in two stages:

1. During `scan`, it loads enabled extensions from `.sysextensions.json`.
2. For each scanned file, it only attempts this extension if:
   - `picasa_ini` is enabled
   - the file path matches the configured `file_patterns`

If those checks pass, `sysmvp.py` runs `extractors/picasa_ini/run.py` and
expects JSON on stdout.

## Contract

- input: file path as the first argument
- optional input: `--mime <mime-type>`
- success with data: print one JSON object to stdout
- success with no data: print nothing and exit `0`
- failure: write diagnostics to stderr and exit non-zero

## Current Behavior

- reads one `.picasa.ini` file and returns folder metadata, album definitions, and per-image records
- extracts image filename, caption, starred status, album ids, and resolved album names
- preserves missing album references as `[missing:<id>]`
- ignores most edit metadata such as filters, crop, rotate, backup hashes, and `BKTag` lines
- intended target attribute: `picasa/ini`

## Output Shape

The emitted JSON object contains:

- `folder`: top-level Picasa folder metadata such as `name`, `category`, and `date`
- `albums`: an object keyed by album id with album metadata and image membership
- `images`: a list of image records with captions, star flags, album ids, and resolved album names

## Notes

- `albums=` is parsed as a comma-separated list
- album definitions are indexed by the album id from sections like `[.album:<id>]`
- some old Picasa captions may be inaccurate; this extension extracts them faithfully and does not attempt corrections

## Example query

```sql
WITH picasa AS (
  SELECT
    fe.file_id,
    fe.current_path AS picasa_ini,
    substr(fe.current_path, 1, length(fe.current_path) - length(fe.current_name)) AS dir_prefix,
    vcf.value_json
  FROM file_entry fe
  JOIN v_current_fact vcf ON vcf.entity_id = fe.file_id
  JOIN attribute a ON a.attr_id = vcf.attr_id
  WHERE a.ident = 'picasa/ini'
)
SELECT
  p.picasa_ini,
  p.dir_prefix || json_extract(img.value, '$.file') AS current_path,
  json_extract(img.value, '$.caption') AS caption,
  json_extract(img.value, '$.star') AS starred,
  group_concat(album.value, ', ') AS albums,
  fe_img.current_path AS matched_file
FROM picasa p
JOIN json_each(p.value_json, '$.images') AS img
LEFT JOIN json_each(img.value, '$.albums') AS album
LEFT JOIN file_entry fe_img
  ON fe_img.current_path = p.dir_prefix || json_extract(img.value, '$.file')
GROUP BY
  p.picasa_ini,
  img.key,
  caption,
  starred,
  matched_file
ORDER BY p.picasa_ini, current_path;
```