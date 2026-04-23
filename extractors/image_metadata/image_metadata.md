# Image Metadata Extension

This extension extracts structured metadata for image files and emits JSON on
stdout.

## Entry Point

- `run.py`
- default applicability lives in `extension.json`

## Prerequisites

- enable the extension in repo-root `.sysextensions.json` under `extensions.image_metadata.enabled`
- run scans through `python3 sysmvp.py scan`
- the scanned file must be detected as an image MIME type
- the scanned file path must match the default patterns in `extension.json`, unless the repo overrides them in `.sysextensions.json`
- `python3` must be available so `sysmvp.py` can execute `run.py`
- `exiftool` is optional; if present, this extension prefers it for richer metadata extraction

## When It Runs

`sysmvp.py` decides whether to run this extension in two stages:

1. During `scan`, it loads enabled extensions from `.sysextensions.json`.
2. For each scanned file, it only attempts this extension if:
   - `image_metadata` is enabled
   - the file MIME matches the configured `mime_prefixes`
   - the file path matches the configured `file_patterns`

If those checks pass, `sysmvp.py` runs `extractors/image_metadata/run.py` and
expects JSON on stdout.

## Contract

- input: file path as the first argument
- optional input: `--mime <mime-type>`
- success with data: print one JSON object to stdout
- success with no data: print nothing and exit `0`
- failure: write diagnostics to stderr and exit non-zero

## Current Behavior

- prefers `exiftool` when available
- falls back to scanning embedded XMP description text
- intended target attribute: `image/metadata`
- if `run.py` is missing, fails, or returns invalid JSON, `sysmvp.py` falls back to its built-in image metadata extractor
