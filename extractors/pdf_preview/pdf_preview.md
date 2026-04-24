# pdf_preview extension contract

This extension renders each matching PDF into page images and writes a JSON
summary fact under `pdf/preview`.

## Applicability

- default applicability lives in `extension.json`
- enable the extension in repo-root `.sysextensions.json` under `extensions.pdf_preview.enabled`
- the scanned file path must match the default `*.pdf` pattern, unless the repo overrides it in `.sysextensions.json`
- `pdftoppm` must be available on `PATH`

## Scan behavior

`sysmvp.py` decides whether to run this extension in two stages:

1. During `scan`, it loads enabled extensions from `.sysextensions.json`.
2. For each scanned file, it only attempts this extension if:
   - `pdf_preview` is enabled
   - the detected MIME matches `application/pdf`
   - the relative path matches `*.pdf` unless repo config overrides that pattern

If those checks pass, `sysmvp.py` runs `extractors/pdf_preview/run.py` and
stores the returned JSON as `pdf/preview`.

## Output shape

The extractor returns:

- `tool`: currently `pdftoppm`
- `format`: `png` or `jpeg`
- `dpi`: render resolution
- `source`: repo-relative PDF path when available
- `output_dir`: repo-relative output directory when available
- `page_count`: number of rendered pages
- `images`: ordered list of rendered page image paths

## Notes

- output defaults to `.sysstore/pdf_preview/`
- filenames include a hash prefix derived from the PDF bytes to avoid collisions across revisions
- if `pdftoppm` is missing or conversion fails, the scan continues and logs the extractor failure
