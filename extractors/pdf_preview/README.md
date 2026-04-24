# pdf_preview

Render PDF pages into repo-local image files and emit a JSON summary fact.

By default this extension writes derived files under `.sysstore/pdf_preview/`,
which is already ignored by the default `.sysignore`, so generated page images
do not get re-scanned as normal source files.

## Usage

Requires `pdftoppm`.

Run the extension directly:

```bash
python3 run.py /path/to/file.pdf
```

Or enable `pdf_preview` in repo-root `.sysextensions.json` and let
`python3 sysmvp.py scan` run it automatically for matching `*.pdf` files.

For the extension contract and scan behavior, see `pdf_preview.md`.

## Optional settings

These keys may be set under `extensions.pdf_preview` in `.sysextensions.json`:

- `output`: repo-relative or absolute output directory
- `format`: `png` or `jpeg`
- `dpi`: integer render resolution
- `after`: only render PDFs newer than this ISO date/datetime

## Example output

```json
{
  "dpi": 300,
  "format": "png",
  "images": [
    ".sysstore/pdf_preview/docs/guide-8a2f7b2f5dfad8d4-1.png",
    ".sysstore/pdf_preview/docs/guide-8a2f7b2f5dfad8d4-2.png"
  ],
  "output_dir": ".sysstore/pdf_preview/docs",
  "page_count": 2,
  "source": "docs/guide.pdf",
  "tool": "pdftoppm"
}
```
