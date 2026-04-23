# AsciiDoc Header Extension

This extension extracts the first `=`-style header from `.adoc` files and
emits it as JSON on stdout.

## Entry Point

- `run.py`
- default applicability lives in `extension.json`

## Prerequisites

- enable the extension in repo-root `.sysextensions.json` under `extensions.asciidoc_header.enabled`
- run scans through `python3 sysmvp.py scan`
- the scanned file path must match the default `*.adoc` pattern, unless the repo overrides it in `.sysextensions.json`
- `python3` must be available so `sysmvp.py` can execute `run.py`

## Contract

- input: file path as the first argument
- optional input: `--mime <mime-type>`
- success with data: print one JSON object to stdout
- success with no data: print nothing and exit `0`
- failure: write diagnostics to stderr and exit non-zero

## Current Behavior

- scans lines from top to bottom
- ignores blank lines and `//` comment lines
- matches the first `=`-prefixed header such as `= Title`
- writes the JSON fact to `asciidoc/header`

## Example query

```sql
SELECT
  fe.current_path,
  json_extract(vcf.value_json, '$.header') AS header
FROM file_entry fe
JOIN v_current_fact vcf ON vcf.entity_id = fe.file_id
JOIN attribute a ON a.attr_id = vcf.attr_id
WHERE a.ident = 'asciidoc/header'
  AND lower(json_extract(vcf.value_json, '$.header')) LIKE '%test%'
ORDER BY fe.current_path;
```