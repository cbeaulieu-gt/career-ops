---
name: career-ops-plugin-adzuna
description: Scan Adzuna through its authenticated Jobs Search API.
license: MIT
---

# adzuna plugin

This keyed provider searches Adzuna's current v1 Jobs API. It runs only for a
`portals.yml` entry with `provider: adzuna`; it is never auto-detected.

## Setup

1. Create an application at https://developer.adzuna.com/ and copy its app ID
   and key into `.env` as `ADZUNA_APP_ID` and `ADZUNA_APP_KEY`.
2. Run `node plugins.mjs enable adzuna --confirm`.
3. Add an explicit search entry to `portals.yml`:

```yaml
tracked_companies:
  - name: "Adzuna — AI leadership in Canada"
    provider: adzuna
    country: ca              # required two-letter Adzuna country code
    what: "head of ai"       # `query` is also accepted
    where: "Toronto"         # `location` is also accepted
    results_per_page: 50     # 1–50; default 50
    max_pages: 3             # 1–20; default 1
    max_days_old: 14         # optional positive integer
    enabled: false
```

Set `enabled: true` when the query is ready. Results pass through the normal
scanner title, location, content, deduplication, and pipeline-writing stages.
