# dagster-magento

A reusable Dagster resource for talking to Magento 2's REST API: admin-token
authentication, paginated reads, and a resilient chunked/per-row upload
helper with Dagster-integrated logging.

Targets standard Magento 2 REST endpoints only — no dependency on any
project-specific custom API modules.

## Installation

```
pip install git+https://git.sutunam.com/PATH/TO/dagster-magento.git@v0.1.0
```

## Status

v1 in progress — see `docs/superpowers/plans/2026-08-08-dagster-magento-v1.md`
for what's built and what's planned.
