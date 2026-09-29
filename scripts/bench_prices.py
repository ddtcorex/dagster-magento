"""Benchmark the two ways to write base prices for a large catalog.

Path A submits price-only product payloads to the async bulk route
`PUT async/bulk/V1/products/bySku` and times the wall clock until every
operation has finished. Path B hands the same kind of prices to
`import_prices`, which writes them through the native price storage list
endpoint in sync mode.

Both paths write real changes on every run (`diff=False` for path B), so the
numbers compare writes, not no-ops. Products must already exist; seed them
first, for example with `import_products(..., mode="bulk")`.

    eval "$(scripts/sandbox.sh env)"
    MAGENTO_CA_BUNDLE=~/.govard/ssl/root.crt \\
        .venv/bin/python scripts/bench_prices.py --rows 10000

The script prints one line per path and one ratio. It changes prices, so run it
against a sandbox, never a store you care about.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from dagster_magento import MagentoResource, import_prices
from dagster_magento.bulk import STATUS_COMPLETE, wait_bulk
from dagster_magento.models import PriceRow

SKU_PAGE_SIZE = 1000


def make_resource() -> MagentoResource:
    return MagentoResource(
        base_url=os.environ["MAGENTO_BASE_URL"],
        username=os.environ["MAGENTO_ADMIN_USERNAME"],
        password=os.environ["MAGENTO_ADMIN_PASSWORD"],
        store_view=os.environ.get("MAGENTO_STORE_VIEW", "all"),
    )


def read_skus(resource: MagentoResource, rows: int) -> list[str]:
    """The first `rows` SKUs of the catalog, a page at a time."""
    skus: list[str] = []
    page = 1
    while len(skus) < rows:
        response = resource.get(
            "products",
            params={
                "searchCriteria[pageSize]": min(SKU_PAGE_SIZE, rows - len(skus)),
                "searchCriteria[currentPage]": page,
                "fields": "items[sku]",
            },
        )
        items = response.get("items") or []
        if not items:
            break
        skus.extend(item["sku"] for item in items)
        page += 1
    return skus[:rows]


def price_for(index: int, base: float, offset: float) -> float:
    """A stable price per position, so a read-back can be checked."""
    return round(base + index % 90 + offset, 2)


def bench_bulk_by_sku(
    resource: MagentoResource,
    skus: list[str],
    base: float,
    timeout_s: float,
    chunk_size: int = 500,
) -> tuple[float, int]:
    """Path A: price-only payloads through the async bulk bySku route.

    The submission is chunked. One request carrying 10,000 operations makes
    Magento create and publish every operation before it answers, which takes
    minutes and runs into the client's request timeout, so the chunks go out
    back to back and are then all waited on: the wall clock still covers the
    whole write, and the native price storage path chunks its own requests the
    same way (1000 rows each).
    """
    started = time.monotonic()

    submitted: list[tuple[str, int]] = []
    for start in range(0, len(skus), chunk_size):
        chunk = skus[start : start + chunk_size]
        items = [
            {"product": {"sku": sku, "price": price_for(start + index, base, 0.99)}}
            for index, sku in enumerate(chunk)
        ]
        submitted.append((resource.submit_bulk("PUT", "products/bySku", items), len(items)))

    complete = 0
    for bulk_uuid, count in submitted:
        statuses = wait_bulk(
            resource, bulk_uuid, count=count, timeout_s=timeout_s, poll_interval_s=5
        )
        complete += sum(1 for status, _ in statuses if status == STATUS_COMPLETE)

    seconds = time.monotonic() - started

    return seconds, complete


def bench_import_prices(
    resource: MagentoResource, skus: list[str], base: float, use_bridge: str
) -> tuple[float, int]:
    """Path B: the native price storage list endpoint through import_prices."""
    rows = [
        PriceRow(sku=sku, price=price_for(index, base, 0.49))
        for index, sku in enumerate(skus)
    ]

    started = time.monotonic()
    result = import_prices(resource, rows, diff=False, use_bridge=use_bridge)
    seconds = time.monotonic() - started

    return seconds, result.succeeded


def read_back_price(resource: MagentoResource, sku: str) -> float | None:
    """The price Magento reports for one SKU, to prove a write landed."""
    product = resource.get(f"products/{sku}", params={"fields": "sku,price"})
    return product.get("price")


def hardware_line() -> str:
    """The machine the sandbox containers run on."""
    cores = os.cpu_count() or 0
    total_kib = 0
    meminfo = Path("/proc/meminfo")
    if meminfo.is_file():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal:"):
                total_kib = int(line.split()[1])
                break
    return f"{cores} cores, {total_kib / 1024 / 1024:.1f} GiB RAM"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rows", type=int, default=10000, help="how many SKUs to price")
    parser.add_argument("--price-base", type=float, default=100.0)
    parser.add_argument(
        "--bulk-timeout", type=float, default=3600.0, help="seconds to wait for the bulk to finish"
    )
    parser.add_argument(
        "--use-bridge",
        choices=["auto", "never", "require"],
        default="never",
        help="bridge mode for the import_prices path",
    )
    parser.add_argument(
        "--skip-bulk", action="store_true", help="only run the import_prices path"
    )
    parser.add_argument(
        "--bulk-chunk", type=int, default=500, help="operations per bulk submission"
    )
    args = parser.parse_args()

    resource = make_resource()
    skus = read_skus(resource, args.rows)
    if not skus:
        print("no products found: seed the catalog before benchmarking", flush=True)
        return 1

    print(f"rows={len(skus)} hardware={hardware_line()}", flush=True)

    if not args.skip_bulk:
        seconds, complete = bench_bulk_by_sku(
            resource, skus, args.price_base, args.bulk_timeout, args.bulk_chunk
        )
        price = read_back_price(resource, skus[0])
        print(
            f"path A async/bulk products/bySku: {seconds:.1f}s "
            f"({complete}/{len(skus)} operations complete, {len(skus) / seconds:.1f} rows/s, "
            f"read back {skus[0]}={price})",
            flush=True,
        )

    seconds, succeeded = bench_import_prices(resource, skus, args.price_base, args.use_bridge)
    price = read_back_price(resource, skus[0])
    print(
        f"path B import_prices: {seconds:.1f}s "
        f"({succeeded}/{len(skus)} rows succeeded, {len(skus) / seconds:.1f} rows/s, "
        f"read back {skus[0]}={price})",
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
