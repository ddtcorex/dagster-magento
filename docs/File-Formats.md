# File Formats

`dagster_magento.formats` turns import files into [row models](Row-Models) in
two steps. `read_rows(path)` reads a `.csv`, `.json` or `.xlsx` file and yields
`(line_number, row_dict)` pairs without interpreting any column. The
`*_from_rows` mappers then understand the column layout of Magento's native
catalog import/export files (and the common extended sample layout built on
it): `additional_attributes`, `configurable_variations`, `bundle_values`,
`associated_skus`, `downloadable_links`, `categories`, the image columns and the
advanced pricing tier columns. Each mapper returns the models plus a list of
`RowError`s that point back to the source line; a bad cell becomes a row error,
not an exception. This page gives the exact syntax each parser accepts, what is
dropped, what passes through, and the gotchas. It was checked against
`formats/readers.py`, `formats/columns.py` and `formats/catalog.py` at version
0.4.1.

## Quick start

```python
from pathlib import Path

from dagster_magento import import_products, import_media
from dagster_magento.formats import products_from_rows, read_rows

rows = list(read_rows(Path("imports/products.csv")))
products, errors = products_from_rows(rows)
for error in errors:
    print(error.row_ref, error.message)   # e.g. "line 7: TSHIRT-RED-S", "price: invalid number 'abc'"

result = import_products(resource, products)
media = import_media(resource, [p for p in products if p.images])
```

## read_rows

`read_rows(path: Path) -> Iterator[tuple[int, dict]]`. Pass a `pathlib.Path`
(the suffix decides the reader, compared case-insensitively). Any other
suffix raises `ValueError("unsupported catalog import file suffix: ...")`.

| Suffix | Reader | Line number yielded |
| --- | --- | --- |
| `.csv` | `csv.DictReader`, UTF-8 with an optional BOM (`utf-8-sig`), comma delimiter, first line is the header | `DictReader.line_num`: the physical line of the record. For a record with a newline inside a quoted cell it is the record's last physical line |
| `.json` | `json.load`; the top level must be a list of objects, or an object with exactly one list-valued key (that list is used) | The 1-based index in the list |
| `.xlsx` | `openpyxl` (install the extra: `pip install "dagster-magento[xlsx]"`), first worksheet only, row 1 is the header, formulas read as their cached values | The sheet row number (first data row is 2) |

xlsx cell conversion: empty cells become `""`, whole-number floats become
integers without `.0` (`5.0` becomes `"5"`), every other value goes through
`str()` (a date cell becomes `"2026-01-01 00:00:00"`). Cells beyond the last
header are dropped. Without openpyxl, reading an `.xlsx` raises `ImportError`
with the install hint.

### JSON values must be strings

The mappers expect string cells, which is what csv and xlsx yield, so the json
reader converts every scalar to the text a csv cell would hold: `9.5` becomes
`"9.5"`, `12.0` becomes `"12"`, `true` and `false` become `"1"` and `"0"`,
`null` becomes `""`, and a nested list or object becomes its json text (which
the mapper then reports as a bad cell). When your JSON is already shaped like
the row models, skip the mappers and pass the objects straight to the importer:

```python
import json
from dagster_magento import import_products

rows = json.loads(Path("products.json").read_text())   # [{"sku": "A", "price": 9.5}, ...]
result = import_products(resource, rows)               # validated as ProductRow dicts
```

## The mappers

All of them are importable from `dagster_magento.formats`.

| Function | Returns | Expected file |
| --- | --- | --- |
| `products_from_rows(rows, warn=..., global_store_codes=("", "default"))` | `(list[ProductRow], list[RowError])` | Native product export layout |
| `categories_from_rows(rows, warn=..., global_store_codes=("", "default"))` | `(list[CategoryRow], list[RowError])` | Category file with a `name` column holding the full path |
| `attributes_from_rows(rows, warn=...)` | `(list[AttributeRow], list[RowError])` | Attribute file, one row per option |
| `attribute_set_assignments_from_rows(rows)` | `list[AttributeSetRow]` | The same attribute file |
| `prices_from_rows(rows, warn=...)` | `(list[PriceRow], list[RowError])` | Native advanced pricing file (tier columns) |
| `source_items_from_rows(rows, warn=...)` | `(list[SourceItemRow], list[RowError])` | Source item file (`source_code,sku,status,quantity`) |

`rows` is any iterable of `(line_number, dict)` pairs, so you can also feed
rows you read yourself. `warn` receives one message per dropped column name
(`dropping unsupported column '<name>'`), at most once per mapper call; it
defaults to the Dagster logger's `warning`. Pass `warn=lambda message: None` to
silence it.

Row references in errors are `line <n>: <id>` (`line 12: TSHIRT-RED-S`) or
`line <n>` when the id column is empty.

Blank cells are skipped by the product and category mappers: a blank cell never
sets a field, so it cannot clear a value in Magento. One exception sends a value
anyway: an empty value inside `additional_attributes` (`code=` or a bare `code`)
is sent as the empty string, which clears a text attribute and, for a select
attribute, would try to create an option with an empty label. The source item
mapper is stricter: a blank `quantity` or `status` is a row error
(`quantity is empty`), never a silent `0`.

## Products: products_from_rows

`sku` is required (`missing sku` otherwise). Every other column maps as
follows.

| Column | Becomes | Syntax |
| --- | --- | --- |
| `store_view_code` | global row or `store_values` fold | see [Store view rows](#store-view-rows) |
| `attribute_set_code` | `attribute_set` | set name or numeric id |
| `product_type` | `type` | `simple`, `configurable`, `bundle`, `grouped`, `downloadable`, `virtual`, ... |
| `product_websites` | `websites` | comma separated website codes: `base,eu` |
| `categories` | `categories` | comma separated paths: `Default Category/Men/Tops,Default Category/Sale` |
| `name` | `name` | text |
| `price` | `price` | number; anything else is a row error `price: invalid number '<v>'` |
| `weight` | `weight` | number, same rule |
| `product_online` | `status` | `1` becomes 1 (enabled), `0` becomes 2 (disabled); any other value is ignored |
| `visibility` | `visibility` | `Not Visible Individually`, `Catalog`, `Search`, `Catalog, Search` (case and spacing insensitive) or `1` to `4`; anything else is a row error `unknown visibility: '<v>'` |
| `additional_attributes` | merged into `attributes` | `code=value,code=value` |
| `configurable_variations` | `variations` and `configurable_attributes` | see below |
| `bundle_values` | `bundle_options` | see below |
| `bundle_price_type`, `bundle_sku_type`, `bundle_weight_type` | `attributes.price_type`, `sku_type`, `weight_type` | `dynamic` becomes 0, `fixed` becomes 1 (exact spelling); other values pass through |
| `bundle_price_view` | `attributes.price_view` | `Price range` becomes 0, `As low as` becomes 1 (exact spelling); other values pass through |
| `bundle_shipment_type` | `attributes.shipment_type` | as given; wins over a `shipment_type` inside `additional_attributes` |
| `associated_skus` | `grouped_links` | see below |
| `downloadable_links` | `downloadable_links` | see below |
| image columns | `images` | see [Images](#images) |
| `tax_class_name` | `attributes.tax_class_id` | label, resolved to an option id by the writer (`Taxable Goods`) |
| `new_from_date`, `new_to_date` | `attributes.news_from_date`, `news_to_date` | date text |
| `display_product_options_in` | `attributes.options_container` | as given |
| `map_price` | `attributes.minimal_price` | as given |
| `msrp_price` | `attributes.msrp` | as given |
| `meta_keywords` | `attributes.meta_keyword` | as given |
| any other column | `attributes[<column>]` | as given (string) |

### Unknown columns pass through

A column the mapper does not know (`description`, `url_key`, `meta_title`,
`country_of_manufacture`, a custom `material` column, ...) is copied into
`attributes` under its own name. The mapper does not check it: the product
writer looks every key up as an attribute code, and a column that is not an
attribute fails the row with `unknown attribute: <column>`. Rename or drop such
columns before importing.

### Dropped columns

These columns are dropped with one warning each:

| Group | Columns | Why |
| --- | --- | --- |
| Extended-layout only | `group`, `tier_prices`, any column starting with `attribute\|` | No native equivalent |
| Stock | `qty`, `out_of_stock_qty`, `use_config_min_qty`, `is_qty_decimal`, `allow_backorders`, `use_config_backorders`, `min_cart_qty`, `use_config_min_sale_qty`, `max_cart_qty`, `use_config_max_sale_qty`, `is_in_stock`, `notify_on_stock_below`, `use_config_notify_stock_qty`, `manage_stock`, `use_config_manage_stock`, `use_config_qty_increments`, `qty_increments`, `use_config_enable_qty_inc`, `enable_qty_increments`, `is_decimal_divided`, `website_id`, `deferred_stock_update`, `use_config_deferred_stock_update` | Stock is written through `SourceItemRow` and `import_source_items` |
| Special price | `special_price`, `special_price_from_date`, `special_price_to_date` | Prices go through `PriceRow`; note that `prices_from_rows` does not read these columns either, so build `PriceRow`s yourself for special prices |
| Labels | `configurable_variation_labels` | The option label comes from the attribute |
| Timestamps | `created_at`, `updated_at` | Set by Magento |
| Out of scope | `related_skus`, `crosssell_skus`, `upsell_skus`, `custom_options`, `hide_from_product_page` | Not imported by this library |
| Not native on 2.4 | `map_enabled`, `product_options_container` | No attribute, or a duplicate of `display_product_options_in` |
| Derived | `has_options`, `required_options`, `quantity_and_stock_status` (as columns or inside `additional_attributes`) | Recomputed by Magento; keeping them would make every rerun look changed |

### additional_attributes

```
color=Gray,size=S,description="Soft, warm cotton"
```

- Tokens are separated by commas outside double quotes.
- Each token splits on its first `=`; key and value are stripped.
- A value wrapped in double quotes has the quotes removed, which lets it
  contain commas. A value may itself contain `=`.
- A token without `=` gives that key an empty string value. Empty tokens are
  skipped.

### configurable_variations

```
sku=TSHIRT-RED-S,size=S,color=Red,default=1|sku=TSHIRT-RED-M,size=M,color=Red
```

- Variations are separated by `|` (not quote aware). Each variation is a
  `key=value` list with the same rules as `additional_attributes`.
- `sku` is the child SKU; `default` is dropped; every other key is a
  configurable attribute code with its option label.
- `configurable_attributes` is the attribute codes in order of first
  appearance (`["size", "color"]` above), which is the option position order.

### bundle_values

```
name=Size option,type=select,required=1,sku=STRAP-S,price=15.0000,default=0,default_qty=1.0000,price_type=fixed|name=Size option,type=select,required=1,sku=STRAP-L,price=10.0000,default=1,default_qty=1.0000,price_type=fixed
```

- Entries are separated by `|`; entries with the same `name` form one
  option, in first-seen order. The option's `type` (default `select`) and
  `required` (`1` is true, anything else false) come from its first entry.
- Per entry: `sku`, `price` (number), `default_qty` (number, becomes `qty`,
  default 1), `price_type` (`fixed` or `percent`), `default` (`1` is the
  default selection).
- A non-numeric `price` or `default_qty` is a row error. An unknown `type` or
  `price_type` fails model validation for the row.

### associated_skus

```
SHIRT-S=2.0000,SHIRT-M=1,SHIRT-L
```

- Comma separated (quote aware). `SKU=qty` or a bare `SKU` (qty 0).
- A repeated SKU keeps its first occurrence. `position` is the index.
- A non-numeric qty is a row error naming the SKU.

### downloadable_links

```
title=Manual,url=https://downloads.example.test/manual.pdf,price=0,downloads=5|title=Sources,url=https://downloads.example.test/src.zip
```

- Links separated by `|`, each a `key=value` list.
- Read keys: `title`, `url`, `price` (number, default 0), `downloads`
  (integer), `type`. `type` must be empty or `url` (any case); `file` or any
  other value is a row error `unsupported downloadable link type`.
- Other keys (`group_title`, `sortable`, `shareable`, ...) are ignored, so
  `is_shareable` is sent as 2 (use config).
- There is no `downloadable_samples` parser: a column of that name passes
  through as an unknown attribute and fails the row in the writer.

### categories

Comma separated paths, each stripped, empty entries dropped. There is no
quoting, so a category name that contains a comma cannot be expressed in this
column. Paths are matched as described in
[Catalog Importers](Catalog-Importers#category-paths); they must already exist
(run `import_categories` with those paths first).

### Images

| Column | Role | Label column |
| --- | --- | --- |
| `base_image` | `image` | `base_image_label` |
| `small_image` | `small_image` | `small_image_label` |
| `thumbnail_image` | `thumbnail` | `thumbnail_image_label` |
| `swatch_image` | `swatch_image` | `swatch_image_label` |
| `additional_images` | none | `additional_image_labels` |

- The same source in several columns becomes one `Image` with several roles;
  its label comes from the first role column that has one.
- `additional_images` and `additional_image_labels` are comma separated (no
  quoting) and paired by original position, so a blank label keeps its slot
  (`a,b,c` with labels `A,,C` gives `a` the label `A`, `b` none and `c` the label
  `C`). A source already seen keeps its earlier label when it has one.
- Positions are 1, 2, 3, ... in order of first appearance: base, small,
  thumbnail, swatch, then additional images.
- A source is kept as written: an `http://` or `https://` URL is downloaded by
  `import_media`, anything else is read as a local file path, relative paths
  resolving against the working directory of the process that runs the
  import. Nothing is sent to Magento as a path: the bytes are uploaded base64
  encoded, and the MIME type is detected from the bytes.

Example:

```csv
sku,base_image,base_image_label,small_image,thumbnail_image,additional_images,additional_image_labels
TSHIRT,/data/img/front.jpg,Front,/data/img/front.jpg,/data/img/thumb.jpg,"/data/img/front.jpg,/data/img/back.jpg","Front,Back"
```

gives `front.jpg` (position 1, roles `image` and `small_image`, label
`Front`), `thumb.jpg` (position 2, `thumbnail`), `back.jpg` (position 3,
label `Back`).

### Store view rows

Each row's `store_view_code` decides whether it is the global row of its SKU
or a store-view override.

- Codes in `global_store_codes` (default `("", "default")`, compared after
  stripping and lower-casing) mark the global row. The default treats
  `default` as global because both the extended sample export and a native
  single-store export tag every row, the only one per SKU included, with
  `default`.
- On a multi-store install where `default` is a real store view you want to
  override, pass `global_store_codes=("",)`; `default` rows then fold into
  `store_values["default"]`.
- Every other code folds into `store_values[<code>]` of the SKU's global row,
  after all global rows were read. A store row for a SKU without a global row
  is an error `store-view row for unknown sku (store_view_code=<code>)`.
- In the fold, `name`, `status` (from `product_online`) and `visibility` stay
  top-level keys of the entry; `additional_attributes` and plain columns are
  merged in. Every other parsed column (`price`, `weight`, `categories`,
  `product_websites`, images, ...) is global, so it is ignored on a store-view
  row with one warning per row that names the columns.
- A second row for the same SKU and store code replaces the first.

```csv
sku,store_view_code,name,description,url_key
TSHIRT,,Classic T-shirt,<p>Cotton tee</p>,classic-t-shirt
TSHIRT,fr,T-shirt classique,<p>Coton</p>,t-shirt-classique
```

gives `store_values == {"fr": {"name": "T-shirt classique", "description": "<p>Coton</p>", "url_key": "t-shirt-classique"}}`.

## Categories: categories_from_rows

| Column | Becomes |
| --- | --- |
| `name` | `path` (required, the full path such as `Default Category/Men/Tops`; `missing name` otherwise) |
| `store_view` | global row or `store_values` fold, same `global_store_codes` rule as products (default `("", "default")`) |
| `include_in_menu`, `is_active`, `is_anchor`, `custom_apply_to_products`, `custom_use_parent_settings` | `1` when the cell is `yes` (any case), else `0` |
| `display_mode` | `Products only` to `PRODUCTS`, `Static block only` to `PAGE`, `Static block and products` to `PRODUCTS_AND_PAGE` (case-insensitive); other values pass through |
| `page_layout` | `Empty` to `empty`, `1 column` to `1column`, `2 columns with left bar` to `2columns-left`, `2 columns with right bar` to `2columns-right`, `3 columns` to `3columns`; other values pass through |
| `default_sort_by` | `Position` to `position`, `Product Name` to `name`, `Price` to `price`; other values pass through |
| `custom_design_from`, `custom_design_to` | `m/d/yy` dates converted to `YYYY-MM-DD`; anything else passes through |
| `entity_id`, `url_path`, `group`, `landing_page`, `custom_design`, `image` | dropped with one warning each |
| any other column | `attributes[<column>]` as given (`url_key`, `description`, `available_sort_by`, `position`, `meta_title`, ...) |

A store row for a path without a global row is an error
`store-view row for unknown category (store_view=<code>)`.

```csv
name,store_view,is_active,include_in_menu,url_key,display_mode,description
Default Category/Men,,Yes,Yes,men,Products only,<p>Men</p>
Default Category/Men,fr,,,hommes,,<p>Hommes</p>
```

## Attributes: attributes_from_rows

The file has one row per option; rows are grouped by `attribute_code`.

| Column | Becomes |
| --- | --- |
| `store_id` | only `0` or blank rows are used; other values are skipped with one warning per value |
| `attribute_code` | `code` (required, `missing attribute_code` otherwise) |
| `frontend_label` | `label`, first non-empty value of the group |
| `frontend_input` | `frontend_input`, first non-empty value |
| `is_global` | `scope`: `0` store, `1` global, `2` website (the last row with a valid value wins; default `store`) |
| `option:value` | one `AttributeOption` per distinct label (exact comparison) |
| `option:sort_order` | that option's `sort_order` (digits, else 0) |
| `is_required`, `is_unique`, `is_searchable`, `is_filterable`, `is_comparable`, `is_visible_on_front`, `is_html_allowed_on_front`, `is_filterable_in_search`, `used_in_product_listing`, `used_for_sort_by`, `is_visible_in_advanced_search`, `is_wysiwyg_enabled`, `is_used_for_promo_rules`, `is_used_in_grid`, `is_visible_in_grid`, `is_filterable_in_grid` | `flags[<column>]`: `True` when the cell is `1`, else `False` |
| `position` | `flags.position`, integer when numeric |
| `default_value`, `note` | `flags[<column>]` as given |
| `apply_to` | `flags.apply_to` as a list (`simple,virtual` becomes `["simple", "virtual"]`) |
| `search_weight`, `is_used_for_price_rules` | dropped with one warning each (Magento's REST attribute DTO rejects them) |
| any other column | ignored without a warning |

For each flag the first non-empty value in the group wins. Option store
labels and attribute store labels are not read from this file; set
`store_labels` on the models yourself when you need them.

Magento accepts only letters, digits and underscores in attribute codes; the
public sample file uses hyphenated codes, which Magento rejects.

## Attribute sets: attribute_set_assignments_from_rows

Reads the same attribute file. A row is used only when `attribute_set`,
`attribute_code` and `group:name` are all non-empty. The result is one
`AttributeSetRow(name=<attribute_set>, groups={<group:name>: [codes...]})`
per set, codes in first-seen order without duplicates, `based_on` left at
`"Default"`. This function returns no errors and does not warn.

## Advanced pricing: prices_from_rows

```csv
sku,tier_price_website,tier_price_customer_group,tier_price_qty,tier_price,tier_price_value_type
TSHIRT-RED-S,All Websites [USD],ALL GROUPS,5,17,Fixed
TSHIRT-RED-S,base,Wholesale,10,10,Discount
```

| Column | Becomes |
| --- | --- |
| `sku` | `PriceRow.sku` (required) |
| `tier_price_website` | `TierPrice.website`: empty or starting with `All Websites` becomes `all`; otherwise the value as given (website code, or digits for an id) |
| `tier_price_customer_group` | `customer_group`, default `ALL GROUPS` when empty |
| `tier_price_qty` | `qty` (required, number) |
| `tier_price` | `price` (required, number) |
| `tier_price_value_type` | `price_type`, lower-cased: `Fixed` or `Discount` |

All rows of one SKU become one `PriceRow` with `price=None` and every tier in
`tiers`. Importing it therefore replaces all existing tiers of that SKU with
exactly the file's tiers. A missing qty or price is an error
`advanced_pricing: missing tier_price_qty or tier_price`; one bad line drops
the whole SKU (an error per bad line). Base and special prices are not read
from this file.

## Source items: source_items_from_rows

```csv
source_code,sku,status,quantity
default,TSHIRT-RED-S,1,12
eu-warehouse,TSHIRT-RED-S,1,4
```

| Column | Becomes |
| --- | --- |
| `sku` | `sku` |
| `source_code` | `source_code` |
| `quantity` | `quantity` (float; blank is a row error) |
| `status` | `status` (integer; blank is a row error) |

A non-numeric value is a row error with Python's parse message; a `status`
other than 0 or 1 fails model validation.

## Sample files used by the tests

The test suite proves the mappers against the public sample repository
`firebearstudio/magento2-import-export-sample-files`, pinned at commit
`7fec061a837288718c1d84455ae8f1a0acde00c4`. The files are never vendored
(GPL-3.0): `tests/samples.py` `fetch(name)` downloads one from
`raw.githubusercontent.com` at that commit into `tests/.samples/` (gitignored)
and reuses the cached copy afterwards.

| File | Folder in the sample repo |
| --- | --- |
| `product_all_types.csv`, `products_all_types.xlsx`, `categories.csv`, `attributes.csv`, `advanced_pricing.csv`, `msi_source_qty.csv` | `Improved Import : Export - Sample Files` |
| `catalog_product.csv` | `Magento 2 Default import sample files` |

`pytest -m samples` (excluded from the default run) checks that every file
maps with zero row errors, that the product file contains all five types
(simple, configurable, bundle, grouped, downloadable) and that the xlsx and csv
product files yield the same SKUs. The live end-to-end test imports the same
files into a sandbox, after three fixups worth copying for your own data:
hyphenated attribute codes are renamed with underscores, every category path a
product names is added as a `CategoryRow`, and the sources named in the
source item file are created as `SourceRow`s.

## Gotchas

- A second global row for the same SKU replaces the first, and that SKU then
  appears twice in the returned list (both entries are the last row).
  Deduplicate before importing.
- `product_online` values other than `1` and `0` are ignored silently (the
  extended sample file uses `2` in places).
- The category yes/no columns accept `yes`, `1` and `true` for 1 and `no`, `0`
  and `false` for 0 (any case). Any other value is a row error naming the column.
- `additional_images`, `additional_image_labels` and `categories` cannot hold
  values containing commas.
- `|` inside a value of `configurable_variations`, `bundle_values` or
  `downloadable_links` always splits the entry, even inside quotes.
- The default `warn` goes to the Dagster logger; outside a Dagster run pass
  your own `warn` to see the dropped columns.

## See also

- [Home](Home)
- [Row Models](Row-Models)
- [Catalog Importers](Catalog-Importers)
- [Price Import](Price-Import)
- [Getting Started](Getting-Started)
- [Dagster Assets](Dagster-Assets)
- [Results and Errors](Results-and-Errors)
- [Troubleshooting](Troubleshooting)
