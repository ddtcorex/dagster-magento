# Row Models

Every importer takes rows that validate against one pydantic v2 model from
`dagster_magento.models`. The models are the library's canonical catalog
shape: they are what the [file mappers](File-Formats) produce, and what you
build yourself when your data comes from an ERP, a PIM or anything else. This
page lists every model and every field with its type, default and what it
becomes in the Magento REST payload, the validation rules that turn a row into
a `RowError`, how `validate_rows` works, and the partial-update rule for
products. Everything here was checked against `models.py`,
`writers/*.py` and `diff.py` at version 0.4.0.

## General rules

- You can pass model instances or plain dicts to an importer. Both are
  validated again inside the importer; instances are dumped with
  `exclude_unset=True` first, so which fields you set explicitly is kept.
- Unknown keys are silently ignored (pydantic's default): a typo such as
  `"atributes"` is dropped without an error. Check your keys.
- Integer `Literal` fields are strict about strings: `"status": "1"` fails
  with `status: Input should be 1 or 2`. Convert to int first. Float fields do
  accept numeric strings (`"price": "9.5"` becomes `9.5`).
- A row that fails validation is not sent; it is counted as `failed` with the
  pydantic message. Model validation never raises out of an importer.

## validate_rows

```python
from dagster_magento.models import ProductRow, validate_rows

valid, errors = validate_rows(
    ProductRow,
    [{"sku": "A-1", "status": 3}, {"name": "no sku"}],
    "sku",
)
# valid  == []
# errors == [RowError(row_ref='A-1', message='status: Input should be 1 or 2'),
#            RowError(row_ref='row 1', message='sku: Field required')]
```

Signature: `validate_rows(model, raw, id_field) -> (list[model], list[RowError])`.

- `raw` is a list of dicts.
- `row_ref` is the value of `id_field` in the raw dict, or `row <index>` with a
  zero-based index when that field is missing.
- `message` joins every pydantic error as `<field path>: <message>` with `; `.
  Nested paths use dots (`variations.0.sku: Field required`). A model-level
  rule has an empty path, so its message starts with `: Value error, ...`.
- Only `pydantic.ValidationError` is caught; any other exception propagates.

The id fields the importers use: `sku` (products, media, prices, source
items), `code` (attributes), `name` (attribute sets, stocks), `path`
(categories), `source_code` (sources), `stock` (stock source links).

## ProductRow

Used by `import_products` and `import_media`.

| Field | Type | Default | Magento payload |
| --- | --- | --- | --- |
| `sku` | `str` | required | `sku`; also the URL key of updates (`PUT products/{sku}`, URL-encoded) |
| `type` | `str` | `"simple"` | `type_id`. Any string is accepted; `configurable`, `bundle`, `grouped`, `downloadable` add [type parts](#product-type-parts) |
| `attribute_set` | `str` | `"Default"` | `attribute_set_id`, resolved by name (case-insensitive) or used as is when all digits |
| `name` | `str \| None` | `None` | `name` when not `None` |
| `price` | `float \| None` | `None` | `price` when not `None` |
| `status` | `Literal[1, 2] \| None` | `None` | `status`: 1 enabled, 2 disabled |
| `visibility` | `Literal[1, 2, 3, 4] \| None` | `None` | `visibility`: 1 not visible individually, 2 catalog, 3 search, 4 catalog and search |
| `weight` | `float \| None` | `None` | `weight` when not `None` |
| `websites` | `list[str]` | `["base"]` | `extension_attributes.website_ids`, each code resolved through `GET store/websites` |
| `categories` | `list[str]` | `[]` | `extension_attributes.category_links`, one `{"position": 0, "category_id": "<id>"}` per path; not sent when empty |
| `attributes` | `dict[str, Any]` | `{}` | `custom_attributes`, one `{"attribute_code", "value"}` per key, value resolved by input type (see below) |
| `store_values` | `dict[str, dict[str, Any]]` | `{}` | One extra `PUT products/{sku}` per store code, through `/rest/<store_code>/` |
| `variations` | `list[Variation]` | `[]` | Configurable child links (`POST configurable-products/{sku}/child`) |
| `configurable_attributes` | `list[str]` | `[]` | Configurable options (`POST configurable-products/{sku}/options`), in this order |
| `bundle_options` | `list[BundleOption]` | `[]` | `extension_attributes.bundle_product_options` |
| `grouped_links` | `list[GroupedLink]` | `[]` | `product_links` with `link_type: "associated"` |
| `downloadable_links` | `list[DownloadableLink]` | `[]` | `extension_attributes.downloadable_product_links` |
| `downloadable_samples` | `list[DownloadableSample]` | `[]` | `extension_attributes.downloadable_product_samples` |
| `images` | `list[Image]` | `[]` | Ignored by `import_products`; written by `import_media` |

### How `attributes` values are converted

The writer looks up each code's `frontend_input` (a filtered
`GET products/attributes`, issued twice per `import_products` call: once by the
importer and once by the writer for the rows that changed):

| `frontend_input` | Value you give | Value sent |
| --- | --- | --- |
| `boolean` | `Yes`/`No`, `true`/`false`, `1`/`0` (any case, int or string) | `1` or `0`; anything else fails the row (`invalid boolean ...`) |
| `select` | a string label, digits included (`"32"` is a label) | the option id; a missing label is created first |
| `select` | a non-string (int) | sent as is, taken as an option id |
| `multiselect` | `"Red, Blue"` or `["Red", "Blue"]` | option ids joined with commas; missing labels are created |
| anything else | any value | sent as is |

An unknown attribute code fails the row with `unknown attribute: <code>`.

Rejected keys: `attributes` may not contain `sku`, `type_id`,
`attribute_set_id`, `name`, `price`, `status`, `visibility`, `weight`,
`custom_attributes` or `extension_attributes`. The row fails with
`attributes shadow writer-owned keys: ...` instead of guessing which value
you meant.

### store_values

`store_values` maps a store view code to the values that store overrides.
`name`, `status` and `visibility` go to the top level of that store's
payload; every other key is a custom attribute (converted as above). Keep
these entries to localized values only: everything in them is written as a
store-level override.

### Validation rule

When `type == "configurable"` and `variations` is non-empty:

- `configurable_attributes` must be non-empty
  (`configurable_attributes must be non-empty when variations are present`);
- every variation's `attributes` must contain every configurable attribute
  (`variation <i> is missing configurable attributes: [...]`).

### Minimal example

```json
{"sku": "TSHIRT-RED-S"}
```

On a new SKU this creates a `simple` product in set `Default` on website
`base`. On an existing SKU it sends only `sku` and an empty
`custom_attributes` list (see [partial updates](#partial-update-semantics)).

### Full example

```json
{
  "sku": "TSHIRT",
  "type": "configurable",
  "attribute_set": "Apparel",
  "name": "Classic T-shirt",
  "price": 19.9,
  "status": 1,
  "visibility": 4,
  "weight": 0.3,
  "websites": ["base"],
  "categories": ["Default Category/Men/Tops", "Men/Sale"],
  "attributes": {"description": "<p>Cotton tee</p>", "url_key": "classic-t-shirt", "material": "Cotton"},
  "store_values": {"fr": {"name": "T-shirt classique", "description": "<p>Coton</p>"}},
  "configurable_attributes": ["color", "size"],
  "variations": [
    {"sku": "TSHIRT-RED-S", "attributes": {"color": "Red", "size": "S"}},
    {"sku": "TSHIRT-RED-M", "attributes": {"color": "Red", "size": "M"}}
  ],
  "images": [
    {"source": "https://cdn.example.test/tshirt.jpg", "position": 1,
     "roles": ["image", "small_image", "thumbnail"], "label": "Front"}
  ]
}
```

`Men/Sale` is read as `Default Category/Men/Sale`. `material` must be an
existing attribute; if it is a `select`, the label `Cotton` is created when
missing.

## Product type parts

### Variation

| Field | Type | Default | Magento payload |
| --- | --- | --- | --- |
| `sku` | `str` | required | `childSku` of the child link |
| `attributes` | `dict[str, str]` | required | Option labels per configurable attribute; the distinct labels become the option `values` (`value_index` = resolved option id) |

The configurable option payload is `{"attribute_id", "label": <attribute code>,
"position": <index>, "is_use_default": true, "values": [...]}`.

### BundleOption and BundleSelection

| Field | Type | Default | Magento payload |
| --- | --- | --- | --- |
| `BundleOption.title` | `str` | required | `title` |
| `BundleOption.type` | `Literal["select", "radio", "checkbox", "multi"]` | required | `type` |
| `BundleOption.required` | `bool` | required | `required` |
| `BundleOption.selections` | `list[BundleSelection]` | required | `product_links`; option `position` is its index |
| `BundleSelection.sku` | `str` | required | `sku` of the link |
| `BundleSelection.qty` | `float` | `1` | `qty` |
| `BundleSelection.price` | `float \| None` | `None` | `price` (sent even when `None`) |
| `BundleSelection.price_type` | `Literal["fixed", "percent"] \| None` | `None` | `price_type` 0 (fixed) or 1 (percent); omitted when `None` |
| `BundleSelection.is_default` | `bool` | `False` | `is_default` |

Every selection also gets `can_change_quantity: 0` and `position` (its index).
Bundle level flags (price type, SKU type, weight type, price view, shipment
type) are ordinary custom attributes: put `price_type`, `sku_type`,
`weight_type`, `price_view`, `shipment_type` in `attributes`.

### GroupedLink

| Field | Type | Default | Magento payload |
| --- | --- | --- | --- |
| `sku` | `str` | required | `linked_product_sku` |
| `qty` | `float` | `0` | `extension_attributes.qty` |
| `position` | `int` | `0` | `position` |

Each link is sent with `link_type: "associated"` and
`linked_product_type: "simple"`.

### DownloadableLink and DownloadableSample

| Field | Type | Default | Magento payload |
| --- | --- | --- | --- |
| `DownloadableLink.title` | `str` | required | `title` |
| `DownloadableLink.url` | `str` | required | `link_url`, with `link_type: "url"` |
| `DownloadableLink.price` | `float` | `0` | `price` |
| `DownloadableLink.sortable` | `bool \| None` | `None` | Not sent; `sort_order` is the link's index |
| `DownloadableLink.shareable` | `bool \| None` | `None` | `is_shareable`: true 1, false 0, `None` 2 (use config) |
| `DownloadableLink.downloads` | `int \| None` | `None` | `number_of_downloads`, `None` sent as 0 |
| `DownloadableSample.title` | `str` | required | `title` |
| `DownloadableSample.url` | `str` | required | `sample_url`, with `sample_type: "url"` |

Only URL links and samples are supported; there is no file upload for
downloadable content.

### Image

| Field | Type | Default | Magento payload (`POST products/{sku}/media` entry) |
| --- | --- | --- | --- |
| `source` | `str` | required | Read from a local path or downloaded from http(s); sent as `content.base64_encoded_data`, `content.name` is the file name |
| `position` | `int` | required | `position`; also half of the image identity |
| `roles` | `list[Literal["image", "small_image", "thumbnail", "swatch_image"]]` | `[]` | `types` |
| `label` | `str \| None` | `None` | `label` (`None` sent as `""`); the other half of the identity |
| `disabled` | `bool` | `False` | `disabled` |

See [the media identity rule](Catalog-Importers#media-identity-rule).

## Partial update semantics

For a SKU that already exists in Magento (the snapshot decides), the product
writer sends only what the row says:

- `name`, `price`, `status`, `visibility`, `weight` are sent only when not
  `None`. Setting one to `None` never clears it in Magento.
- `type`, `attribute_set` and `websites` are listed in
  `ProductRow.CREATE_DEFAULTS`. Their defaults (`simple`, `Default`,
  `["base"]`) apply only when creating. On an update they are sent, and
  compared by the diff, only when the row set them explicitly
  (`ProductRow.applies_on_update(field)` checks `model_fields_set`). A row that
  names three columns never turns an existing configurable into a simple in
  `Default` on `base`.
- `categories` is sent only when non-empty; an empty list leaves the
  assignments alone.
- `custom_attributes` carries only the keys in `attributes`.

"Explicitly set" means present in the dict you passed, or passed to the model
constructor. Re-validating a full `model_dump()` would mark every default as
explicit; the importers avoid that by dumping with `exclude_unset=True`, and
you should do the same if you copy rows yourself:

```python
row = ProductRow(sku="TSHIRT", price=17.5)
row.model_fields_set               # {'sku', 'price'}
row.applies_on_update("type")      # False: type is not sent on update
row.model_dump(exclude_unset=True) # {'sku': 'TSHIRT', 'price': 17.5}
```

Passing `"websites": []` explicitly on an update sends an empty
`website_ids` list; how Magento treats that is not covered by the library's
tests.

## CategoryRow

| Field | Type | Default | Magento payload |
| --- | --- | --- | --- |
| `path` | `str` | required | Resolved or created category; levels separated by `/`, root `Default Category` prefixed when missing, matched case-insensitively |
| `attributes` | `dict[str, Any]` | `{}` | `PUT categories/{id}`: `name`, `is_active`, `position`, `include_in_menu`, `available_sort_by` at the top level, everything else in `custom_attributes` |
| `store_values` | `dict[str, dict[str, Any]]` | `{}` | One `PUT categories/{id}` per store code through `/rest/<store_code>/`, same split |

Rejected keys: `attributes` may not contain `id`, `parent_id`, `path`, `name`;
a `store_values` entry may not contain `id`, `parent_id`, `path` (a localized
`name` is fine). Use the last path segment to name a category.

```json
{"path": "Default Category/Men/Tops"}
```

```json
{
  "path": "Men/Tops",
  "attributes": {"is_active": 1, "include_in_menu": 1, "url_key": "men-tops",
                 "description": "<p>Tops</p>", "available_sort_by": "position,name,price",
                 "default_sort_by": "position", "display_mode": "PRODUCTS"},
  "store_values": {"fr": {"name": "Hauts", "url_key": "hauts"}}
}
```

## AttributeRow and AttributeOption

| Field | Type | Default | Magento payload (`products/attributes`) |
| --- | --- | --- | --- |
| `code` | `str` | required | `attribute_code` on create, URL key on update |
| `frontend_input` | `str` | required | `frontend_input`; not validated by the model (`select`, `multiselect`, `boolean`, `text`, `textarea`, `price`, `date`, ...) |
| `label` | `str` | required | `default_frontend_label` |
| `scope` | `Literal["global", "website", "store"]` | `"store"` | `scope` |
| `options` | `list[AttributeOption]` | `[]` | `options` on create; on update only the missing labels, one options POST each |
| `store_labels` | `dict[str, str]` | `{}` | `frontend_labels`, store code resolved to `store_id` |
| `flags` | `dict[str, Any]` | `{}` | Spread into the payload as is (`is_required`, `is_filterable`, `apply_to`, `default_value`, ...) |
| `AttributeOption.label` | `str` | required | option `label` |
| `AttributeOption.store_labels` | `dict[str, str]` | `{}` | option `store_labels`, store code resolved to `store_id` |
| `AttributeOption.sort_order` | `int` | `0` | option `sort_order` |

Every payload also has `is_user_defined: true`. `flags` may not contain
`attribute_code`, `attribute_id`, `frontend_input`, `default_frontend_label`,
`frontend_labels`, `scope`, `is_user_defined` or `options`.

```json
{"code": "material", "frontend_input": "select", "label": "Material"}
```

```json
{
  "code": "material",
  "frontend_input": "select",
  "label": "Material",
  "scope": "global",
  "store_labels": {"fr": "Matiere"},
  "options": [
    {"label": "Cotton", "sort_order": 1, "store_labels": {"fr": "Coton"}},
    {"label": "Wool", "sort_order": 2}
  ],
  "flags": {"is_required": false, "is_filterable": true, "apply_to": ["simple", "configurable"]}
}
```

## AttributeSetRow

| Field | Type | Default | Magento payload |
| --- | --- | --- | --- |
| `name` | `str` | required | `attributeSet.attribute_set_name`; matched case-insensitively |
| `based_on` | `str` | `"Default"` | `skeletonId`, resolved by name; only used when the set is created |
| `groups` | `dict[str, list[str]]` | `{}` | Group name to attribute codes; missing groups are created, each code is assigned with `sortOrder` = its index |

```json
{"name": "Apparel"}
```

```json
{"name": "Apparel", "based_on": "Default",
 "groups": {"Product Details": ["material", "color", "size"], "Care": ["care_instructions"]}}
```

## PriceRow and TierPrice

| Field | Type | Default | Magento payload |
| --- | --- | --- | --- |
| `sku` | `str` | required | `sku` of every price item |
| `price` | `float \| None` | `None` | `products/base-prices` item `{sku, price, store_id}` |
| `store_id` | `int` | `0` | `store_id` of base and special prices (numeric store id, 0 = global) |
| `special_price` | `float \| None` | `None` | `products/special-price` item `{sku, price, store_id}` |
| `special_from` | `str \| None` | `None` | `price_from`, omitted when `None` |
| `special_to` | `str \| None` | `None` | `price_to`, omitted when `None` |
| `tiers` | `list[TierPrice] \| None` | `None` | `None`: tiers untouched. A list: replaces all tiers of the SKU. `[]`: deletes them |
| `TierPrice.qty` | `float` | required | `quantity` |
| `TierPrice.price` | `float` | required | `price` |
| `TierPrice.customer_group` | `str` | `"ALL GROUPS"` | `customer_group` |
| `TierPrice.website` | `str` | `"all"` | `website_id`: `all` is 0, digits are an id, anything else a website code resolved through `GET store/websites` (unknown code fails the row) |
| `TierPrice.price_type` | `Literal["fixed", "discount"]` | `"fixed"` | `price_type` |

Validation rule: when both `special_from` and `special_to` parse as ISO
dates and the end is before the start, the row fails with
`special_to is before special_from` (Magento itself would store the inverted
range and report success). Unparseable dates are not checked here.

```json
{"sku": "TSHIRT-RED-S", "price": 19.9}
```

```json
{"sku": "TSHIRT-RED-S", "price": 19.9, "store_id": 0,
 "special_price": 14.9, "special_from": "2026-11-01", "special_to": "2026-11-30",
 "tiers": [{"qty": 5, "price": 17.0},
           {"qty": 10, "price": 10, "price_type": "discount", "customer_group": "Wholesale", "website": "base"}]}
```

See [Price Import](Price-Import) for merging and diff rules.

## SourceItemRow

| Field | Type | Default | Magento payload (`inventory/source-items`) |
| --- | --- | --- | --- |
| `sku` | `str` | required | `sku` |
| `source_code` | `str` | required | `source_code` |
| `quantity` | `float` | required | `quantity` |
| `status` | `Literal[0, 1]` | required | `status`: 1 in stock, 0 out of stock (int, not string) |

```json
{"sku": "TSHIRT-RED-S", "source_code": "default", "quantity": 12, "status": 1}
```

All four fields are required, so the minimal and full examples are the same.

## SourceRow

| Field | Type | Default | Magento payload (`inventory/sources`) |
| --- | --- | --- | --- |
| `source_code` | `str` | required | `source_code` |
| `name` | `str` | required | `name` |
| `enabled` | `bool` | `True` | `enabled` |
| `country_id` | `str` | required | `country_id` (ISO code such as `FR`) |
| `postcode` | `str` | required | `postcode` |

```json
{"source_code": "eu-warehouse", "name": "EU warehouse", "country_id": "FR", "postcode": "75001"}
```

```json
{"source_code": "eu-warehouse", "name": "EU warehouse", "enabled": false, "country_id": "FR", "postcode": "75001"}
```

## StockRow

| Field | Type | Default | Magento payload (`inventory/stocks`) |
| --- | --- | --- | --- |
| `name` | `str` | required | `stock.name` |
| `websites` | `list[str]` | required | `extension_attributes.sales_channels`, one `{"type": "website", "code": ...}` per code; each code must exist |

```json
{"name": "EU stock", "websites": ["base", "eu"]}
```

## StockSourceLinkRow

| Field | Type | Default | Magento payload (`inventory/stock-source-links`) |
| --- | --- | --- | --- |
| `stock` | `str` | required | Stock name, resolved to `stock_id` (exact match) |
| `source_code` | `str` | required | `source_code` |
| `priority` | `int` | required | `priority` |

```json
{"stock": "EU stock", "source_code": "eu-warehouse", "priority": 1}
```

## Errors that are not model errors

Some rows validate but still fail during planning, with the row's id as
`row_ref`. The most common ones:

| Message | Cause |
| --- | --- |
| `unknown attribute: <code>` | A product `attributes` or `store_values` key that is not an attribute code |
| `unknown attribute set: <name>` | A product `attribute_set` that does not exist |
| `unknown website: <code>` | A product website code that does not exist |
| `unknown category path: <path>` | A product category not created by `import_categories` |
| `invalid boolean '<v>' for attribute '<code>'` | A boolean attribute value outside yes/no/true/false/1/0 |
| `attributes shadow writer-owned keys: ...` | A reserved key in `attributes` (products, categories) |
| `flags shadow writer-owned keys: ...` | A reserved key in attribute `flags` |
| `unknown website code: <code>` | A tier price or stock website that does not exist |
| `unknown stock name: <name>` | A stock source link to a stock that does not exist |
| `sku does not exist` | `behavior="disable"` on a SKU Magento does not have |

## See also

- [Home](Home)
- [Catalog Importers](Catalog-Importers)
- [File Formats](File-Formats)
- [Optional Bridge](Optional-Bridge)
- [Price Import](Price-Import)
- [Results and Errors](Results-and-Errors)
- [Getting Started](Getting-Started)
- [Architecture](Architecture)
- [Troubleshooting](Troubleshooting)
