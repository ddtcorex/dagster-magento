"""Canonical catalog row models for Magento 2 ETL imports."""

from datetime import datetime
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, Field, model_validator, ValidationError
from dagster_magento.operation import RowError


class Variation(BaseModel):
    """Variation for configurable products."""
    sku: str
    attributes: dict[str, str]


class BundleSelection(BaseModel):
    """Selection option within a bundle option."""
    sku: str
    qty: float = 1
    price: float | None = None
    price_type: Literal["fixed", "percent"] | None = None
    is_default: bool = False


class BundleOption(BaseModel):
    """Bundle product option."""
    title: str
    type: Literal["select", "radio", "checkbox", "multi"]
    required: bool
    selections: list[BundleSelection]


class DownloadableLink(BaseModel):
    """Downloadable product link."""
    title: str
    url: str
    price: float = 0
    sortable: bool | None = None
    shareable: bool | None = None
    downloads: int | None = None


class DownloadableSample(BaseModel):
    """Downloadable product sample."""
    title: str
    url: str


class Image(BaseModel):
    """Product image."""
    source: str
    position: int
    roles: list[Literal["image", "small_image", "thumbnail", "swatch_image"]] = Field(
        default_factory=list
    )
    label: str | None = None
    disabled: bool = False


class GroupedLink(BaseModel):
    """Product link for grouped products."""
    sku: str
    qty: float = 0
    position: int = 0


class ProductRow(BaseModel):
    """Product catalog row."""
    sku: str
    type: str = "simple"
    attribute_set: str = "Default"
    name: str | None = None
    price: float | None = None
    status: Literal[1, 2] | None = None
    visibility: Literal[1, 2, 3, 4] | None = None
    weight: float | None = None
    websites: list[str] = Field(default_factory=lambda: ["base"])
    categories: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)
    store_values: dict[str, dict[str, Any]] = Field(default_factory=dict)
    variations: list[Variation] = Field(default_factory=list)
    configurable_attributes: list[str] = Field(default_factory=list)
    bundle_options: list[BundleOption] = Field(default_factory=list)
    grouped_links: list[GroupedLink] = Field(default_factory=list)
    downloadable_links: list[DownloadableLink] = Field(default_factory=list)
    downloadable_samples: list[DownloadableSample] = Field(default_factory=list)
    images: list[Image] = Field(default_factory=list)

    # Fields whose default only makes sense for a product being created. On
    # an update the writer sends them, and the diff compares them, only when
    # the row set them explicitly: a row naming a few columns must never
    # reset an existing configurable to a simple in "Default" on "base".
    CREATE_DEFAULTS: ClassVar[frozenset[str]] = frozenset({"type", "attribute_set", "websites"})

    def applies_on_update(self, field: str) -> bool:
        """Whether `field` belongs in an update of an existing product."""
        return field not in self.CREATE_DEFAULTS or field in self.model_fields_set

    @model_validator(mode="after")
    def validate_configurable(self):
        """Validate that configurable products with variations have configurable_attributes."""
        # If type is configurable and variations exist, configurable_attributes must be non-empty
        if self.type == "configurable" and self.variations:
            if not self.configurable_attributes:
                raise ValueError(
                    "configurable_attributes must be non-empty when variations are present"
                )

            # Every variation's attributes must include all configurable attributes
            for i, var in enumerate(self.variations):
                missing = set(self.configurable_attributes) - set(var.attributes.keys())
                if missing:
                    raise ValueError(
                        f"variation {i} is missing configurable attributes: {sorted(missing)}"
                    )

        return self


class CategoryRow(BaseModel):
    """Category catalog row."""
    path: str
    attributes: dict[str, Any] = Field(default_factory=dict)
    store_values: dict[str, dict[str, Any]] = Field(default_factory=dict)


class AttributeOption(BaseModel):
    """Attribute option value."""
    label: str
    store_labels: dict[str, str] = Field(default_factory=dict)
    sort_order: int = 0


class AttributeRow(BaseModel):
    """Product attribute row."""
    code: str
    frontend_input: str
    label: str
    scope: Literal["global", "website", "store"] = "store"
    options: list[AttributeOption] = Field(default_factory=list)
    store_labels: dict[str, str] = Field(default_factory=dict)
    flags: dict[str, Any] = Field(default_factory=dict)


class AttributeSetRow(BaseModel):
    """Attribute set row."""
    name: str
    based_on: str = "Default"
    groups: dict[str, list[str]] = Field(default_factory=dict)


class TierPrice(BaseModel):
    """Tier price entry."""
    qty: float
    price: float
    customer_group: str = "ALL GROUPS"
    website: str = "all"
    price_type: Literal["fixed", "discount"] = "fixed"


class PriceRow(BaseModel):
    """Advanced pricing row."""
    sku: str
    price: float | None = None
    store_id: int = 0
    special_price: float | None = None
    special_from: str | None = None
    special_to: str | None = None
    tiers: list[TierPrice] | None = None

    @model_validator(mode="after")
    def _special_range_is_ordered(self):
        # Magento's price storage stores an inverted range and reports it
        # as a success (verified live), so the library is the only gate.
        if self.special_from and self.special_to:
            try:
                start = datetime.fromisoformat(self.special_from)
                end = datetime.fromisoformat(self.special_to)
            except ValueError:
                return self
            if end < start:
                raise ValueError("special_to is before special_from")
        return self


class SourceItemRow(BaseModel):
    """Inventory source item row."""
    sku: str
    source_code: str
    quantity: float
    status: Literal[0, 1]


class SourceRow(BaseModel):
    """Inventory source row."""
    source_code: str
    name: str
    enabled: bool = True
    country_id: str
    postcode: str


class StockRow(BaseModel):
    """Inventory stock row."""
    name: str
    websites: list[str]


class StockSourceLinkRow(BaseModel):
    """Inventory stock-source link row."""
    stock: str
    source_code: str
    priority: int


def validate_rows(
    model: type[BaseModel],
    raw: list[dict],
    id_field: str,
) -> tuple[list[BaseModel], list[RowError]]:
    """Validate a list of raw dicts against a Pydantic model.

    Returns tuple of (valid_rows, errors). For each row that fails validation,
    a RowError is created with row_ref from the id_field (or row index if missing)
    and message from the pydantic validation error.

    Args:
        model: The Pydantic BaseModel class to validate against
        raw: List of raw dicts to validate
        id_field: Field name to use as row_ref (e.g. "sku", "code")

    Returns:
        Tuple of (list of validated model instances, list of RowErrors)
    """
    valid = []
    errors = []

    for idx, row in enumerate(raw):
        try:
            instance = model.model_validate(row)
            valid.append(instance)
        except ValidationError as e:
            # Get row_ref from id_field if present, otherwise use index
            row_ref = str(row.get(id_field, f"row {idx}"))

            # Build compact message from pydantic ValidationError
            error_parts = []
            for err in e.errors():
                field = ".".join(str(x) for x in err["loc"])
                msg = err["msg"]
                error_parts.append(f"{field}: {msg}")
            message = "; ".join(error_parts)

            errors.append(RowError(row_ref=row_ref, message=message))

    return valid, errors
