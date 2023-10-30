from acit import bq_table_pb2 as _bq_table_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CloudExportAdditionalProperties(_message.Message):
    __slots__ = ["bool_value", "float_value", "int_value", "max_value", "min_value", "property_name", "text_value", "unit_code"]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    FLOAT_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    MAX_VALUE_FIELD_NUMBER: _ClassVar[int]
    MIN_VALUE_FIELD_NUMBER: _ClassVar[int]
    PROPERTY_NAME_FIELD_NUMBER: _ClassVar[int]
    TEXT_VALUE_FIELD_NUMBER: _ClassVar[int]
    UNIT_CODE_FIELD_NUMBER: _ClassVar[int]
    bool_value: bool
    float_value: _containers.RepeatedScalarFieldContainer[float]
    int_value: _containers.RepeatedScalarFieldContainer[int]
    max_value: float
    min_value: float
    property_name: str
    text_value: _containers.RepeatedScalarFieldContainer[str]
    unit_code: str
    def __init__(self, property_name: _Optional[str] = ..., text_value: _Optional[_Iterable[str]] = ..., bool_value: bool = ..., int_value: _Optional[_Iterable[int]] = ..., float_value: _Optional[_Iterable[float]] = ..., min_value: _Optional[float] = ..., max_value: _Optional[float] = ..., unit_code: _Optional[str] = ...) -> None: ...

class CustomAttribute(_message.Message):
    __slots__ = ["group_values", "name", "value"]
    GROUP_VALUES_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    group_values: _containers.RepeatedCompositeFieldContainer[CustomAttribute]
    name: str
    value: str
    def __init__(self, name: _Optional[str] = ..., value: _Optional[str] = ..., group_values: _Optional[_Iterable[_Union[CustomAttribute, _Mapping]]] = ...) -> None: ...

class Installment(_message.Message):
    __slots__ = ["amount", "months"]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    MONTHS_FIELD_NUMBER: _ClassVar[int]
    amount: Price
    months: int
    def __init__(self, months: _Optional[int] = ..., amount: _Optional[_Union[Price, _Mapping]] = ...) -> None: ...

class LoyaltyPoints(_message.Message):
    __slots__ = ["name", "points_value", "ratio"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    POINTS_VALUE_FIELD_NUMBER: _ClassVar[int]
    RATIO_FIELD_NUMBER: _ClassVar[int]
    name: str
    points_value: int
    ratio: float
    def __init__(self, name: _Optional[str] = ..., points_value: _Optional[int] = ..., ratio: _Optional[float] = ...) -> None: ...

class Price(_message.Message):
    __slots__ = ["currency", "value"]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    currency: str
    value: str
    def __init__(self, value: _Optional[str] = ..., currency: _Optional[str] = ...) -> None: ...

class Product(_message.Message):
    __slots__ = ["additional_image_links", "additional_size_type", "ads_grouping", "ads_labels", "ads_redirect", "adult", "age_group", "availability", "availability_date", "brand", "canonical_link", "certifications", "channel", "cloud_export_additional_properties", "color", "condition", "content_language", "cost_of_goods_sold", "custom_attributes", "custom_label0", "custom_label1", "custom_label2", "custom_label3", "custom_label4", "description", "disclosure_date", "display_ads_id", "display_ads_link", "display_ads_similar_ids", "display_ads_title", "display_ads_value", "energy_efficiency_class", "excluded_destinations", "expiration_date", "external_seller_id", "feed_label", "gender", "google_product_category", "gtin", "id", "identifier_exists", "image_link", "included_destinations", "installment", "is_bundle", "item_group_id", "kind", "lifestyle_image_links", "link", "link_template", "loyalty_points", "material", "max_energy_efficiency_class", "max_handling_time", "min_energy_efficiency_class", "min_handling_time", "mobile_link", "mobile_link_template", "mpn", "multipack", "offer_id", "pattern", "pause", "pickup_method", "pickup_sla", "price", "product_details", "product_height", "product_highlights", "product_length", "product_types", "product_weight", "product_width", "promotion_ids", "sale_price", "sale_price_effective_date", "sell_on_google_quantity", "shipping", "shipping_height", "shipping_label", "shipping_length", "shipping_weight", "shipping_width", "shopping_ads_excluded_countries", "size_system", "size_type", "sizes", "source", "subscription_cost", "target_country", "tax_category", "taxes", "title", "transit_time_label", "unit_pricing_base_measure", "unit_pricing_measure", "virtual_model_link"]
    ADDITIONAL_IMAGE_LINKS_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_SIZE_TYPE_FIELD_NUMBER: _ClassVar[int]
    ADS_GROUPING_FIELD_NUMBER: _ClassVar[int]
    ADS_LABELS_FIELD_NUMBER: _ClassVar[int]
    ADS_REDIRECT_FIELD_NUMBER: _ClassVar[int]
    ADULT_FIELD_NUMBER: _ClassVar[int]
    AGE_GROUP_FIELD_NUMBER: _ClassVar[int]
    AVAILABILITY_DATE_FIELD_NUMBER: _ClassVar[int]
    AVAILABILITY_FIELD_NUMBER: _ClassVar[int]
    BRAND_FIELD_NUMBER: _ClassVar[int]
    CANONICAL_LINK_FIELD_NUMBER: _ClassVar[int]
    CERTIFICATIONS_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    CLOUD_EXPORT_ADDITIONAL_PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    COLOR_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    COST_OF_GOODS_SOLD_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_LABEL0_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_LABEL1_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_LABEL2_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_LABEL3_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_LABEL4_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    DISCLOSURE_DATE_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_ADS_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_ADS_LINK_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_ADS_SIMILAR_IDS_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_ADS_TITLE_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_ADS_VALUE_FIELD_NUMBER: _ClassVar[int]
    ENERGY_EFFICIENCY_CLASS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDED_DESTINATIONS_FIELD_NUMBER: _ClassVar[int]
    EXPIRATION_DATE_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_SELLER_ID_FIELD_NUMBER: _ClassVar[int]
    FEED_LABEL_FIELD_NUMBER: _ClassVar[int]
    GENDER_FIELD_NUMBER: _ClassVar[int]
    GOOGLE_PRODUCT_CATEGORY_FIELD_NUMBER: _ClassVar[int]
    GTIN_FIELD_NUMBER: _ClassVar[int]
    IDENTIFIER_EXISTS_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    IMAGE_LINK_FIELD_NUMBER: _ClassVar[int]
    INCLUDED_DESTINATIONS_FIELD_NUMBER: _ClassVar[int]
    INSTALLMENT_FIELD_NUMBER: _ClassVar[int]
    IS_BUNDLE_FIELD_NUMBER: _ClassVar[int]
    ITEM_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    LIFESTYLE_IMAGE_LINKS_FIELD_NUMBER: _ClassVar[int]
    LINK_FIELD_NUMBER: _ClassVar[int]
    LINK_TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    LOYALTY_POINTS_FIELD_NUMBER: _ClassVar[int]
    MATERIAL_FIELD_NUMBER: _ClassVar[int]
    MAX_ENERGY_EFFICIENCY_CLASS_FIELD_NUMBER: _ClassVar[int]
    MAX_HANDLING_TIME_FIELD_NUMBER: _ClassVar[int]
    MIN_ENERGY_EFFICIENCY_CLASS_FIELD_NUMBER: _ClassVar[int]
    MIN_HANDLING_TIME_FIELD_NUMBER: _ClassVar[int]
    MOBILE_LINK_FIELD_NUMBER: _ClassVar[int]
    MOBILE_LINK_TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    MPN_FIELD_NUMBER: _ClassVar[int]
    MULTIPACK_FIELD_NUMBER: _ClassVar[int]
    OFFER_ID_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    PAUSE_FIELD_NUMBER: _ClassVar[int]
    PICKUP_METHOD_FIELD_NUMBER: _ClassVar[int]
    PICKUP_SLA_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_DETAILS_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_HIGHLIGHTS_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_LENGTH_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_TYPES_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_WEIGHT_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_WIDTH_FIELD_NUMBER: _ClassVar[int]
    PROMOTION_IDS_FIELD_NUMBER: _ClassVar[int]
    SALE_PRICE_EFFECTIVE_DATE_FIELD_NUMBER: _ClassVar[int]
    SALE_PRICE_FIELD_NUMBER: _ClassVar[int]
    SELL_ON_GOOGLE_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_LABEL_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_LENGTH_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_WEIGHT_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_WIDTH_FIELD_NUMBER: _ClassVar[int]
    SHOPPING_ADS_EXCLUDED_COUNTRIES_FIELD_NUMBER: _ClassVar[int]
    SIZES_FIELD_NUMBER: _ClassVar[int]
    SIZE_SYSTEM_FIELD_NUMBER: _ClassVar[int]
    SIZE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIPTION_COST_FIELD_NUMBER: _ClassVar[int]
    TARGET_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    TAXES_FIELD_NUMBER: _ClassVar[int]
    TAX_CATEGORY_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    TRANSIT_TIME_LABEL_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICING_BASE_MEASURE_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICING_MEASURE_FIELD_NUMBER: _ClassVar[int]
    VIRTUAL_MODEL_LINK_FIELD_NUMBER: _ClassVar[int]
    additional_image_links: _containers.RepeatedScalarFieldContainer[str]
    additional_size_type: str
    ads_grouping: str
    ads_labels: _containers.RepeatedScalarFieldContainer[str]
    ads_redirect: str
    adult: bool
    age_group: str
    availability: str
    availability_date: str
    brand: str
    canonical_link: str
    certifications: _containers.RepeatedCompositeFieldContainer[ProductCertification]
    channel: str
    cloud_export_additional_properties: _containers.RepeatedCompositeFieldContainer[CloudExportAdditionalProperties]
    color: str
    condition: str
    content_language: str
    cost_of_goods_sold: Price
    custom_attributes: _containers.RepeatedCompositeFieldContainer[CustomAttribute]
    custom_label0: str
    custom_label1: str
    custom_label2: str
    custom_label3: str
    custom_label4: str
    description: str
    disclosure_date: str
    display_ads_id: str
    display_ads_link: str
    display_ads_similar_ids: _containers.RepeatedScalarFieldContainer[str]
    display_ads_title: str
    display_ads_value: float
    energy_efficiency_class: str
    excluded_destinations: _containers.RepeatedScalarFieldContainer[str]
    expiration_date: str
    external_seller_id: str
    feed_label: str
    gender: str
    google_product_category: str
    gtin: str
    id: str
    identifier_exists: bool
    image_link: str
    included_destinations: _containers.RepeatedScalarFieldContainer[str]
    installment: Installment
    is_bundle: bool
    item_group_id: str
    kind: str
    lifestyle_image_links: _containers.RepeatedScalarFieldContainer[str]
    link: str
    link_template: str
    loyalty_points: LoyaltyPoints
    material: str
    max_energy_efficiency_class: str
    max_handling_time: int
    min_energy_efficiency_class: str
    min_handling_time: int
    mobile_link: str
    mobile_link_template: str
    mpn: str
    multipack: int
    offer_id: str
    pattern: str
    pause: str
    pickup_method: str
    pickup_sla: str
    price: Price
    product_details: _containers.RepeatedCompositeFieldContainer[ProductProductDetail]
    product_height: ProductDimension
    product_highlights: _containers.RepeatedScalarFieldContainer[str]
    product_length: ProductDimension
    product_types: _containers.RepeatedScalarFieldContainer[str]
    product_weight: ProductWeight
    product_width: ProductDimension
    promotion_ids: _containers.RepeatedScalarFieldContainer[str]
    sale_price: Price
    sale_price_effective_date: str
    sell_on_google_quantity: int
    shipping: _containers.RepeatedCompositeFieldContainer[ProductShipping]
    shipping_height: ProductShippingDimension
    shipping_label: str
    shipping_length: ProductShippingDimension
    shipping_weight: ProductShippingWeight
    shipping_width: ProductShippingDimension
    shopping_ads_excluded_countries: _containers.RepeatedScalarFieldContainer[str]
    size_system: str
    size_type: str
    sizes: _containers.RepeatedScalarFieldContainer[str]
    source: str
    subscription_cost: ProductSubscriptionCost
    target_country: str
    tax_category: str
    taxes: _containers.RepeatedCompositeFieldContainer[ProductTax]
    title: str
    transit_time_label: str
    unit_pricing_base_measure: ProductUnitPricingBaseMeasure
    unit_pricing_measure: ProductUnitPricingMeasure
    virtual_model_link: str
    def __init__(self, id: _Optional[str] = ..., offer_id: _Optional[str] = ..., title: _Optional[str] = ..., description: _Optional[str] = ..., link: _Optional[str] = ..., image_link: _Optional[str] = ..., additional_image_links: _Optional[_Iterable[str]] = ..., lifestyle_image_links: _Optional[_Iterable[str]] = ..., content_language: _Optional[str] = ..., target_country: _Optional[str] = ..., feed_label: _Optional[str] = ..., channel: _Optional[str] = ..., expiration_date: _Optional[str] = ..., disclosure_date: _Optional[str] = ..., adult: bool = ..., kind: _Optional[str] = ..., brand: _Optional[str] = ..., color: _Optional[str] = ..., google_product_category: _Optional[str] = ..., gtin: _Optional[str] = ..., item_group_id: _Optional[str] = ..., material: _Optional[str] = ..., mpn: _Optional[str] = ..., pattern: _Optional[str] = ..., price: _Optional[_Union[Price, _Mapping]] = ..., sale_price: _Optional[_Union[Price, _Mapping]] = ..., sale_price_effective_date: _Optional[str] = ..., product_height: _Optional[_Union[ProductDimension, _Mapping]] = ..., product_length: _Optional[_Union[ProductDimension, _Mapping]] = ..., product_width: _Optional[_Union[ProductDimension, _Mapping]] = ..., product_weight: _Optional[_Union[ProductWeight, _Mapping]] = ..., shipping: _Optional[_Iterable[_Union[ProductShipping, _Mapping]]] = ..., shipping_weight: _Optional[_Union[ProductShippingWeight, _Mapping]] = ..., sizes: _Optional[_Iterable[str]] = ..., taxes: _Optional[_Iterable[_Union[ProductTax, _Mapping]]] = ..., custom_attributes: _Optional[_Iterable[_Union[CustomAttribute, _Mapping]]] = ..., identifier_exists: bool = ..., installment: _Optional[_Union[Installment, _Mapping]] = ..., loyalty_points: _Optional[_Union[LoyaltyPoints, _Mapping]] = ..., multipack: _Optional[int] = ..., custom_label0: _Optional[str] = ..., custom_label1: _Optional[str] = ..., custom_label2: _Optional[str] = ..., custom_label3: _Optional[str] = ..., custom_label4: _Optional[str] = ..., is_bundle: bool = ..., mobile_link: _Optional[str] = ..., availability_date: _Optional[str] = ..., shipping_label: _Optional[str] = ..., unit_pricing_measure: _Optional[_Union[ProductUnitPricingMeasure, _Mapping]] = ..., unit_pricing_base_measure: _Optional[_Union[ProductUnitPricingBaseMeasure, _Mapping]] = ..., shipping_length: _Optional[_Union[ProductShippingDimension, _Mapping]] = ..., shipping_width: _Optional[_Union[ProductShippingDimension, _Mapping]] = ..., shipping_height: _Optional[_Union[ProductShippingDimension, _Mapping]] = ..., display_ads_id: _Optional[str] = ..., display_ads_similar_ids: _Optional[_Iterable[str]] = ..., display_ads_title: _Optional[str] = ..., display_ads_link: _Optional[str] = ..., display_ads_value: _Optional[float] = ..., sell_on_google_quantity: _Optional[int] = ..., promotion_ids: _Optional[_Iterable[str]] = ..., max_handling_time: _Optional[int] = ..., min_handling_time: _Optional[int] = ..., cost_of_goods_sold: _Optional[_Union[Price, _Mapping]] = ..., source: _Optional[str] = ..., included_destinations: _Optional[_Iterable[str]] = ..., excluded_destinations: _Optional[_Iterable[str]] = ..., ads_grouping: _Optional[str] = ..., ads_labels: _Optional[_Iterable[str]] = ..., ads_redirect: _Optional[str] = ..., product_types: _Optional[_Iterable[str]] = ..., age_group: _Optional[str] = ..., availability: _Optional[str] = ..., condition: _Optional[str] = ..., gender: _Optional[str] = ..., size_system: _Optional[str] = ..., size_type: _Optional[str] = ..., additional_size_type: _Optional[str] = ..., energy_efficiency_class: _Optional[str] = ..., min_energy_efficiency_class: _Optional[str] = ..., max_energy_efficiency_class: _Optional[str] = ..., tax_category: _Optional[str] = ..., transit_time_label: _Optional[str] = ..., shopping_ads_excluded_countries: _Optional[_Iterable[str]] = ..., pickup_method: _Optional[str] = ..., pickup_sla: _Optional[str] = ..., link_template: _Optional[str] = ..., mobile_link_template: _Optional[str] = ..., product_details: _Optional[_Iterable[_Union[ProductProductDetail, _Mapping]]] = ..., product_highlights: _Optional[_Iterable[str]] = ..., subscription_cost: _Optional[_Union[ProductSubscriptionCost, _Mapping]] = ..., canonical_link: _Optional[str] = ..., external_seller_id: _Optional[str] = ..., pause: _Optional[str] = ..., virtual_model_link: _Optional[str] = ..., cloud_export_additional_properties: _Optional[_Iterable[_Union[CloudExportAdditionalProperties, _Mapping]]] = ..., certifications: _Optional[_Iterable[_Union[ProductCertification, _Mapping]]] = ...) -> None: ...

class ProductCertification(_message.Message):
    __slots__ = ["certification_authority", "certification_code", "certification_name"]
    CERTIFICATION_AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    CERTIFICATION_CODE_FIELD_NUMBER: _ClassVar[int]
    CERTIFICATION_NAME_FIELD_NUMBER: _ClassVar[int]
    certification_authority: str
    certification_code: str
    certification_name: str
    def __init__(self, certification_authority: _Optional[str] = ..., certification_name: _Optional[str] = ..., certification_code: _Optional[str] = ...) -> None: ...

class ProductDimension(_message.Message):
    __slots__ = ["unit", "value"]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    unit: str
    value: float
    def __init__(self, value: _Optional[float] = ..., unit: _Optional[str] = ...) -> None: ...

class ProductProductDetail(_message.Message):
    __slots__ = ["attribute_name", "attribute_value", "section_name"]
    ATTRIBUTE_NAME_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_VALUE_FIELD_NUMBER: _ClassVar[int]
    SECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    attribute_name: str
    attribute_value: str
    section_name: str
    def __init__(self, section_name: _Optional[str] = ..., attribute_name: _Optional[str] = ..., attribute_value: _Optional[str] = ...) -> None: ...

class ProductShipping(_message.Message):
    __slots__ = ["country", "location_group_name", "location_id", "max_handling_time", "max_transit_time", "min_handling_time", "min_transit_time", "postal_code", "price", "region", "service", "service_alternative_list"]
    COUNTRY_FIELD_NUMBER: _ClassVar[int]
    LOCATION_GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    LOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    MAX_HANDLING_TIME_FIELD_NUMBER: _ClassVar[int]
    MAX_TRANSIT_TIME_FIELD_NUMBER: _ClassVar[int]
    MIN_HANDLING_TIME_FIELD_NUMBER: _ClassVar[int]
    MIN_TRANSIT_TIME_FIELD_NUMBER: _ClassVar[int]
    POSTAL_CODE_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    SERVICE_ALTERNATIVE_LIST_FIELD_NUMBER: _ClassVar[int]
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    country: str
    location_group_name: str
    location_id: int
    max_handling_time: int
    max_transit_time: int
    min_handling_time: int
    min_transit_time: int
    postal_code: str
    price: Price
    region: str
    service: str
    service_alternative_list: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, price: _Optional[_Union[Price, _Mapping]] = ..., country: _Optional[str] = ..., region: _Optional[str] = ..., service: _Optional[str] = ..., service_alternative_list: _Optional[_Iterable[str]] = ..., location_id: _Optional[int] = ..., location_group_name: _Optional[str] = ..., postal_code: _Optional[str] = ..., min_handling_time: _Optional[int] = ..., max_handling_time: _Optional[int] = ..., min_transit_time: _Optional[int] = ..., max_transit_time: _Optional[int] = ...) -> None: ...

class ProductShippingDimension(_message.Message):
    __slots__ = ["unit", "value"]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    unit: str
    value: float
    def __init__(self, value: _Optional[float] = ..., unit: _Optional[str] = ...) -> None: ...

class ProductShippingWeight(_message.Message):
    __slots__ = ["unit", "value"]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    unit: str
    value: float
    def __init__(self, value: _Optional[float] = ..., unit: _Optional[str] = ...) -> None: ...

class ProductStatus(_message.Message):
    __slots__ = ["creation_date", "destination_statuses", "google_expiration_date", "item_level_issues", "kind", "last_update_date", "link", "product_id", "title"]
    CREATION_DATE_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_STATUSES_FIELD_NUMBER: _ClassVar[int]
    GOOGLE_EXPIRATION_DATE_FIELD_NUMBER: _ClassVar[int]
    ITEM_LEVEL_ISSUES_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    LAST_UPDATE_DATE_FIELD_NUMBER: _ClassVar[int]
    LINK_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    creation_date: str
    destination_statuses: _containers.RepeatedCompositeFieldContainer[ProductStatusDestinationStatus]
    google_expiration_date: str
    item_level_issues: _containers.RepeatedCompositeFieldContainer[ProductStatusItemLevelIssue]
    kind: str
    last_update_date: str
    link: str
    product_id: str
    title: str
    def __init__(self, product_id: _Optional[str] = ..., title: _Optional[str] = ..., link: _Optional[str] = ..., destination_statuses: _Optional[_Iterable[_Union[ProductStatusDestinationStatus, _Mapping]]] = ..., kind: _Optional[str] = ..., creation_date: _Optional[str] = ..., last_update_date: _Optional[str] = ..., google_expiration_date: _Optional[str] = ..., item_level_issues: _Optional[_Iterable[_Union[ProductStatusItemLevelIssue, _Mapping]]] = ...) -> None: ...

class ProductStatusDestinationStatus(_message.Message):
    __slots__ = ["approved_countries", "destination", "disapproved_countries", "pending_countries", "status"]
    APPROVED_COUNTRIES_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    DISAPPROVED_COUNTRIES_FIELD_NUMBER: _ClassVar[int]
    PENDING_COUNTRIES_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    approved_countries: _containers.RepeatedScalarFieldContainer[str]
    destination: str
    disapproved_countries: _containers.RepeatedScalarFieldContainer[str]
    pending_countries: _containers.RepeatedScalarFieldContainer[str]
    status: str
    def __init__(self, destination: _Optional[str] = ..., status: _Optional[str] = ..., approved_countries: _Optional[_Iterable[str]] = ..., pending_countries: _Optional[_Iterable[str]] = ..., disapproved_countries: _Optional[_Iterable[str]] = ...) -> None: ...

class ProductStatusItemLevelIssue(_message.Message):
    __slots__ = ["applicable_countries", "attribute_name", "code", "description", "destination", "detail", "documentation", "resolution", "servability"]
    APPLICABLE_COUNTRIES_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_NAME_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    DOCUMENTATION_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    SERVABILITY_FIELD_NUMBER: _ClassVar[int]
    applicable_countries: _containers.RepeatedScalarFieldContainer[str]
    attribute_name: str
    code: str
    description: str
    destination: str
    detail: str
    documentation: str
    resolution: str
    servability: str
    def __init__(self, code: _Optional[str] = ..., servability: _Optional[str] = ..., resolution: _Optional[str] = ..., attribute_name: _Optional[str] = ..., destination: _Optional[str] = ..., description: _Optional[str] = ..., detail: _Optional[str] = ..., documentation: _Optional[str] = ..., applicable_countries: _Optional[_Iterable[str]] = ...) -> None: ...

class ProductSubscriptionCost(_message.Message):
    __slots__ = ["amount", "period", "period_length"]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    PERIOD_LENGTH_FIELD_NUMBER: _ClassVar[int]
    amount: Price
    period: str
    period_length: int
    def __init__(self, period: _Optional[str] = ..., period_length: _Optional[int] = ..., amount: _Optional[_Union[Price, _Mapping]] = ...) -> None: ...

class ProductTax(_message.Message):
    __slots__ = ["country", "location_id", "postal_code", "rate", "region", "tax_ship"]
    COUNTRY_FIELD_NUMBER: _ClassVar[int]
    LOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    POSTAL_CODE_FIELD_NUMBER: _ClassVar[int]
    RATE_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    TAX_SHIP_FIELD_NUMBER: _ClassVar[int]
    country: str
    location_id: int
    postal_code: str
    rate: float
    region: str
    tax_ship: bool
    def __init__(self, rate: _Optional[float] = ..., country: _Optional[str] = ..., region: _Optional[str] = ..., tax_ship: bool = ..., location_id: _Optional[int] = ..., postal_code: _Optional[str] = ...) -> None: ...

class ProductUnitPricingBaseMeasure(_message.Message):
    __slots__ = ["unit", "value"]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    unit: str
    value: int
    def __init__(self, value: _Optional[int] = ..., unit: _Optional[str] = ...) -> None: ...

class ProductUnitPricingMeasure(_message.Message):
    __slots__ = ["unit", "value"]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    unit: str
    value: float
    def __init__(self, value: _Optional[float] = ..., unit: _Optional[str] = ...) -> None: ...

class ProductWeight(_message.Message):
    __slots__ = ["unit", "value"]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    unit: str
    value: float
    def __init__(self, value: _Optional[float] = ..., unit: _Optional[str] = ...) -> None: ...

class WideProduct(_message.Message):
    __slots__ = ["account_id", "approved_countries", "disapproved_countries", "has_performance_max_targeting", "has_shopping_targeting", "in_stock", "offer_id", "pending_countries", "performance_max_campaign_ids", "product", "shopping_campaign_ids", "status"]
    ACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    APPROVED_COUNTRIES_FIELD_NUMBER: _ClassVar[int]
    DISAPPROVED_COUNTRIES_FIELD_NUMBER: _ClassVar[int]
    HAS_PERFORMANCE_MAX_TARGETING_FIELD_NUMBER: _ClassVar[int]
    HAS_SHOPPING_TARGETING_FIELD_NUMBER: _ClassVar[int]
    IN_STOCK_FIELD_NUMBER: _ClassVar[int]
    OFFER_ID_FIELD_NUMBER: _ClassVar[int]
    PENDING_COUNTRIES_FIELD_NUMBER: _ClassVar[int]
    PERFORMANCE_MAX_CAMPAIGN_IDS_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_FIELD_NUMBER: _ClassVar[int]
    SHOPPING_CAMPAIGN_IDS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    account_id: str
    approved_countries: _containers.RepeatedScalarFieldContainer[str]
    disapproved_countries: _containers.RepeatedScalarFieldContainer[str]
    has_performance_max_targeting: bool
    has_shopping_targeting: bool
    in_stock: bool
    offer_id: str
    pending_countries: _containers.RepeatedScalarFieldContainer[str]
    performance_max_campaign_ids: _containers.RepeatedScalarFieldContainer[int]
    product: Product
    shopping_campaign_ids: _containers.RepeatedScalarFieldContainer[int]
    status: ProductStatus
    def __init__(self, product: _Optional[_Union[Product, _Mapping]] = ..., status: _Optional[_Union[ProductStatus, _Mapping]] = ..., has_performance_max_targeting: bool = ..., performance_max_campaign_ids: _Optional[_Iterable[int]] = ..., has_shopping_targeting: bool = ..., shopping_campaign_ids: _Optional[_Iterable[int]] = ..., in_stock: bool = ..., approved_countries: _Optional[_Iterable[str]] = ..., pending_countries: _Optional[_Iterable[str]] = ..., disapproved_countries: _Optional[_Iterable[str]] = ..., offer_id: _Optional[str] = ..., account_id: _Optional[str] = ...) -> None: ...
