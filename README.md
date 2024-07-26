##Use Case to Ingest AMPM PDI Data (Full Load)

**Source Location:** raw/thorntons_fna/thorntonsposdata/datahub_ds=YYYYMMDD/datahub_ts=HHMMSS/YYYYMMDD**AMPMPDI_FULL**.zip

**Schedule:** 15:30 UTC Daily

**HWM_ID:** mobconv#ampm-pdi-ing#thorntons_fna#thorntonsposdata

**Crawler Name:** mobconv-ampm-pdi-ing-crawler

**Target Database:** transform_main_mobconv_pdi

**Target Tables:**
| Dataset Name | Load Type | Partition Columns | Target Location |
|---|---|---|---
| gl_accounts_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/gl_accounts_ampm/ |
| gl_account_majors_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/gl_accounts_majors_ampm/ |
| gl_accounts_types_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/gl_accounts_types_ampm/ |
| item_brands_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/item_brands_ampm/ |
| item_group_levels_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/item_group_levels_ampm/ |
| item_group_membership_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/item_group_membership_ampm/ |
| item_groups_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/item_groups_ampm/ |
| item_manufacturers_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/item_manufacturers_ampm/ |
| item_packages_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/item_packages_ampm/ |
| item_sizes_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/item_sizes_ampm/ |
| item_standard_info_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/item_standard_info_ampm/ |
| item_upcs_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/item_upcs_ampm/ |
| items_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/items_ampm/ |
| profit_centers_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/profit_centers_ampm/ |
| retail_packages_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/retail_packages_ampm/ |
| sites_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/sites_ampm/ |
| vendorcostzonesites_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/vendor_costzonesites_ampm/ |
| paperwork_batches_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/paperwork_batches_ampm/ |
| vendor_costs_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/vendor_costs_ampm/ |
| vendor_items_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/vendor_items_ampm/ |
| vendors_ampm | daily | [ source_system ] | transform/custom/main/mobconv/pdi/dimension_tables_parquet/vendors_ampm/ |

**Misc. Information:** Pipeline disabled in DEV and PRE-PROD.
