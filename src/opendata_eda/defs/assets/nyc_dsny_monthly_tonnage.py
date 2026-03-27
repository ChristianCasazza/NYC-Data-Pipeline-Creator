import polars as pl

from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
)

dsny_tonnage_schema: SchemaContract = {
    "month": ("month", pl.Utf8, "Year and month of collection."),
    "borough": ("borough", pl.Utf8, "One of the 5 boroughs within NYC."),
    "communitydistrict": ("community_district", pl.Utf8, "One of NYC's 59 community districts (corresponds to Sanitation districts)."),
    "refusetonscollected": ("refuse_tons_collected", pl.Float64, "Tons of trash/refuse collected per month."),
    "papertonscollected": ("paper_tons_collected", pl.Float64, "Tons of recyclable paper collected per month."),
    "mgptonscollected": ("mgp_tons_collected", pl.Float64, "Tons of metal/glass/plastic/beverage cartons collected per month."),
    "resorganicstons": ("res_organics_tons", pl.Float64, "Tons of residential organics collected per month."),
    "schoolorganictons": ("school_organic_tons", pl.Float64, "Tons of school organics collected per month."),
    "leavesorganictons": ("leaves_organic_tons", pl.Float64, "Tons of leaves collected per month."),
    "xmastreetons": ("xmas_tree_tons", pl.Float64, "Tons of Christmas trees collected in January."),
    "otherorganicstons": ("other_organics_tons", pl.Float64, "Tons of other organic material collected per month."),
    "borough_id": ("borough_id", pl.Utf8, "Borough ID (1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island)."),
}

dsny_tonnage_pipeline = create_socrata_pipeline(
    name="nyc_dsny_monthly_tonnage",
    socrata_config=SocrataIngestConfig(
        endpoint="ebb7-mvp5",
        time_col="month",
        base_domain="data.cityofnewyork.us",
    ),
    schema=dsny_tonnage_schema,
    description="DSNY monthly collection tonnage data by community district — refuse, recycling, and organics.",
)

nyc_dsny_monthly_tonnage = dsny_tonnage_pipeline.clean
