from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource

from .constants import DBT_DIRECTORY


dbt_manifest_path = DBT_DIRECTORY.joinpath("target", "manifest.json")


@dbt_assets(
    manifest=dbt_manifest_path,
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
