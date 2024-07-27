from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
from ..resources import dbt_resource
import os

from .constants import DBT_DIRECTORY


if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt_resource.cli(["--quiet", "parse"]).wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = os.path.join(DBT_DIRECTORY, "target", "manifest.json")

@dbt_assets(
    manifest=dbt_manifest_path,
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
