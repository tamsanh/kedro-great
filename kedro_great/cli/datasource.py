import os
import sys
from pathlib import Path
from typing import List

import click
from great_expectations import DataContext
from great_expectations.cli import toolkit
from great_expectations.cli.datasource import DatasourceTypes
from great_expectations.cli.util import cli_message
from great_expectations.data_context.types.base import DatasourceConfigSchema
from great_expectations import exceptions as ge_exceptions
from kedro.framework.context import KedroContext
from kedro.io import AbstractDataSet

from ..data import identify_dataset_type, generate_datasource_name


def _add_pandas_datasource(
    datasource_name: str, dataset: AbstractDataSet, ge_context: DataContext
) -> str:
    from great_expectations.datasource import PandasDatasource

    path = str(dataset._filepath.parent)

    if path.startswith("./"):
        path = path[2:]

    configuration = PandasDatasource.build_configuration(
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": os.path.join("..", path),
            }
        }
    )

    configuration["class_name"] = "PandasDatasource"
    errors = DatasourceConfigSchema().validate(configuration)
    if len(errors) != 0:
        raise ge_exceptions.GreatExpectationsError(
            "Invalid Datasource configuration: {0:s}".format(errors)
        )

    ge_context.add_datasource(name=datasource_name, **configuration)
    return datasource_name


def _add_spark_datasource(
    datasource_name: str, dataset: AbstractDataSet, ge_context: DataContext
) -> str:
    from great_expectations.datasource import SparkDFDatasource

    path = str(dataset._filepath.parent)

    if path.startswith("./"):
        path = path[2:]

    configuration = SparkDFDatasource.build_configuration(
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": os.path.join("..", path),
            }
        }
    )

    configuration["class_name"] = "SparkDFDatasource"
    errors = DatasourceConfigSchema().validate(configuration)
    if len(errors) != 0:
        raise ge_exceptions.GreatExpectationsError(
            "Invalid Datasource configuration: {0:s}".format(errors)
        )

    ge_context.add_datasource(name=datasource_name, **configuration)
    return datasource_name


def generate_datasources(
    kedro_context: KedroContext, ge_context: DataContext
) -> List[str]:
    catalog = kedro_context.catalog
    new_datasources = []
    existing_datasource_names = {ds["name"] for ds in ge_context.list_datasources()}
    for dataset_name in catalog.list():
        datasource_name = generate_datasource_name(dataset_name)
        if datasource_name in existing_datasource_names:
            continue

        dataset = catalog._get_dataset(dataset_name)
        datasource_type = identify_dataset_type(dataset)

        if datasource_type == DatasourceTypes.PANDAS:
            name = _add_pandas_datasource(datasource_name, dataset, ge_context)
            new_datasources.append(name)
        elif datasource_type == DatasourceTypes.SPARK:
            name = _add_spark_datasource(datasource_name, dataset, ge_context)
            new_datasources.append(name)
    return new_datasources


@click.command(name="datasources")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
def datasource_new(directory):
    """
    Create Great Expectation Datasources based on the kedro catalog.
    Will create one Datasource each dataset in the catalog.
    Only supports Spark and Pandas type datasets.
    """
    from kedro.framework.context import load_context

    ge_context = toolkit.load_data_context_with_error_handling(directory)
    kedro_context = load_context(Path.cwd())
    new_datasources = generate_datasources(kedro_context, ge_context)

    if new_datasources:
        cli_message(
            "Added {} New datasources to your project.".format(len(new_datasources))
        )
    else:  # no datasource was created
        sys.exit(1)
