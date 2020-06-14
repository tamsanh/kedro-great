import datetime
import os
from pathlib import Path

import click
from great_expectations import DataContext
from great_expectations.cli import toolkit
from great_expectations.cli.toolkit import create_empty_suite
from kedro.framework.context import KedroContext, load_context

from ..data import generate_datasource_name


@click.command(name="suites")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@click.option("--empty", "empty", flag_value=True, help="Creates empty suites.")
@click.option(
    "--replace", "replace", flag_value=True, help="Replace any existing suites."
)
@click.option(
    "--batch-kwargs",
    default=None,
    help="Additional keyword arguments to be provided to get_batch when loading the data asset. Must be a valid JSON dictionary",
)
def suite_new(directory, empty, replace, batch_kwargs):
    """
    Create Great Expectation Suites based on the kedro catalog using the BasicSuiteBuilderProfiler.

    If you wish to create suites without using the BasicSuiteBuilderProfiler, add the `--empty` flag.
    """

    kedro_context = load_context(Path.cwd())
    ge_context = toolkit.load_data_context_with_error_handling(directory)
    generate_basic_suites(kedro_context, ge_context, empty, replace, batch_kwargs)


def generate_basic_suite_name(dataset_name: str) -> str:
    return f"{dataset_name}.basic"


def generate_basic_suites(
    kedro_context: KedroContext,
    ge_context: DataContext,
    empty=False,
    replace=False,
    batch_kwargs=None,
):
    from great_expectations.profile import BasicSuiteBuilderProfiler

    if batch_kwargs is None:
        batch_kwargs = {}
    catalog = kedro_context.catalog

    existing_datasource_names = {ds["name"] for ds in ge_context.list_datasources()}
    for dataset_name in catalog.list():
        suite_name = generate_basic_suite_name(dataset_name)
        if suite_name in ge_context.list_expectation_suite_names() and not replace:
            continue

        datasource_name = generate_datasource_name(dataset_name)
        if datasource_name not in existing_datasource_names:
            continue

        dataset = catalog._get_dataset(dataset_name)
        data_path = str(dataset._filepath)
        dataasset_name, _ = os.path.splitext(os.path.basename(data_path))

        suite_batch_kwargs = {
            "datasource": datasource_name,
            "data_asset_name": dataasset_name,
            "path": data_path,
            "reader_options": dataset._load_args,
        }

        batch_kwargs_generator_name = "path"
        profiler_configuration = "demo"
        additional_batch_kwargs = batch_kwargs

        run_id = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%dT%H%M%S.%fZ"
        )

        if empty:
            create_empty_suite(ge_context, suite_name, suite_batch_kwargs)
        else:
            ge_context.profile_data_asset(
                datasource_name,
                batch_kwargs_generator_name=batch_kwargs_generator_name,
                data_asset_name=dataasset_name,
                batch_kwargs=suite_batch_kwargs,
                profiler=BasicSuiteBuilderProfiler,
                profiler_configuration=profiler_configuration,
                expectation_suite_name=suite_name,
                run_id=run_id,
                additional_batch_kwargs=additional_batch_kwargs,
            )
