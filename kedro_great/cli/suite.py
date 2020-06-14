import json
import datetime
import os
import sys
from pathlib import Path
from typing import Optional

import click
from great_expectations import DataContext
from great_expectations.cli import toolkit
from great_expectations.cli.suite import _suite_edit, SQLAlchemyError
from great_expectations.cli.util import cli_message

import great_expectations.exceptions as ge_exceptions
from kedro.framework.context import KedroContext, load_context

from data import generate_datasource_name


@click.group()
def suite():
    """Kedro Great Suite Operations"""
    pass


@suite.command(name="generate")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@click.option("--empty", "empty", flag_value=True, help="Create an empty suite.")
@click.option(
    "--batch-kwargs",
    default=None,
    help="Additional keyword arguments to be provided to get_batch when loading the data asset. Must be a valid JSON dictionary",
)
def suite_new(directory, empty, batch_kwargs):
    # TODO update docstring on next major release
    """
    Create a new Expectation Suite.

    Great Expectations will choose a couple of columns and generate expectations about them
    to demonstrate some examples of assertions you can make about your data.

    If you wish to skip the examples, add the `--empty` flag.
    """

    kedro_context = load_context(Path.cwd())
    ge_context = toolkit.load_data_context_with_error_handling(directory)
    generate_basic_suites(kedro_context, ge_context)


def generate_basic_suite_name(dataset_name: str) -> str:
    return f"{dataset_name}.basic"


def generate_basic_suites(kedro_context: KedroContext, ge_context: DataContext):
    from great_expectations.profile import BasicSuiteBuilderProfiler

    catalog = kedro_context.catalog

    existing_datasource_names = {ds["name"] for ds in ge_context.list_datasources()}
    for dataset_name in catalog.list():
        suite_name = generate_basic_suite_name(dataset_name)
        if suite_name in ge_context.list_expectation_suite_names():
            continue

        datasource_name = generate_datasource_name(dataset_name)
        if datasource_name not in existing_datasource_names:
            continue

        dataset = catalog._get_dataset(dataset_name)
        data_path = str(dataset._filepath)
        dataasset_name, _ = os.path.splitext(os.path.basename(data_path))

        batch_kwargs = {
            "datasource": datasource_name,
            "data_asset_name": dataasset_name,
            "path": data_path,
        }

        batch_kwargs_generator_name = "path"
        profiler_configuration = "demo"
        additional_batch_kwargs = {}

        run_id = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%dT%H%M%S.%fZ"
        )

        ge_context.profile_data_asset(
            datasource_name,
            batch_kwargs_generator_name=batch_kwargs_generator_name,
            data_asset_name=dataasset_name,
            batch_kwargs=batch_kwargs,
            profiler=BasicSuiteBuilderProfiler,
            profiler_configuration=profiler_configuration,
            expectation_suite_name=suite_name,
            run_id=run_id,
            additional_batch_kwargs=additional_batch_kwargs,
        )


def generate_basic_suite(
    suite: Optional[str],
    directory: Optional[str],
    empty: bool,
    jupyter: bool,
    view: bool,
    batch_kwargs,
    usage_event: str,
) -> None:
    context = toolkit.load_data_context_with_error_handling(directory)

    datasource_name = None
    generator_name = None
    data_asset_name = None

    try:
        if batch_kwargs is not None:
            batch_kwargs = json.loads(batch_kwargs)

        success, suite_name, profiling_results = toolkit.create_expectation_suite(
            context,
            datasource_name=datasource_name,
            batch_kwargs_generator_name=generator_name,
            data_asset_name=data_asset_name,
            batch_kwargs=batch_kwargs,
            expectation_suite_name=suite,
            additional_batch_kwargs={"limit": 1000},
            empty_suite=empty,
            show_intro_message=False,
            open_docs=view,
        )
        if success:
            if empty:
                if jupyter:
                    cli_message(
                        """<green>Because you requested an empty suite, we'll open a notebook for you now to edit it!
If you wish to avoid this you can add the `--no-jupyter` flag.</green>\n\n"""
                    )
            _suite_edit(
                suite_name,
                datasource_name,
                directory,
                jupyter=jupyter,
                batch_kwargs=batch_kwargs,
                usage_event=usage_event,
            )
    except (
        ge_exceptions.DataContextError,
        ge_exceptions.ProfilerError,
        IOError,
        SQLAlchemyError,
    ) as e:
        cli_message("<red>{}</red>".format(e))
        sys.exit(1)
    except Exception as e:
        raise e
