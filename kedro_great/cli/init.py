import os
import sys
from pathlib import Path

import click
from great_expectations import DataContext
from great_expectations.cli import toolkit
from great_expectations.cli.cli_messages import (
    GREETING,
    PROJECT_IS_COMPLETE,
    ONBOARDING_COMPLETE,
    LETS_BEGIN_PROMPT,
    RUN_INIT_AGAIN,
    SETUP_SUCCESS,
)
from great_expectations.cli.init import _get_full_path_to_ge_dir
from great_expectations.cli.util import cli_message
from great_expectations.exceptions import (
    DataContextError,
    DatasourceInitializationError,
)
from kedro.framework.context import load_context

from cli.datasource import generate_datasources


@click.command()
@click.option(
    "--target-directory",
    "-d",
    default="./",
    help="The root of the project directory where you want to initialize Great Expectations.",
)
@click.option(
    "--usage-stats/--no-usage-stats",
    help="By default, usage statistics are enabled unless you specify the --no-usage-stats flag.",
    default=True,
)
def init(target_directory, usage_stats):
    """Generate a new Great Expectations project configuration"""
    target_directory = os.path.abspath(target_directory)
    ge_dir = _get_full_path_to_ge_dir(target_directory)

    if not DataContext.does_config_exist_on_disk(ge_dir):
        if not click.confirm(LETS_BEGIN_PROMPT, default=True):
            cli_message(RUN_INIT_AGAIN)
            # TODO ensure this is covered by a test
            exit(0)
        try:
            DataContext.create(
                target_directory, usage_statistics_enabled=usage_stats
            )
            cli_message(SETUP_SUCCESS)
        except DataContextError as e:
            cli_message("<red>{}</red>".format(e.message))
            exit(5)

    if not click.confirm("Generate Datasources based on Kedro Context?", default=True):
        exit(0)
    kedro_context = load_context(Path.cwd())
    ge_context = toolkit.load_data_context_with_error_handling(ge_dir)
    new_datasources = generate_datasources(kedro_context, ge_context)
    if new_datasources:
        cli_message(
            "Added {} New datasources to your project.".format(len(new_datasources))
        )

    if not click.confirm("Generate Basic Validation Suites based on Kedro Context?", default=True):
        pass
