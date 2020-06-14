import os
import sys

import click
from great_expectations import DataContext
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
    cli_message(GREETING)

    if DataContext.does_config_exist_on_disk(ge_dir):
        try:
            if DataContext.is_project_initialized(ge_dir):
                # Ensure the context can be instantiated
                cli_message(PROJECT_IS_COMPLETE)
        except (DataContextError, DatasourceInitializationError) as e:
            cli_message("<red>{}</red>".format(e.message))
            sys.exit(1)

        try:
            DataContext.create(
                target_directory, usage_statistics_enabled=usage_stats
            )
            cli_message(ONBOARDING_COMPLETE)
            # TODO if this is correct, ensure this is covered by a test
            cli_message(SETUP_SUCCESS)
            from great_expectations.cli.datasource import datasource_new
            datasource_new(target_directory)
            exit(0)
        except DataContextError as e:
            cli_message("<red>{}</red>".format(e.message))
            # TODO ensure this is covered by a test
            exit(5)

    else:
        if not click.confirm(LETS_BEGIN_PROMPT, default=True):
            cli_message(RUN_INIT_AGAIN)
            # TODO ensure this is covered by a test
            exit(0)
