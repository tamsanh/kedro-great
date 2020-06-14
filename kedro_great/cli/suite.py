import json
import sys

import click
from great_expectations.cli import toolkit
from great_expectations.cli.suite import _suite_edit, SQLAlchemyError
from great_expectations.cli.util import cli_message

import great_expectations.exceptions as ge_exceptions


@click.group()
def suite():
    """Expectation Suite operations"""
    pass


@suite.command(name="new")
@click.option("--suite", "-es", default=None, help="Expectation suite name.")
@click.option("--empty", "empty", flag_value=True, help="Create an empty suite.")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@click.option(
    "--jupyter/--no-jupyter",
    is_flag=True,
    help="By default launch jupyter notebooks unless you specify the --no-jupyter flag",
    default=True,
)
@click.option(
    "--view/--no-view",
    help="By default open in browser unless you specify the --no-view flag",
    default=True,
)
@click.option(
    "--batch-kwargs",
    default=None,
    help="Additional keyword arguments to be provided to get_batch when loading the data asset. Must be a valid JSON dictionary",
)
def suite_new(suite, directory, empty, jupyter, view, batch_kwargs):
    # TODO update docstring on next major release
    """
    Create a new Expectation Suite.

    Great Expectations will choose a couple of columns and generate expectations about them
    to demonstrate some examples of assertions you can make about your data.

    If you wish to skip the examples, add the `--empty` flag.
    """
    _suite_new(
        suite=suite,
        directory=directory,
        empty=empty,
        jupyter=jupyter,
        view=view,
        batch_kwargs=batch_kwargs,
        usage_event="cli.suite.new",
    )


def _suite_new(
    suite: str,
    directory: str,
    empty: bool,
    jupyter: bool,
    view: bool,
    batch_kwargs,
    usage_event: str,
) -> None:
    # TODO break this up into demo and new
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
