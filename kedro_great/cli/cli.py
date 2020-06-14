import os
import sys
from pathlib import Path

import click
from great_expectations import DataContext
from great_expectations.cli.cli_messages import (
    GREETING,
    PROJECT_IS_COMPLETE,
    ONBOARDING_COMPLETE,
    LETS_BEGIN_PROMPT,
    RUN_INIT_AGAIN,
    BUILD_DOCS_PROMPT,
    SETUP_SUCCESS,
)
from great_expectations.cli.util import cli_message
from great_expectations.exceptions import (
    DataContextError,
    DatasourceInitializationError,
)

from .init import init


@click.group(name="great")
def cli():
    raise NotImplementedError


cli.add_command(init)


def main():
    cli()


if __name__ == "__main__":
    main()
