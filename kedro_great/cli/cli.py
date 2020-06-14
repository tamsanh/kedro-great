import click


@click.group(name="Kedro-Great")
def commands():
    """Kedro Great Command collections"""


@commands.group()
def great():
    """Run Kedro Great Commands"""


from .init import init
from .suite import suite
from .datasource import datasource

great.add_command(init)
great.add_command(suite)
great.add_command(datasource)


def main():
    commands()


if __name__ == "__main__":
    main()
