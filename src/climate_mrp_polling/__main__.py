import rich_click as click
from .create_overlap_information import run_conversion
from .convert_specific_polling import convert_all


@click.group()
def cli():
    pass


def main():
    cli()


@cli.command()
def create_area_intersection():
    run_conversion()


@cli.command()
def convert_polling():
    convert_all()


if __name__ == "__main__":
    main()
