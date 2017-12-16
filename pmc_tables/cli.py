"""Console script for pmc_tables."""

import click

# from typing import List


class Repo:

    def __init__(self, home, verbose):
        self.home = home
        self.verbose = verbose


@click.group()
@click.option('--pmc-home', envvar='PMC_HOME', default='.pmc')
@click.option('-v', '--verbose', count=True)
@click.pass_context
def main(ctx, pmc_home, verbose):
    """Console script for pmc_tables."""
    click.echo("Replace this message by putting your code into " "pmc_tables.cli.main")
    click.echo("See click documentation at http://click.pocoo.org/")
    ctx.obj = Repo(pmc_home, verbose)


# @click.command()
# @click.argument()
# @click.pass_obj
# def download_single(pmc_id, output_dir, archive_dir=None):
#     pass


# @click.command()
# @click.option('--pmc-list-file')
# def download_multiple(input_file, output_dir, archive_dir=None):
#     pmc_ids = _read_pmc_list_file(input_file)
#     with click.progressbar(pmc_ids, label='Downloading PMC data', length=len(pmc_ids)) as bar:
#         for pmc_id in bar:
#             download_single(pmc_id, output_dir, archive_dir)


# def _read_pmc_list_file(input_file: str) -> List[str]:
#     pmc_ids = []
#     with open(input_file) as fin:
#         for line in fin:
#             line = line.strip()
#             if line:
#                 pmc_ids.append(line)
#     return pmc_ids


if __name__ == "__main__":
    main()
