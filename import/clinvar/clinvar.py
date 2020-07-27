# TODO: finish typing
# type: ignore

"""ClinVar integration script"""
from pathlib import Path

import fire
import fsspec
import pandas as pd
from data_source import catalog
from data_source.core import entry_key_str
from data_source.prefect.tasks import constant
from data_source.utils import get_df_info
from prefect import Flow, task
from prefect.engine.results import LocalResult


# pylint: disable=no-value-for-parameter
@task(
    target="{flow_name}/{task_name}",
    checkpoint=True,
    result=LocalResult(dir="~/.prefect"),
)
def download(url, csv_path):
    of = fsspec.open(url)
    of.fs.download(url, csv_path)
    return csv_path


@task
def convert_to_parquet(input_path, output_path):
    df = pd.read_csv(input_path, skiprows=15, sep="\t")
    info = get_df_info(df)
    nrow = len(df)
    df.to_parquet(output_path)
    return dict(info=info, nrow=nrow)


@task
def upload(entry, parquet_path, url):
    entry.fs.upload(parquet_path, url)
    return True


@task
def add_entry(entry, info, catalog_path):
    entry.artifact.metadata = dict(info=info["info"], nrow=info["nrow"])
    catalog.add_entry(entry, urlpath=catalog_path, overwrite=True)


def get_entry(version, created):
    dt = pd.to_datetime(created).to_pydatetime()
    return catalog.create_entry(
        source="clinvar",
        slug="submission_summary",
        version=version,
        created=dt,
        format="parquet",
        type="file",
    )


def flow(
    output_dir: str = "/tmp/clinvar",
    raw_url: str = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/archive/submission_summary_2020-06.txt.gz",
) -> Flow:
    """Get ClinVar submission summary import flow

    Parameters
    ----------
    output_dir : str
        Directory in which csv/parquet files are stored
    raw_url : str
        Link to ClinVar submission summary CSV (on ftp.ncbi.nlm.nih.gov).
        Note that the version and creation timestamp associated with this
        artifact are inferred from the link since ClinVar has no
        semantic versioning in its releases, and the FTP site provides
        archived files where the date of creation/release is clear.

    Returns
    -------
    Flow
        Prefect Flow
    """
    created = raw_url.split("/")[-1].split("_")[-1].split(".")[0]
    if not created:
        raise ValueError('Unable to determine archive date from url "{raw_url}"')
    version = f"v{created}"

    output_dir = Path(output_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    entry = get_entry(version, created)
    catalog_path = catalog.default_urlpath()
    filename = raw_url.split("/")[-1]

    with Flow(f"clinvar-{version}") as flow:
        # Add constants with important to DAG (all others are not visualized)
        catalog_path = constant(catalog_path, name="catalog_path")
        url = constant(
            entry.resources["parquet"], name="url"
        )  # pylint:disable=unsubscriptable-object
        entry = constant(
            entry, name=f"entry.key={entry_key_str(entry.key)}", value=False
        )

        # Download and convert to parquet
        csv_path = constant(str(output_dir / filename), name="csv_path")
        parquet_path = constant(
            str(output_dir / filename.split(".")[0]) + ".parquet", name="parquet_path"
        )
        csv_path = download(raw_url, csv_path)
        info = convert_to_parquet(csv_path, parquet_path)

        # Upload results
        # pylint:disable=unexpected-keyword-arg
        status = upload(entry, parquet_path, url, upstream_tasks=[info])
        add_entry(entry, info, catalog_path, upstream_tasks=[status])
        return flow


if __name__ == "__main__":
    fire.Fire({"flow": flow})
