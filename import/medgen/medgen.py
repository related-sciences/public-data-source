# TODO: finish typing
# type: ignore

"""MedGen integration script"""
from datetime import datetime
from pathlib import Path
from typing import Optional

import fire
import fsspec
import pandas as pd
from data_source import catalog
from data_source.core import entry_key_str
from data_source.prefect.tasks import constant
from prefect import Flow, task
from prefect.engine.results import LocalResult
from pyspark.sql.session import SparkSession


# pylint: disable=no-value-for-parameter
@task(
    target="{flow_name}/{task_name}",
    checkpoint=True,
    result=LocalResult(dir="~/.prefect"),
)
def download(ftp_dir, csv_dir, n_mgrel_files):
    csv_dir = Path(csv_dir)
    if not csv_dir.exists():
        csv_dir.mkdir(parents=True, exist_ok=True)

    files = []
    for i in range(n_mgrel_files):
        filename = f"MGREL_{i + 1}.csv.gz"
        path = str(csv_dir / filename)
        url = ftp_dir + "/" + filename
        of = fsspec.open(url)
        of.fs.download(url, path)
        files.append(path)
    return csv_dir


@task
def convert_to_parquet(input_dir, output_dir, n_partitions=8):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(str(input_dir), sep=",")
    df = df.repartition(8)
    df.write.parquet(output_dir, mode="overwrite")
    return dict(schema=df._jdf.schema().treeString(), nrow=df.count())


@task
def upload(entry, parquet_dir, url):
    of = fsspec.open(url)
    if of.fs.exists(url):
        of.fs.delete(url, recursive=True)
    entry.fs.upload(parquet_dir, url, recursive=True)
    return True


@task
def add_entry(entry, info, catalog_path):
    entry.artifact.metadata = dict(schema=info["schema"], nrow=info["nrow"])
    catalog.add_entry(entry, urlpath=catalog_path, overwrite=True)


def get_entry(created):
    dt = pd.to_datetime(created).strftime("%Y-%m")
    version = f"v{dt}"
    dt = datetime.strptime(dt, "%Y-%m")
    return catalog.create_entry(
        source="medgen",
        slug="mgrel",
        version=version,
        created=dt,
        format="parquet",
        type="directory",
    )


def get_n_mgrel_files(csv_url):
    n = 0
    while True:
        url = csv_url + f"/MGREL_{n+1}.csv.gz"
        of = fsspec.open(url)
        if not of.fs.exists(url):
            break
        n += 1
    return n


def flow(
    output_dir: str = "/tmp/medgen",
    csv_url: str = "https://ftp.ncbi.nlm.nih.gov/pub/medgen/csv",
    n_mgrel_files: Optional[int] = None,
    created: str = "today",
) -> Flow:
    """Get MedGen import flow

    Parameters
    ----------
    output_dir : str
        Directory in which csv/parquet files are stored
    csv_url : str
        Link to MedGen CSV exports (e.g.
        https://ftp.ncbi.nlm.nih.gov/pub/medgen/csv).
        Note that MedGen appears to have no archival or
        release process so both versions and created
        timestamps in artifacts will correspond to a
        year-month (e.g. 2020-06).
    n_mgrel_files : Optional[int]
        Number of MGREL files to download.  These contain
        pairwise concept relationships and are often broken
        up into chunks to have < 1M rows for spreadsheet
        users.  At TOW, 2 chunks are present so this can
        be provided explicitly or if left as None, the
        number of files will be inferred by trying increments
        until one fails to exist.
    created: str
        Year-month associated with artifact.  Defaults
        to current year-month.

    Returns
    -------
    Flow
        Prefect Flow
    """
    output_path = Path(output_dir)
    if n_mgrel_files is None:
        n_mgrel_files = get_n_mgrel_files(csv_url)
        if n_mgrel_files <= 0:
            raise ValueError(f"Failed to find any MGREL files at {csv_url}")

    entry = get_entry(created)
    catalog_path = catalog.default_urlpath()

    with Flow(f"medgen-{entry.artifact.version}") as flow:
        catalog_path = constant(catalog_path, name="catalog_path")
        # pylint:disable=unsubscriptable-object
        url = constant(entry.resources["parquet"], name="url")
        entry = constant(
            entry, name=f"entry.key={entry_key_str(entry.key)}", value=False
        )

        # Download and convert to parquet
        csv_dir = constant(str(output_path / "mgrel.csv"), name="csv_dir")
        parquet_dir = constant(str(output_path / "mgrel.parquet"), name="parquet_dir")
        csv_dir = download(csv_url, csv_dir, n_mgrel_files)
        info = convert_to_parquet(csv_dir, parquet_dir)

        # Upload results
        # pylint:disable=unexpected-keyword-arg
        status = upload(entry, parquet_dir, url, upstream_tasks=[info])
        add_entry(entry, info, catalog_path, upstream_tasks=[status])
        return flow


if __name__ == "__main__":
    fire.Fire({"flow": flow})
