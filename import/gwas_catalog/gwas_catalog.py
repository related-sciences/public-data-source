import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import fire
import fsspec
from data_source import catalog, public_catalog

# TODO: we need to type core
from data_source.core import Entry, entry_key_str  # type: ignore
from data_source.prefect.tasks import constant
from prefect import Flow, task
from pyspark.sql import SparkSession


def get_entry(created: datetime) -> Entry:
    dt = created.strftime("%Y-%m-%d")
    return catalog.create_entry(  # type: ignore
        source="gwas_catalog",
        slug="gwas_catalog",
        version=f"v{dt}",
        created=created,
        format="parquet",
        type="directory",
    )


@task  # type: ignore
def download(download_url: str, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    # NOTE: block_size 0 to disallow random access requests which is not
    #       supported by our url
    fsspec.open(download_url, block_size=0).fs.download(download_url, local_path)
    return local_path


def sanitize_column_name(column_name: str) -> str:
    name = re.sub("[^0-9a-z]+", "_", column_name.lower())
    return name.rstrip("_")


@task  # type: ignore
def convert_to_parquet(
    local_csv: Path, output_dir: Path, n_shards: int = 1
) -> Dict[str, Any]:
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(local_csv.as_posix(), sep="\t", header=True)
    # NOTE: header is upper cased, contains non alphanumeric characters etc
    #       so we sanitize it a little
    for old, new in zip(df.columns, [sanitize_column_name(c) for c in df.columns]):
        df = df.withColumnRenamed(old, new)
    df = df.repartition(n_shards)
    df.write.parquet(output_dir.as_posix(), mode="overwrite")
    return dict(schema=df._jdf.schema().treeString(), nrow=df.count())


@task  # type: ignore
def upload(entry: Entry, parquet_path: Path, url: str) -> bool:
    # TODO: why bool?
    if entry.fs.exists(url):
        raise ValueError("Output already exists!")
    entry.fs.upload(parquet_path.as_posix(), url, recursive=True)
    return True


@task  # type: ignore
def add_entry(entry: Entry, info: Dict[str, Any], catalog_path: Path) -> None:
    entry.artifact.metadata = dict(schema=info["schema"], nrow=info["nrow"])
    catalog.add_entry(entry, urlpath=catalog_path, overwrite=True)  # type: ignore


def flow(
    output_dir: Optional[str] = None,
    download_url: str = "https://www.ebi.ac.uk/gwas/api/search/downloads/alternative",
    created: datetime = datetime.utcnow(),
    catalog_path: Path = public_catalog.get_path(),
) -> Flow:
    output_dir_path: Path = Path(output_dir) if output_dir else Path(tempfile.mkdtemp())

    entry = get_entry(created)

    with Flow(f"gwas-catalog-{entry.artifact.version}") as flow:
        catalog_path = constant(catalog_path, name="catalog_path")
        url = constant(entry.resources["parquet"], name="url")
        entry = constant(
            entry, name=f"entry.key={entry_key_str(entry.key)}", value=False
        )

        # Download and convert to parquet
        local_csv = constant(
            output_dir_path.joinpath("gwas_catalog.csv"), name="local_csv"
        )
        parquet_dir = constant(
            output_dir_path.joinpath("gwas_catalog_parquet"), name="parquet_dir"
        )
        local_csv = download(download_url, local_csv)
        info = convert_to_parquet(local_csv, parquet_dir)

        status = upload(entry, parquet_dir, url, upstream_tasks=[info])
        add_entry(entry, info, catalog_path, upstream_tasks=[status])
        return flow


if __name__ == "__main__":
    fire.Fire({"flow": flow})
