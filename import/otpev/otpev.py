# TODO: finish typing
# type: ignore

"""OTP evidence source integration script"""
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

OT_URL_FMT = "https://storage.googleapis.com/open-targets-data-releases/{version}/input/evidence-files"

# Dates correspond to the approximate release date of each OT version; see:
# http://blog.opentargets.org/tag/release-notes/
# https://docs.targetvalidation.org/technical-pipeline/technical-notes
OT_VERSION_RELEASE_DATES = {
    "20.06": "2020-06-16",
    "20.04": "2020-04-27",
    "20.02": "2020-03-02",
    "19.11": "2019-11-28",
}


# pylint: disable=no-value-for-parameter
@task(
    target="{flow_name}/{task_name}",
    checkpoint=True,
    result=LocalResult(dir="~/.prefect"),
)
def download(url, json_path):
    of = fsspec.open(url)
    of.fs.download(url, json_path)
    return json_path


@task
def convert_to_parquet(json_path, parquet_path, n_partitions=None):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.json(json_path)
    if n_partitions is not None:
        df = df.repartition(n_partitions)
    df.write.parquet(parquet_path, mode="overwrite")
    info = dict(schema=df._jdf.schema().treeString(), nrow=df.count())
    # Do this in a local session to avoid JVM shutdown warnings
    # (remove entirely if this ever runs on a cluster)
    spark.stop()
    return info


@task
def upload(entry, parquet_path, url):
    of = fsspec.open(url)
    if of.fs.exists(url):
        of.fs.delete(url, recursive=True)
    entry.fs.upload(parquet_path, url, recursive=True)
    return True


@task
def add_entry(entry, info, catalog_path):
    entry.artifact.metadata = dict(schema=info["schema"], nrow=info["nrow"])
    catalog.add_entry(entry, urlpath=catalog_path, overwrite=True)


def get_entry(source, version, created, format, type, properties):
    dt = pd.to_datetime(created).to_pydatetime()
    return catalog.create_entry(
        source="otpev",
        slug=source,
        version="v" + version,
        created=dt,
        format=format,
        type=type,
        properties=properties,
    )


def flow(
    source: str,
    relpath: str,
    convert: bool = True,
    output_dir: str = "/tmp/otpev",
    version: str = "20.06",
    created: Optional[str] = None,
    n_partitions: Optional[int] = None,
) -> Flow:
    """Get OTP evidence import flow

    Parameters
    ----------
    source : str
        OTP evidence source (e.g. eva, l2g, uniprot)
    relpath : str
        Path relative from `gs://open-targets-data-releases/$VERSION/input/evidence-files` to
        data file or directory (e.g. "progeny-2018-07-23.json.gz" or "evidences_protein_fix/chembl_dataset")
    output_dir : str
        Directory in which temporary json/parquet files are stored
    version : str
        OTP release version
    created: str, optional
        Date at which OTP version was created.  This should NOT
        be a time at which data was collected -- it is intended to
        reflect when OT created the release and should never change
        for the same `version`.  For this reason, `created` will
        default to known release dates (see `OT_VERSION_RELEASE_DATES`).
    n_partitions: int, optional
        Number of partitions used to write parquet result.
        Set as None to use default partitioning.

    Raises
    ------
    KeyError
        If `created` is not provided and no known release date
        was previously recorded for the specified `version`

    Returns
    -------
    Flow
        Prefect Flow
    """
    version = str(version)
    if created is None:
        if version not in OT_VERSION_RELEASE_DATES:
            raise KeyError(
                f'No release date known for version "{version}" '
                "(pass `created` explicitly or add date to `OT_VERSION_RELEASE_DATES`)"
            )
        created = OT_VERSION_RELEASE_DATES[version]

    output_dir = Path(output_dir) / source
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    is_file = relpath.endswith("json.gz")
    src_url = OT_URL_FMT.format(version=version) + f'/{relpath.lstrip("/")}'
    entry = get_entry(
        source,
        version,
        created,
        format="parquet" if is_file else "json.gz",
        type="file" if is_file else "directory",
        properties=None if is_file else dict(compression="gzip"),
    )
    catalog_path = catalog.default_urlpath()

    with Flow(f"otpev-{source}-v{version}") as flow:
        # Add constants with important to DAG (all others are not visualized)
        catalog_path = constant(catalog_path, name="catalog_path")
        dst_url = next(iter(entry.resources.values()))
        entry = constant(
            entry, name=f"entry.key={entry_key_str(entry.key)}", value=False
        )
        n_partitions = constant(n_partitions, name="n_partitions")
        if is_file:
            filename = src_url.split("/")[-1]
            src_url = constant(src_url, name="src_url")
            dst_url = constant(dst_url, name="dst_url")

            # Download and convert to parquet
            json_path = constant(str(output_dir / filename), name="json_path")
            parquet_path = constant(
                str(output_dir / filename.split(".")[0]) + ".parquet",
                name="parquet_path",
            )
            json_path = download(src_url, json_path)
            info = convert_to_parquet(
                json_path, parquet_path, n_partitions=n_partitions
            )

            # Upload results
            # pylint:disable=unexpected-keyword-arg
            status = upload(entry, parquet_path, dst_url, upstream_tasks=[info])
            add_entry(entry, info, catalog_path, upstream_tasks=[status])
        else:
            raise NotImplementedError(
                "Integration of data directories (rather than single files) not yet implemented"
            )

        return flow


if __name__ == "__main__":
    fire.Fire({"flow": flow})
