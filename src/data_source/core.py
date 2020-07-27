# TODO: finish typing
# type: ignore

from __future__ import annotations

import os
import re
import shutil
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import List, Mapping, Optional

from pydantic import (  # pylint:disable=no-name-in-module
    BaseModel,
    BaseSettings,
    validator,
)

from . import ENV_GCS_BUCKET, ENV_GCS_PROJECT, ENV_GCS_ROOT

IS_SLUG_REGEX = r"^[A-Za-z0-9_]+$"
IS_SLUG = re.compile(IS_SLUG_REGEX)
ARTIFACT_FILENAME = "data"
ARTIFACT_DT_FMT = "%Y%m%dT%H%M%S"
DATA_SOURCE_CACHE_DIR = os.getenv("DATA_SOURCE_CACHE_DIR", "/tmp/data_source_cache")


def _check_slug(v):
    if not IS_SLUG.match(v):
        raise ValueError(f"Slug must match {IS_SLUG_REGEX} (value={v})")
    return v


class Format(BaseModel):
    """Format model for data serialization method"""

    name: str  # ['parquet', 'csv', 'json']
    type: str  # ['file', 'directory', 'archive']
    default: bool = True  # Is this the default format to be used on load?
    # Properties contain anything necessary to parameterize deserialize
    # data in the underlying format e.g. csv delimiters, compression
    # options, archive group names, etc.
    properties: Optional[dict] = None


class Source(BaseModel):
    """Source model for originating, external data services"""

    # pylint:disable=no-self-argument
    slug: str
    name: Optional[str] = None
    description: Optional[str] = None

    @validator("slug")
    def slug_alphanumeric(cls, v):
        return _check_slug(v)


class Artifact(BaseModel):
    """Artifact model for data origination"""

    # pylint:disable=no-self-argument
    slug: str
    version: str
    created: datetime
    formats: List[Format]
    name: Optional[str] = None
    metadata: Optional[dict] = None
    filename: str = ARTIFACT_FILENAME

    @validator("version")
    def version_must_be_prefixed(cls, v):
        if not v.startswith("v"):
            raise ValueError(f'Version must start with "v" (value={v})')
        return v

    @validator("slug")
    def slug_alphanumeric(cls, v):
        return _check_slug(v)

    @validator("formats")
    def formats_must_be_unique(cls, v):
        if len(v) < 1:
            raise ValueError("At least one format for artfiact must be specified")
        names = [f.name for f in v]
        if len(names) != len(set(names)):
            raise ValueError(f"Formats must be unique by name (value={v})")
        return v

    @property
    def default_format(self) -> Format:
        for f in self.formats:
            if f.default:
                return f
        return self.formats[0]


class Storage(BaseSettings):
    """Storage model for remote filesystems"""

    # pylint:disable=no-self-argument
    slug: str
    scheme: str
    bucket: str
    root: Optional[str]
    project: Optional[str]

    class Config:
        fields = {
            "project": {"env": ENV_GCS_PROJECT},
            "bucket": {"env": ENV_GCS_BUCKET},
            "root": {"env": ENV_GCS_ROOT},
        }

    @validator("slug")
    def slug_alphanumeric(cls, v):
        return _check_slug(v)

    def url(self, path: str) -> str:
        url = f"{self.scheme}://{self.bucket}"
        if self.root:
            url += "/" + self.root
        return url + "/" + path

    @property
    def fs(self):
        import fsspec

        return fsspec.filesystem(self.scheme)


def _default_storage():
    if ENV_GCS_BUCKET not in os.environ or not os.environ[ENV_GCS_BUCKET]:
        raise ValueError(
            "Default storage parameters not set in environment "
            f"(must set {ENV_GCS_BUCKET}, {ENV_GCS_PROJECT} and {ENV_GCS_ROOT})."
        )
    return Storage(slug="gcs", scheme="gs")


DEFAULT_STORAGE = _default_storage()

EntryKey = namedtuple(
    "EntryKey", ["source", "storage", "artifact", "version", "created"]
)


def entry_key_str(key: EntryKey, date_format=ARTIFACT_DT_FMT):
    key = key._asdict()
    key["created"] = key["created"].strftime(date_format)
    key = "\n".join([f"{k}: {v}" for k, v in key.items()])
    return "{\n" + key + "\n}"


def resource_path(
    source: Source,
    artifact: Artifact,
    storage: Storage,
    filename: Optional[str] = None,
    format: Optional[str] = None,
    relative=False,
):
    """Create URL or relative path for artifact resource

    Examples
    --------
    >>> resource_url(source, artifact, storage, filename='data', format='parquet', relative=False)
    > "gs://public-data-source/catalog/clinvar/submission_summary/v2020-06/20200601T000000/data.parquet"

    >>> resource_url(source, artifact, storage, filename='data', format='parquet', relative=True)
    > "clinvar/submission_summary/v2020-06/20200601T000000/data.parquet"

    """
    format = format or artifact.default_format.name
    basename = (filename or artifact.filename) + "." + format
    timestamp = artifact.created.strftime(ARTIFACT_DT_FMT)
    path = storage.fs.pathsep.join(
        [source.slug, artifact.slug, artifact.version, timestamp, basename]
    )
    if relative:
        return path
    return storage.url(path)


class Entry(BaseModel):
    """Catalog entry model"""

    # pylint:disable=no-self-argument
    source: Source
    artifact: Artifact
    storage: Storage
    resources: Mapping[str, str] = None

    # see: https://github.com/samuelcolvin/pydantic/issues/259
    @validator("resources", pre=True, always=True)
    def default_resources(cls, v, *, values, **kwargs):
        if v is None:
            v = {
                f.name: resource_path(
                    values["source"],
                    values["artifact"],
                    values["storage"],
                    format=f.name,
                )
                for f in values["artifact"].formats
            }
        for value in v.values():
            if not value:
                raise ValueError(f"Resource URLs cannot be empty (value={v})")
        return v

    @property
    def key(self) -> EntryKey:
        return EntryKey(
            source=self.source.slug,
            storage=self.storage.slug,
            artifact=self.artifact.slug,
            version=self.artifact.version,
            created=self.artifact.created,
        )

    @property
    def fs(self):
        return self.storage.fs

    @property
    def url(self):
        # pylint: disable=unsubscriptable-object
        return self.resources[self.artifact.default_format.name]

    @property
    def path(self):
        return resource_path(self.source, self.artifact, self.storage, relative=True)

    def download(
        self, local_dir: str, format: Optional[str] = None, overwrite: bool = True
    ) -> str:
        """Download data for catalog entry to a local directory

        Parameters
        ----------
        local_dir : str
            Path to local directory
        format : Optional[str], optional
            Type of resource to download e.g. "parquet", "csv", "json.gz" etc.
            If not provided, the default format for the artifact will be used.
        overwrite: bool
            If the local path exists and `overwrite` is True, it will be deleted
            and new data downloaded to it.  Otherwise, the path will
            be returned immediately.  If the local path does not exist,
            `overwrite` has no effect.

        Returns
        -------
        str
            Local path containing entry data.
        """
        format = format or self.artifact.default_format.name
        # pylint: disable=unsubscriptable-object
        url = self.resources[format]
        path = Path(local_dir) / self.path
        if overwrite and path.exists():
            shutil.rmtree(path)
        if path.exists():
            return str(path)
        if not path.parent.exists():
            path.parent.mkdir(parents=True)
        self.fs.download(url, path, recursive=self.fs.isdir(url))
        return str(path)


class Catalog(BaseModel):
    entries: List[Entry]

    def to_sorted(self) -> Catalog:
        """Sort entries by key"""
        return Catalog(entries=sorted(self.entries, key=lambda e: e.key))

    def to_dict(self) -> Mapping[EntryKey, Entry]:
        """Map entries by key"""
        return dict(zip([e.key for e in self.entries], self.entries))

    def merge(self, other: Catalog) -> Catalog:
        return Catalog(entries=list(self.entries) + list(other.entries))

    def add(self, entry: Entry):
        """Add new entry

        Raises
        ------
        KeyError
            If the entry is already present, noting that the properties defining 
            hashes for entries are present in `entry.key`
        """
        entries = self.to_dict()
        if entry.key in entries:
            raise KeyError(f"Entry is already in catalog (key = {entry.key})")
        self.entries.append(entry)

    def __contains__(self, entry: Entry) -> bool:
        return self.exists(entry)

    def exists(self, entry: Entry) -> bool:
        """Check if an entry is already present"""
        return entry.key in self.to_dict()

    def remove(self, entry: Entry) -> bool:
        """Remove an entry

        Returns
        -------
        bool
            True if entry was found and removed, false otherwise
        """
        key = entry.key
        n = len(self.entries)
        self.entries = [e for e in self.entries if e.key != key]
        return n > len(self.entries)

    def to_pandas(self, dropna=True):
        """Convert to un-nested pandas dataframe

        Note that individual entry objects are preserved in the `entry` column
        """
        import pandas as pd

        df = pd.json_normalize([e.dict() for e in self.entries], sep="_")
        df = df.assign(entry=self.entries)
        if dropna:
            df = df.dropna(how="all", axis=1)
        return df

    def find(
        self, source: str, artifact: str, version: Optional[str] = "latest"
    ) -> List[Entry]:
        """Find entries for a specific source artifact

        Parameters
        ----------
        source : str
            Source slug (e.g. "clinvar")
        artifact : str
            Artifact slug (e.g. "submission_summary")
        version : str
            Version string, by default 'latest' for most
            recent version by lexsort. If None, no
            version filter is applied.

        Returns
        -------
        List[Entry]
            Matching entries
        """
        df = self.to_pandas().set_index(["source_slug", "artifact_slug"])
        df = df.loc[[(source, artifact)]]
        df = df.sort_values(["artifact_version", "artifact_created"], ascending=False)
        if version:
            if version == "latest" and len(df) > 0:
                version = df["artifact_version"].max()
            df = df[df["artifact_version"] == version]
        return df["entry"].tolist()

    def download(
        self,
        source: str,
        artifact: str,
        version: str = "latest",
        local_dir: str = DATA_SOURCE_CACHE_DIR,
        overwrite: bool = False,
    ) -> str:
        """Download data for a specific source artifact

        Parameters
        ----------
        source : str
            Source slug (e.g. "clinvar")
        artifact : str
            Artifact slug (e.g. "submission_summary")
        version : str
            Version string, by default 'latest' for most
            recent version by lexsort
        local_dir : str
            Path to local directory in which downloaded data
            will be stored, by default environment variable 
            `DATA_SOURCE_CACHE_DIR`
        overwrite : bool
            If the artifact data has already been downloaded,
            the path will be returned as is if `overwrite` is
            False.  Otherwise it will be deleted and 
            re-downloaded

        Returns
        -------
        str
            Path to local file or directory

        Raises
        ------
        ValueError
            If no entries could be found for the source, artifact, 
            and version
        """
        entries = self.find(source, artifact, version)
        if len(entries) == 0:
            raise ValueError(
                f"No catalog entries found for source={source}, "
                f"artifact={artifact}, version={version}"
            )
        # Use max version, max created if multiple entries found
        return entries[0].download(local_dir, overwrite=overwrite)
