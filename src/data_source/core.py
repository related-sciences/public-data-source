from __future__ import annotations
from pydantic import BaseModel, BaseSettings, Field, validator  # pylint:disable=no-name-in-module
from enum import Enum
from datetime import datetime
from typing import Optional, List, Mapping, Hashable, Any
from collections import namedtuple
import os
import re

IS_SLUG_REGEX = r"^[A-Za-z0-9_]+$"
IS_SLUG = re.compile(IS_SLUG_REGEX)
ARTIFACT_FILENAME = 'data'
ARTIFACT_DT_FMT = '%Y%m%dT%H%M%S'

def _check_slug(v):
    if not IS_SLUG.match(v):
        raise ValueError(f'Slug must match {IS_SLUG_REGEX} (value={v})')
    return v


class Format(BaseModel):
    """Format model for data serialization method"""
    name: str # ['parquet', 'csv', 'json']
    type: str # ['file', 'directory', 'archive'] 
    default: bool = True # Is this the default format to be used on load?
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

    @validator('slug')
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

    @validator('version') 
    def version_must_be_prefixed(cls, v):
        if not v.startswith('v'):
            raise ValueError(f'Version must start with "v" (value={v})')
        return v

    @validator('slug')
    def slug_alphanumeric(cls, v):
        return _check_slug(v)

    @validator('formats')
    def formats_must_be_unique(cls, v):
        if len(v) < 1:
            raise ValueError('At least one format for artfiact must be specified')
        names = [f.name for f in v]
        if len(names) != len(set(names)):
            raise ValueError(f'Formats must be unique by name (value={v})')
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
            'project': {
                'env': 'GCS_PROJECT'
            },
            'bucket': {
                'env': 'GCS_BUCKET'
            },
            'root': {
                'env': 'GCS_ROOT'
            }
        }

    @validator('slug')
    def slug_alphanumeric(cls, v):
        return _check_slug(v)

    def url(self, path: str) -> str:
        url = f'{self.scheme}://{self.bucket}'
        if self.root:
            url += '/' + self.root
        return url + '/' + path


DEFAULT_STORAGE = Storage(slug='gcs', scheme='gs')

EntryKey = namedtuple('EntryKey', ['source', 'storage', 'artifact', 'version', 'created'])

def entry_key_str(key: EntryKey, date_format=ARTIFACT_DT_FMT):
    key = key._asdict()
    key['created'] = key['created'].strftime(date_format)
    key = '\n'.join([f'{k}: {v}' for k, v in key.items()])
    return "{\n" + key + "\n}"

def resource_url(
    source: Source, 
    artifact: Artifact, 
    storage: Storage, 
    filename: str=None, 
    format: str=None
):
    """Create URL for artifact resource

    Examples
    --------
    >>> resource_url(source, artifact, storage, filename='data', format='parquet')
    > "gs://public-data-source/catalog/clinvar/submission_summary/v2020-06/20200601T000000/data.parquet"

    """
    basename = (filename or artifact.filename) + '.' + \
        (format or artifact.default_format.name)
    timestamp = artifact.created.strftime(ARTIFACT_DT_FMT)
    path = '/'.join([
        source.slug,
        artifact.slug,
        artifact.version,
        timestamp,
        basename
    ])
    return storage.url(path)

class Entry(BaseModel):
    """Catalog entry model"""
    # pylint:disable=no-self-argument
    source: Source
    artifact: Artifact 
    storage: Storage   
    resources: Mapping[str, str] = None

    # see: https://github.com/samuelcolvin/pydantic/issues/259
    @validator('resources', pre=True, always=True)
    def default_resources(cls, v, *, values, **kwargs):
        if v is None:
            v = {
                f.name: resource_url(
                    values['source'], 
                    values['artifact'], 
                    values['storage'],
                    format=f.name
                )
                for f in values['artifact'].formats
            }
        for value in v.values():
            if not value:
                raise ValueError(f'Resource URLs cannot be empty (value={v})')
        return v

    @property
    def key(self) -> EntryKey:
        return EntryKey(
            source=self.source.slug,
            storage=self.storage.slug,
            artifact=self.artifact.slug,
            version=self.artifact.version,
            created=self.artifact.created
        )

    @property
    def fs(self):
        import fsspec
        return fsspec.filesystem(self.storage.scheme)


class Catalog(BaseModel):
    entries: List[Entry]

    def to_sorted(self) -> Catalog:
        """Sort entries by key"""
        return Catalog(entries=sorted(self.entries, key=lambda e: e.key))

    def to_dict(self) -> Mapping[EntryKey, Entry]:
        """Map entries by key"""
        return dict(zip([e.key for e in self.entries], self.entries))

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
            raise KeyError(f'Entry is already in catalog (key = {entry.key})')
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
        df = pd.json_normalize([e.dict() for e in self.entries], sep='_')
        df = df.assign(entry=self.entries)
        if dropna:
            df = df.dropna(how='all', axis=1)
        return df
