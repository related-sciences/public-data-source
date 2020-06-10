from datetime import datetime, date
from typing import Optional, Union
from pathlib import Path
import yaml
import fsspec
from . import REPO_DIR
from .core import Entry, Catalog, Storage, DEFAULT_STORAGE

DEFAULT_CATALOG_PATH = REPO_DIR / 'catalog.yaml'
PathType = Union[str, Path]

def emtpy() -> Catalog:
    """Create empty `Catalog` instance"""
    return Catalog(entries=[])

def load(urlpath: PathType = DEFAULT_CATALOG_PATH) -> Catalog:
    """Load catalog from url or path
    
    Parameters
    ----------
    urlpath : Union[str, Path]
        URL (http, s3, gs, etc.) or local path to catalog yaml file
    """
    of = fsspec.open(str(urlpath), mode='r')
    with of.open() as f:
        obj = yaml.load(f, Loader=yaml.FullLoader)
        return Catalog(**obj)

def add_entry(entry: Entry, urlpath: PathType = DEFAULT_CATALOG_PATH, overwrite=False):
    cat = load(urlpath=urlpath)
    if overwrite and entry in cat:
        cat.remove(entry)
    cat.add(entry)
    save(cat, urlpath)


def save(catalog: Catalog, urlpath: PathType = DEFAULT_CATALOG_PATH):
    """Save catalog to url or path

    Parameters
    ----------
    catalog : Catalog
        Catalog to save
    urlpath : PathType, optional
        Path/URL for result, by default `catalog.DEFAULT_CATALOG_PATH`
    """
    of = fsspec.open(str(urlpath), mode='w')
    with of.open() as f:
        yaml.dump(catalog.dict(), f)


def create_entry(
    source: Union[str, dict], 
    slug: str, 
    version: str, 
    format: str, 
    type: str, 
    name: Optional[str] = None,
    created: Optional[datetime] = None, 
    storage: Storage=DEFAULT_STORAGE
):
    """Create new catalog entry

    This is a convenience over creating `core.*` objects directly. 

    Parameters
    ----------
    source: Union[str, dict]
        Slug for data source (e.g. 'clinvar', 'otp', 'gnomad') or dict for 
        `Source` instance (e.g. dict(name='gnomAD v3', slug='gnomad_v3'))
    slug: str
        Slug for data artifact
    version: str
        Version string (must start with 'v')
    format: str
        Name of artifact serialization format (e.g. 'parquet', 'csv')
    type: str
        Type of artifact serialization (e.g. 'file', 'directory', 'archive')
    name: Optional[str]
        Name of artifact (e.g. 'ClinVar Association Submission Summary' as 
        compared to slug 'submission_summary')
    created: Optional[datetime]
        Time at which the data artifact was created.  There are three ways in
        which this is most likely to be set:

        1. For sources with semantic versions and no time based releases, 
        this should be a static timestamp approximately equal to the time 
        at which the corresponding semantic release, indicated by `version`, 
        was published.
        2. For sources with no semantic versioning and time-based releases, 
        this should correspond to time-based release from the source (e.g. 
        ClinVar has monthly releases so this timestamp should be truncated 
        to a month).
        3. For sources that continually update (typically those from APIs),
        this should be equal to the time at which data was collected.  In
        other words, the timestamp is not specific to the source but to
        the collection process.

        If not provided, defaults to timestamp for midnight of current day.
    storage: Storage
        Destination for artifact data

    Examples
    --------
    >> create_entry(
        source='clinvar', 
        slug='submission_summary',
        version='v2020-06',
        created=datetime.now(),
        format='parquet',
        type='file'
    )
    Entry(
        source=Source(slug='clinvar', name=None, description=None), 
        artifact=Artifact(
            slug='submission_summary', version='v2020-06', 
            created=datetime.datetime(2020, 6, 10, 12, 12, 13, 859561), 
            formats=[Format(name=<FormatName.parquet: 'parquet'>, type=<FormatType.file: 'file'>, properties=None)], 
            name=None, metadata=None), 
        storage=Storage(slug='gcs', scheme='gs')
    )
    """
    if created is None:
        created = datetime.combine(date.today(), datetime.min.time())
    return Entry(**dict(
        source=dict(slug=source) if isinstance(source, str) else source,
        artifact=dict(
            slug=slug,
            version=version,
            created=created,
            formats=[dict(name=format, type=type)]
        ),
        storage=storage
    ))
    