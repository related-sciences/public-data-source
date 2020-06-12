# Public Data Source Resources

The purpose of this repository is to serve as the first step in a data processing workflow.  The source code within contains utilities for working with external data providers as well as a catalog model for managing the creation of versioned data sources.

The typical flow for adding a new data source is:

1. Add a folder to [`import`](import) for the new source
2. Create a script or notebook that does the following:
  - Download and repackage artifacts from scientific dbs into more portable, efficient formats
  - Create a catalog entry that represents the artifact, e.g.
      ```python
        from data_source import catalog
        entry = catalog.create_entry(
            source='clinvar', 
            slug='submission_summary',
            version='v2020-06',
            created=pd.to_datetime('2020-06-01').to_pydatetime(),
            format='parquet',
            type='file'
        )
        # This will add the entry to the default `catalog.yaml` file
        catalog.add_entry(entry)
      ```
  - Upload optimized files to a remote file store, e.g.
      ```python
        url = entry.url()
        # url = gs://public-data-source/catalog/clinvar/submission_summary/v2020-06/20200601T000000/data.parquet
        entry.fs.upload('/tmp/submission_summary.parquet', url)
      ```
3. In another project that requires access to versioned datasets, use the catalog code to get urls for an artifact or explore those available, e.g.

```python
from data_source import catalog
from pyspark.sql.session import SparkSession

catalog_url = 'https://raw.githubusercontent.com/related-sciences/public-data-source/master/catalog.yaml'
df = catalog.load(catalog_url).to_pandas()
df.query('source_slug == "clinvar"')
```

|    | source_slug   | artifact_slug      | artifact_version   | artifact_created    | artifact_formats                                                           | storage_slug   | storage_scheme   | storage_bucket     | storage_root   | storage_project   |  
|---|--------------|-------------------|-------------------|--------------------|---------------------------------------------------------------------------|---------------|-----------------|-------------------|---------------|------------------|
|  0 | clinvar       | submission_summary | v2020-06           | 2020-06-01 00:00:00 | [{'name': 'parquet', 'type': 'file', 'default': True, 'properties': None}] | gcs            | gs               | public-data-source | catalog        | target-ranking    |
    
Choose an entry and get urls based on files available to load:

```python
import fsspec
spark = SparkSession.builder.getOrCreate()
url = df.query('source_slug == "clinvar"')['entry'].iloc[0].url()
df = spark.read.parquet(fsspec.open_local(f'simplecache::{url}'))
df.select('#VariationID', 'ClinicalSignificance', 'SubmittedPhenotypeInfo').show(5, 50)
+------------+----------------------+--------------------------------------------------+
|#VariationID|  ClinicalSignificance|                            SubmittedPhenotypeInfo|
+------------+----------------------+--------------------------------------------------+
|           2|            Pathogenic|        SPASTIC PARAPLEGIA 48, AUTOSOMAL RECESSIVE|
|           3|            Pathogenic|                             SPASTIC PARAPLEGIA 48|
|           4|Uncertain significance|    RECLASSIFIED - VARIANT OF UNKNOWN SIGNIFICANCE|
|           5|            Pathogenic|MITOCHONDRIAL COMPLEX I DEFICIENCY, NUCLEAR TYP...|
|           5|            Pathogenic|                                      Not Provided|
+------------+----------------------+--------------------------------------------------+
only showing top 5 rows
```
