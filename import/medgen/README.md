## MedGen

MedGen is the primary phenotype ontology used by ClinVar.  It maintains inheritance patterns, but this actually comes from HPO + OrphaNet (see https://www.ncbi.nlm.nih.gov/medgen/docs/help/#moi "Mode of Inheritance" section).  Note that HPO in turn integrates inheritance information from OMIM.  


### FTP Archive

https://ftp.ncbi.nlm.nih.gov/pub/medgen/

See https://ftp.ncbi.nlm.nih.gov/pub/medgen/README.txt for descriptions of csv tables.

#### CSV vs RRF

> Each .RFF file is structured according to the following conventions:
> * A vertical bar (|) is used as delimiter
> * The first line in each file begins with a hash (#) and provides the column names.


> Sub-directory: csv/
> -------------------
> Location: ftp://ftp.ncbi.nlm.nih.gov/pub/medgen/csv/
> Description: The csv subdirectory contains a set of comma-separated files (csv) corresponding to the RRF files in the main path. Some of the files are split to allow loading into spreadsheet software (maximum 1,000,000 lines per file). The csv files also facilitate processing of the MGDEF.RRF file because the DEF column may contain internal line feeds.


### Relations

MGREL files contain pairwise relations between concepts. Diseases and modes of inheritance are both MedGen concepts so associations between the two can be found by looking for specific rows in these files.

File: MGREL.RRF.gz
------------------
Summary data for pairwise relationship between concepts.
 
Description of MGREL csv:

```
 * CUI1:   	 first concept unique identifier
 * AUI1:   	 first atom unique identifier, where an atom is one term from a source
 * STYPE1: 	 the name of the column in MRCONSO.RRF that contains the first identifier to which the relationship is attached
 * REL: 	    relationship label
             values are defined by UMLS
				 https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/abbreviations.html#REL
				 CHD:  has a child relationship
				 PAR:  has a parent relationship
 * CUI2: 	 second concept unique identifier
 * AUI2:	    second atom unique identifier, where an atom is one term from a source
 * RELA:   	 additional relationship label
             use with REL
				 RO/has_manifestation:  used to match disorders and clinical features
				 RO/manifestation_of;   used to match clinical features and disorders
 * RUI: 	    relationship unique identifier  (identifier for this row in the MGREL table)
 * SAB: 	    abbreviation for the source of the term (defined in MedGen_Sources.txt)
 * SL: 	    source of relationship label
 * SUPPRESS: suppressed by UMLS curators   (no reason is reported)
 ```
 
Given that [Autosomal recessive inheritance](https://www.ncbi.nlm.nih.gov/medgen/141025) has concept id `C0441748`, an example set of relations for disease [C3809672](https://www.ncbi.nlm.nih.gov/medgen/C3809672) is:

```bash
> gzip -dc MGREL_1.csv.gz | grep C0441748 | grep C3809672
C0441748,A11979835,AUI,RO,C3809672,A23789374,has_inheritance_type,R148597751,OMIM,OMIM,N
C0441748,A24670442,AUI,RO,C3809672,A23789374,has_inheritance_type,RN07418295,HPO,HPO,N
C0441748,AN0510081,AUI,RO,C3809672,AN1464764,has_inheritance_type,RN07702612,ORDO,ORDO,N
```

`RELA` (relationship label) should be filtered to "has_inheritance_type" and `SAB` should be preserved so we know which sources provide evidence for a relation.

#### MedGen Mapping

To get HPO concepts in MedGen, use `MedGen_HPO_Mapping.txt.gz`:

```bash
> gzip -dc MedGen_HPO_Mapping.txt.gz | head
#CUI|SDUI|HpoStr|MedGenStr|MedGenStr_SAB|STY|
C0444868|HP:0000001|All|All|HPO|Quantitative Concept|
C4025901|HP:0000002|Abnormality of body height|Abnormality of body height|GTR|Finding|
C3714581|HP:0000003|Multicystic kidney dysplasia|Multicystic kidney dysplasia|GTR|Disease or Syndrome|
C1708511|HP:0000005|Mode of inheritance|Mode of inheritance|HPO|Genetic Function|
C0443147|HP:0000006|Autosomal dominant inheritance|Autosomal dominant inheritance|GTR|Intellectual Product|
C0443147|HP:0000006|Autosomal dominant inheritance|Autosomal dominant inheritance|GTR|Genetic Function|
C0441748|HP:0000007|Autosomal recessive inheritance|Autosomal recessive inheritance|HPO|Intellectual Product|
C0441748|HP:0000007|Autosomal recessive inheritance|Autosomal recessive inheritance|HPO|Genetic Function|
C4025900|HP:0000008|Abnormality of female internal genitalia|Abnormality of female internal genitalia|GTR|Anatomical Abnormality|
```

#### ClinVar Mapping

The steps then for assign modes of inheritance to ClinVar records is:

- Choose a set of HPO term ids corresponding to groups of inheritance modes (recessive/dominant)
- Find the corresponding MedGen concept ids
- Given a MedGen concept id for a disease on a ClinVar record, look for `has_inheritance_type` relation in `MGREL` for disease and matching concepts above
