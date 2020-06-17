import io
import fsspec
from pathlib import Path
from typing import Optional
from pandas import DataFrame

def get_df_info(df: DataFrame) -> str:
    """Get DataFrame info as string"""
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()
