from data_source import public_catalog


def test_local_catalog_path() -> None:
    p = public_catalog.get_path()
    assert p.exists()
    assert p.as_posix().endswith("catalog.yaml")
