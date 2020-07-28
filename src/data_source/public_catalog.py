from pathlib import Path


def get_path() -> Path:
    return Path(__file__).parent.parent.parent.joinpath("catalog.yaml")
