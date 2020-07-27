from typing import TypeVar

from prefect import task

T = TypeVar("T")


@task  # type: ignore
def identity(x: T) -> T:
    return x


def constant(x: T, name: str = "value", value: bool = True) -> T:
    name = f"{name}={x}" if value else name
    return identity.copy(name=name)(x)  # type: ignore
