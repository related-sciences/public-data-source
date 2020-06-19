from prefect import task


@task
def identity(x):
    return x


def constant(x, name="value", value=True):
    name = f"{name}={x}" if value else name
    return identity.copy(name=name)(x)
