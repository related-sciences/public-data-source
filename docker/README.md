## Docker

### Build

```bash
# From project root:
# Build the base image
docker build -t public-data-source -f Dockerfile .
# Build image with more opinionated environment (includes
# VScode IDE, jupyterlab extensions, and dev/test packages)
docker build -t public-data-source-dev -f Dockerfile.dev .
```

### Run

Run this command from this repository's root:

```bash
WORK_DIR=/home/jovyan/work
# NOTE: --user=root is only used to provide sudo to NB_USER (jovyan)
# but both jupyterlab and vscode server will run as non-root users
docker run --rm --tty --interactive \
  --env GRANT_SUDO=yes --user=root \
  --volume "$HOME/.rs_auth:$WORK_DIR/auth" \
  --env NB_UID=$(id -u) \
  --mount "type=bind,source=$(pwd),target=$WORK_DIR/repos/public-data-source" \
  --publish 8888:8888 --publish 8887:8887 \
  --env JUPYTER_TOKEN=RmiTyPOSpedGeYERYOnymerj \
  --env VSCODE_TOKEN=RmiTyPOSpedGeYERYOnymerj \
  --env SPARK_DRIVER_MEMORY=64g \
  --env PREFECT__FLOWS__CHECKPOINTING=true \
  public-data-source-dev
# -p 8887:8887 and VSCODE_TOKEN are not necessary with base image
```

Access JupyterLab at 8888 and VSCode at 8887.
Use the following URL to access the notebook server:
<http://localhost:8888/?token=RmiTyPOSpedGeYERYOnymerj>
