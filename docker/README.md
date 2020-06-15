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

```bash
WORK_DIR=/home/jovyan/work
# NOTE: --user=root is only used to provide sudo to NB_USER (jovyan)
# but both jupyterlab and vscode server will run as non-root users
docker run --rm -ti -e GRANT_SUDO=yes --user=root \
-v $HOME/.rs_auth:$WORK_DIR/auth \
-v $HOME/repos/rs/public-data-source:$WORK_DIR/repos/public-data-source \
-p 8888:8888 -p 8887:8887 \
-e JUPYTER_TOKEN=RmiTyPOSpedGeYERYOnymerj \
-e VSCODE_TOKEN=RmiTyPOSpedGeYERYOnymerj \
-e SPARK_DRIVER_MEMORY=64g \
-e PREFECT__FLOWS__CHECKPOINTING=true \
public-data-source-dev
# -p 8887:8887 and VSCODE_TOKEN are not necessary with base image
```

Access JupyterLab at 8888 and VSCode at 8887.
