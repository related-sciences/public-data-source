## Docker

### Build

```bash
cd docker
docker build -t public-data-source .
```

### Run

```bash
WORK_DIR=/home/jovyan/work
docker run --rm -ti -e GRANT_SUDO=yes --user=root \
-v $HOME/.rs_auth:$WORK_DIR/auth \
-v $HOME/repos/rs/public-data-source:$WORK_DIR/repos/public-data-source \
-p 8888:8888 \
-e JUPYTER_TOKEN=RmiTyPOSpedGeYERYOnymerj \
-e SPARK_DRIVER_MEMORY=64g \
public-data-source
```
