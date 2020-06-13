# https://hub.docker.com/r/jupyter/pyspark-notebook/tags
FROM jupyter/pyspark-notebook:dc9744740e12
ENV WORK_DIR=$HOME/work
RUN mkdir $WORK_DIR/repos $WORK_DIR/auth $WORK_DIR/data

# https://jupyter-docker-stacks.readthedocs.io/en/latest/using/recipes.html#using-pip-install-or-conda-install-in-a-child-docker-image
COPY requirements.txt /tmp/
RUN conda install --yes --file /tmp/requirements.txt && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

# TODO: switch to package install
ENV PYTHONPATH="${PYTHONPATH}:$WORK_DIR/repos/public-data-source/src"

# Ignore prefect warning from https://github.com/PrefectHQ/prefect/issues/2677
ENV PYTHONWARNINGS="ignore::UserWarning:prefect.core.flow,$PYTHONWARNINGS"

ENV GCS_PROJECT=target-ranking \
  GCS_BUCKET=public-data-source \
  GCS_ROOT=catalog \
  CATALOG_PATH=$WORK_DIR/repos/public-data-source/catalog.yaml
