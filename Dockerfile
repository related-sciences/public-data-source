# https://hub.docker.com/r/jupyter/pyspark-notebook/tags
# https://github.com/jupyter/docker-stacks/wiki
FROM jupyter/pyspark-notebook:5197709e9f23
ENV WORK_DIR=$HOME/work
RUN mkdir $WORK_DIR/repos $WORK_DIR/auth $WORK_DIR/data

# https://jupyter-docker-stacks.readthedocs.io/en/latest/using/recipes.html#using-pip-install-or-conda-install-in-a-child-docker-image
COPY requirements-conda.txt requirements-pip.txt /tmp/
RUN conda install --yes \
    --file /tmp/requirements-conda.txt && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER && \
    pip install --requirement /tmp/requirements-pip.txt

# TODO: switch to package install
ENV PYTHONPATH="${PYTHONPATH}:$WORK_DIR/repos/public-data-source/src"

# Ignore prefect warning from https://github.com/PrefectHQ/prefect/issues/2677
ENV PYTHONWARNINGS="ignore::UserWarning:prefect.core.flow,$PYTHONWARNINGS"
