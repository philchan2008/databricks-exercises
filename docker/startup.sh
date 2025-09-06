#!/bin/bash

# Install TabNine and enable extensions
pip install jupyterlab jupyterlab-lsp python-lsp-server
# pip install jupyter-tabnine
# jupyter serverextension enable --py jupyter_tabnine

# Start Jupyter Notebook
start-notebook.sh

