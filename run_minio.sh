#!/bin/bash

# Activar virtualenv (ruta relativa desde el script)
source "$(dirname "$0")/venv/bin/activate"

# Ejecutar script
python "$(dirname "$0")/script_minio.py"

# Desactivar virtualenv
deactivate
