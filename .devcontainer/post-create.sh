#!/bin/bash
apk add --no-cache python3 py3-pip
pip install --upgrade pip
curl -sSL https://install.astronomer.io | bash
pip install apache-airflow==2.8.0
astro dev init