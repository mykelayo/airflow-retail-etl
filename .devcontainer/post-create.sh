#!/bin/bash
curl -sSL https://install.astronomer.io | sudo bash
pip install apache-airflow==2.8.0
astro dev init