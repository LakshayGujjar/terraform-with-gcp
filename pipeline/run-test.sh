#
# Copyright 2022 Google LLC. This software is provided as is, without warranty
# or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.
#
set -e

pip3 install --no-cache-dir -r requirements.txt
python3 -m pytest --cov-report html --cov-report xml --cov=dags -sxl tests/unit_test.py --html=report.html --self-contained-html \
 --junitxml=test.xml