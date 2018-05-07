#!/bin/bash
wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
wget ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
mkdir -p resources
mv NASA_access_log_Jul95.gz resources/
mv NASA_access_log_Aug95.gz resources/
gzip -d resources/NASA_access_log_Jul95.gz
gzip -d resources/NASA_access_log_Aug95.gz
