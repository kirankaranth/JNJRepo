#!/bin/bash

/usr/local/bin/databricks-connect configure << EOF
y
$DATABRICKS_HOST
$DATABRICKS_TOKEN
$DATABRICKS_CLUSTER
$DATABRICKS_ORG_ID
15001
EOF

/usr/local/bin/python /tmp/robot/Execution/driver.py $TAGS
