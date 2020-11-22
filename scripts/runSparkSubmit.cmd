:: cmd for spark running on windows in local mode
spark-submit^
    --master local[*]^
    --driver-memory 2g^
    --jars ..\dependencies\spark-xml_2.11-0.10.0.jar,..\dependencies\postgresql-42.2.18.jar^
    ..\tfi_etl\%1 -config %2