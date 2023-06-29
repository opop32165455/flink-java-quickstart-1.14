CREATE TABLE ODS_MES_G_QC_SN_DEFECT
(
    after ROW <QC_LOTNO STRING,
    SERIAL_NUMBER STRING,
    QC_CNT        INT,
    DEFECT_ID     INT,
    DEFECT_LEVEL  STRING,
    LOCATION      STRING,
    DEFECT_QTY    INT,
    ITEM_TYPE_ID  STRING>
    WITH ( 'connector' = 'kafka',
        'topic' = 'ODS_MES_G_QC_SN_DEFECT',
        'properties.bootstrap.servers' =
        'node-123:9092,node-124:9092,node-125:9092',
        'properties.group.id' = 'DwdGsnDefectApp-consumer1687946923419',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json') WHERE op_type = 'I'