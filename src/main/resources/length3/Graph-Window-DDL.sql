CREATE TABLE Graph (
    src INT,
    dst INT,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
    'connector' = 'filesystem',
    'path' = 'src/main/resources/length3/Graph-Window-Data.csv',
    'format' = 'csv'
)