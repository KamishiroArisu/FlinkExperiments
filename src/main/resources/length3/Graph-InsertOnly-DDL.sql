CREATE TABLE Graph (
    src INT,
    dst INT
) WITH (
    'connector' = 'filesystem',
    'path' = 'src/main/resources/length3/Graph-InsertOnly-Data.csv',
    'format' = 'csv'
)