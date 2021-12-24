CREATE TABLE Path (
    src INT,
    via1 INT,
    via2 INT,
    dst INT,
    ws TIMESTAMP(3),
    we TIMESTAMP(3)
) WITH (
    'connector' = 'filesystem',
    'path' = '/tmp/Path-Window-Result',
    'format' = 'csv'
)