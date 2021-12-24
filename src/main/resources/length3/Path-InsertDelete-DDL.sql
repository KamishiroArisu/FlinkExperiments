CREATE TABLE Path (
    src INT,
    via1 INT,
    via2 INT,
    dst INT
) WITH (
    'connector' = 'filesystem',
    'path' = '/tmp/Path-InsertDelete-Result',
    'format' = 'csv-changelog'
)