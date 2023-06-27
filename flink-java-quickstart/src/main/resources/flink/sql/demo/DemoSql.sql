--********************************************************************--
-- Author:         Write your name here
-- Created Time:   2023-06-25 16:08:14
-- Description:    Write your description here
--********************************************************************--
CREATE TEMPORARY TABLE source_table (
    user_id INT,
    cost INT,
    current_data_time AS localtimestamp,
    WATERMARK FOR current_data_time AS current_data_time
) WITH (
    'connector' = 'datagen',
    'rows-per-second'='5',

    'fields.user_id.kind'='random',
    'fields.user_id.min'='1',
    'fields.user_id.max'='5',

    'fields.cost.kind'='random',
    'fields.cost.min'='1',
    'fields.cost.max'='100'
);


CREATE TEMPORARY TABLE IF NOT EXISTS print_table
WITH (
    'connector' = 'print'
)
LIKE source_table (EXCLUDING ALL);

INSERT INTO print_table
select user_id, sum(cost) as cost_sum
from source_table
group by user_id;

