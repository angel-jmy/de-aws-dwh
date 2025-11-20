use warehouse DE_0928_WH;
use database PROJECT0928;
use schema DIMENSIONS;


show stages in schema DIMENSIONS;

-- drop stage team2_stage;
-- show stages;


create or replace table customer_scd2_dim_team2 (
    customer_id varchar(20) not null,
	email varchar(100),
	"name" varchar(100),
	loyalty_tier varchar(20),
	address varchar(255) NULL,
	city varchar(100) NULL,
	state varchar(50) NULL,
	phone varchar(30) NULL,
	updated_at timestamp_ntz,

    -- SCD2
    effective_date   timestamp_ntz,
    end_date         timestamp_ntz,
    current_flag     number(1,0) not null,
    is_deleted       number(1,0) not null
);


-- Create parquet format
-- create or replace file format
-- customer_scd_ff
-- type = parquet;


-- Copy into customer table from S3
truncate table customer_scd2_dim_team2;
copy into customer_scd2_dim_team2
from @batch_stage_team2
file_format = (
    type = parquet
)
match_by_column_name = case_insensitive; -- vv important!!

select * from customer_scd2_dim_team2
limit 10;

-- Total Rows
select count(*) as tot_rows
from customer_scd2_dim_team2;

-- Total active customers
select count(*) as tot_active
from customer_scd2_dim_team2
where current_flag = 1 and is_deleted = 0;

select * from customer_scd2_dim_team2
order by customer_id, updated_at;