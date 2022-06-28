-- This is run as part of the setup_aws.sh script

CREATE EXTERNAL SCHEMA spectrum
FROM DATA CATALOG DATABASE  'spectrumdb' iam_role 'arn:aws:iam::"$AWS_ID":role/"$IAM_ROLE_NAME"' CREATE EXTERNAL DATABASE IF NOT EXISTS;
DROP TABLE IF EXISTS spectrum.user_purchase_stg;
CREATE EXTERNAL TABLE spectrum.user_purchase_stg (
    invoice_number VARCHAR(10),
    stock_code VARCHAR(20),
    detail VARCHAR(1000),
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price DECIMAL(8, 3),
    customer_id INTEGER,
    country VARCHAR(20)
) PARTITIONED BY (insert_date DATE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS textfile LOCATION 's3://"$1"/stage/user_purchase/' TABLE PROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS spectrum.classified_movie_review;
CREATE EXTERNAL TABLE spectrum.classified_movie_review (
    cid VARCHAR(100),
    positive_review BOOLEAN,
    insert_date VARCHAR(12)
) STORED AS PARQUET  LOCATION 's3://"$1"/stage/movie_review/';

DROP TABLE IF EXISTS public.user_behavior_metric;
CREATE TABLE public.user_behavior_metric (
    customer_id INTEGER,
    amount_spent DECIMAL(18, 5),
    review_score INTEGER,
    review_count INTEGER,
    insert_date DATE
);