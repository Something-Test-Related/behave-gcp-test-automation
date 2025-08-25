@bq @storage
Feature: Load the contents of a CSV file from a bucket in Storage into a table in Big Query using a DAG running in Airflow


    Scenario: Check all components exist before we run the DAG
        Given a DAG called "load_csv" exists 
        And a DAG file called "dags/load_csv.py" exists in Storage
        And a table called "test_schemas.basic_table" exists
        And a file called "data.csv" exists at path "automation_data_bucket/"


    Scenario: Check the file being loaded contains the expected data
        Given the file called "data.csv" at path "automation_data_bucket/" contains the following data
            | string_column     | int_column    | float_column  | numeric_column    | bool_column   | timestamp_column      | date_column   | datetime_column       | json_column           |
            | stringvalue       | 78            | 34.04         | 10002.32          | 0             | 2020-06-02 23:57:12   | 2023-12-24    | 2016-05-19T10:38:47   | {}                    |
            | stringvalue2      | 874           | 3354.19       | 120432.12         | 1             | 2021-06-12 13:17:11   | 2022-02-22    | 2014-04-19T14:48:44   | {"key1":"value1"}     |
            | ehhsdfsdfgh       | 678           | 845.76        | 60384.12          | 0             | 2022-03-02 11:57:12   | 2022-12-24    | 2021-05-19T10:38:47   | {}                    |
            | fdgsdfgdsfsgd     | 798           | 875.01        | 832838.88         | 1             | 2021-11-12 16:17:11   | 2021-02-22    | 2021-04-19T14:48:44   | {"key12":"value12"}   |
            | ghjfghsdfs        | 340           | 785.0         | 7495.91           | 0             | 2019-01-02 21:57:12   | 2019-12-24    | 2016-05-19T10:38:47   | {}                    |
            | fdgsdfjsdjk       | 1             | 14778.29      | 777777.77         | 1             | 2024-10-12 17:17:11   | 2022-02-22    | 2014-04-19T14:48:44   | {"key11":"value11"}   |
            | sdflkj3452hgf     | 52            | 84735.83      | 11111.1           | 0             | 2024-12-02 03:57:12   | 2023-12-24    | 2024-05-19T10:38:47   | {}                    |
            | asdjk6799ffyt     | 22            | 3.98          | 42.0              | 1             | 2022-07-12 10:17:11   | 2022-02-22    | 2019-04-19T14:48:44   | {"key17":"value17"}   |
            | sd3453flkjg       | 123           | 6575.33       | 3.2               | 0             | 2017-06-02 03:57:12   | 2023-12-24    | 2019-05-19T10:38:47   | {}                    |
            | suybgf345345df2   | 456           | 233444.12     | 8.5               | 1             | 2000-03-12 01:17:11   | 2022-02-22    | 2014-04-19T14:48:44   | {"key01":"value01"}   |


    Scenario: Check the table we are loading into is the expected structure and empty
        Given the table "test_schemas.basic_table" is partitioned by column "date_column"
        And table "test_schemas.basic_table" contains "0" rows
        And the table "test_schemas.basic_table" contains the following structure
            | COLUMN_NAME      | DATA_TYPE |
            | string_column    | STRING    |
            | int_column       | INT64     |
            | float_column     | FLOAT64   |
            | numeric_column   | NUMERIC   | 
            | bool_column      | BOOL      |
            | timestamp_column | TIMESTAMP |
            | date_column      | DATE      |
            | datetime_column  | DATETIME  |
            | json_column      | JSON      |


    Scenario: Run a DAG with no parameters which loads data from a CSV into BigQuery, wait for the DAG to finish running then check it ran sucessfully
        When a DAG called "load_csv" is run
        Then the last run of DAG "load_csv" should have state "success"


    Scenario: Check the data in the Big Query output table 
        Then table "test_schemas.basic_table" should contain "10" rows
        And table "test_schemas.basic_table" should contain the following data 
            | string_column     | int_column    | float_column  | numeric_column    | bool_column   | timestamp_column      | date_column   | datetime_column       | json_column           |
            | stringvalue       | 78            | 34.04         | 10002.32          | false         | 2020-06-02 23:57:12   | 2023-12-24    | 2016-05-19T10:38:47   | {}                    |
            | stringvalue2      | 874           | 3354.19       | 120432.12         | true          | 2021-06-12 13:17:11   | 2022-02-22    | 2014-04-19T14:48:44   | {"key1":"value1"}     |
            | ehhsdfsdfgh       | 678           | 845.76        | 60384.12          | false         | 2022-03-02 11:57:12   | 2022-12-24    | 2021-05-19T10:38:47   | {}                    |
            | fdgsdfgdsfsgd     | 798           | 875.01        | 832838.88         | true          | 2021-11-12 16:17:11   | 2021-02-22    | 2021-04-19T14:48:44   | {"key12":"value12"}   |
            | ghjfghsdfs        | 340           | 785.0         | 7495.91           | false         | 2019-01-02 21:57:12   | 2019-12-24    | 2016-05-19T10:38:47   | {}                    |
            | fdgsdfjsdjk       | 1             | 14778.29      | 777777.77         | true          | 2024-10-12 17:17:11   | 2022-02-22    | 2014-04-19T14:48:44   | {"key11":"value11"}   |
            | sdflkj3452hgf     | 52            | 84735.83      | 11111.1           | false         | 2024-12-02 03:57:12   | 2023-12-24    | 2024-05-19T10:38:47   | {}                    |
            | asdjk6799ffyt     | 22            | 3.98          | 42.0              | true          | 2022-07-12 10:17:11   | 2022-02-22    | 2019-04-19T14:48:44   | {"key17":"value17"}   |
            | sd3453flkjg       | 123           | 6575.33       | 3.2               | false         | 2017-06-02 03:57:12   | 2023-12-24    | 2019-05-19T10:38:47   | {}                    |
            | suybgf345345df2   | 456           | 233444.12     | 8.5               | true          | 2000-03-12 01:17:11   | 2022-02-22    | 2014-04-19T14:48:44   | {"key01":"value01"}   |