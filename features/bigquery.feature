@bq
Feature: Integrate with BigQuery and run some checks


    Scenario: Check a table exists in BigQuery with the correct structure
        Given a table called "test_schemas.basic_table" exists
        Then the table "test_schemas.basic_table" should contain the following structure
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
        And the table "test_schemas.basic_table" should be partitioned by column "date_column"


    Scenario: Clear down pre-existing data from a table in BigQuery
        Given all rows are deleted from table "test_schemas.basic_table"


    Scenario: Insert fixed CSV data into a table in BigQuery
        Given the data in CSV "data/fixed/sample_load.csv" has been inserted into table "test_schemas.basic_table" with null_marker " "
        Then table "test_schemas.basic_table" should contain "3" rows


    Scenario: Insert fixed SQL data into a table in BigQuery
        Given the data in the SQL insert file "data/fixed/sample_load.sql" has been inserted into table "test_schemas.basic_table"
        Then table "test_schemas.basic_table" should contain "6" rows


    Scenario: Insert data from a Gherkin table into a table in BigQuery
        Given the Gherkin table of data has been inserted into BigQuery table "test_schemas.basic_table"
            | string_column  | int_column  | float_column | numeric_column | bool_column | timestamp_column    | date_column | datetime_column     | json_column         |
            | GherkinSample1 | 12345       | 123.45       | 678.90         | true        | 2025-12-25 12:12:12 | 2025-12-12  | 2025-12-25 12:12:12 | {"key12":"value12"} |
            | GherkinSample2 | 67890       | 555.44       | 111.22         | false       | 2025-12-26 13:13:13 | 2025-12-26  | 2025-12-26 13:13:13 | NULL                |
            | GherkinSample3 | 11111       | 10.90        | NULL           | true        | 2025-12-27 14:14:14 | 2025-12-27  | 2025-12-27 14:14:14 | {"key13":"value13"} |
        Then table "test_schemas.basic_table" should contain "9" rows