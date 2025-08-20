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