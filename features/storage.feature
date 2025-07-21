@storage
Feature: Integrate with Cloud Storage and run some checks


    Scenario: Check a bucket exists in Storage and contains the expected file 
        Given a bucket called "automation_data_bucket" exists
        Then a file called "data.csv" should exist at path "automation_data_bucket/sub_folder/"