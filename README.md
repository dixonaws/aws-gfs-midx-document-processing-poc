Welcome to the AWS Document Processing Sample
==============================================
This code is using Serverless SAM model to create a document processing pipeline and search application based on Amazon Textract, Amazon Comprehend and Amazon Elasticsearch.
This code is using Lambda S3 event trigger to invoke lambda which orchestrates all the natural language processing pipeline

What's Here
-----------

This sample includes:

* README.md - this file
* buildspec.yml - this file is used by AWS CodeBuild to package your
  application for deployment to AWS Lambda
* comprehend.py - this file contains the main orchestrator lambda
* template.yml - this file contains the AWS Serverless Application Model (AWS SAM) used
  by AWS CloudFormation to deploy your application to AWS Lambda.
* tests/ - this directory contains unit tests for your application (no tests right now)
* Rest of the folders are lambda dependencies that could be moved to a layer in later development.


What Do I Do Next?
------------------
To deploy just run:

sam deploy --guided 

The parameters should be the default one if working in my account. If deploying in a new account, make sure you dont use the default s3 SAM uses for deployment

WIP
