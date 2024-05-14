boto3.client:
this is the original boto3 API abstraction
it provides low-level AWS service access
all AWS service operations are supported by clients
it exposes botocore client to the developer
it typically maps 1:1 with the AWS service API
it exposes snake-cased method names (e.g. ListBuckets API => list_buckets method)
typically yields primitive, non-marshalled data (e.g. DynamoDB attributes are dicts representing primitive DynamoDB values)
requires you to code result pagination
it is generated from an AWS service description