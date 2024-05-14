import boto3
import os
from botocore.exceptions import ClientError

def create_bucket():
    s3_client = boto3.client('s3')

    while True:
        bucket_name = input('Please enter a bucket name: ')
        try:
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
            print("Bucket created successfully.")
        except ClientError as e:
            err_message = e.response['Error']['Code']
            if err_message == 'BucketAlreadyExists' or err_message == 'BucketAlreadyOwnedByYou':
                print("Bucket already exists.")
                choice = input('would you like to continue with the existing bucket? (Y/N): ')
                if choice != 'Y' and choice != 'y':
                    continue
            else:
                print("An error occurred:", e)
        break

    return bucket_name

def upload_objects(path, bucket_name):
    s3_client = boto3.client('s3')

    for root, _, files in os.walk(path):
        for file in files:
            s3_client.upload_file(os.path.join(root,file), bucket_name,
                                   "customer_details/" + file)

def move_objects(bucket_name, source_folder, destination_folder):
    message = ""
    # Initialize the S3 client
    s3_client = boto3.client('s3')

    prefix = source_folder + "sr1_"

    # List objects in the source folder
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )

    files = response.get('Contents', [])
    assert len(files) > 0, f"No files starting with sr1_ exist in {source_folder}"

    for obj in files:
        # Construct the source and destination object key
        source_object_key = obj['Key']

        # Replace the string prefix (customer_details/) with the new prefix (sr1/)
        destination_object_key = source_object_key.replace(source_folder, destination_folder, 1)

        # Copy the object
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_object_key},
            Key=destination_object_key
        )

        s3_client.delete_object(
            Bucket=bucket_name,
            Key=source_object_key
        )
        message += source_object_key.split('/')[1] + "\n"
    
    return "Moved Successfully:\n" + message

""" Create a new SNS topic.
    This function uses the boto3 SNS client to create a new SNS topic with the specified name.

    :param topic_name: The name of the new SNS topic : String
    :return: The ARN (Amazon Resource Name) of the newly created SNS topic : String.
"""
def create_sns_topic():
    sns_client = boto3.client('sns')

    while True:
        topic_name = input('Please enter an SNS topic name: ')
        try:
            response = sns_client.create_topic(Name=topic_name)
            topic_arn = response['TopicArn']
            print(f"SNS topic '{topic_name}' created with ARN: {topic_arn}")
            return topic_arn
        except ClientError as e:
            err_message = e.response['Error']['Code']
            if err_message == 'TopicAlreadyExists' or err_message == 'TopicAlreadyOwnedByYou':
                print("SNS topic already exists.")
                choice = input('would you like to continue with the existing topic? (Y/N): ')
                if choice != 'Y' and choice != 'y':
                    continue
        except Exception as e:
            print(f"Error creating SNS topic '{topic_name}': {e}")
            return None
        return ""

def send_notification(topic_arn, message):
    sns_client = boto3.client('sns')

    try:
        sns_client.publish(
            TopicArn=topic_arn,
            Message=message
        )
        print("Notification sent successfully.")
    except Exception as e:
        print(f"Error sending notification to SNS topic '{topic_arn}': {e}")


if __name__ == '__main__':
    bucket_name = create_bucket()
    upload_objects("customer_details/", bucket_name)
    topic_arn = create_sns_topic()

    email_address = 'rogeazzam22@gmail.com'
    sns_client = boto3.client('sns')
    response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol='email',
            Endpoint=email_address
        )
    # topic_arn = "arn:aws:sns:us-west-2:590184012973:MyTOpic"
    message = ""
     
    try:
        message = move_objects(bucket_name, 'customer_details/', 'sr1/')
    except AssertionError as e:
        message = str(e)
        print("AssertionError:", e)
    except Exception as e:
        message = str(e)
        print("Exception:", e)
    
    send_notification(topic_arn, message)