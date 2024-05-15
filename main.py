import boto3
import os
from botocore.exceptions import ClientError
import time

""" Create a new S3 bucket
    This function uses the boto3 S3 client to create a new S3 bucket with the specified name.

    :param: None
    :return: The Name of the newly created S3 bucket: String.
"""
def create_bucket():
    s3_client = boto3.client('s3')

    # Wait until a bucket is created.
    while True:
        bucket_name = input('Please enter a bucket name: ')
        try:
            s3_client.create_bucket(Bucket=bucket_name, 
                    CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
            print("Bucket created successfully.")

            # Upload the initial files.
            upload_objects("customer_details/", bucket_name)
        except ClientError as e:
            err_message = e.response['Error']['Code']
            # Check if the bucket already exists.
            if err_message == 'BucketAlreadyExists' or err_message == 'BucketAlreadyOwnedByYou':
                print("Bucket already exists.")
                # Allow the user to continue with the existing bucket, or to choose another name.
                choice = input('would you like to continue with the existing bucket? (Y/N): ')
                if choice != 'Y' and choice != 'y':
                    continue
            else:
                print(f"Error creating Bucket '{bucket_name}': {e}")
        except Exception as e:
            print(f"Error creating Bucket '{bucket_name}': {e}")
            continue
        break

    return bucket_name


""" Create a new S3 bucket
    This function uses the boto3 S3 client to create a new S3 bucket with the specified name.

    :param: path: local path of the directory to upload: String,
    bucket_name: bucket name of the S3 bucket used for uploading: String
    :return: None.
"""
def upload_objects(path, bucket_name):
    print("Uploading customer_details/ directory")
    s3_client = boto3.client('s3')

    for root, _, files in os.walk(path):
        for file in files:
            s3_client.upload_file(os.path.join(root,file), bucket_name,
                                   "customer_details/" + file)


""" Move objects function
    This function Moves objects from one given directory to another within the given S3 bucket.

    :param: path: bucket_name: bucket name of the S3 bucket used for uploading: String,
    source_dir: path of the directory to copy from: String,
    destination_dir: path of the directory to copy to: String,
    start_txt: the prefix of files to be moved (default = "sr1_"): String.
    :return: message specifying the moved objects: String.
"""
def move_objects(bucket_name, source_dir, destination_dir, start_txt="sr1_"):
    print(f"Moving files starting with {start_txt} from {source_dir} to {destination_dir}")
    message = ""
    # Initialize the S3 client
    s3_client = boto3.client('s3')

    prefix = source_dir + start_txt

    # List objects in the source direcgtory
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )

    files = response.get('Contents', [])
    assert len(files) > 0, f"No files starting with sr1_ exist in {source_dir}"

    for obj in files:
        # Construct the source and destination object key
        source_object_key = obj['Key']

        # Replace the string prefix (customer_details/) with the new prefix (sr1/)
        destination_object_key = source_object_key.replace(source_dir, destination_dir, 1)

        # Copy the object to the destination directory
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_object_key},
            Key=destination_object_key
        )

        # Delete the object from the source directory after copying it.
        s3_client.delete_object(
            Bucket=bucket_name,
            Key=source_object_key
        )

        # Inserting file names of files that has been moved.
        message += source_object_key.split('/')[1] + "\n"
    
    print("Files moved successfully.")
    return "Moved Successfully:\n" + message


""" Create a new SNS topic.
    This function uses the boto3 SNS client to create a new SNS topic with the specified name.

    :param: topic_name: The name of the new SNS topic : String
    :return: The ARN (Amazon Resource Name) of the newly created SNS topic, 
    or the old topic arn in case it already exists : String.
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
        except Exception as e:
            print(f"Error creating SNS topic '{topic_name}': {e}")
            return None


""" Subscribe email to SNS topic
    This function uses the boto3 SNS client subscribe function to subscribe the given email to the SNS.

    :param: topic_arn: The ARN (identifier) of the SNS topic: String,
    email_address: email address that will be subscribed to the SNS topic: String.
    :return: None.
"""
def subscribe_email(topic_arn, email_address):
    sns_client = boto3.client('sns')
    response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol='email',
            Endpoint=email_address
        )
    print("Subscription confirmation was sent to your email.")
    time.sleep(60)


""" Send message notification to the endpoints
    This function sends the given message to the endpoints of the given SNS topic ARN.

    :param: topic_arn: The ARN (identifier) of the SNS topic: String,
    message: The message to be sent to the endpoints: String.
    :return: None.
"""
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


""" Main function
    The main function to run the script sequentially

    :param: None.
    :return: None.
"""
def main():
    # Creating bucket, or retrieving existing one.
    bucket_name = create_bucket()
    # Creating a new SNS topic, or retrieving existing one.
    topic_arn = create_sns_topic()

    subscribe_email(topic_arn, 'rogeazzam22@gmail.com')
    # topic_arn = "arn:aws:sns:us-west-2:590184012973:MyTOpic"

    message = ""
    try:
        ent = input('Press enter to move files ')
        # Moving objects from customer_details/ to sr1/ directory
        message = move_objects(bucket_name, 'customer_details/', 'sr1/')
    except AssertionError as e:
        message = str(e)
        print("AssertionError:", e)
    except Exception as e:
        message = str(e)
        print("Exception:", e)
    
    send_notification(topic_arn, message)


if __name__ == '__main__':
    main()