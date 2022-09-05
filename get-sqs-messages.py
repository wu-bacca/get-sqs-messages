import sys
import getopt
import boto3


def get_arguments(argv):
    queue_url = ""
    profile = ""
    region = ""
    receive_count = ""
    arg_help = "{0} --queue_url <queue_url> --profile <profile> --region <region> --receive_count <max_receive_count>".format(
        argv[0])

    try:
        opts, args = getopt.getopt(argv[1:], "queue_url:profile:region:receive_count:", [
                                   "help", "queue_url=", "profile=", "region=", "receive_count="])
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                print(arg_help)
                sys.exit(2)
            elif opt in ("--queue_url"):
                queue_url = arg
            elif opt in ("--profile"):
                profile = arg
            elif opt in ("--region"):
                region = arg
            elif opt in ("--receive_count"):
                receive_count = arg
    except:
        print(arg_help)
        sys.exit(2)

    return queue_url, profile, region, receive_count


def get_messages(queue_url, profile, region, receive_count):
    session = boto3.Session(
        profile_name=profile, region_name=region)
    sqs_client = session.client('sqs')

    resp = sqs_client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=int(receive_count)
    )

    return resp['Messages']


def write_message_body_to_dir(message):
    filename = f'{message["MessageId"]}.json'
    content = message['Body']

    with open(filename, 'w') as outfile:
        outfile.write(content)
    return


if __name__ == "__main__":
    queue_url, profile, region, receive_count = get_arguments(sys.argv)
    messages = get_messages(queue_url, profile, region, receive_count)
    for message in messages:
        write_message_body_to_dir(message)
