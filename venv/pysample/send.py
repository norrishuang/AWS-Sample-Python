import boto3
import gzip
import argparse
import time

firehose = boto3.client('firehose')

parser = argparse.ArgumentParser(__file__, description="Firehose event generator")
parser.add_argument("--input", "-i", dest='input_file', help="Input log file, default is http.log", default="http.log.gz")
parser.add_argument("--stream", "-s", dest='output_stream', help="Firehose Stream name")
parser.add_argument("--num", "-n", dest='num_messages', help="Number of messages to send, 0 for inifnite, default is 1000", type=int, default=1000)

args = parser.parse_args()

input_file = args.input_file
num_messages = args.num_messages
output_stream = args.output_stream

if not output_stream:
    print("Output stream is required. Use -o to specify the name of the output stream")
    exit(1)

print(f"Sending {num_messages} messages to {output_stream}...")

sent = 0
with gzip.open(input_file, "rt") as f:
    line = f.readline()
    while line:
        msg = line.strip() + "\n"

        firehose.put_record(
            DeliveryStreamName=output_stream,
            Record={
                'Data': msg
            }
        )

        line = f.readline()

        sent += 1
        if sent % 100 == 0:
            print(f"{sent} sent")

        if sent >= num_messages and num_messages > 0:
            break;

        time.sleep(0.01)
