import boto3
import gzip
import argparse
import time
import re
import json

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

fields = [
    "ts", "uid", "id_orig_h", "id_resp_h", "id_orig_p", "id_resp_p", "trans_depth", "method", "host",
    "uri", "referrer", "user_agent", "request_body_len", "response_body_len", "status_code", "status_msg",
    "info_code", "info_msg", "filename", "tags", "username", "password", "proxied", "orig_fuids", "orig_mime_types",
    "resp_fuids", "resp_mime_types"]

sent = 0
with gzip.open(input_file, "rt") as f:
    line = f.readline()
    while line:
        data1 = line.strip().split("\t")
        data2 = dict(zip(fields, data1))
        data2["timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
        data2["host"] = data2["host"][:50]
        data2["uri"] = data2["uri"][:10000]
        data2["user_agent"] = data2["user_agent"][:10000]
        del data2["ts"]
        msg = json.dumps(data2).strip()

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
            break

        time.sleep(0.01)
