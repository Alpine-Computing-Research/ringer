# Ringer #

Basic latency ringer that connects via Websockets to different exchanges and collects latency statistics.

## Usage ##

```
$ uv run ringer.py
usage: ringer.py [-h] --target TARGET [--port PORT] --bucket BUCKET [--regions [REGIONS ...]] [--timeout TIMEOUT] [--debug] [--ws] [--ws-path WS_PATH]
                 [--subscribe SUBSCRIBE]

options:
  -h, --help            show this help message and exit
  --target TARGET       hostname to probe
  --port PORT
  --bucket BUCKET       S3 bucket for result collection
  --regions [REGIONS ...]
                        specific regions to test (default: all)
  --timeout TIMEOUT     seconds to wait for results
  --debug               keep instances running for console inspection
  --ws                  use WebSocket instead of raw TCP
  --ws-path WS_PATH     WebSocket path (default: /)
  --subscribe SUBSCRIBE
                        JSON subscription message to send after WS connect
```

### Websocket Handshake ###

Using Bybit as an example:

```
$ uv run --with boto3 python3 ringer.py \
    --target stream.bybit.com \
    --port 443 \
    --bucket my-latency-results-bucket \
    --ws \
    --ws-path /v5/public/spot \
    --subscribe '{"op":"subscribe","args":["orderbook.1.BTCUSDT"]}'
```

## Setup ##

### 1. AWS Enrolment ###

You'll need an AWS account with either real funds or free-tier usage credits.

### 2. Create a single root IAM user ###

You'll need to create a single IAM user for using `ringer`.

### 3. IAM Policy Configuration ###

For the chosen IAM user, you'll need to use this policy.

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"ec2:DescribeRegions",
				"ec2:DescribeImages",
				"ec2:DescribeInstances",
				"ec2:DescribeSubnets",
				"ec2:RunInstances",
				"ec2:CreateTags",
				"ec2:GetConsoleOutput",
				"ec2:TerminateInstances"
			],
			"Resource": "*"
		},
		{
			"Effect": "Allow",
			"Action": [
				"s3:CreateBucket",
				"s3:PutObject",
				"s3:GetObject",
				"s3:ListBucket",
				"s3:GetBucketLocation"
			],
			"Resource": [
				"arn:aws:s3:::latency-sweep-hyperliquid",
				"arn:aws:s3:::latency-sweep-hyperliquid/*"
			]
		},
		{
            "Effect": "Allow",
            "Action": "iam:CreateServiceLinkedRole",
            "Resource": "arn:aws:iam::*:role/aws-service-role/spot.amazonaws.com/*",
            "Condition": {
              "StringLike": { "iam:AWSServiceName": "spot.amazonaws.com" }
            }
        }
	]
}
```

### 4. AWS Authentication ###

The above steps set up everything on the AWS side of things. Now we need to authenticate with AWS from the CLI. In the interactive prompt, it's recommended to use `json` as the default output format.

```
$ aws configure
```

### 5. Create S3 bucket ###

Finally, create an S3 bucket in order to store our results during a given sweep. The selected region here is largely arbitrary.

```
aws s3 mb s3://your-bucket-name --region your-region
```

