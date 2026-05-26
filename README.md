# Ringer #

Basic latency ringer that connects via Websockets to different exchanges and collects latency statistics.

## Usage ##

```
$ uv run ringer.py
usage: ringer.py [-h] --target TARGET [--port PORT] --bucket BUCKET [--regions [REGIONS ...]] [--timeout TIMEOUT] [--debug] [--ws] [--ws-path WS_PATH]
                 [--subscribe SUBSCRIBE]
ringer.py: error: the following arguments are required: --target, --bucket
```

## Setup ##

### IAM Policy ###

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

