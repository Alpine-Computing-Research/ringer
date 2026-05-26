#!/usr/bin/env python3
"""
Sweep all AWS regions to find the one with minimum latency to a target endpoint.
Instances are spot, self-terminating, and push results via presigned S3 URL.

Usage:
    # TCP connect time
    python3 ringer.py --target api.hyperliquid.xyz --port 443 --bucket your-bucket

    # WebSocket handshake + time-to-first-message
    python3 ringer.py --target api.hyperliquid.xyz --port 443 --bucket your-bucket \\
        --ws --subscribe '{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}'

    python3 ringer.py --target stream.bybit.com --port 443 --bucket your-bucket \\
        --ws --subscribe '{"op":"subscribe","args":["orderbook.1.BTCUSDT"]}'
"""

import argparse
import json
from typing import Any
from botocore.config import Config
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.exceptions import ClientError

INSTANCE_TYPE = "t3.micro"
SAMPLES = 100
RESULT_TTL = 600


PROBE_SCRIPT_TCP = """\
#!/bin/bash
python3 - <<'PYEOF'
import socket, time, json

host = "{host}"
port = {port}
samples = {samples}

rtts = []
for _ in range(samples):
    t0 = time.perf_counter()
    try:
        s = socket.create_connection((host, port), timeout=2)
        s.close()
        rtts.append((time.perf_counter() - t0) * 1000)
    except OSError:
        pass

if not rtts:
    result = {{"error": "all connects failed", "region": "{region}"}}
else:
    s = sorted(rtts)
    result = {{
        "region": "{region}",
        "mode": "tcp",
        "min":  s[0],
        "p50":  s[len(s) // 2],
        "p99":  s[int(len(s) * 0.99)],
        "samples": len(s),
    }}

with open("/tmp/result.json", "w") as f:
    json.dump(result, f)
PYEOF

curl -s -X PUT --data-binary "@/tmp/result.json" "{presigned_url}"
{poweroff}
"""


PROBE_SCRIPT_WS = """\
#!/bin/bash
python3 - <<'PYEOF'
import socket, ssl, os, struct, base64, time, json

HOST = "{host}"
PORT = {port}
PATH = "{path}"
SUBSCRIBE = {subscribe}
SAMPLES = {samples}

def ws_connect():
    raw = socket.create_connection((HOST, PORT), timeout=5)
    sock = ssl.create_default_context().wrap_socket(raw, server_hostname=HOST)
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall((
        f"GET {{PATH}} HTTP/1.1\\r\\n"
        f"Host: {{HOST}}\\r\\n"
        f"Upgrade: websocket\\r\\n"
        f"Connection: Upgrade\\r\\n"
        f"Sec-WebSocket-Key: {{key}}\\r\\n"
        f"Sec-WebSocket-Version: 13\\r\\n"
        f"\\r\\n"
    ).encode())
    buf = b""
    while b"\\r\\n\\r\\n" not in buf:
        buf += sock.recv(4096)
    if b"101" not in buf:
        raise Exception("upgrade failed")
    return sock

def ws_send(sock, text):
    payload = text.encode()
    mask = os.urandom(4)
    n = len(payload)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    hdr = bytes([0x81, 0x80 | n]) if n < 126 else bytes([0x81, 0xFE]) + struct.pack(">H", n)
    sock.sendall(hdr + mask + masked)

def ws_recv(sock):
    def read_exactly(n):
        buf = b""
        while len(buf) < n:
            buf += sock.recv(n - len(buf))
        return buf
    b0, b1 = read_exactly(2)
    n = b1 & 0x7F
    if n == 126:
        n = struct.unpack(">H", read_exactly(2))[0]
    elif n == 127:
        n = struct.unpack(">Q", read_exactly(8))[0]
    if b1 & 0x80:
        mask = read_exactly(4)
        data = read_exactly(n)
        return bytes(b ^ mask[i % 4] for i, b in enumerate(data)).decode()
    return read_exactly(n).decode()

def measure_once():
    t0 = time.perf_counter()
    sock = ws_connect()
    handshake_ms = (time.perf_counter() - t0) * 1000
    first_msg_ms = None
    if SUBSCRIBE is not None:
        ws_send(sock, json.dumps(SUBSCRIBE))
        t1 = time.perf_counter()
        sock.settimeout(5)
        try:
            ws_recv(sock)
            first_msg_ms = (time.perf_counter() - t1) * 1000
        except socket.timeout:
            pass
    sock.close()
    return handshake_ms, first_msg_ms

handshake_rtts = []
first_msg_rtts = []
for _ in range(SAMPLES):
    try:
        h, f = measure_once()
        handshake_rtts.append(h)
        if f is not None:
            first_msg_rtts.append(f)
    except Exception:
        pass

if not handshake_rtts:
    result = {{"error": "all connects failed", "region": "{region}"}}
else:
    s = sorted(handshake_rtts)
    result = {{
        "region": "{region}",
        "mode": "ws",
        "min":  s[0],
        "p50":  s[len(s) // 2],
        "p99":  s[int(len(s) * 0.99)],
        "samples": len(s),
    }}
    if first_msg_rtts:
        f = sorted(first_msg_rtts)
        result["first_msg_min"] = f[0]
        result["first_msg_p50"] = f[len(f) // 2]
        result["first_msg_p99"] = f[int(len(f) * 0.99)]

with open("/tmp/result.json", "w") as fh:
    json.dump(result, fh)
PYEOF

curl -s -X PUT --data-binary "@/tmp/result.json" "{presigned_url}"
{poweroff}
"""


def get_regions() -> list[str]:
    ec2 = boto3.client("ec2", region_name="us-east-1")
    return [
        r["RegionName"]
        for r in ec2.describe_regions(
            Filters=[
                {"Name": "opt-in-status", "Values": ["opt-in-not-required", "opted-in"]}
            ]
        )["Regions"]
    ]


def get_latest_al2023_ami(region: str) -> str:
    ec2 = boto3.client("ec2", region_name=region)
    images = ec2.describe_images(
        Owners=["amazon"],
        Filters=[
            {"Name": "name", "Values": ["al2023-ami-*-x86_64"]},
            {"Name": "state", "Values": ["available"]},
        ],
    )["Images"]
    if not images:
        raise RuntimeError(f"no AL2023 AMI found in {region}")
    return sorted(images, key=lambda x: x["CreationDate"])[-1]["ImageId"]


def get_default_subnet(ec2) -> str | None:
    subnets = ec2.describe_subnets(
        Filters=[{"Name": "defaultForAz", "Values": ["true"]}]
    )["Subnets"]
    return subnets[0]["SubnetId"] if subnets else None


def _run(ec2, ami: str, user_data: str, subnet_id: str, spot: bool) -> str:
    kwargs: dict[str, Any] = dict(
        ImageId=ami,
        InstanceType=INSTANCE_TYPE,
        MinCount=1,
        MaxCount=1,
        UserData=user_data,
        InstanceInitiatedShutdownBehavior="terminate",
        NetworkInterfaces=[
            {
                "DeviceIndex": 0,
                "AssociatePublicIpAddress": True,
                "SubnetId": subnet_id,
            }
        ],
    )
    if spot:
        kwargs["InstanceMarketOptions"] = {
            "MarketType": "spot",
            "SpotOptions": {"SpotInstanceType": "one-time"},
        }
    return ec2.run_instances(**kwargs)["Instances"][0]["InstanceId"]


def launch_spot(region: str, ami: str, user_data: str) -> str | None:
    ec2 = boto3.client("ec2", region_name=region)
    subnet_id = get_default_subnet(ec2)
    if not subnet_id:
        print(f"[{region}] no default subnet found, skipping")
        return None
    try:
        return _run(ec2, ami, user_data, subnet_id, spot=True)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in (
            "InsufficientInstanceCapacity",
            "SpotMaxPriceTooLow",
            "Unsupported",
            "AuthFailure.ServiceLinkedRoleCreationNotPermitted",
        ):
            print(f"[{region}] spot unavailable ({code}), falling back to on-demand")
            try:
                return _run(ec2, ami, user_data, subnet_id, spot=False)
            except ClientError as e2:
                print(f"[{region}] on-demand fallback also failed: {e2}")
                return None
        print(f"[{region}] launch failed: {e}")
        return None


def build_script(args, region: str, presigned_url: str) -> str:
    poweroff = "" if args.debug else "poweroff"
    if args.ws:
        subscribe = repr(json.loads(args.subscribe)) if args.subscribe else "None"
        return PROBE_SCRIPT_WS.format(
            host=args.target,
            port=args.port,
            path=getattr(args, "ws_path", "/"),
            subscribe=subscribe,
            samples=SAMPLES,
            region=region,
            presigned_url=presigned_url,
            poweroff=poweroff,
        )
    else:
        return PROBE_SCRIPT_TCP.format(
            host=args.target,
            port=args.port,
            samples=SAMPLES,
            region=region,
            presigned_url=presigned_url,
            poweroff=poweroff,
        )


def probe_region(args, region: str, presigned_url: str) -> str | None:
    try:
        ami = get_latest_al2023_ami(region)
    except Exception as e:
        print(f"[{region}] AMI lookup failed: {e}")
        return None

    script = build_script(args, region, presigned_url)
    instance_id = launch_spot(region, ami, script)
    if instance_id:
        print(f"[{region}] launched {instance_id}")
    return instance_id


def all_terminated(region_instances: dict[str, str]) -> bool:
    by_region: dict[str, list[str]] = {}
    for region, iid in region_instances.items():
        by_region.setdefault(region, []).append(iid)

    for region, iids in by_region.items():
        ec2 = boto3.client("ec2", region_name=region)
        resp = ec2.describe_instances(InstanceIds=iids)
        for reservation in resp["Reservations"]:
            for inst in reservation["Instances"]:
                if inst["State"]["Name"] != "terminated":
                    return False
    return True


def collect_results(
    bucket: str,
    bucket_region: str,
    keys: list[str],
    region_instances: dict[str, str],
    timeout: int = 300,
) -> list[dict]:
    s3 = boto3.client(
        "s3",
        region_name=bucket_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}),
    )
    results = []
    pending = set(keys)
    deadline = time.time() + timeout

    while pending and time.time() < deadline:
        for key in list(pending):
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                data = json.loads(obj["Body"].read())
                results.append(data)
                pending.discard(key)
                region = data.get("region", key)
                if "error" in data:
                    print(f"[{region}] probe error: {data['error']}")
                else:
                    print(f"[{region}] p50={data['p50']:.2f}ms")
            except ClientError as e:
                if e.response["Error"]["Code"] != "NoSuchKey":
                    print(f"S3 error for {key}: {e}")

        if pending:
            if all_terminated(region_instances):
                print("\nAll instances terminated — no more results coming.")
                break
            time.sleep(5)

    if pending:
        print(f"\nWarning: {len(pending)} region(s) never reported results")

    return results


def print_results(results: list[dict]) -> None:
    successful = [r for r in results if "error" not in r]
    if not successful:
        print("No successful results.")
        return

    has_first_msg = any("first_msg_p50" in r for r in successful)

    if has_first_msg:
        print(
            f"\n{'Region':<25} {'hs_min':>8} {'hs_p50':>8} {'hs_p99':>8} {'msg_p50':>8}  samples"
        )
        print("-" * 75)
        for r in sorted(successful, key=lambda x: x.get("first_msg_p50", x["p50"])):
            msg = (
                f"{r['first_msg_p50']:>7.2f}ms" if "first_msg_p50" in r else "       —"
            )
            print(
                f"{r['region']:<25} {r['min']:>7.2f}ms {r['p50']:>7.2f}ms {r['p99']:>7.2f}ms {msg}  {r['samples']}"
            )
        winner = min(successful, key=lambda x: x.get("first_msg_p50", x["p50"]))
        metric = (
            f"first_msg_p50={winner['first_msg_p50']:.2f}ms"
            if "first_msg_p50" in winner
            else f"p50={winner['p50']:.2f}ms"
        )
    else:
        print(f"\n{'Region':<25} {'min':>8} {'p50':>8} {'p99':>8}  samples")
        print("-" * 65)
        for r in sorted(successful, key=lambda x: x["p50"]):
            print(
                f"{r['region']:<25} {r['min']:>7.2f}ms {r['p50']:>7.2f}ms {r['p99']:>7.2f}ms  {r['samples']}"
            )
        winner = min(successful, key=lambda x: x["p50"])
        metric = f"p50={winner['p50']:.2f}ms"

    print(f"\nBest region: {winner['region']} ({metric})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, help="hostname to probe")
    parser.add_argument("--port", type=int, default=443)
    parser.add_argument(
        "--bucket", required=True, help="S3 bucket for result collection"
    )
    parser.add_argument(
        "--regions", nargs="*", help="specific regions to test (default: all)"
    )
    parser.add_argument(
        "--timeout", type=int, default=300, help="seconds to wait for results"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="keep instances running for console inspection",
    )
    parser.add_argument(
        "--ws", action="store_true", help="use WebSocket instead of raw TCP"
    )
    parser.add_argument("--ws-path", default="/", help="WebSocket path (default: /)")
    parser.add_argument(
        "--subscribe", help="JSON subscription message to send after WS connect"
    )
    args = parser.parse_args()

    if args.subscribe:
        try:
            json.loads(args.subscribe)
        except json.JSONDecodeError as e:
            parser.error(f"--subscribe is not valid JSON: {e}")

    regions = args.regions or get_regions()
    mode = "websocket" if args.ws else "tcp"
    print(f"Probing {len(regions)} regions → {args.target}:{args.port} ({mode})")

    s3_base = boto3.client("s3")
    bucket_region = (
        s3_base.get_bucket_location(Bucket=args.bucket)["LocationConstraint"]
        or "us-east-1"
    )
    s3_config = Config(signature_version="s3v4", s3={"addressing_style": "virtual"})
    s3 = boto3.client("s3", region_name=bucket_region, config=s3_config)
    region_keys = {r: f"sweep/{uuid.uuid4()}.json" for r in regions}

    presigned = {
        r: s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": args.bucket, "Key": k},
            ExpiresIn=RESULT_TTL,
            HttpMethod="PUT",
        )
        for r, k in region_keys.items()
    }

    with ThreadPoolExecutor(max_workers=len(regions)) as pool:
        launched = list(
            pool.map(lambda r: (r, probe_region(args, r, presigned[r])), regions)
        )

    region_instances = {r: iid for r, iid in launched if iid}
    print("\nAll instances launched — waiting for results...\n")
    results = collect_results(
        args.bucket,
        bucket_region,
        list(region_keys.values()),
        region_instances,
        timeout=args.timeout,
    )
    print_results(results)


if __name__ == "__main__":
    main()
