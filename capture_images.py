import argparse
import hashlib
import os
import time
from datetime import datetime

import requests


def make_output_dir(base_dir: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(base_dir, f"raw_capture_{timestamp}")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def save_frame(jpeg_bytes: bytes, out_dir: str, frame_index: int) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"frame_{frame_index:06d}_{ts}.jpg"
    path = os.path.join(out_dir, filename)
    with open(path, "wb") as f:
        f.write(jpeg_bytes)
    return path


def main():
    parser = argparse.ArgumentParser(
        description="Capture raw JPEG frames from the websocket /view endpoint and save them to a folder."
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:8080",
        help="Base URL of the websocket service, e.g. http://localhost:8080 or your Cloud Run URL",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between captures",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of images to save",
    )
    parser.add_argument(
        "--output-dir",
        default="captures",
        help="Parent folder where the new capture directory will be created",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--skip-duplicates",
        action="store_true",
        help="Skip saving frames if the JPEG bytes are identical to the previous frame",
    )

    args = parser.parse_args()

    view_url = args.base_url.rstrip("/") + "/view"
    out_dir = make_output_dir(args.output_dir)

    print(f"[info] Saving images to: {out_dir}")
    print(f"[info] Capturing from: {view_url}")
    print(f"[info] Interval: {args.interval}s | Count: {args.count}")

    saved = 0
    attempts = 0
    last_hash = None

    while saved < args.count:
        attempts += 1
        try:
            response = requests.get(view_url, timeout=args.timeout)
            if response.status_code != 200:
                print(f"[warn] Attempt {attempts}: server returned {response.status_code} - {response.text}")
                time.sleep(args.interval)
                continue

            content_type = response.headers.get("Content-Type", "")
            if "image/jpeg" not in content_type:
                print(f"[warn] Attempt {attempts}: unexpected content type: {content_type}")
                time.sleep(args.interval)
                continue

            jpeg_bytes = response.content
            if not jpeg_bytes:
                print(f"[warn] Attempt {attempts}: empty frame")
                time.sleep(args.interval)
                continue

            current_hash = hashlib.sha256(jpeg_bytes).hexdigest()

            if args.skip_duplicates and current_hash == last_hash:
                print(f"[skip] Attempt {attempts}: duplicate frame")
                time.sleep(args.interval)
                continue

            path = save_frame(jpeg_bytes, out_dir, saved + 1)
            last_hash = current_hash
            saved += 1

            print(f"[saved] {saved}/{args.count}: {path}")

        except requests.RequestException as e:
            print(f"[error] Attempt {attempts}: {e}")

        time.sleep(args.interval)

    print(f"[done] Saved {saved} image(s) to {out_dir}")


if __name__ == "__main__":
    main()