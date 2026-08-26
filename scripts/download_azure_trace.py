"""Download a slice of the Microsoft Azure Functions public dataset.

The dataset is research-friendly and publicly hosted:
    https://github.com/Azure/AzurePublicDataset

The raw per-day CSVs live on Azure blob storage; this script downloads one or
more days into a local directory so `mint/trace_profile.load_trace_profile`
can calibrate the MINT workloads.  Network access and several GB of free disk
are required; the actual data is NOT bundled in this repository.
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path


AZURE_DATASET_BASE = (
    "https://azurepublicdataset.blob.core.windows.net/"
    "azurepublicdataset/function_benchmark_data/"
)
AZURE_README_URL = "https://raw.githubusercontent.com/Azure/AzurePublicDataset/master/README.md"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Azure Functions public dataset slices.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--days", nargs="+", type=int, default=[1], help="Dataset days to download (1..14).")
    parser.add_argument("--dry-run", action="store_true", help="Only print the target URLs.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    urls = [f"{AZURE_DATASET_BASE}function_benchmark_data_{day}.csv" for day in args.days]
    if args.dry_run:
        for url in urls:
            print(url)
        print(f"README: {AZURE_README_URL}")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    for url in urls:
        filename = url.rsplit("/", 1)[-1]
        target = output_dir / filename
        print(f"Downloading {url}")
        try:
            urllib.request.urlretrieve(url, target)
        except Exception as exc:
            print(f"Failed to download {url}: {exc}", file=sys.stderr)
            return 1
        print(f"Wrote {target}")
    print("Calibrate with: python -c 'from mint.trace_profile import load_trace_profile; "
          "p = load_trace_profile(<downloaded csv>); print(p.call_counts)'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
