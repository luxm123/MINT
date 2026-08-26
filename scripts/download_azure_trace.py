"""Download the Microsoft Azure Functions public dataset (2019) for MINT calibration.

The Azure Functions 2019 dataset (Shahrad et al., USENIX ATC'20, "Serverless in
the Wild") is distributed from the Azure/AzurePublicDataset GitHub Releases,
which avoids the legacy blob-storage hostname that is unreachable from some
networks:

    https://github.com/Azure/AzurePublicDataset/releases/download/\
dataset-functions-2019/\
azurefunctions_dataset2019_azurefunctions-dataset2019.tar.xz

The archive contains aggregate per-day CSVs (not per-invocation rows):
    invocations_per_function_md.anon.dNN.csv      per-minute invocation counts
    function_durations_percentiles.anon.dNN.csv   duration percentiles (ms)
    app_memory_percentiles.anon.dNN.csv           memory percentiles, days 1..12

This script downloads the archive once (cached), extracts only the requested
days, and writes the three CSVs per day into --output-dir so that
scripts/apply_trace_calibration.py --trace-dir <dir> can calibrate the MINT
core matrix.  The archive is removed after extraction unless --keep-archive is
passed, to keep the EC2 root volume small.

Usage:
    python scripts/download_azure_trace.py --output-dir data/azure_trace --days 1 2
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tarfile
import urllib.request
from pathlib import Path


ARCHIVE_URL = (
    "https://github.com/Azure/AzurePublicDataset/releases/download/"
    "dataset-functions-2019/"
    "azurefunctions_dataset2019_azurefunctions-dataset2019.tar.xz"
)

# Legacy fallbacks (kept for networks where GitHub Releases is blocked but the
# historical blob endpoints still resolve).  These endpoints are deprecated
# upstream and may redirect or disappear.
LEGACY_ARCHIVE_URLS = (
    "https://azurepublicdatasettraces.blob.core.windows.net/"
    "azurepublicdatasetv2/azurefunctions_dataset2019/"
    "azurefunctions-dataset2019.tar.xz",
    "https://azurepublicdataset.blob.core.windows.net/"
    "azurepublicdataset/function_benchmark_data.tar.xz",
)

DATASET_README_URL = (
    "https://raw.githubusercontent.com/Azure/AzurePublicDataset/master/"
    "AzureFunctionsDataset2019.md"
)

ATTRIBUTION = (
    "Mohammad Shahrad, Rodrigo Fonseca, Inigo Goiri, Gohar Chaudhry, "
    "Paul Batum, Jason Cooke, Eduardo Laureano, Colby Tresness, "
    "Mark Russinovich, Ricardo Bianchini. Serverless in the Wild: "
    "Characterizing and Optimizing the Serverless Workload at a Large Cloud "
    "Provider. USENIX ATC 2020. Dataset: CC-BY (Azure/AzurePublicDataset)."
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download the Azure Functions 2019 public dataset slices."
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--days",
        nargs="+",
        type=int,
        default=[1],
        help="Dataset days to keep (1..14).  Memory files exist for days 1..12.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download the archive even if a cached copy exists.",
    )
    parser.add_argument(
        "--keep-archive",
        action="store_true",
        help="Keep the downloaded tar.xz in --output-dir after extraction.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=5,
        help="Download attempts per URL before giving up.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print the target URLs and per-day file names.",
    )
    return parser.parse_args(argv)


def _day_from_name(name: str) -> int | None:
    match = re.search(r"\.d(\d{2})\.", name)
    return int(match.group(1)) if match else None


def expected_files(day: int) -> dict[str, str]:
    """Return {kind: filename} for one dataset day."""
    return {
        "invocations": f"invocations_per_function_md.anon.d{day:02d}.csv",
        "durations": f"function_durations_percentiles.anon.d{day:02d}.csv",
        "memory": f"app_memory_percentiles.anon.d{day:02d}.csv",
    }


def _download(url: str, target: Path, retries: int) -> None:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            print(f"Downloading {url} -> {target} (attempt {attempt}/{retries})")
            urllib.request.urlretrieve(url, target)
            if target.stat().st_size == 0:
                raise RuntimeError("downloaded file is empty")
            return
        except Exception as exc:  # noqa: BLE001 - report and retry any download error
            last_error = exc
            print(f"  attempt {attempt} failed: {exc}", file=sys.stderr)
    raise RuntimeError(f"download failed after {retries} attempts: {last_error}")


def _extract_member(tar: tarfile.TarFile, member: tarfile.TarInfo, target: Path) -> None:
    source = tar.extractfile(member)
    if source is None:
        raise RuntimeError(f"archive member {member.name} is not a regular file")
    with target.open("wb") as fh:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            fh.write(chunk)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    days = sorted(set(args.days))
    if any(day < 1 or day > 14 for day in days):
        print("--days must be in 1..14", file=sys.stderr)
        return 1

    if args.dry_run:
        print(f"archive: {ARCHIVE_URL}")
        print(f"README: {DATASET_README_URL}")
        for day in days:
            print(f"day {day:02d}: " + ", ".join(expected_files(day).values()))
        print(f"attribution: {ATTRIBUTION}")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / "azurefunctions_dataset2019_azurefunctions-dataset2019.tar.xz"

    if archive_path.exists() and archive_path.stat().st_size > 0 and not args.force:
        print(f"Using cached archive {archive_path} ({archive_path.stat().st_size} bytes)")
    else:
        urls = [ARCHIVE_URL, *LEGACY_ARCHIVE_URLS]
        last_error: Exception | None = None
        downloaded = False
        for url in urls:
            try:
                _download(url, archive_path, args.retries)
                downloaded = True
                break
            except Exception as exc:  # noqa: BLE001 - try the next mirror
                last_error = exc
                print(f"Mirror failed, trying next: {url}", file=sys.stderr)
        if not downloaded:
            print(f"All mirrors failed: {last_error}", file=sys.stderr)
            return 1

    written: dict[int, dict[str, str]] = {}
    memory_fallback: dict[int, str] = {}
    with tarfile.open(archive_path, "r:xz") as tar:
        by_kind: dict[str, dict[int, tarfile.TarInfo]] = {}
        for member in tar.getmembers():
            if not member.isfile():
                continue
            day = _day_from_name(member.name)
            if day is None:
                continue
            for kind, filename in expected_files(day).items():
                if member.name.endswith(filename):
                    by_kind.setdefault(kind, {})[day] = member
        if not by_kind.get("invocations") or not by_kind.get("durations"):
            print(
                "Archive does not contain the expected 2019 aggregate CSVs "
                f"({by_kind})",
                file=sys.stderr,
            )
            return 1
        for day in days:
            day_files: dict[str, str] = {}
            for kind, filename in expected_files(day).items():
                member = by_kind.get(kind, {}).get(day)
                if member is None:
                    # Memory files are only published for days 1..12; reuse the
                    # last published memory day and record the fallback.
                    if kind == "memory" and by_kind.get("memory"):
                        fallback_day = max(by_kind["memory"])
                        member = by_kind["memory"][fallback_day]
                        memory_fallback[day] = expected_files(fallback_day)["memory"]
                        print(
                            f"WARNING day {day:02d}: no memory file; reusing "
                            f"{member.name}",
                            file=sys.stderr,
                        )
                    else:
                        print(
                            f"ERROR day {day:02d}: missing {filename} in archive",
                            file=sys.stderr,
                        )
                        return 1
                target = output_dir / filename
                if not target.exists() or target.stat().st_size == 0:
                    _extract_member(tar, member, target)
                day_files[kind] = target.name
            written[day] = day_files

    if not args.keep_archive:
        archive_path.unlink(missing_ok=True)

    manifest = {
        "dataset": "AzureFunctionsDataset2019",
        "source_url": ARCHIVE_URL,
        "readme": DATASET_README_URL,
        "days": days,
        "files": {str(day): files for day, files in written.items()},
        "memory_fallback": {str(day): name for day, name in memory_fallback.items()},
        "attribution": ATTRIBUTION,
        "note": "Aggregate CSVs; use apply_trace_calibration.py --trace-dir",
    }
    manifest_path = output_dir / "download_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    for day in days:
        files = ", ".join(written[day].values())
        print(f"Wrote day {day:02d}: {files}")
    print(f"Wrote {manifest_path}")
    print(
        "Calibrate with: python scripts/apply_trace_calibration.py "
        f"--trace-dir {output_dir} --config <base-config> --output <calibrated.yaml>"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
