"""
TaxiPulse — Download NYC TLC Yellow Taxi Trip Data
Downloads Parquet files from the NYC TLC public dataset.
"""

import os
import sys
from pathlib import Path
import requests
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import TLCConfig


# Local directory to store downloaded files temporarily
DOWNLOAD_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "raw"


def download_file(url: str, dest_path: Path) -> bool:
    """
    Download a file from URL to local path with progress logging.

    Args:
        url: Source URL to download from
        dest_path: Local file path to save to

    Returns:
        True if download succeeded, False otherwise
    """
    try:
        logger.info(f"⬇️  Downloading: {url}")
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()

        # Get file size for progress logging
        total_size = int(response.headers.get("content-length", 0))
        total_mb = total_size / (1024 * 1024)
        logger.info(f"   File size: {total_mb:.1f} MB")

        # Write to file in chunks
        downloaded = 0
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)

        actual_mb = dest_path.stat().st_size / (1024 * 1024)
        logger.success(f"✅ Downloaded: {dest_path.name} ({actual_mb:.1f} MB)")
        return True

    except requests.exceptions.HTTPError as e:
        logger.error(f"❌ HTTP Error downloading {url}: {e}")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"❌ Connection Error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False


def download_tlc_data(
    year: int = None,
    months: list = None
) -> list[Path]:
    """
    Download NYC TLC Yellow Taxi data for specified year and months.

    Args:
        year: Year to download (default: from config)
        months: List of month strings like ["01", "02"] (default: from config)

    Returns:
        List of paths to downloaded Parquet files
    """
    year = year or TLCConfig.DATA_YEAR
    months = months or TLCConfig.DATA_MONTHS

    # Create download directory
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    downloaded_files = []

    for month in months:
        month = month.strip().zfill(2)
        url = TLCConfig.get_parquet_url(year, month)
        filename = TLCConfig.get_filename(year, month)
        dest_path = DOWNLOAD_DIR / filename

        # Skip if already downloaded
        if dest_path.exists():
            size_mb = dest_path.stat().st_size / (1024 * 1024)
            logger.info(f"⏭️  Already exists: {filename} ({size_mb:.1f} MB)")
            downloaded_files.append(dest_path)
            continue

        if download_file(url, dest_path):
            downloaded_files.append(dest_path)

    logger.info(f"📦 Total files downloaded: {len(downloaded_files)}/{len(months)}")
    return downloaded_files


if __name__ == "__main__":
    logger.info("🚕 TaxiPulse — NYC TLC Data Downloader")
    logger.info(f"   Year: {TLCConfig.DATA_YEAR}")
    logger.info(f"   Months: {TLCConfig.DATA_MONTHS}")
    logger.info("")

    files = download_tlc_data()

    if files:
        logger.info("")
        logger.info("📂 Downloaded files:")
        for f in files:
            size_mb = f.stat().st_size / (1024 * 1024)
            logger.info(f"   {f.name} — {size_mb:.1f} MB")
    else:
        logger.warning("No files were downloaded.")