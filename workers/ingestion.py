# workers/ingestion.py
#
# This script connects to NOAA's public AWS S3 bucket,
# finds the most recent GOES-19 satellite file,
# and loads it into an xarray dataset so we can analyze it later.

import s3fs          # lets us browse public S3 buckets (like a cloud filesystem)
import xarray as xr  # works with large multidimensional datasets (like netCDF)
import datetime      # used to build the folder path for today’s date/time
import subprocess
import os

def get_latest_goes_file(product="ABI-L2-MCMIPC", satellite="noaa-goes19"):
    """
    Finds the latest GOES file in the NOAA AWS S3 bucket for a given product.
    Default product is ABI Cloud & Moisture Imagery (MCMIPC).
    """
    fs = s3fs.S3FileSystem(anon=True)
    now = datetime.datetime.now(datetime.timezone.utc)
    year = now.strftime("%Y")
    day_of_year = now.strftime("%j")
    hour = now.strftime("%H")

    prefix = f"{satellite}/{product}/{year}/{day_of_year}/{hour}/"

    try:
        files = fs.ls(prefix)
        if not files:
            raise ValueError("No files found for this hour")
        return files[-1]
    except Exception as e:
        print(f"Error accessing {prefix}: {e}")
        return None

def run_wget(url, output_path=None):
    """
    Downloads a file using wget.
    Returns the local file path if successful, else None.
    """
    if output_path is None:
        output_path = os.path.basename(url)  # use filename from URL

    command = ["wget", "-q", url, "-O", output_path]
    result = subprocess.run(command)

    if result.returncode == 0:
        print(f"Successfully downloaded {url} → {output_path}")
        return output_path
    else:
        print(f"Error downloading {url}")
        return None

if __name__ == "__main__":
    latest = get_latest_goes_file()
    if latest:
        print("Latest file:", latest)

        # Build HTTPS URL properly
        satellite = latest.split("/")[0]  # "noaa-goes19"
        path = latest.split("/", 1)[1]    # "ABI-L2-MCMIPC/2025/080/12/OR_..."
        url = f"https://{satellite}.s3.amazonaws.com/{path}"

        # Download file locally
        local_file = run_wget(url)
        if local_file:
            try:
                # Load with xarray
                ds = xr.open_dataset(local_file)
                print(ds)
                print("Variables:", list(ds.data_vars))
            finally:
                # Cleanup
                os.remove(local_file)
                print(f"Removed temp file {local_file}")