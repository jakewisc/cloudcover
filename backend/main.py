# backend/main.py
#
# Minimal FastAPI backend for Cloud Cover App.
# Provides a simple API to return latest cloud cover %
# using the ingestion and processing workers.

from fastapi import FastAPI
import xarray as xr
import os

# Import your worker functions
from workers.ingestion import get_latest_goes_file, run_wget
from workers.processing import calc_cloud_fraction

app = FastAPI(title="Cloud Cover App", version="0.1")


@app.get("/health")
def health_check():
    """
    Basic test endpoint to confirm API is running.
    """
    return {"status": "ok"}


@app.get("/cloud-cover")
def get_cloud_cover():
    """
    Fetch the latest GOES data, process it,
    and return estimated cloud cover %.
    """
    latest = get_latest_goes_file()
    if not latest:
        return {"error": "No recent GOES files found"}

    # Build URL
    satellite = latest.split("/")[0]
    path = latest.split("/", 1)[1]
    url = f"https://{satellite}.s3.amazonaws.com/{path}"

    # Download file
    local_file = run_wget(url)
    if not local_file:
        return {"error": "Failed to download GOES file"}

    try:
        ds = xr.open_dataset(local_file)
        cloud_percent = calc_cloud_fraction(ds)
        return {
            "cloud_cover_percent": round(float(cloud_percent), 2),
            "source_file": path
        }
    finally:
        # Always clean up
        os.remove(local_file)
