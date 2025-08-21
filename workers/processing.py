# workers/processing.py
#
# This script processes GOES-19 data downloaded via ingestion.py.
# It computes cloud cover percentage using:
#   - IR Band 13 at night (brightness temperature threshold)
#   - IR + Visible bands (2 & 3) during daytime
# Also includes optional visualization functions.

import numpy as np
import matplotlib.pyplot as plt
import xarray as xr
import datetime
import os

# -----------------------------
# Core Cloud Detection Functions
# -----------------------------

def is_daytime(ds):
    """
    Rough day/night check based on dataset time and satellite projection.
    Uses 't' attribute from GOES file.
    """
    # Extract scan start time from dataset metadata
    if "time_coverage_start" in ds.attrs:
        scan_start = datetime.datetime.fromisoformat(
            ds.attrs["time_coverage_start"].replace("Z", "+00:00")
        )
    else:
        # fallback: assume day for testing
        return True

    # For MVP: simple check using UTC hour
    hour = scan_start.hour
    return 6 <= hour <= 18  # crude, 6 UTC–18 UTC = daytime


def calc_cloud_fraction_ir(ds, band="CMI_C13", threshold=280.0):
    """
    Cloud fraction from IR Band 13.
    """
    data = ds[band].values
    mask = data < threshold
    cloud_pixels = np.sum(mask)
    total_pixels = np.sum(~np.isnan(data))
    return (cloud_pixels / total_pixels) * 100 if total_pixels > 0 else np.nan


def calc_cloud_fraction_multiband(ds, ir_band="CMI_C13", vis_bands=["CMI_C02", "CMI_C03"],
                                  ir_threshold=280.0, vis_threshold=0.3):
    """
    Cloud fraction using IR + VIS bands (daytime only).
    """
    ir_data = ds[ir_band].values
    ir_mask = ir_data < ir_threshold

    vis_masks = []
    for vb in vis_bands:
        if vb in ds:
            vis_data = ds[vb].values
            vis_masks.append(vis_data > vis_threshold)
    if vis_masks:
        vis_mask = np.logical_or.reduce(vis_masks)
    else:
        vis_mask = np.zeros_like(ir_mask, dtype=bool)

    combined = np.logical_or(ir_mask, vis_mask)
    total_pixels = np.sum(~np.isnan(ir_data))
    cloud_pixels = np.sum(combined)
    return (cloud_pixels / total_pixels) * 100 if total_pixels > 0 else np.nan


def calc_cloud_fraction(ds):
    """
    Main wrapper: choose IR-only (night) vs IR+VIS (day).
    """
    if is_daytime(ds):
        return calc_cloud_fraction_multiband(ds)
    else:
        return calc_cloud_fraction_ir(ds)


# -----------------------------
# Visualization Helpers
# -----------------------------

def plot_band(ds, band="CMI_C13", save_path="band_plot.png"):
    plt.figure(figsize=(10, 8))
    plt.imshow(ds[band], cmap="gray")
    plt.colorbar(label="Value")
    plt.title(f"GOES {band} - Raw Data")
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"Saved {save_path}")


def plot_cloud_mask(ds, save_path="cloud_mask.png"):
    if is_daytime(ds):
        mask_value = ds["CMI_C13"].values < 280.0
        for vb in ["CMI_C02", "CMI_C03"]:
            if vb in ds:
                mask_value |= (ds[vb].values > 0.3)
    else:
        mask_value = ds["CMI_C13"].values < 280.0

    plt.figure(figsize=(10, 8))
    plt.imshow(mask_value, cmap="Blues")
    plt.title("Cloud Mask")
    plt.savefig(save_path, dpi=150)
    plt.close()
    print(f"Saved {save_path}")


# -----------------------------
# Script Entrypoint (test mode)
# -----------------------------

if __name__ == "__main__":
    from ingestion import get_latest_goes_file, run_wget
    
    latest = get_latest_goes_file()
    if latest:
        satellite = latest.split("/")[0]
        path = latest.split("/", 1)[1]
        url = f"https://{satellite}.s3.amazonaws.com/{path}"

        local_file = run_wget(url)
        if local_file:
            try:
                ds = xr.open_dataset(local_file)

                cloud_percent = calc_cloud_fraction(ds)
                print(f"Estimated Cloud Cover: {cloud_percent:.2f}%")

                plot_band(ds, band="CMI_C13")
                plot_cloud_mask(ds)

            finally:
                os.remove(local_file)
                print(f"Removed temp file {local_file}")