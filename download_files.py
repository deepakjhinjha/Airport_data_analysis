import os
import requests
import zipfile
from pathlib import Path

BTS_WEBSITE = "https://transtats.bts.gov/PREZIP/"

LOCAL_SAVE_FOLDER = "./downloaded_files"
Path(LOCAL_SAVE_FOLDER).mkdir(parents=True, exist_ok=True)
print(f"Save folder ready: {LOCAL_SAVE_FOLDER}")

YEARS_TO_DOWNLOAD = [2021, 2022]


def download_one_month(year, month, save_folder):
    """
    Downloads flight data for a single month from the BTS website.
    Extracts the CSV and returns its path.
    """

    zip_filename = (
        f"On_Time_Reporting_Carrier_On_Time_Performance_"
        f"1987_present_{year}_{month}.zip"
    )

    download_url = BTS_WEBSITE + zip_filename

    zip_save_path = os.path.join(save_folder, zip_filename)
    csv_save_path = zip_save_path.replace(".zip", ".csv")

    if os.path.exists(csv_save_path):
        print(f"Already downloaded: {year}-{month:02d} (skipping)")
        return csv_save_path

    print(f"Downloading {year}-{month:02d}")
    print(f"From: {download_url}")

    try:
        response = requests.get(download_url, stream=True, timeout=120)
        response.raise_for_status()

        # Download in chunks (1MB)
        with open(zip_save_path, "wb") as zip_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    zip_file.write(chunk)

        print(f"Downloaded: {zip_filename}")

        # Extract ZIP
        with zipfile.ZipFile(zip_save_path, "r") as zip_ref:
            csv_inside = [name for name in zip_ref.namelist() if name.endswith(".csv")][0]
            zip_ref.extract(csv_inside, save_folder)

            extracted_path = os.path.join(save_folder, csv_inside)
            os.rename(extracted_path, csv_save_path)

        # Remove ZIP file after extraction
        os.remove(zip_save_path)

        print(f"Extracted CSV: {csv_save_path}")

        return csv_save_path

    except Exception as error:
        print(f"ERROR downloading {year}-{month:02d}: {error}")
        return None


# Run download
for year in YEARS_TO_DOWNLOAD:
    print(f"\nYear {year}")

    for month in range(1, 4):
        download_one_month(year, month, LOCAL_SAVE_FOLDER)