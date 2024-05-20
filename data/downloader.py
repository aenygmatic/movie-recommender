import logging
import os
import zipfile

import requests

logger = logging.getLogger(__name__)
package_dir = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level=logging.CRITICAL)


def download(dataset: str):
    if dataset in ["ml-100k", "ml-10m", "ml-25m"]:
        return __download_and_extract(
            url=f"https://files.grouplens.org/datasets/movielens/{dataset}.zip",
            target_dir=dataset,
            zip_filename=f"{dataset}.zip")
    else:
        raise ValueError(f"Dataset {dataset} not supported")


def __download_and_extract(url: str, target_dir: str, zip_filename: str):
    zip_filepath = os.path.join(package_dir, zip_filename)
    if not os.path.exists(os.path.join(package_dir, target_dir)):
        os.makedirs(os.path.dirname(zip_filepath), exist_ok=True)

        __download_dataset(url, zip_filepath)
        __extract_dataset(zip_filepath, package_dir)

        os.remove(zip_filepath)
    else:
        logger.info(f"The dataset is already downloaded and extracted in the '{target_dir}' directory.")


def __download_dataset(url, filepath):
    logger.info(f"Downloading dataset from {url} to {filepath}")
    response = requests.get(url, stream=True)
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=128):
            f.write(chunk)
    logger.info(f"Download complete.")


def __extract_dataset(zip_filepath, extract_to):
    logger.info(f"Extracting dataset from {zip_filepath} to {extract_to}")

    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

    logger.info(f"Extraction complete.")
