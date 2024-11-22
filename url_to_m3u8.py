


import os
import csv
import shutil
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re
import boto3
from botocore.exceptions import NoCredentialsError


# AWS S3 Configuration
AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
S3_BUCKET_NAME = ''


def upload_to_s3(local_path, s3_path):
    """Uploads a file or folder to S3."""
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    try:
        if os.path.isdir(local_path):
            # Upload each file in the folder
            for root, _, files in os.walk(local_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_path, local_path)
                    s3.upload_file(full_path, S3_BUCKET_NAME, f"{s3_path}/{relative_path}")
        else:
            # Upload single file
            s3.upload_file(local_path, S3_BUCKET_NAME, s3_path)

        return True
    except NoCredentialsError:
        return False
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return False


def delete_local_folder(folder_path):
    """Deletes a local folder."""
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)


def file_operation(output_path, r):
    with open(output_path, 'wb') as f:
        total_size = int(r.headers.get('content-length', 0))
        with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))
    return True


def is_valid_url(url):
    pattern = re.compile(r'^https?://(?:www\.)?[\w-]+(?:\.[\w-]+)+[\w.,@?^=%&:/~+#-]*$')
    return bool(pattern.match(url))


def parse_master_playlist_and_update(master_playlist_path, url):
    with open(master_playlist_path, 'r') as f:
        lines = f.readlines()

    variants = {}
    for i, line in enumerate(lines):
        if line.startswith('#EXT-X-STREAM-INF'):
            bandwidth_match = re.search(r'BANDWIDTH=(\d+)', line)
            if bandwidth_match:
                bandwidth = int(bandwidth_match.group(1))
                file_name = lines[i + 1].strip()
                variants[bandwidth] = file_name

    if not variants:
        raise Exception("No valid variant found in master.m3u8")

    max_bandwidth = max(variants.keys())
    best_variant = variants[max_bandwidth]

    base_url = url.rsplit('/', 1)[0]
    best_variant_url = f"{base_url}/{best_variant}"

    r = requests.get(best_variant_url, stream=True)
    file_operation(master_playlist_path, r)

    return master_playlist_path


def download_m3u8_and_ts(url, output_dir):
    master_playlist_path = os.path.join(output_dir, 'master.m3u8')
    r = requests.get(url, stream=True)
    file_operation(master_playlist_path, r)

    master_playlist_path = parse_master_playlist_and_update(master_playlist_path, url)

    with open(master_playlist_path, 'r') as f:
        lines = f.readlines()

    ts_files = [line.strip() for line in lines if line and not line.startswith('#')]

    ts_output_dir = os.path.join(output_dir, 'ts_files')
    if not os.path.exists(ts_output_dir):
        os.makedirs(ts_output_dir)

    updated_lines = []
    for line in lines:
        if line.strip() in ts_files:
            ts_file_name = os.path.basename(line.strip())
            updated_lines.append(f"ts_files/{ts_file_name}\n")
        else:
            updated_lines.append(line)

    with open(master_playlist_path, 'w') as f:
        f.writelines(updated_lines)

    for ts_file in tqdm(ts_files, desc="Downloading .ts files"):
        ts_url = f"{url.rsplit('/', 1)[0]}/{ts_file}"
        ts_output_path = os.path.join(ts_output_dir, os.path.basename(ts_file))
        with requests.get(ts_url, stream=True) as ts_r:
            file_operation(ts_output_path, ts_r)

    return ts_files


def process_url(row):
    url = row['real_live_url']
    title = row['title'].replace(' ', '_')
    filename = f"{row['id']}__{title}"
    output_dir = os.path.join('video_files', filename)

    if not is_valid_url(url):
        return filename, "failed", f"Invalid URL: {url}", "failed"

    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        ts_files = download_m3u8_and_ts(url, output_dir)
        if ts_files:
            # Upload to S3
            s3_upload_status = upload_to_s3(output_dir, filename)
            s3_status = "success" if s3_upload_status else "failed"

            # Delete local video if uploaded to S3
            if s3_status == "success":
                delete_local_folder(output_dir)

            return filename, "success", None, s3_status
        else:
            return filename, "failed", "No .ts files found in playlist", "failed"
    except Exception as e:
        return filename, "failed", f"Error processing URL {url}: {str(e)}", "failed"


def main():
    if not os.path.exists('video_files'):
        os.makedirs('video_files')

    input_csv_path = './temp.csv'
    output_csv_path = 'video_download_status.csv'

    with open(input_csv_path, 'r') as csvfile, open(output_csv_path, 'w', newline='') as output_csv:
        fieldnames = ['id', 'title', 'real_live_url', 'status', 'error', 'uploaded_to_S3']
        reader = csv.DictReader(csvfile, fieldnames=fieldnames[:3])
        writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        writer.writeheader()
        next(reader)
        urls = list(reader)

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(process_url, row) for row in urls]
            for future in as_completed(futures):
                filename, status, error_message, s3_status = future.result()
                row = {
                    'id': filename.split('__')[0],
                    'title': filename.split('__')[1].replace('_', ' '),
                    'real_live_url': [row['real_live_url'] for row in urls if row['id'] in filename][0],
                    'status': status,
                    'error': error_message if error_message else "",
                    'uploaded_to_S3': s3_status
                }
                writer.writerow(row)


if __name__ == "__main__":
    main()
