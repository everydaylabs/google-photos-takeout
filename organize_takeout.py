import os
import shutil
import uuid
import sys
import configparser
import logging
import json
from datetime import datetime
from exiftool import ExifToolHelper
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

datetime_tags = ['EXIF:DateTimeOriginal', 'EXIF:CreateDate', 'EXIF:ModifyDate', 'EXIF:DateTimeDigitized', 'QuickTime:CreateDate', 'ICC_Profile:ProfileDateTime']

class OrganizeTakeout:
    def __init__(self, config_path):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.photo_extensions = tuple(self.config.get('Extensions', 'PICTURE_EXTENSIONS').split(', '))
        self.video_extensions = tuple(self.config.get('Extensions', 'VIDEO_EXTENSIONS').split(', '))
        self.files_without_datetime = []  # List to store filenames without datetime

    def get_photo_date(self, file_path):
        """Get the date the photo was taken using ExifToolHelper."""
        with ExifToolHelper() as et:
            metadata = et.get_tags(files=[file_path], tags=datetime_tags)
            for tag in datetime_tags:
                if tag in metadata[0]:
                    date_str = metadata[0][tag]
                    logging.info(f"DateTime for {file_path}: {date_str}")
                    return datetime.strptime(date_str, '%Y:%m:%d %H:%M:%S')
            logging.info(f"No DateTime found for {file_path}")
            self.files_without_datetime.append(file_path)  # Add to list if no datetime found
        return None

    def export_to_json(self, filename='files_without_datetime.json'):
        """Exports filenames without datetime to a JSON file."""
        with open(filename, 'w') as f:
            json.dump(self.files_without_datetime, f, indent=4)
        logging.info(f"Exported filenames without datetime to {filename}")

    def process_file(self, file_path, destination_folder):
        """Process a single file by moving it to the appropriate folder based on its date."""
        try:
            date_taken = self.get_photo_date(file_path)
            if date_taken:
                year = date_taken.year
                month = f"{date_taken.month:02d}"
                day = f"{date_taken.day:02d}"
                target_folder = os.path.join(destination_folder, str(year), month, day)
            else:
                target_folder = os.path.join(destination_folder, "undated")

            extension = os.path.splitext(file_path)[1]  # Capture the file extension
            new_filenpath = f"{uuid.uuid4()}{extension}"  # Create a new filename with UUID

            if not os.path.exists(target_folder):
                os.makedirs(target_folder)
            shutil.move(file_path, os.path.join(target_folder, os.path.basename(new_filenpath)))
            logging.info(f"Moved {file_path} to {target_folder} {new_filenpath}")
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")

    def organize_photos_by_date(self, source_folder, destination_folder, max_workers=4):
        """Organize photos by year, month, and day based on EXIF data using parallel processing."""
        files_to_process = []
        for root, _, files in os.walk(source_folder):
            for file in files:
                if file.lower().endswith(self.photo_extensions + self.video_extensions):
                    files_to_process.append(os.path.join(root, file))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.process_file, file_path, destination_folder): file_path for file_path in files_to_process}
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error in future execution: {e}")
        self.export_to_json()  # Export files without datetime after processing

def main():
    parser = argparse.ArgumentParser(description="Organize photos by year, month, and day based on EXIF data.")
    parser.add_argument('source_folder', type=str, help='Path to the source folder containing photos')
    parser.add_argument('destination_folder', type=str, help='Path to the destination folder to organize photos')
    parser.add_argument('--config', type=str, default='config.ini', help='Path to the configuration file')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    
    args = parser.parse_args()
    
    organizer = OrganizeTakeout(args.config)
    
    if not os.path.isdir(args.source_folder):
        logging.error(f"The path {args.source_folder} is not a valid directory.")
        sys.exit(1)
    
    organizer.organize_photos_by_date(args.source_folder, args.destination_folder, args.workers)

if __name__ == "__main__":
    main()