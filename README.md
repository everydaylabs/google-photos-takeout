# Google Photos Takeout 🛍️ 🛒

A project where I want to analyze, organize and cleanup my Google Photos Takeout exports worth 200+ Gigs

## Analyze Takeout

Analyze the photos and media for valid media files who extensions are defined in the config.ini. Helps understanding how many files are non media files and how to accommodate them if required

## Organize Takeout

Organizes the valid media files in the target directory as `year/month/day`. Files with missing DateTimeOriginal EXIF metadata are grouped into a `undated` folder under the target. Still working on extracting other DateTime files for undated files to reorganize them in the second pass

## Cleanup Takeout

Recursively iterate the original takeout directory (assuming most media files have been moved into a new organized target directory) and deletes the folders if it only contains non-media files whose count matches the total count of files in the directory
