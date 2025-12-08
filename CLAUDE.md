# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project processes Google Photos Takeout exports (200+ GB) through a three-stage pipeline:

1. **Analyze** (`analyze_takeout.py`) - Scans takeout data to identify media files vs non-media files
2. **Organize** (`organize_takeout.py`) - Moves valid media files to organized folder structure by date (`year/month/day`)
3. **Cleanup** (`cleanup_takeout.py`) - Removes empty folders and non-media files from original takeout directory

## Architecture

### Core Components

- **AnalyzeTakeout class** (`analyze_takeout.py:16`) - Analyzes file types and generates metrics
- **OrganizeTakeout class** (`organize_takeout.py:18`) - Handles date-based file organization using EXIF data
- **CleanupTakeout class** (`cleanup_takeout.py:14`) - Manages cleanup of processed takeout directories

### Configuration System

Media file extensions are defined in `config.ini` with separate lists for photos and videos. All scripts load these extensions dynamically rather than hardcoding them.

### EXIF Processing

The system uses PyExifTool to extract datetime metadata from media files. Multiple datetime tags are checked in priority order (`organize_takeout.py:16`):

- EXIF:DateTimeOriginal
- EXIF:CreateDate  
- EXIF:ModifyDate
- EXIF:DateTimeDigitized
- QuickTime:CreateDate
- ICC_Profile:ProfileDateTime

Files without extractable datetime are moved to an `undated` folder and tracked in `files_without_datetime.json`.

## Development Commands

### Dependencies

```bash
pip3 install -r requirements.txt
```

### Running Scripts

**Analyze takeout data:**

```bash
python3 analyze_takeout.py <takeout_folder> <export_folder>
```

**Organize media files by date:**

```bash
python3 organize_takeout.py <source_folder> <destination_folder> [--config config.ini] [--workers N]
```

**Cleanup processed takeout:**

```bash
python3 cleanup_takeout.py <root_folder> <output_file> <exif_metadata_file> [--dry-run]
```

### Testing

```bash
python3 -m pytest test_analyze_takeout.py
```

## Key Files

- `config.ini` - Defines supported media file extensions
- `files_without_datetime.json` - Generated list of files without EXIF datetime data
- `requirements.txt` - Python dependencies (PyExifTool, pytest)