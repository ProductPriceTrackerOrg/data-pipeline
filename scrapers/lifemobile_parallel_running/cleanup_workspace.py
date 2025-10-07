#!/usr/bin/env python3
"""
LifeMobile Scraper - Workspace Cleanup Tool
Removes all JSON files, completion markers, and temporary files from the workspace
"""

import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format"""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def cleanup_workspace():
    """Clean up all scraper-generated files"""
    logger.info("🧹 Starting workspace cleanup...")
    logger.info("=" * 60)

    # Define files to delete
    files_to_delete = []

    # JSON files in root directory
    json_files = [
        "lifemobile_products_script1.json",
        "lifemobile_products_script2.json",
        "lifemobile_products_script3.json",
        "lifemobile_products_script4.json",
        "lifemobile_products_merged.json",
    ]
    files_to_delete.extend(json_files)

    # Completion marker files
    completion_markers = [
        "script1.complete",
        "script2.complete",
        "script3.complete",
        "script4.complete",
    ]
    files_to_delete.extend(completion_markers)

    # Check for additional lifemobile files
    current_dir = Path(".")
    for file_path in current_dir.glob("lifemobile_products_*.json"):
        if str(file_path.name) not in files_to_delete:
            files_to_delete.append(str(file_path.name))

    # Check for CSV files
    for file_path in current_dir.glob("lifemobile_products_*.csv"):
        files_to_delete.append(str(file_path.name))

    # Check jsonfiles directory
    jsonfiles_dir = Path("jsonfiles")
    if jsonfiles_dir.exists():
        for file_path in jsonfiles_dir.glob("lifemobile_*.json"):
            files_to_delete.append(str(file_path))
        for file_path in jsonfiles_dir.glob("lifemobile_*.csv"):
            files_to_delete.append(str(file_path))

    # Remove duplicates and sort
    files_to_delete = sorted(list(set(files_to_delete)))

    logger.info(f"🔍 Found {len(files_to_delete)} files to delete:")

    # Show what will be deleted
    total_size = 0
    existing_files = []
    for file_path in files_to_delete:
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            total_size += file_size
            existing_files.append(file_path)
            logger.info(f"   📄 {file_path} ({format_file_size(file_size)})")
        else:
            logger.debug(f"   ❓ {file_path} (not found)")

    if not existing_files:
        logger.info("✅ Workspace is already clean! No files to delete.")
        return True

    logger.info(f"💾 Total size to be deleted: {format_file_size(total_size)}")
    logger.info("")

    # Ask for confirmation
    try:
        confirmation = (
            input("❓ Do you want to delete these files? (y/N): ").lower().strip()
        )
        if confirmation not in ["y", "yes"]:
            logger.info("❌ Cleanup cancelled by user")
            return False
    except KeyboardInterrupt:
        logger.info("\n❌ Cleanup cancelled by user")
        return False

    # Delete files
    logger.info("🗑️  Starting file deletion...")
    deleted_count = 0
    deleted_size = 0
    errors = []

    for file_path in existing_files:
        try:
            file_size = os.path.getsize(file_path)
            os.remove(file_path)
            deleted_count += 1
            deleted_size += file_size
            logger.info(f"✅ Deleted: {file_path}")
        except Exception as e:
            error_msg = f"❌ Failed to delete {file_path}: {e}"
            logger.error(error_msg)
            errors.append(error_msg)

    # Summary
    logger.info("=" * 60)
    logger.info("🧹 CLEANUP SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Files deleted: {deleted_count}")
    logger.info(f"💾 Space freed: {format_file_size(deleted_size)}")

    if errors:
        logger.warning(f"⚠️  Errors encountered: {len(errors)}")
        for error in errors:
            logger.warning(f"   {error}")
        return False
    else:
        logger.info("🎉 Workspace cleaned successfully!")
        logger.info(
            "✨ Your workspace is now clean and ready for the next scraping session!"
        )
        return True


def main():
    """Main function"""
    logger.info("LifeMobile Scraper - Workspace Cleanup Tool")
    logger.info("=" * 60)

    try:
        success = cleanup_workspace()

        if success:
            logger.info("\n✅ Cleanup completed successfully!")
            return 0
        else:
            logger.error("\n❌ Cleanup completed with errors!")
            return 1

    except KeyboardInterrupt:
        logger.info("\n❌ Cleanup interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
