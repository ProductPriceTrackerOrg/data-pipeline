"""
LifeMobile Scraper - Main Orchestrator
Runs all 4 scrapers in parallel, monitors completion, then merges and uploads to Azure
"""

import subprocess
import os
import time
import sys
from datetime import datetime
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ScraperOrchestrator:
    """Orchestrates parallel scraping, monitoring, and data processing"""

    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent
        self.scripts = [
            {
                "name": "Script 1",
                "file": "scripts/script1.py",
                "description": "Main Categories + Smartphones",
                "json_file": "lifemobile_products_script1.json",
                "completion_marker": "script1.complete",
                "process": None,
            },
            {
                "name": "Script 2",
                "file": "scripts/script2_accessories.py",
                "description": "Accessories",
                "json_file": "lifemobile_products_script2.json",
                "completion_marker": "script2.complete",
                "process": None,
            },
            {
                "name": "Script 3",
                "file": "scripts/script3_brands.py",
                "description": "Major Brands",
                "json_file": "lifemobile_products_script3.json",
                "completion_marker": "script3.complete",
                "process": None,
            },
            {
                "name": "Script 4",
                "file": "scripts/script4_misc.py",
                "description": "Other Brands + Misc",
                "json_file": "lifemobile_products_script4.json",
                "completion_marker": "script4.complete",
                "process": None,
            },
        ]
        self.start_time = None
        self.merge_script = self.base_dir / "merge_json_files.py"

    def cleanup_old_files(self):
        """Delete old JSON files and completion markers"""
        logger.info("🧹 Cleaning up old files...")

        files_to_delete = [
            self.base_dir / "lifemobile_products_script1.json",
            self.base_dir / "lifemobile_products_script2.json",
            self.base_dir / "lifemobile_products_script3.json",
            self.base_dir / "lifemobile_products_script4.json",
            self.base_dir / "lifemobile_products_merged.json",
            self.base_dir / "script1.complete",
            self.base_dir / "script2.complete",
            self.base_dir / "script3.complete",
            self.base_dir / "script4.complete",
        ]

        deleted_count = 0
        for file_path in files_to_delete:
            if file_path.exists():
                try:
                    file_path.unlink()
                    deleted_count += 1
                except Exception as e:
                    rel_path = file_path.relative_to(self.base_dir)
                    logger.warning(f"Could not delete {rel_path}: {e}")

        if deleted_count > 0:
            logger.info(f"✅ Cleaned up {deleted_count} old files")
        else:
            logger.info("✅ No old files to clean")

    def start_scrapers(self):
        """Start all 4 scrapers in parallel"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("🚀 STARTING PARALLEL SCRAPING")
        logger.info("=" * 60)

        self.start_time = datetime.now()

        for script in self.scripts:
            logger.info(f"Starting {script['name']} ({script['description']})...")

            try:
                script_path = self.base_dir / script["file"]
                # Start process in separate visible console window
                process = subprocess.Popen(
                    [sys.executable, str(script_path)],
                    cwd=self.base_dir,
                    creationflags=(
                        subprocess.CREATE_NEW_CONSOLE if os.name == "nt" else 0
                    ),
                )
                script["process"] = process
                logger.info(f"✅ {script['name']} started (PID: {process.pid})")
                time.sleep(1)  # Small delay between starts

            except Exception as e:
                logger.error(f"❌ Failed to start {script['name']}: {e}")
                return False

        logger.info("")
        logger.info("✅ All 4 scripts launched successfully!")
        logger.info("⏳ Monitoring completion...")
        logger.info("")
        return True

    def get_script_status(self, script):
        """Get current status of a script"""
        completion_marker = self.base_dir / script["completion_marker"]
        json_file = self.base_dir / script["json_file"]

        # Check completion marker first
        if completion_marker.exists():
            size = self.get_file_size(json_file)
            return "COMPLETED", size

        # Check if JSON file exists (script is running)
        if json_file.exists():
            size = self.get_file_size(json_file)
            return "RUNNING", size

        # Check if process is still alive
        if script["process"] and script["process"].poll() is None:
            return "STARTING", 0

        # Process finished but no marker - might be error
        if script["process"] and script["process"].poll() is not None:
            return "ERROR", 0

        return "WAITING", 0

    def get_file_size(self, filename):
        """Get file size in human-readable format"""
        path = filename if isinstance(filename, Path) else Path(filename)

        if not path.exists():
            return "0 B"

        size = path.stat().st_size
        for unit in ["B", "KB", "MB", "GB"]:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"

    def monitor_progress(self, check_interval=10):
        """Monitor scraping progress until all scripts complete"""
        logger.info("=" * 60)
        logger.info("📊 MONITORING PROGRESS")
        logger.info("=" * 60)

        last_status = {}
        iteration = 0

        while True:
            iteration += 1
            time.sleep(check_interval)

            # Clear screen (optional)
            if os.name == "nt":
                os.system("cls")
            else:
                os.system("clear")

            elapsed = datetime.now() - self.start_time
            elapsed_str = str(elapsed).split(".")[0]  # Remove microseconds

            print("")
            print("=" * 60)
            print("📊 SCRAPING PROGRESS MONITOR")
            print("=" * 60)
            print(f"⏱️  Running time: {elapsed_str}")
            print(f"🔄 Check #{iteration} (every {check_interval} seconds)")
            print("")

            all_completed = True

            for script in self.scripts:
                status, size = self.get_script_status(script)

                # Status icons
                if status == "COMPLETED":
                    icon = "✅"
                    all_completed = all_completed and True
                elif status == "RUNNING":
                    icon = "⏳"
                    all_completed = False
                elif status == "STARTING":
                    icon = "🔄"
                    all_completed = False
                elif status == "ERROR":
                    icon = "❌"
                    all_completed = False
                else:
                    icon = "⏸️"
                    all_completed = False

                # Display status
                status_line = f"{icon} {script['name']}: {status:<12} - {size:>10}"
                print(status_line)

                # Log status changes
                key = script["name"]
                if key not in last_status or last_status[key] != status:
                    logger.info(f"{icon} {script['name']}: {status} - {size}")
                    last_status[key] = status

            print("")

            # Check if all completed
            if all_completed:
                print("=" * 60)
                print("🎉 ALL SCRIPTS COMPLETED!")
                print("=" * 60)
                logger.info("🎉 All scripts completed successfully!")
                break

            # Check for errors
            error_count = sum(
                1 for s in self.scripts if self.get_script_status(s)[0] == "ERROR"
            )
            if error_count > 0:
                print(f"⚠️  {error_count} script(s) encountered errors")

            print("⏳ Waiting for all scripts to complete...")
            print("")

        elapsed = datetime.now() - self.start_time
        logger.info(f"✅ Total scraping time: {elapsed}")
        return True

    def run_merge_and_upload(self):
        """Run the merge, upload, and cleanup process"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("📦 MERGING, UPLOADING & CLEANING UP")
        logger.info("=" * 60)
        logger.info("")

        try:
            # Run merge_json_files.py
            logger.info("Running merge_json_files.py...")
            result = subprocess.run(
                [sys.executable, str(self.merge_script)],
                cwd=self.base_dir,
                capture_output=True,
                text=True,
            )

            # Print output
            if result.stdout:
                print(result.stdout)

            if result.returncode == 0:
                logger.info("✅ Merge, upload, and cleanup completed successfully!")
                return True
            else:
                logger.error("❌ Merge process failed!")
                if result.stderr:
                    logger.error(f"Error: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"❌ Error running merge process: {e}")
            logger.info(
                "💡 Note: JSON files are kept for debugging. Run 'python quick_cleanup.py' to clean manually."
            )
            return False

    def automatic_cleanup(self):
        """Automatically clean up all JSON files and completion markers"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("🧹 AUTOMATIC WORKSPACE CLEANUP")
        logger.info("=" * 60)

        # Files to delete
        files_to_delete = {
            self.base_dir / "lifemobile_products_script1.json",
            self.base_dir / "lifemobile_products_script2.json",
            self.base_dir / "lifemobile_products_script3.json",
            self.base_dir / "lifemobile_products_script4.json",
            self.base_dir / "lifemobile_products_merged.json",
            self.base_dir / "script1.complete",
            self.base_dir / "script2.complete",
            self.base_dir / "script3.complete",
            self.base_dir / "script4.complete",
        }

        # Check for additional files
        for file in self.base_dir.iterdir():
            if file.is_file() and file.name.startswith("lifemobile_products_"):
                if file.suffix in {".json", ".csv"}:
                    files_to_delete.add(file)

        # Check jsonfiles directory
        jsonfiles_dir = self.base_dir / "jsonfiles"
        if jsonfiles_dir.exists():
            for file in jsonfiles_dir.iterdir():
                if file.is_file() and file.name.startswith("lifemobile_"):
                    if file.suffix in {".json", ".csv"}:
                        files_to_delete.add(file)

        deleted_count = 0
        total_size_deleted = 0

        logger.info(f"🔍 Found {len(files_to_delete)} files to clean up")

        for file_path in sorted(files_to_delete):
            if file_path.exists():
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    size_str = self.get_file_size_formatted(file_size)
                    rel_path = file_path.relative_to(self.base_dir)
                    logger.info(f"🗑️  Deleted: {rel_path} ({size_str})")
                    deleted_count += 1
                    total_size_deleted += file_size
                except Exception as e:
                    rel_path = file_path.relative_to(self.base_dir)
                    logger.warning(f"⚠️  Could not delete {rel_path}: {e}")
            else:
                rel_path = file_path.relative_to(self.base_dir)
                logger.debug(f"📂 File not found: {rel_path}")

        if deleted_count > 0:
            total_size_str = self.get_file_size_formatted(total_size_deleted)
            logger.info(
                f"✅ Cleanup complete: {deleted_count} files deleted ({total_size_str})"
            )
            logger.info("✨ Your workspace is now clean!")
        else:
            logger.info("📂 No files to clean up - workspace was already clean")

        return deleted_count > 0

    def get_file_size_formatted(self, size_bytes):
        """Format file size in human-readable format"""
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"

    def check_prerequisites(self):
        """Check if all required files exist"""
        required_files = [
            self.base_dir / script["file"] for script in self.scripts
        ] + [self.merge_script]

        missing_files = [path for path in required_files if not path.exists()]

        if missing_files:
            logger.error("❌ Missing required files:")
            for file_path in missing_files:
                try:
                    rel_path = file_path.relative_to(self.base_dir)
                except ValueError:
                    rel_path = file_path
                logger.error(f"   - {rel_path}")
            return False

        return True

    def run(self):
        """Main execution method"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("🏁 LIFEMOBILE SCRAPER - MAIN ORCHESTRATOR")
        logger.info("=" * 60)
        logger.info("")

        # Check prerequisites
        if not self.check_prerequisites():
            logger.error("❌ Prerequisites check failed!")
            return False

        # Step 1: Cleanup
        self.cleanup_old_files()

        # Step 2: Start scrapers
        if not self.start_scrapers():
            logger.error("❌ Failed to start scrapers!")
            return False

        # Step 3: Monitor progress
        try:
            self.monitor_progress(check_interval=10)
        except KeyboardInterrupt:
            logger.warning("⚠️  Interrupted by user!")
            logger.info("Scripts are still running in background...")
            return False

        # Step 4: Merge and upload
        if not self.run_merge_and_upload():
            logger.error("❌ Merge and upload failed!")
            return False

        # Note: Automatic cleanup will happen in merge_json_files.py after successful Azure upload
        logger.info(
            "📝 Note: File cleanup will happen automatically after successful Azure upload"
        )

        # Success!
        total_time = datetime.now() - self.start_time
        logger.info("")
        logger.info("=" * 60)
        logger.info("🎉 COMPLETE SUCCESS!")
        logger.info("=" * 60)
        logger.info(f"⏱️  Total time: {total_time}")
        logger.info("✅ Data scraped from all sources")
        logger.info("✅ Data merged and deduplicated")
        logger.info("☁️  Data uploaded to Azure Data Lake Storage (raw-data container)")
        logger.info(
            "🗑️  All local JSON and CSV files will be cleaned up after Azure upload"
        )
        logger.info("")
        logger.info("✨ Your workspace is clean and data is safely stored in Azure!")
        logger.info("📊 Data is partitioned by source_website and scrape_date")
        logger.info("=" * 60)

        return True


def main():
    """Entry point"""
    orchestrator = ScraperOrchestrator()

    try:
        success = orchestrator.run()

        if success:
            logger.info("")
            logger.info("Press Enter to exit...")
            input()
            sys.exit(0)
        else:
            logger.error("")
            logger.error("Process failed! Check logs above for details.")
            logger.error("Press Enter to exit...")
            input()
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        logger.error("Press Enter to exit...")
        input()
        sys.exit(1)


if __name__ == "__main__":
    main()
