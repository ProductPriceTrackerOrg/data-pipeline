# 🚀 Main.py - Automated Scraper Orchestrator

## 📋 Overview

`main.py` is a Python-based orchestrator that manages the entire scraping workflow:

- ✅ Starts all 4 scrapers in parallel
- ✅ Monitors completion with real-time progress
- ✅ Automatically merges, uploads, and cleans up
- ✅ Better error handling and logging
- ✅ Cross-platform (Windows, Linux, Mac)

## 🎯 Usage

### **Simple Command:**

```bash
python main.py
```

That's it! Everything else is automatic.

---

## 🔄 Complete Workflow

```
┌─────────────────────────────────────────────────────────┐
│  1. Prerequisites Check                                 │
│     ✅ Verify all script files exist                    │
│     ✅ Verify merge script exists                       │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  2. Cleanup Old Files                                   │
│     🗑️ Delete old JSON files                            │
│     🗑️ Delete old completion markers                    │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  3. Start All 4 Scrapers (Parallel)                    │
│     🚀 Launch Newone.py (Script 1)                      │
│     🚀 Launch script2_accessories.py                    │
│     🚀 Launch script3_brands.py                         │
│     🚀 Launch script4_misc.py                           │
│     ⏱️ Track process IDs                                │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  4. Monitor Progress (Every 10 seconds)                 │
│     📊 Check completion markers                         │
│     📊 Show file sizes                                  │
│     📊 Display status (STARTING/RUNNING/COMPLETED)      │
│     📊 Calculate elapsed time                           │
│     📊 Clear screen and refresh display                 │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  5. Wait for ALL 4 Completion Markers                   │
│     ⏳ Loop until all 4 scripts done                    │
│     ✅ Verify all completion markers exist              │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  6. Merge, Upload, Cleanup                              │
│     📦 Run merge_json_files.py                          │
│     ☁️ Upload to Azure Data Lake Storage                │
│     🗑️ Delete all local files                           │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│  7. Success! ✅                                          │
│     📊 Show total time                                  │
│     ✨ Clean workspace                                  │
│     ☁️ Data safely in Azure                             │
└─────────────────────────────────────────────────────────┘
```

---

## 📊 Real-Time Progress Display

### During Scraping:

```
============================================================
📊 SCRAPING PROGRESS MONITOR
============================================================
⏱️  Running time: 0:03:45
🔄 Check #23 (every 10 seconds)

✅ Script 1: COMPLETED   -    125.4 KB
⏳ Script 2: RUNNING     -     98.2 KB
✅ Script 3: COMPLETED   -     87.5 KB
⏳ Script 4: RUNNING     -    110.1 KB

⏳ Waiting for all scripts to complete...
```

### Status Icons:

- 🔄 `STARTING` - Script just launched
- ⏳ `RUNNING` - Script is actively scraping
- ✅ `COMPLETED` - Script finished (marker file exists)
- ❌ `ERROR` - Script failed
- ⏸️ `WAITING` - Script not yet started

### When Complete:

```
============================================================
🎉 ALL SCRIPTS COMPLETED!
============================================================

============================================================
📦 MERGING, UPLOADING & CLEANING UP
============================================================

[Merge process output...]

============================================================
🎉 COMPLETE SUCCESS!
============================================================
⏱️  Total time: 0:05:32
✅ Data scraped from all sources
✅ Data merged and deduplicated
☁️  Data uploaded to Azure Data Lake Storage
🗑️  All local files cleaned up

✨ Your workspace is clean and data is safely in Azure!
============================================================
```

---

## ✨ Features

### 1. **Smart Process Management**

- Starts all 4 scripts as background processes
- Tracks process IDs
- Monitors process status
- Handles process failures gracefully

### 2. **Real-Time Monitoring**

- Live status updates every 10 seconds
- Shows file sizes as they grow
- Clear visual progress indicators
- Auto-refreshing display

### 3. **Completion Detection**

- Uses completion marker files (100% reliable)
- Checks all 4 markers before proceeding
- No premature merge triggers
- Safe and robust

### 4. **Error Handling**

- Detects script failures
- Shows error counts
- Provides detailed logs
- Allows for debugging

### 5. **Automatic Cleanup**

- Deletes old files before starting
- Cleans up after successful upload
- Removes completion markers
- Maintains clean workspace

### 6. **Cross-Platform**

- Works on Windows, Linux, Mac
- Uses subprocess for process management
- Platform-specific screen clearing
- Universal Python code

---

## 🛠️ Technical Details

### Class: `ScraperOrchestrator`

#### Methods:

**`cleanup_old_files()`**

- Deletes old JSON files and completion markers
- Ensures clean start

**`start_scrapers()`**

- Launches all 4 scripts in parallel
- Uses `subprocess.Popen` for background execution
- Tracks process IDs
- Returns success/failure status

**`get_script_status(script)`**

- Returns: `(status, file_size)`
- Status: COMPLETED, RUNNING, STARTING, ERROR, WAITING
- Checks completion marker first
- Then checks JSON file existence
- Finally checks process status

**`monitor_progress(check_interval=10)`**

- Monitors all scripts until completion
- Updates display every 10 seconds
- Shows elapsed time
- Logs status changes
- Detects when all scripts complete

**`run_merge_and_upload()`**

- Executes `merge_json_files.py`
- Captures output and errors
- Returns success/failure status

**`check_prerequisites()`**

- Verifies all required files exist
- Prevents execution if files missing

**`run()`**

- Main execution method
- Orchestrates entire workflow
- Returns success/failure status

---

## 📦 Requirements

### Files Required:

- ✅ `main.py` (this file)
- ✅ `Newone.py` (Script 1)
- ✅ `script2_accessories.py` (Script 2)
- ✅ `script3_brands.py` (Script 3)
- ✅ `script4_misc.py` (Script 4)
- ✅ `merge_json_files.py` (Merge & Upload)

### Python Version:

- Python 3.7 or higher

### Dependencies:

- Standard library only (no external packages needed!)
- `subprocess`, `os`, `time`, `logging`, `datetime`, `pathlib`

---

## 🎮 Usage Examples

### Basic Usage:

```bash
python main.py
```

### With Virtual Environment:

```bash
# Windows
venv\Scripts\activate
python main.py

# Linux/Mac
source venv/bin/activate
python main.py
```

### From Different Directory:

```bash
cd "d:\My Campus Work\Sem 05\Projects\Data Science Project Git\data-pipeline\scrapers\lifemobile"
python main.py
```

---

## 🔍 Troubleshooting

### Issue: "Missing required files"

**Solution:** Ensure all script files are in the same directory as `main.py`

### Issue: Scripts not starting

**Solution:**

- Check Python path in error messages
- Verify Python is in system PATH
- Run `python --version` to confirm Python installation

### Issue: Merge process fails

**Solution:**

- Check Azure connection string in `.env` file
- Verify `merge_json_files.py` is working
- Check error messages in logs

### Issue: Process hangs

**Solution:**

- Press `Ctrl+C` to interrupt
- Check individual script windows for errors
- Verify internet connection
- Check if scripts are actually running

---

## 🆚 Comparison with Batch File

| Feature              | Batch File      | main.py          |
| -------------------- | --------------- | ---------------- |
| **Cross-Platform**   | ❌ Windows only | ✅ All platforms |
| **Progress Display** | Basic           | Advanced         |
| **Error Handling**   | Limited         | Comprehensive    |
| **Process Tracking** | Window titles   | Process IDs      |
| **Screen Refresh**   | Manual          | Automatic        |
| **Logging**          | Basic           | Detailed         |
| **Code Quality**     | Shell script    | Python OOP       |
| **Maintainability**  | Low             | High             |
| **Extensibility**    | Limited         | Easy             |

---

## 🚀 Advantages of main.py

1. ✅ **Better Process Management** - Direct subprocess control
2. ✅ **Cleaner Output** - Auto-refreshing display
3. ✅ **Better Logging** - Structured logging with timestamps
4. ✅ **Error Detection** - Catches and reports errors
5. ✅ **Cross-Platform** - Works on any OS
6. ✅ **Maintainable** - Object-oriented Python code
7. ✅ **Extensible** - Easy to add features
8. ✅ **Professional** - Production-ready code quality

---

## 📝 Configuration

### Change Check Interval:

Edit the `monitor_progress()` call in `run()` method:

```python
self.monitor_progress(check_interval=10)  # Change to 5, 15, 30, etc.
```

### Change Max Products:

Edit each script file:

```python
self.max_products = 50  # Change in Newone.py, script2, script3, script4
```

### Disable Screen Clearing:

Comment out in `monitor_progress()`:

```python
# if os.name == 'nt':
#     os.system('cls')
# else:
#     os.system('clear')
```

---

## 📚 Additional Resources

- **Batch File Alternative:** `run_parallel_scraping.bat` (deprecated)
- **PowerShell Alternative:** `run_parallel_scraping.ps1` (deprecated)
- **Main Documentation:** `README.md`

---

## ✅ Recommended Usage

**Use `main.py` for:**

- ✅ Production deployments
- ✅ Scheduled/automated runs
- ✅ Better monitoring and logging
- ✅ Cross-platform compatibility
- ✅ Integration with other Python tools

**Use batch/PowerShell for:**

- Quick manual runs
- Simple testing
- Windows-only environments
- When Python subprocess is problematic

---

## 🎉 Summary

`main.py` provides a robust, professional-grade orchestrator for your scraping workflow:

**Single Command:**

```bash
python main.py
```

**Complete Automation:**

- 🚀 Starts all scrapers
- 📊 Monitors progress
- 📦 Merges data
- ☁️ Uploads to Azure
- 🗑️ Cleans up files
- ✅ Reports success

**No manual intervention needed!** 🎯

---

**Created:** October 6, 2025  
**Version:** 1.0  
**Status:** ✅ Production Ready  
**Platform:** Cross-platform (Windows, Linux, Mac)
