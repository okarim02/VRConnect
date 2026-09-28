// /src/output/file.rs
// Module: output.file
// Purpose: File output for complete data recording with rotation and archiving

use crate::domain::ProcessedData;
use crate::error::{Result, VitalError};
use chrono::{DateTime, Local, NaiveDate};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// ID SRS: SRS-MOD-FILEOUTPUT-001
/// Title: FileOutput
///
/// Description: VRConnect shall provide file output for complete data recording
/// with automatic rotation (500MB), archiving (5GB threshold), and disk monitoring.
///
/// Version: V1.0
pub struct FileOutput {
    base_path: PathBuf,
    max_size_bytes: u64,
    archive_threshold_bytes: u64,
    critical_disk_percent: u8,
    current_file: Arc<RwLock<Option<ActiveFile>>>,
}

/// Active file information
struct ActiveFile {
    path: PathBuf,
    handle: File,
    start_time: DateTime<Local>,
    current_size: u64,
    current_date: NaiveDate,
}

impl FileOutput {
    /// ID SRS: SRS-FN-FILEOUTPUT-001
    /// Title: new
    ///
    /// Description: VRConnect shall construct a FileOutput instance with
    /// configuration parameters and initialize directory structure.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `base_path` - Base directory path
    /// * `max_size_mb` - Maximum file size in MB
    /// * `archive_threshold_gb` - Archive threshold in GB
    /// * `critical_disk_percent` - Critical disk usage percentage
    ///
    /// # Returns
    /// New FileOutput instance or error
    pub async fn new(
        base_path: String,
        max_size_mb: u64,
        archive_threshold_gb: u64,
        critical_disk_percent: u8,
    ) -> Result<Self> {
        let base = PathBuf::from(base_path);

        // Create directory structure
        let data_dir = base.join("data");
        let archive_dir = base.join("archive");

        fs::create_dir_all(&data_dir).map_err(VitalError::Io)?;
        fs::create_dir_all(&archive_dir).map_err(VitalError::Io)?;

        log::info!("🗃️  File output initialized: {}", base.display());
        log::info!("  Data directory: {}", data_dir.display());
        log::info!("  Archive directory: {}", archive_dir.display());

        let file_output = Self {
            base_path: base,
            max_size_bytes: max_size_mb * 1024 * 1024,
            archive_threshold_bytes: archive_threshold_gb * 1024 * 1024 * 1024,
            critical_disk_percent,
            current_file: Arc::new(RwLock::new(None)),
        };

        // Archive old files on startup
        file_output.archive_old_files_on_startup().await?;

        Ok(file_output)
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-002
    /// Title: archive_old_files_on_startup
    ///
    /// Description: VRConnect shall archive all files from previous days
    /// found in data directory at application startup.
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Result indicating success or error
    async fn archive_old_files_on_startup(&self) -> Result<()> {
        let data_dir = self.base_path.join("data");
        let today = Local::now().date_naive();

        log::info!("Checking for old files to archive at startup...");

        if !data_dir.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&data_dir).map_err(VitalError::Io)?;

        for entry in entries {
            let entry = entry.map_err(VitalError::Io)?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    // Parse date from directory name (YYYYMMDD)
                    if let Ok(dir_date) = NaiveDate::parse_from_str(dir_name, "%Y%m%d") {
                        if dir_date < today {
                            log::info!("Found old directory: {} - archiving", dir_name);
                            self.archive_daily_folder(&path, dir_date).await?;
                        }
                    }
                }
            }
        }

        log::info!("✓ Old files archived at startup");
        Ok(())
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-003
    /// Title: output
    ///
    /// Description: VRConnect shall write ProcessedData to current file,
    /// handling rotation, archiving, and disk space monitoring.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `data` - Processed vital data
    ///
    /// # Returns
    /// Result indicating success or error
    pub async fn output(&self, data: &ProcessedData) -> Result<()> {
        // Check disk space first
        self.check_disk_space().await?;

        // Chaos: simulate a disk-full write failure (env-driven, non-production only).
        // Controlled by ENABLE_CHAOS_MONKEY + CHAOS_DISK_FULL. See src/utils/chaos.rs.
        if crate::utils::chaos::maybe_disk_full("file.rs") {
            return Err(VitalError::Processing(
                "[CHAOS] Simulated disk-full: write aborted".to_string(),
            ));
        }

        // Serialize to JSON line
        let json_line = serde_json::to_string(data)
            .map_err(|e| VitalError::Processing(format!("JSON serialization failed: {}", e)))?;
        let mut line = json_line;
        line.push('\n');

        let line_bytes = line.as_bytes();
        let line_size = line_bytes.len() as u64;

        // Get or create file
        let mut file_lock = self.current_file.write().await;

        // Check if we need to rotate (date change or size limit)
        let need_rotation = if let Some(ref active) = *file_lock {
            let now = Local::now();
            let date_changed = active.current_date != now.date_naive();
            let size_exceeded = active.current_size + line_size > self.max_size_bytes;

            date_changed || size_exceeded
        } else {
            true // No active file
        };

        if need_rotation {
            // Archive old day if date changed
            if let Some(ref active) = *file_lock {
                let now = Local::now();
                if active.current_date != now.date_naive() {
                    let old_date = active.current_date;
                    drop(file_lock); // Release lock for archiving
                    self.archive_previous_day(old_date).await?;
                    file_lock = self.current_file.write().await;
                }
            }

            // Close current file and create new one. `rotate_file` only swaps
            // `*file_lock`'s contents — it does NOT archive; see the comment below on
            // why that check is deliberately made outside the lock.
            let archive_check_dir = self.rotate_file(&mut file_lock).await?;

            // The date-change branch above already drops the lock before archiving
            // (`archive_previous_day`). This threshold-triggered check must do the
            // same: it can run for tens of seconds (the zip in `create_zip_archive`),
            // and every other call to `output()` blocks on `self.current_file.write()`
            // for as long as this guard is held — holding it here would stall every
            // incoming sample, not just BLE emission, for the archive's full duration.
            // Safe to check after the new file already exists in `daily_dir`: its name
            // ends in `_ongoing.json`, which `get_completed_files()` always excludes.
            if let Some(daily_dir) = archive_check_dir {
                drop(file_lock);
                self.check_and_archive_if_needed(&daily_dir).await?;
                file_lock = self.current_file.write().await;
            }
        }

        // Write to current file
        if let Some(ref mut active) = *file_lock {
            active
                .handle
                .write_all(line_bytes)
                .map_err(VitalError::Io)?;
            active.current_size += line_size;
        }

        Ok(())
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-004
    /// Title: rotate_file
    ///
    /// Description: VRConnect shall rotate current file by closing it,
    /// renaming with end timestamp, and creating new file with start timestamp.
    ///
    /// Deliberately does NOT call `check_and_archive_if_needed()` itself: that would
    /// hold the caller's `current_file` write lock for the full duration of a
    /// same-day, size-triggered archive (the zip can take tens of seconds). The date-changed rotation path in `output()` already drops
    /// the lock before archiving; this function instead returns the directory that
    /// needs checking, if any, so the caller can do the same for the size-triggered
    /// path.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `file_lock` - Mutable reference to current file lock
    ///
    /// # Returns
    /// The directory to pass to `check_and_archive_if_needed()` once the lock has
    /// been dropped, or `None` if there was no prior file to close (nothing to check).
    async fn rotate_file(&self, file_lock: &mut Option<ActiveFile>) -> Result<Option<PathBuf>> {
        let now = Local::now();
        let mut archive_check_dir = None;

        // Close and rename current file if exists
        if let Some(active) = file_lock.take() {
            drop(active.handle); // Close file

            let end_time = now;
            let new_name = format!(
                "vrconnect_{}_{}.json",
                active.start_time.format("%Y%m%d_%H%M%S"),
                end_time.format("%H%M%S")
            );

            let new_path = active.path.parent().unwrap().join(new_name);
            fs::rename(&active.path, &new_path).map_err(VitalError::Io)?;

            log::info!(
                "File rotation: {} → {}",
                active.path.file_name().unwrap().to_string_lossy(),
                new_path.file_name().unwrap().to_string_lossy()
            );

            archive_check_dir = Some(active.path.parent().unwrap().to_path_buf());
        }

        // Create new file
        let date_str = now.format("%Y%m%d").to_string();
        let time_str = now.format("%H%M%S").to_string();
        let daily_dir = self.base_path.join("data").join(&date_str);

        fs::create_dir_all(&daily_dir).map_err(VitalError::Io)?;

        let filename = format!("vrconnect_{}_{}_ongoing.json", date_str, time_str);
        let file_path = daily_dir.join(filename);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .map_err(VitalError::Io)?;

        log::info!(
            "New file started: {}",
            file_path.file_name().unwrap().to_string_lossy()
        );

        *file_lock = Some(ActiveFile {
            path: file_path,
            handle: file,
            start_time: now,
            current_size: 0,
            current_date: now.date_naive(),
        });

        Ok(archive_check_dir)
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-005
    /// Title: check_and_archive_if_needed
    ///
    /// Description: VRConnect shall check daily folder size and trigger
    /// archiving if threshold (5GB) is exceeded.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `daily_dir` - Path to daily directory
    ///
    /// # Returns
    /// Result indicating success or error
    async fn check_and_archive_if_needed(&self, daily_dir: &Path) -> Result<()> {
        let total_size = self.calculate_folder_size(daily_dir)?;

        log::debug!(
            "Daily folder size: {:.2} GB",
            total_size as f64 / (1024.0 * 1024.0 * 1024.0)
        );

        if total_size >= self.archive_threshold_bytes {
            log::info!(
                "Archive threshold reached ({:.2} GB) - archiving completed files",
                total_size as f64 / (1024.0 * 1024.0 * 1024.0)
            );

            let date = daily_dir
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| NaiveDate::parse_from_str(s, "%Y%m%d").ok())
                .ok_or_else(|| VitalError::Processing("Invalid date folder".to_string()))?;

            self.archive_completed_files(daily_dir, date).await?;
        }

        Ok(())
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-006
    /// Title: archive_previous_day
    ///
    /// Description: VRConnect shall archive all files from previous day
    /// when date changes.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `previous_date` - Date of previous day
    ///
    /// # Returns
    /// Result indicating success or error
    async fn archive_previous_day(&self, previous_date: NaiveDate) -> Result<()> {
        let date_str = previous_date.format("%Y%m%d").to_string();
        let daily_dir = self.base_path.join("data").join(&date_str);

        if daily_dir.exists() {
            log::info!("Day changed - archiving previous day: {}", date_str);
            self.archive_daily_folder(&daily_dir, previous_date).await?;
        }

        Ok(())
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-007
    /// Title: archive_daily_folder
    ///
    /// Description: VRConnect shall archive entire daily folder,
    /// including all files, and remove source files after successful archiving.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `daily_dir` - Path to daily directory
    /// * `date` - Date of folder
    ///
    /// # Returns
    /// Result indicating success or error
    async fn archive_daily_folder(&self, daily_dir: &Path, date: NaiveDate) -> Result<()> {
        // Get all JSON files
        let files = self.get_completed_files(daily_dir)?;

        if files.is_empty() {
            log::debug!("No files to archive in {}", daily_dir.display());
            return Ok(());
        }

        let (first_time, last_time) = self.get_time_range(&files)?;

        let archive_name = format!(
            "archive_{}_{}_{}.zip",
            date.format("%Y%m%d"),
            first_time,
            last_time
        );

        let archive_dir = self
            .base_path
            .join("archive")
            .join(date.format("%Y%m%d").to_string());
        fs::create_dir_all(&archive_dir).map_err(VitalError::Io)?;

        let archive_path = archive_dir.join(archive_name);

        self.create_zip_archive(&archive_path, &files).await?;

        // Remove archived files
        for file in &files {
            fs::remove_file(file).map_err(VitalError::Io)?;
        }

        // Remove daily directory if empty
        if let Ok(entries) = fs::read_dir(daily_dir) {
            if entries.count() == 0 {
                fs::remove_dir(daily_dir).map_err(VitalError::Io)?;
            }
        }

        log::info!(
            "✓ Archive created: {} ({} files compressed)",
            archive_path.file_name().unwrap().to_string_lossy(),
            files.len()
        );

        Ok(())
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-008
    /// Title: archive_completed_files
    ///
    /// Description: VRConnect shall archive only completed files (non-ongoing)
    /// from daily folder when size threshold is reached.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `daily_dir` - Path to daily directory
    /// * `date` - Date of folder
    ///
    /// # Returns
    /// Result indicating success or error
    async fn archive_completed_files(&self, daily_dir: &Path, date: NaiveDate) -> Result<()> {
        let files = self.get_completed_files(daily_dir)?;

        if files.is_empty() {
            log::debug!("No completed files to archive");
            return Ok(());
        }

        let (first_time, last_time) = self.get_time_range(&files)?;

        let archive_name = format!(
            "archive_{}_{}_{}.zip",
            date.format("%Y%m%d"),
            first_time,
            last_time
        );

        let archive_dir = self
            .base_path
            .join("archive")
            .join(date.format("%Y%m%d").to_string());
        fs::create_dir_all(&archive_dir).map_err(VitalError::Io)?;

        let archive_path = archive_dir.join(archive_name);

        log::info!(
            "Archiving {} completed files ({:.2} GB total)",
            files.len(),
            self.calculate_files_size(&files)? as f64 / (1024.0 * 1024.0 * 1024.0)
        );

        self.create_zip_archive(&archive_path, &files).await?;

        // Remove archived files
        for file in &files {
            fs::remove_file(file).map_err(VitalError::Io)?;
        }

        log::info!(
            "✓ Archive created: {} ({} files compressed)",
            archive_path.file_name().unwrap().to_string_lossy(),
            files.len()
        );

        Ok(())
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-009
    /// Title: get_completed_files
    ///
    /// Description: VRConnect shall retrieve list of completed files
    /// (not ending with _ongoing.json) sorted by name.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `dir` - Directory path
    ///
    /// # Returns
    /// Vector of file paths or error
    fn get_completed_files(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        if !dir.exists() {
            return Ok(files);
        }

        let entries = fs::read_dir(dir).map_err(VitalError::Io)?;

        for entry in entries {
            let entry = entry.map_err(VitalError::Io)?;
            let path = entry.path();

            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("vrconnect_")
                        && name.ends_with(".json")
                        && !name.ends_with("_ongoing.json")
                    {
                        files.push(path);
                    }
                }
            }
        }

        files.sort();
        Ok(files)
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-010
    /// Title: get_time_range
    ///
    /// Description: VRConnect shall extract time range from file names,
    /// returning start time of first file and end time of last file.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `files` - Vector of file paths
    ///
    /// # Returns
    /// Tuple of (start_time, end_time) strings or error
    fn get_time_range(&self, files: &[PathBuf]) -> Result<(String, String)> {
        if files.is_empty() {
            return Err(VitalError::Processing(
                "No files to get time range".to_string(),
            ));
        }

        // First file: vrconnect_YYYYMMDD_HHMMSS_HHMMSS.json
        let first_name = files[0]
            .file_stem()
            .and_then(|n| n.to_str())
            .ok_or_else(|| VitalError::Processing("Invalid filename".to_string()))?;

        let first_parts: Vec<&str> = first_name.split('_').collect();
        let start_time = if first_parts.len() >= 3 {
            first_parts[2].to_string()
        } else {
            return Err(VitalError::Processing(
                "Invalid filename format".to_string(),
            ));
        };

        // Last file
        let last_name = files[files.len() - 1]
            .file_stem()
            .and_then(|n| n.to_str())
            .ok_or_else(|| VitalError::Processing("Invalid filename".to_string()))?;

        let last_parts: Vec<&str> = last_name.split('_').collect();
        let end_time = if last_parts.len() >= 4 {
            last_parts[3].to_string()
        } else {
            return Err(VitalError::Processing(
                "Invalid filename format".to_string(),
            ));
        };

        Ok((start_time, end_time))
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-011
    /// Title: create_zip_archive
    ///
    /// Description: VRConnect shall create ZIP archive containing specified files
    /// with compression, running the blocking read + deflate work on the
    /// blocking-thread-pool rather than on the caller's tokio task.
    ///
    /// The whole output pipeline is driven by a single task (`processing_task` in
    /// `core/processor.rs`) which awaits console → BLE → file in sequence for every
    /// sample, so a synchronous zip here halts BLE emission for its full duration.
    /// The stall grows with the number of files to compress (tens of seconds of zero
    /// `Data_OUT` at midnight in long runs). Same remedy as `notify()` in
    /// `ble_gatt.rs`.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `archive_path` - Output archive path
    /// * `files` - Files to archive
    ///
    /// # Returns
    /// Result indicating success or error
    async fn create_zip_archive(&self, archive_path: &Path, files: &[PathBuf]) -> Result<()> {
        let archive_path = archive_path.to_path_buf();
        let files = files.to_vec();

        tokio::task::spawn_blocking(move || Self::write_zip_archive(&archive_path, &files))
            .await
            .map_err(|e| VitalError::Processing(format!("ZIP blocking task panicked: {}", e)))?
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-016
    /// Title: write_zip_archive
    ///
    /// Description: VRConnect shall perform the synchronous read + deflate of an
    /// archive. Always invoked from the blocking-thread-pool through
    /// `create_zip_archive` — never called directly from an async task.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `archive_path` - Output archive path
    /// * `files` - Files to archive
    ///
    /// # Returns
    /// Result indicating success or error
    fn write_zip_archive(archive_path: &Path, files: &[PathBuf]) -> Result<()> {
        use zip::write::FileOptions;

        let file = File::create(archive_path).map_err(VitalError::Io)?;

        let mut zip = zip::ZipWriter::new(file);
        let options = FileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .unix_permissions(0o644);

        for file_path in files {
            let name = file_path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| VitalError::Processing("Invalid filename".to_string()))?;

            zip.start_file(name, options)
                .map_err(|e| VitalError::Processing(format!("ZIP error: {}", e)))?;

            let mut f = File::open(file_path).map_err(VitalError::Io)?;

            std::io::copy(&mut f, &mut zip).map_err(VitalError::Io)?;
        }

        zip.finish()
            .map_err(|e| VitalError::Processing(format!("ZIP finish error: {}", e)))?;

        Ok(())
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-012
    /// Title: calculate_folder_size
    ///
    /// Description: VRConnect shall calculate total size of all files
    /// in specified directory in bytes.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `dir` - Directory path
    ///
    /// # Returns
    /// Total size in bytes or error
    fn calculate_folder_size(&self, dir: &Path) -> Result<u64> {
        let mut total = 0u64;

        if !dir.exists() {
            return Ok(0);
        }

        let entries = fs::read_dir(dir).map_err(VitalError::Io)?;

        for entry in entries {
            let entry = entry.map_err(VitalError::Io)?;
            let path = entry.path();

            if path.is_file() {
                let metadata = fs::metadata(&path).map_err(VitalError::Io)?;
                total += metadata.len();
            }
        }

        Ok(total)
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-013
    /// Title: calculate_files_size
    ///
    /// Description: VRConnect shall calculate total size of specified files.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `files` - Vector of file paths
    ///
    /// # Returns
    /// Total size in bytes or error
    fn calculate_files_size(&self, files: &[PathBuf]) -> Result<u64> {
        let mut total = 0u64;

        for file in files {
            let metadata = fs::metadata(file).map_err(VitalError::Io)?;
            total += metadata.len();
        }

        Ok(total)
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-014
    /// Title: check_disk_space
    ///
    /// Description: VRConnect shall check available disk space and
    /// trigger graceful shutdown if usage exceeds critical threshold (95%).
    ///
    /// Version: V1.0
    ///
    /// # Returns
    /// Result indicating success or critical error requiring shutdown
    #[cfg(not(tarpaulin_include))]
    async fn check_disk_space(&self) -> Result<()> {
        use std::process;

        let usage_percent = self.get_disk_usage_percent(&self.base_path)?;

        if usage_percent >= self.critical_disk_percent {
            log::error!(
                "CRITICAL: Disk usage {}% >= {}% threshold",
                usage_percent,
                self.critical_disk_percent
            );
            log::error!("Base path: {}", self.base_path.display());
            log::error!("Initiating graceful shutdown to prevent data loss");

            // Close current file properly
            let mut file_lock = self.current_file.write().await;
            if let Some(active) = file_lock.take() {
                drop(active.handle);
                log::info!("Current file closed: {}", active.path.display());
            }

            // Exit with error code
            process::exit(1);
        }

        Ok(())
    }

    /// ID SRS: SRS-FN-FILEOUTPUT-015
    /// Title: get_disk_usage_percent
    ///
    /// Description: VRConnect shall calculate disk usage percentage
    /// for filesystem containing specified path.
    ///
    /// Version: V1.0
    ///
    /// # Arguments
    /// * `path` - Path to check
    ///
    /// # Returns
    /// Usage percentage (0-100) or error
    fn get_disk_usage_percent(&self, path: &Path) -> Result<u8> {
        #[cfg(unix)]
        {
            // Get filesystem stats
            use std::ffi::CString;
            let path_cstr = CString::new(path.to_string_lossy().as_bytes())
                .map_err(|e| VitalError::Processing(format!("Path conversion error: {}", e)))?;

            let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
            let result = unsafe { libc::statvfs(path_cstr.as_ptr(), &mut stat) };

            if result != 0 {
                return Err(VitalError::Processing(
                    "Failed to get filesystem stats".to_string(),
                ));
            }

            let total = stat.f_blocks * stat.f_frsize;
            let available = stat.f_bavail * stat.f_frsize;
            let used = total - available;

            let usage_percent = if total > 0 {
                ((used as f64 / total as f64) * 100.0) as u8
            } else {
                0
            };

            Ok(usage_percent)
        }

        #[cfg(windows)]
        {
            use windows::core::PCWSTR;
            use windows::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

            // Resolve drive root from path (e.g. "C:\some\path" → "C:\")
            let drive_root = {
                let s = path.to_string_lossy();
                if s.len() >= 2 && s.chars().nth(1) == Some(':') {
                    format!("{}\\", &s[..2])
                } else {
                    "C:\\".to_string()
                }
            };
            let wide: Vec<u16> = drive_root
                .encode_utf16()
                .chain(std::iter::once(0))
                .collect();

            let mut free_bytes_caller: u64 = 0;
            let mut total_bytes: u64 = 0;
            let mut total_free_bytes: u64 = 0;

            unsafe {
                GetDiskFreeSpaceExW(
                    PCWSTR(wide.as_ptr()),
                    Some(&mut free_bytes_caller),
                    Some(&mut total_bytes),
                    Some(&mut total_free_bytes),
                )
            }
            .map_err(|e| VitalError::Processing(format!("GetDiskFreeSpaceExW: {e}")))?;

            let usage_percent = if total_bytes > 0 {
                let used = total_bytes.saturating_sub(total_free_bytes);
                ((used as f64 / total_bytes as f64) * 100.0) as u8
            } else {
                0
            };
            Ok(usage_percent)
        }

        #[cfg(not(any(unix, windows)))]
        {
            log::debug!("Disk usage checking not implemented on this platform");
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests;
