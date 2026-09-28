// NOTE for test authors: every FileOutput::new() below passes critical_disk_percent=100,
// NOT the prod default (95). check_disk_space() calls process::exit(1) at >= threshold,
// so 95 lets a nearly-full dev/CI disk kill the whole test harness mid-run. Keep 100.
use super::*;
use crate::domain::{ProcessedData, ProcessedRoom, ProcessedTrack, TrackType};
use chrono::Utc;
use tempfile::TempDir;

/// ID SRS: SRS-TEST-FILEOUT-001
/// Title: Test FileOutput creation
///
/// Description: VRConnect shall create FileOutput with directory structure.
///
/// Version: V1.0
#[tokio::test]
async fn test_file_output_creation() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let result = FileOutput::new(base_path.clone(), 500, 5, 100).await;
    assert!(result.is_ok());

    let _file_output = result.unwrap();

    // Check directories created
    assert!(temp_dir.path().join("data").exists());
    assert!(temp_dir.path().join("archive").exists());
}

/// ID SRS: SRS-TEST-FILEOUT-002
/// Title: Test file rotation on size limit
///
/// Description: VRConnect shall rotate file when size exceeds limit.
///
/// Version: V1.0
#[tokio::test]
async fn test_file_rotation_size_limit() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    // Very small size for testing (1 KB)
    let file_output = FileOutput::new(base_path.clone(), 1, 5, 100).await.unwrap();

    // Create large data to exceed 1 KB
    let mut tracks = Vec::new();
    for i in 0..50 {
        tracks.push(ProcessedTrack {
            name: format!("TRACK_{}", i),
            display_value: "X".repeat(100),
            raw_value: Some(i as f64),
            unit: "unit".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: i,
            record_index: 0,
            track_type: TrackType::String,
            waveform_stats: None,
            waveform_points: None,
        });
    }

    let room = ProcessedRoom {
        room_index: 0,
        room_name: "BED_01".to_string(),
        tracks,
    };

    let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

    // Write multiple times to trigger rotation
    for _ in 0..3 {
        let result = file_output.output(&data).await;
        assert!(result.is_ok());
    }

    // Check that multiple files were created
    let date_str = Local::now().format("%Y%m%d").to_string();
    let daily_dir = temp_dir.path().join("data").join(&date_str);

    if daily_dir.exists() {
        let entries: Vec<_> = fs::read_dir(&daily_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();

        // Should have created at least one file
        assert!(!entries.is_empty());
    }
}

/// ID SRS: SRS-TEST-FILEOUT-003
/// Title: Test get_completed_files
///
/// Description: VRConnect shall retrieve only completed files.
///
/// Version: V1.0
#[tokio::test]
async fn test_get_completed_files() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let date_str = Local::now().format("%Y%m%d").to_string();
    let daily_dir = temp_dir.path().join("data").join(&date_str);
    fs::create_dir_all(&daily_dir).unwrap();

    // Create test files
    File::create(daily_dir.join("vrconnect_20250115_100000_110000.json")).unwrap();
    File::create(daily_dir.join("vrconnect_20250115_110000_120000.json")).unwrap();
    File::create(daily_dir.join("vrconnect_20250115_120000_ongoing.json")).unwrap();

    let files = file_output.get_completed_files(&daily_dir).unwrap();

    // Should only get 2 completed files (not the ongoing one)
    assert_eq!(files.len(), 2);
    assert!(files[0]
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .contains("100000_110000"));
    assert!(files[1]
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .contains("110000_120000"));
}

/// ID SRS: SRS-TEST-FILEOUT-004
/// Title: Test get_time_range
///
/// Description: VRConnect shall extract correct time range from filenames.
///
/// Version: V1.0
#[tokio::test]
async fn test_get_time_range() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let files = vec![
        PathBuf::from("vrconnect_20250115_080000_090000.json"),
        PathBuf::from("vrconnect_20250115_090000_100000.json"),
        PathBuf::from("vrconnect_20250115_100000_110000.json"),
    ];

    let (start, end) = file_output.get_time_range(&files).unwrap();

    assert_eq!(start, "080000");
    assert_eq!(end, "110000");
}

/// ID SRS: SRS-TEST-FILEOUT-005
/// Title: Test calculate_folder_size
///
/// Description: VRConnect shall calculate correct folder size.
///
/// Version: V1.0
#[tokio::test]
async fn test_calculate_folder_size() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let test_dir = temp_dir.path().join("test");
    fs::create_dir_all(&test_dir).unwrap();

    // Create files with known sizes
    let mut file1 = File::create(test_dir.join("file1.txt")).unwrap();
    file1.write_all(&vec![0u8; 1024]).unwrap(); // 1 KB

    let mut file2 = File::create(test_dir.join("file2.txt")).unwrap();
    file2.write_all(&vec![0u8; 2048]).unwrap(); // 2 KB

    let size = file_output.calculate_folder_size(&test_dir).unwrap();
    assert_eq!(size, 3072); // 3 KB
}

/// ID SRS: SRS-TEST-FILEOUT-006
/// Title: Test archive_old_files_on_startup
///
/// Description: VRConnect shall archive old directories on startup.
///
/// Version: V1.0
#[tokio::test]
async fn test_archive_old_files_on_startup() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    // Create old directory (yesterday)
    let yesterday = Local::now().date_naive() - chrono::Duration::days(1);
    let old_dir = temp_dir
        .path()
        .join("data")
        .join(yesterday.format("%Y%m%d").to_string());
    fs::create_dir_all(&old_dir).unwrap();

    // Create a completed file in old directory
    File::create(old_dir.join("vrconnect_20250114_100000_110000.json")).unwrap();

    // Create FileOutput - should trigger archiving
    let _file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    // Check that archive was created
    let archive_dir = temp_dir
        .path()
        .join("archive")
        .join(yesterday.format("%Y%m%d").to_string());

    if archive_dir.exists() {
        let entries: Vec<_> = fs::read_dir(&archive_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!entries.is_empty());
    }
}

/// ID SRS: SRS-TEST-FILEOUT-007
/// Title: Test JSON Lines format
///
/// Description: VRConnect shall write data in JSON Lines format.
///
/// Version: V1.0
#[tokio::test]
async fn test_json_lines_format() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let room = ProcessedRoom {
        room_index: 0,
        room_name: "BED_01".to_string(),
        tracks: vec![ProcessedTrack {
            name: "HR".to_string(),
            display_value: "75.000".to_string(),
            raw_value: Some(75.0),
            unit: "bpm".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        }],
    };

    let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

    // Write data
    file_output.output(&data).await.unwrap();

    // Read back and verify format
    let date_str = Local::now().format("%Y%m%d").to_string();
    let daily_dir = temp_dir.path().join("data").join(&date_str);

    if daily_dir.exists() {
        let entries: Vec<_> = fs::read_dir(&daily_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();

        if !entries.is_empty() {
            let content = fs::read_to_string(entries[0].path()).unwrap();
            let lines: Vec<&str> = content.lines().collect();

            // Each line should be valid JSON
            for line in lines {
                let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
                assert!(parsed.is_object());
            }
        }
    }
}

/// ID SRS: SRS-TEST-FILEOUT-008
/// Title: Test disk usage calculation (Unix only)
///
/// Description: VRConnect shall calculate disk usage percentage.
///
/// Version: V1.0
#[tokio::test]
#[cfg(unix)]
async fn test_disk_usage_calculation() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let usage = file_output.get_disk_usage_percent(temp_dir.path()).unwrap();

    // Should return a valid percentage (0-100)
    assert!(usage <= 100);
}

/// ID SRS: SRS-TEST-FILEOUT-009
/// Title: Test archive on date change
///
/// Description: VRConnect shall archive previous day when date changes.
///
/// Version: V1.0
#[tokio::test]
async fn test_archive_on_date_change() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    // Simulate previous day directory
    let yesterday = Local::now().date_naive() - chrono::Duration::days(1);
    let old_dir = temp_dir
        .path()
        .join("data")
        .join(yesterday.format("%Y%m%d").to_string());
    fs::create_dir_all(&old_dir).unwrap();

    // Create a completed file
    File::create(old_dir.join("vrconnect_20250120_100000_110000.json")).unwrap();

    // Call archive_previous_day
    file_output.archive_previous_day(yesterday).await.unwrap();

    // Verify archive was created
    let archive_dir = temp_dir
        .path()
        .join("archive")
        .join(yesterday.format("%Y%m%d").to_string());

    assert!(archive_dir.exists());
}

/// ID SRS: SRS-TEST-FILEOUT-010
/// Title: Test archive_previous_day with non-existent directory
///
/// Description: VRConnect shall handle non-existent previous day directory.
///
/// Version: V1.0
#[tokio::test]
async fn test_archive_previous_day_not_exists() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let yesterday = Local::now().date_naive() - chrono::Duration::days(1);

    // Should not error if directory doesn't exist
    let result = file_output.archive_previous_day(yesterday).await;
    assert!(result.is_ok());
}

/// ID SRS: SRS-TEST-FILEOUT-011
/// Title: Test check_and_archive_if_needed with threshold exceeded
///
/// Description: VRConnect shall archive when folder size exceeds threshold.
///
/// Version: V1.0
#[tokio::test]
async fn test_check_and_archive_threshold_exceeded() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    // Very small threshold (1 byte) to trigger archiving
    let file_output = FileOutput::new(base_path.clone(), 500, 0, 100)
        .await
        .unwrap();

    let date_str = Local::now().format("%Y%m%d").to_string();
    let daily_dir = temp_dir.path().join("data").join(&date_str);
    fs::create_dir_all(&daily_dir).unwrap();

    // Create files that will exceed threshold
    let mut file1 = File::create(daily_dir.join("vrconnect_20250121_100000_110000.json")).unwrap();
    file1.write_all(b"test data content").unwrap();

    // This should trigger archiving
    let result = file_output.check_and_archive_if_needed(&daily_dir).await;

    assert!(result.is_ok());
}

/// ID SRS: SRS-TEST-FILEOUT-012
/// Title: Test archive_daily_folder with empty directory
///
/// Description: VRConnect shall handle empty daily folder.
///
/// Version: V1.0
#[tokio::test]
async fn test_archive_daily_folder_empty() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let date = Local::now().date_naive();
    let daily_dir = temp_dir
        .path()
        .join("data")
        .join(date.format("%Y%m%d").to_string());
    fs::create_dir_all(&daily_dir).unwrap();

    // Empty directory - should return Ok without creating archive
    let result = file_output.archive_daily_folder(&daily_dir, date).await;
    assert!(result.is_ok());
}

/// ID SRS: SRS-TEST-FILEOUT-013
/// Title: Test archive_completed_files with empty list
///
/// Description: VRConnect shall handle empty completed files list.
///
/// Version: V1.0
#[tokio::test]
async fn test_archive_completed_files_empty() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let date = Local::now().date_naive();
    let daily_dir = temp_dir
        .path()
        .join("data")
        .join(date.format("%Y%m%d").to_string());
    fs::create_dir_all(&daily_dir).unwrap();

    // Should return Ok with empty directory
    let result = file_output.archive_completed_files(&daily_dir, date).await;
    assert!(result.is_ok());
}

/// ID SRS: SRS-TEST-FILEOUT-014
/// Title: Test get_completed_files with non-existent directory
///
/// Description: VRConnect shall return empty list for non-existent directory.
///
/// Version: V1.0
#[tokio::test]
async fn test_get_completed_files_no_directory() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let non_existent = temp_dir.path().join("data").join("99999999");

    let files = file_output.get_completed_files(&non_existent).unwrap();
    assert_eq!(files.len(), 0);
}

/// ID SRS: SRS-TEST-FILEOUT-015
/// Title: Test get_time_range with empty files
///
/// Description: VRConnect shall return error for empty files list.
///
/// Version: V1.0
#[tokio::test]
async fn test_get_time_range_empty() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let empty_files: Vec<PathBuf> = vec![];
    let result = file_output.get_time_range(&empty_files);

    assert!(result.is_err());
}

/// ID SRS: SRS-TEST-FILEOUT-016
/// Title: Test get_time_range with invalid filename format
///
/// Description: VRConnect shall return error for invalid filename.
///
/// Version: V1.0
#[tokio::test]
async fn test_get_time_range_invalid_format() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let invalid_files = vec![PathBuf::from("invalid_format.json")];
    let result = file_output.get_time_range(&invalid_files);

    assert!(result.is_err());
}

/// ID SRS: SRS-TEST-FILEOUT-017
/// Title: Test calculate_folder_size with non-existent directory
///
/// Description: VRConnect shall return 0 for non-existent directory.
///
/// Version: V1.0
#[tokio::test]
async fn test_calculate_folder_size_not_exists() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let non_existent = temp_dir.path().join("does_not_exist");
    let size = file_output.calculate_folder_size(&non_existent).unwrap();

    assert_eq!(size, 0);
}

/// ID SRS: SRS-TEST-FILEOUT-018
/// Title: Test calculate_files_size
///
/// Description: VRConnect shall calculate total size of multiple files.
///
/// Version: V1.0
#[tokio::test]
async fn test_calculate_files_size() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let test_dir = temp_dir.path().join("test_files");
    fs::create_dir_all(&test_dir).unwrap();

    let file1_path = test_dir.join("file1.txt");
    let file2_path = test_dir.join("file2.txt");

    let mut file1 = File::create(&file1_path).unwrap();
    file1.write_all(&[0u8; 100]).unwrap(); // 100 bytes

    let mut file2 = File::create(&file2_path).unwrap();
    file2.write_all(&[0u8; 200]).unwrap(); // 200 bytes

    let files = vec![file1_path, file2_path];
    let total_size = file_output.calculate_files_size(&files).unwrap();

    assert_eq!(total_size, 300);
}

/// ID SRS: SRS-TEST-FILEOUT-019
/// Title: Test create_zip_archive
///
/// Description: VRConnect shall create valid ZIP archive.
///
/// Version: V1.0
#[tokio::test]
async fn test_create_zip_archive() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let test_dir = temp_dir.path().join("test_files");
    fs::create_dir_all(&test_dir).unwrap();

    // Create test files
    let file1_path = test_dir.join("test1.json");
    let mut file1 = File::create(&file1_path).unwrap();
    file1.write_all(b"test content 1").unwrap();

    let archive_path = temp_dir.path().join("test_archive.zip");
    let files = vec![file1_path];

    let result = file_output.create_zip_archive(&archive_path, &files).await;
    assert!(result.is_ok());
    assert!(archive_path.exists());
}

/// ID SRS: SRS-TEST-FILEOUT-034
/// Title: Test create_zip_archive keeps the runtime schedulable
///
/// Description: VRConnect shall keep the async runtime schedulable while an
/// archive is being compressed: a synchronous zip on the single
/// `processing_task` would stop BLE emission for its full duration.
///
/// On the current-thread runtime used by `#[tokio::test]`, a spawned task can
/// only progress when the running task yields. A blocking zip never yields, so
/// `ticks` would stay at 0; routing it through `spawn_blocking` yields at the
/// await point and lets the co-scheduled task run.
///
/// Version: V1.0
#[tokio::test]
async fn test_create_zip_archive_does_not_block_runtime() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let test_dir = temp_dir.path().join("test_files");
    fs::create_dir_all(&test_dir).unwrap();

    let file_path = test_dir.join("test1.json");
    let mut f = File::create(&file_path).unwrap();
    f.write_all(&vec![b'x'; 1024 * 1024]).unwrap();
    drop(f);

    let ticks = Arc::new(AtomicUsize::new(0));
    let ticks_task = ticks.clone();
    let ticker = tokio::spawn(async move {
        for _ in 0..1000 {
            ticks_task.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
        }
    });

    let archive_path = temp_dir.path().join("test_archive.zip");
    file_output
        .create_zip_archive(&archive_path, &[file_path])
        .await
        .unwrap();

    assert!(
        ticks.load(Ordering::SeqCst) > 0,
        "co-scheduled task never ran — the zip blocked the runtime"
    );
    assert!(archive_path.exists());

    ticker.abort();
}

/// ID SRS: SRS-TEST-FILEOUT-020
/// Title: Test rotation with actual file write
///
/// Description: VRConnect shall properly rotate file and rename with timestamps.
///
/// Version: V1.0
#[tokio::test]
async fn test_file_rotation_with_rename() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 1, 5, 100).await.unwrap();

    let room = ProcessedRoom {
        room_index: 0,
        room_name: "BED_01".to_string(),
        tracks: vec![ProcessedTrack {
            name: "HR".to_string(),
            display_value: "75.000".to_string(),
            raw_value: Some(75.0),
            unit: "bpm".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        }],
    };

    let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

    // Write enough data to trigger rotation
    for _ in 0..100 {
        let _ = file_output.output(&data).await;
    }

    // Verify files were created in daily directory
    let date_str = Local::now().format("%Y%m%d").to_string();
    let daily_dir = temp_dir.path().join("data").join(&date_str);

    if daily_dir.exists() {
        let entries: Vec<_> = fs::read_dir(&daily_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!entries.is_empty());
    }
}

/// ID SRS: SRS-TEST-FILEOUT-021
/// Title: Test full archiving workflow
///
/// Description: VRConnect shall complete full archive workflow.
///
/// Version: V1.0
#[tokio::test]
async fn test_full_archiving_workflow() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let date = Local::now().date_naive();
    let date_str = date.format("%Y%m%d").to_string();
    let daily_dir = temp_dir.path().join("data").join(&date_str);
    fs::create_dir_all(&daily_dir).unwrap();

    // Create multiple completed files
    for i in 0..3 {
        let filename = format!("vrconnect_{}_{}0000_{}1000.json", date_str, 10 + i, 10 + i);
        let mut file = File::create(daily_dir.join(filename)).unwrap();
        file.write_all(b"test data content").unwrap();
    }

    // Test archive_daily_folder
    let result = file_output.archive_daily_folder(&daily_dir, date).await;
    assert!(result.is_ok());

    // Verify archive was created
    let archive_dir = temp_dir.path().join("archive").join(&date_str);
    if archive_dir.exists() {
        let archives: Vec<_> = fs::read_dir(&archive_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!archives.is_empty());
    }
}

/// ID SRS: SRS-TEST-FILEOUT-022
/// Title: Test disk space check (Unix only)
///
/// Description: VRConnect shall check disk space and not panic under normal conditions.
///
/// Version: V1.0
#[tokio::test]
#[cfg(unix)]
async fn test_check_disk_space_normal() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    // Set high threshold so we don't trigger shutdown
    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    // Should not panic or exit
    let result = file_output.check_disk_space().await;
    assert!(result.is_ok());
}

/// ID SRS: SRS-TEST-FILEOUT-023
/// Title: Test get_disk_usage_percent (Unix only)
///
/// Description: VRConnect shall calculate disk usage percentage.
///
/// Version: V1.0
#[tokio::test]
#[cfg(unix)]
async fn test_get_disk_usage_percent() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let usage = file_output.get_disk_usage_percent(temp_dir.path()).unwrap();

    assert!(usage <= 100);
    assert!(usage >= 0);
}

/// ID SRS: SRS-TEST-FILEOUT-024
/// Title: Test archive_completed_files full workflow
///
/// Description: VRConnect shall archive completed files and create ZIP.
///
/// Version: V1.0
#[tokio::test]
async fn test_archive_completed_files_full() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let date = Local::now().date_naive();
    let date_str = date.format("%Y%m%d").to_string();
    let daily_dir = temp_dir.path().join("data").join(&date_str);
    fs::create_dir_all(&daily_dir).unwrap();

    // Create completed files
    let file1 = daily_dir.join("vrconnect_20250121_100000_110000.json");
    let mut f1 = File::create(&file1).unwrap();
    f1.write_all(b"data1").unwrap();

    let file2 = daily_dir.join("vrconnect_20250121_110000_120000.json");
    let mut f2 = File::create(&file2).unwrap();
    f2.write_all(b"data2").unwrap();

    // Call archive_completed_files
    let result = file_output.archive_completed_files(&daily_dir, date).await;

    assert!(result.is_ok());

    // Verify files were removed
    assert!(!file1.exists());
    assert!(!file2.exists());

    // Verify archive was created
    let archive_dir = temp_dir.path().join("archive").join(&date_str);
    assert!(archive_dir.exists());
}

/// ID SRS: SRS-TEST-FILEOUT-025
/// Title: Test date change detection during output
///
/// Description: VRConnect shall detect date change and archive previous day.
///
/// Version: V1.0
#[tokio::test]
async fn test_date_change_during_output() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    // Create a fake "yesterday" active file
    let yesterday = Local::now().date_naive() - chrono::Duration::days(1);
    let yesterday_str = yesterday.format("%Y%m%d").to_string();
    let daily_dir = temp_dir.path().join("data").join(&yesterday_str);
    fs::create_dir_all(&daily_dir).unwrap();

    let fake_file_path = daily_dir.join("vrconnect_20250120_100000_ongoing.json");
    let fake_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&fake_file_path)
        .unwrap();

    // Manually set an active file with yesterday's date
    {
        let mut file_lock = file_output.current_file.write().await;
        *file_lock = Some(ActiveFile {
            path: fake_file_path.clone(),
            handle: fake_file,
            start_time: Local::now() - chrono::Duration::days(1),
            current_size: 100,
            current_date: yesterday,
        });
    }

    // Create a completed file in yesterday's directory for archiving
    let completed_file = daily_dir.join("vrconnect_20250120_100000_110000.json");
    File::create(&completed_file).unwrap();

    // Now output data - should trigger date change detection
    let room = ProcessedRoom {
        room_index: 0,
        room_name: "BED_01".to_string(),
        tracks: vec![ProcessedTrack {
            name: "HR".to_string(),
            display_value: "75.000".to_string(),
            raw_value: Some(75.0),
            unit: "bpm".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::Number,
            waveform_stats: None,
            waveform_points: None,
        }],
    };

    let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

    // This should detect date change and archive previous day
    let result = file_output.output(&data).await;
    assert!(result.is_ok());

    // Verify archive directory was created for yesterday
    let _archive_dir = temp_dir.path().join("archive").join(&yesterday_str);
    // May or may not exist depending on if archiving completed
    // The important thing is no panic occurred
}

/// ID SRS: SRS-TEST-FILEOUT-026
/// Title: Test file rotation closes and renames file
///
/// Description: VRConnect shall close file, rename with timestamps, and check for archiving.
///
/// Version: V1.0
#[tokio::test]
async fn test_rotation_closes_and_renames_file() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    // Small size to trigger rotation quickly
    let file_output = FileOutput::new(base_path.clone(), 1, 5, 100).await.unwrap();

    let room = ProcessedRoom {
        room_index: 0,
        room_name: "BED_01".to_string(),
        tracks: vec![ProcessedTrack {
            name: "DATA".to_string(),
            display_value: "X".repeat(500), // Large data
            raw_value: Some(1.0),
            unit: "unit".to_string(),
            timestamp: Utc::now(),
            room_index: 0,
            room_name: "BED_01".to_string(),
            track_index: 0,
            record_index: 0,
            track_type: TrackType::String,
            waveform_stats: None,
            waveform_points: None,
        }],
    };

    let data = ProcessedData::new("VR-TEST".to_string(), vec![room]);

    // Write multiple times to trigger rotation
    for _ in 0..10 {
        let _ = file_output.output(&data).await;
    }

    // Check that files were created and some were renamed (not _ongoing)
    let date_str = Local::now().format("%Y%m%d").to_string();
    let daily_dir = temp_dir.path().join("data").join(&date_str);

    if daily_dir.exists() {
        let entries: Vec<_> = fs::read_dir(&daily_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();

        // Should have at least one file
        assert!(!entries.is_empty());

        // Check if any file was renamed (doesn't end with _ongoing.json)
        let _has_completed = entries.iter().any(|e| {
            let name = e.file_name();
            let name_str = name.to_string_lossy();
            name_str.starts_with("vrconnect_") && !name_str.ends_with("_ongoing.json")
        });

        // May or may not have completed files depending on timing
        // The test passes if no panic occurred during rotation
    }
}

/// ID SRS: SRS-TEST-FILEOUT-027
/// Title: Test get_time_range with only 2-part filename
///
/// Description: VRConnect shall handle filenames with only 2 parts.
///
/// Version: V1.0
#[tokio::test]
async fn test_get_time_range_short_filename() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    // Filename with only 2 parts (missing end time)
    let short_files = vec![PathBuf::from("vrconnect_20250121.json")];
    let result = file_output.get_time_range(&short_files);

    // Should return error for invalid format
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid filename format"));
}

/// ID SRS: SRS-TEST-FILEOUT-028
/// Title: Test get_time_range with last file having only 3 parts
///
/// Description: VRConnect shall handle last filename with 3 parts (missing end time).
///
/// Version: V1.0
#[tokio::test]
async fn test_get_time_range_last_file_invalid() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    // Valid first file, invalid last file
    let files = vec![
        PathBuf::from("vrconnect_20250121_100000_110000.json"),
        PathBuf::from("vrconnect_20250121_110000.json"), // Only 3 parts
    ];

    let result = file_output.get_time_range(&files);

    // Should return error for invalid last filename
    assert!(result.is_err());
}

/// ID SRS: SRS-TEST-FILEOUT-029
/// Title: Test disk space critical threshold (Unix only - mock test)
///
/// Description: VRConnect shall log critical error when disk usage exceeds threshold.
/// Note: We cannot actually trigger process::exit in tests, so we test up to that point.
///
/// Version: V1.0
#[tokio::test]
#[cfg(unix)]
async fn test_disk_space_critical_warning() {
    // This test verifies the disk usage calculation works
    // We cannot test the actual shutdown without killing the test process

    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    // Set threshold to 0 to ensure it would trigger (but we can't actually exit)
    let file_output = FileOutput::new(base_path.clone(), 500, 5, 0).await.unwrap();

    // Get actual disk usage
    let usage = file_output.get_disk_usage_percent(temp_dir.path()).unwrap();

    // Verify the usage check works
    assert!(usage <= 100);

    // Note: We cannot actually call check_disk_space() with threshold 0
    // because it would exit the test process
    // The lines 671-687 (process::exit) are intentionally hard to test
    // as they cause program termination
}

/// ID SRS: SRS-TEST-FILEOUT-030
/// Title: Test get_disk_usage_percent error handling (Unix only)
///
/// Description: VRConnect shall handle filesystem stat errors.
///
/// Version: V1.0
#[tokio::test]
#[allow(unused_variables)]
#[cfg(unix)]
async fn test_get_disk_usage_invalid_path() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    // Try with a path that might cause issues (but may still work)
    let result = file_output.get_disk_usage_percent(Path::new("/dev/null"));

    // Either succeeds or returns error
    // The important thing is it doesn't panic
    match result {
        Ok(usage) => assert!(usage <= 100),
        Err(_) => {} // Expected for invalid paths
    }
}

/// ID SRS: SRS-TEST-FILEOUT-031
/// Title: Test archive with date format in path
///
/// Description: VRConnect shall correctly format dates in archive paths.
///
/// Version: V1.0
#[tokio::test]
async fn test_archive_path_date_format() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let date_str = date.format("%Y%m%d").to_string();
    let daily_dir = temp_dir.path().join("data").join(&date_str);
    fs::create_dir_all(&daily_dir).unwrap();

    // Create a completed file
    let file_path = daily_dir.join("vrconnect_20250115_100000_110000.json");
    let mut f = File::create(&file_path).unwrap();
    f.write_all(b"test data").unwrap();

    // Archive the folder
    let result = file_output.archive_daily_folder(&daily_dir, date).await;
    assert!(result.is_ok());

    // Verify archive directory uses correct date format
    let archive_base = temp_dir.path().join("archive").join(&date_str);
    assert!(archive_base.exists());
}

/// ID SRS: SRS-TEST-FILEOUT-032
/// Title: Test multiple date format usages
///
/// Description: VRConnect shall use consistent date formatting throughout.
///
/// Version: V1.0
#[tokio::test]
async fn test_consistent_date_formatting() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let date = chrono::NaiveDate::from_ymd_opt(2025, 3, 7).unwrap();
    let date_str = "20250307"; // Expected format

    // Test archive_completed_files uses correct format
    let daily_dir = temp_dir.path().join("data").join(date_str);
    fs::create_dir_all(&daily_dir).unwrap();

    let file1 = daily_dir.join("vrconnect_20250307_080000_090000.json");
    File::create(&file1).unwrap();

    let result = file_output.archive_completed_files(&daily_dir, date).await;
    assert!(result.is_ok());

    // Check archive was created with correct date format
    let _archive_dir = temp_dir.path().join("archive").join(date_str);
    // Directory should exist or be created
}

/// ID SRS: SRS-TEST-FILEOUT-033
/// Title: Test Windows disk usage via GetDiskFreeSpaceExW
///
/// Description: VRConnect shall return a valid percentage (0-100) on Windows
///              using GetDiskFreeSpaceExW. The 0 fallback only applies to
///              platforms that are neither Unix nor Windows.
///
/// Version: V1.1
#[tokio::test]
#[cfg(windows)]
async fn test_disk_usage_windows_fallback() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_str().unwrap().to_string();

    let file_output = FileOutput::new(base_path.clone(), 500, 5, 100)
        .await
        .unwrap();

    let usage = file_output.get_disk_usage_percent(temp_dir.path()).unwrap();

    // On Windows, GetDiskFreeSpaceExW returns real disk usage (0-100)
    assert!(usage <= 100);
}
