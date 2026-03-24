// /src/main.rs
// Module: main
// Purpose: Application entry point with initialization and lifecycle management

use vrconnect::config::Config;
use vrconnect::core::VitalProcessor;
use vrconnect::utils::logger::Logger;

/// ID SRS: SRS-MAIN-001
/// Title: main
///
/// Description: VRConnect shall initialize the application with configuration,
/// setup logging, and start the vital processor with proper error handling.
///
/// Version: V1.0
///
/// # Returns
/// Unit type on success, exits with error code on failure
#[tokio::main]
async fn main() {
    // Parse CLI arguments first
    let config = Config::parse();

    // Initialize logger
    if let Err(e) = Logger::init(&config) {
        eprintln!("Failed to initialize logger: {}", e);
        std::process::exit(1);
    }

    log::info!("VRConnect v1.0.0 starting...");

    // Print banner
    print_banner(&config);

    // Create and run processor
    let processor = VitalProcessor::new(config);

    if let Err(e) = processor.run().await {
        log::error!("Fatal error: {}", e);
        std::process::exit(1);
    }

    log::info!("VRConnect stopped gracefully");
}

/// ID SRS: SRS-UTIL-001
/// Title: print_banner
///
/// Description: VRConnect shall display the application banner with current
/// configuration parameters for user information.
///
/// Version: V1.0
///
/// # Arguments
/// * `config` - Application configuration
fn print_banner(config: &Config) {
    println!("\n{}", "═".repeat(70));
    println!("  VRConnect - Medical Vital Data Middleware v1.0.0");
    println!("{}", "═".repeat(70));
    println!(
        "  Socket.IO Server: {}:{}",
        config.socketio_host, config.socketio_port
    );
    println!(
        "  Console Output:   {}",
        if config.output_console_enabled {
            "Enabled"
        } else {
            "Disabled"
        }
    );

    if config.output_console_enabled {
        println!("    └─ Verbose:     {}", config.output_console_verbose);
        println!("    └─ Colorized:   {}", config.output_console_colorized);
    }

    println!(
        "  BLE Output:       {}",
        if config.output_ble_enabled {
            "Enabled"
        } else {
            "Disabled"
        }
    );

    if config.output_ble_enabled {
        println!("    └─ Device Name: {}", config.output_ble_device_name);
        println!("    └─ Service UUID: {}", config.output_ble_service_uuid);
        println!("    └─ Tracks: {}", config.output_ble_values);
        println!("    └─ Empty Value: '{}'", config.output_ble_empty_value);
        println!("    └─ Update Interval: {}ms", config.output_ble_update_interval_ms);
        println!("    └─ Data Source: Room 0 (BED_01)");
    }

    println!(
        "  File Output:      {}",
        if config.output_file_enabled {
            "Enabled"
        } else {
            "Disabled"
        }
    );

    if config.output_file_enabled {
        println!("    └─ Base Path:   {}", config.output_file_base_path);
        println!(
            "    └─ Max Size:    {} MB per file",
            config.output_file_max_size_mb
        );
        println!(
            "    └─ Archive At:  {} GB",
            config.output_file_archive_threshold_gb
        );
        println!(
            "    └─ Disk Alert:  {}%",
            config.output_file_critical_disk_percent
        );
    }

    println!(
        "  Debug Mode:       {}",
        if config.debug_enabled {
            "Enabled"
        } else {
            "Disabled"
        }
    );

    if config.debug_enabled {
        println!("    └─ Output File: {}", config.debug_output_path);
    }

    println!("  Log Level:        {}", config.log_level);
    println!("  Log Directory:    {}", config.log_dir);
    println!("{}", "═".repeat(70));
    println!("  Press Ctrl+C to stop");
    println!("{}\n", "═".repeat(70));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_main_skeleton() {
        // TODO: Implement main initialization tests
        assert!(true);
    }

    #[test]
    fn test_banner_display() {
        // TODO: Implement banner formatting tests
        assert!(true);
    }
}
