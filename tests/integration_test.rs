use tempfile::TempDir;

#[test]
fn test_help_output() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_snap-fetch"))
        .arg("--help")
        .output()
        .expect("Failed to execute snap-fetch");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("High performance file downloader"));
    assert!(stdout.contains("--output-dir"));
    assert!(stdout.contains("--concurrency"));
    assert!(stdout.contains("--chunk-size"));
}

#[test]
fn test_version_output() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_snap-fetch"))
        .arg("--version")
        .output()
        .expect("Failed to execute snap-fetch");

    // Version might not be successful if not specified in Cargo.toml
    // Just check it runs without panic
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should at least contain the program name in output
    assert!(stdout.contains("snap-fetch") || stderr.contains("snap-fetch"));
}

#[test]
fn test_invalid_url() {
    let temp_dir = TempDir::new().unwrap();
    let output_dir = temp_dir.path().to_str().unwrap();

    // Run with an invalid URL
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_snap-fetch"))
        .args(&[
            "--output-dir",
            output_dir,
            "--status-url",
            "http://invalid-url-that-does-not-exist.local/status",
            "--quiet",
            "--no-progress",
        ])
        .output()
        .expect("Failed to execute snap-fetch");

    // Should fail gracefully
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Failed") || stderr.contains("error"));
}

#[test]
fn test_output_directory_creation() {
    let temp_dir = TempDir::new().unwrap();
    let nested_dir = temp_dir.path().join("nested/directory/path");
    let output_dir = nested_dir.to_str().unwrap();

    // Directory shouldn't exist yet
    assert!(!nested_dir.exists());

    // Run snap-fetch with nested output directory (it will fail on network but create the dir)
    let _output = std::process::Command::new(env!("CARGO_BIN_EXE_snap-fetch"))
        .args(&[
            "--output-dir",
            output_dir,
            "--status-url",
            "http://invalid-url.local/status",
            "--quiet",
            "--no-progress",
        ])
        .output()
        .expect("Failed to execute snap-fetch");

    // Directory should have been created
    assert!(nested_dir.exists());
}

#[test]
fn test_conflicting_args() {
    // Test that conflicting args are handled
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_snap-fetch"))
        .args(&[
            "--quiet",
            "--verbose", // Conflicts with --quiet
        ])
        .output()
        .expect("Failed to execute snap-fetch");

    // Should fail due to conflicting arguments
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot be used") || stderr.contains("conflicts"));
}

#[test]
fn test_show_tuning() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_snap-fetch"))
        .arg("--show-tuning")
        .output()
        .expect("Failed to execute snap-fetch");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("System Tuning") || stdout.contains("Performance"));
}

// More comprehensive integration tests would require a real test server
// or more sophisticated mocking. These basic tests ensure the binary
// compiles and handles basic argument parsing correctly.
