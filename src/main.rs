use std::net::TcpStream;
use anyhow::{Result, Context, anyhow};
use std::env;
use std::io::Read;
use ssh2::{Session, File};
use std::path::Path;
use std::fs;
use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};

// --- AWS Imports ---
use aws_config::{self};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Builder, primitives::ByteStream, Client as S3Client,
    types::{CompletedMultipartUpload, CompletedPart},
};
use aws_types::region::Region;

// --- Tokio/Bytes Imports ---
use bytes::Bytes;
use tokio;

use std::process;

// --- Multi-threading Imports ---
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
struct Config {
    bank_host: String,
    bank_port: u16,
    bank_username: String,
    bank_private_key_path: Option<String>,
    bank_private_key_content: Option<String>,
    bank_remote_path: String,
    bank_password: String,

    storage_endpoint: String,
    storage_access_key: String,
    storage_secret_key: String,
    storage_bucket: String,
    storage_region: String,

    upload_chunk_size: usize, // e.g., 10 * 1024 * 1024 for 10MB
    max_concurrent_uploads: usize, // Number of parallel threads
}

impl Config {
    fn from_env() -> Result<Self> {
        dotenv::dotenv().ok();

        Ok(Self {
            bank_host: env::var("BANK_HOST")?,
            bank_port: env::var("BANK_PORT")?.parse()?,
            bank_username: env::var("BANK_USERNAME")?,

            // Optional authentication fields
            bank_private_key_path: env::var("BANK_PRIVATE_KEY_PATH").ok(),
            bank_private_key_content: env::var("BANK_PRIVATE_KEY_CONTENT").ok(),
            bank_password: env::var("BANK_PASSWORD").unwrap_or_default(),

            bank_remote_path: env::var("BANK_REMOTE_PATH")?,

            storage_endpoint: env::var("STORAGE_ENDPOINT")?,
            storage_access_key: env::var("STORAGE_ACCESS_KEY")?,
            storage_secret_key: env::var("STORAGE_SECRET_KEY")?,
            storage_bucket: env::var("STORAGE_BUCKET")?,
            storage_region: env::var("STORAGE_REGION")?,
            upload_chunk_size: env::var("UPLOAD_CHUNK_SIZE_MB")
                .unwrap_or_else(|_| "10".to_string())
                .parse::<usize>()? * 1024 * 1024,
            max_concurrent_uploads: env::var("MAX_CONCURRENT_UPLOADS")
                .unwrap_or_else(|_| "4".to_string())
                .parse::<usize>()?,
        })
    }
}

// In-memory file tracking structure
#[derive(Debug, Clone)]
struct FileTracker {
    pending: Arc<Mutex<Vec<String>>>,
    running: Arc<Mutex<HashSet<String>>>,
    completed: Arc<Mutex<HashSet<String>>>,
    failed: Arc<Mutex<HashMap<String, String>>>, // file_path -> error_message
}

impl FileTracker {
    fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(Mutex::new(HashSet::new())),
            completed: Arc::new(Mutex::new(HashSet::new())),
            failed: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn add_pending_files(&self, files: Vec<String>) {
        let mut pending = self.pending.lock().unwrap();
        pending.extend(files);
    }

    fn claim_next_file(&self) -> Option<String> {
        let mut pending = self.pending.lock().unwrap();
        if let Some(file) = pending.pop() {
            let mut running = self.running.lock().unwrap();
            running.insert(file.clone());
            Some(file)
        } else {
            None
        }
    }

    fn mark_completed(&self, file: &str) {
        let mut running = self.running.lock().unwrap();
        running.remove(file);
        let mut completed = self.completed.lock().unwrap();
        completed.insert(file.to_string());
    }

    fn mark_failed(&self, file: &str, error: &str) {
        let mut running = self.running.lock().unwrap();
        running.remove(file);
        // let mut failed = self.failed.lock().unwrap();
        // failed.insert(file.to_string(), error.to_string());
        // Add back to pending for retry
        let mut pending = self.pending.lock().unwrap();
        pending.push(file.to_string());
    }

    fn get_stats(&self) -> (usize, usize, usize, usize) {
        let pending = self.pending.lock().unwrap().len();
        let running = self.running.lock().unwrap().len();
        let completed = self.completed.lock().unwrap().len();
        let failed = self.failed.lock().unwrap().len();
        (pending, running, completed, failed)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("PROCESS ID: {}", process::id());
    let cfg = Config::from_env()?;

    println!("Loaded config: {:?}", cfg);
    println!("Max concurrent uploads: {}", cfg.max_concurrent_uploads);

    // Create S3 client
    let s3_client = Arc::new(create_s3_client(&cfg).await?);

    // Step 1: Get all files from SFTP server
    println!("\n=== Step 1: Listing files from SFTP server ===");
    let initial_sftp = create_sftp_session(&cfg).await?;
    let sftp_files = list_sftp_files_recursively(&initial_sftp, Path::new(&cfg.bank_remote_path))?;
    println!("Found {} files on SFTP server", sftp_files.len());
    
    if sftp_files.is_empty() {
        println!("No files to process.");
        return Ok(());
    }

    // Step 2: Get all files from S3
    println!("\n=== Step 2: Listing files from S3 bucket ===");
    let s3_files = list_s3_files(&s3_client, &cfg.storage_bucket).await?;
    println!("Found {} files in S3 bucket", s3_files.len());

    // Step 3: Find incomplete files (files in SFTP but not in S3)
    println!("\n=== Step 3: Comparing files ===");
    let s3_set: HashSet<String> = s3_files.into_iter().collect();
    let incomplete_files: Vec<String> = sftp_files
        .into_iter()
        .filter(|f| !s3_set.contains(f))
        .collect();
    
    println!("Found {} incomplete files to upload", incomplete_files.len());
    
    if incomplete_files.is_empty() {
        println!("All files are already in S3. Nothing to do.");
        return Ok(());
    }

    // Step 4: Initialize in-memory file tracker
    let file_tracker = Arc::new(FileTracker::new());
    file_tracker.add_pending_files(incomplete_files);

    // Create semaphore to limit concurrent uploads
    let semaphore = Arc::new(Semaphore::new(cfg.max_concurrent_uploads));
    
    // Spawn worker tasks
    let mut handles = vec![];
    
    println!("\n=== Step 4: Starting {} workers to process files ===", cfg.max_concurrent_uploads);
    
    for worker_id in 0..cfg.max_concurrent_uploads {
        let cfg_clone = cfg.clone();
        let file_tracker_clone = Arc::clone(&file_tracker);
        let semaphore_clone = Arc::clone(&semaphore);
        let s3_client_clone = Arc::clone(&s3_client);
        
        let handle = tokio::spawn(async move {
            worker_task(
                worker_id,
                cfg_clone,
                file_tracker_clone,
                semaphore_clone,
                s3_client_clone,
            ).await
        });
        
        handles.push(handle);
    }
    
    // Wait for all workers to complete
    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("Worker task panicked: {}", e);
        }
    }
    
    // Print final statistics
    let (pending, running, completed, failed) = file_tracker.get_stats();
    println!("\n=== Final Statistics ===");
    println!("Completed: {}", completed);
    println!("Failed: {}", failed);
    println!("Running: {}", running);
    println!("Pending: {}", pending);
    
    if failed > 0 {
        println!("\nSome files failed to upload. Check logs above for details.");
    } else {
        println!("\nAll files transferred successfully!");
    }
    
    Ok(())
}

// Worker task that processes files from the in-memory queue
async fn worker_task(
    worker_id: usize,
    cfg: Config,
    file_tracker: Arc<FileTracker>,
    semaphore: Arc<Semaphore>,
    s3_client: Arc<S3Client>,
) {
    println!("[Worker {}] Started", worker_id);
    
    // Create one SFTP session for this worker (wrapped in Arc)
    let mut sftp_session: Option<Arc<ssh2::Sftp>> = match create_sftp_session(&cfg).await {
        Ok(session) => {
            println!("[Worker {}] SFTP connection established", worker_id);
            Some(Arc::new(session))
        }
        Err(e) => {
            eprintln!("[Worker {}] Failed to create initial SFTP session: {}", worker_id, e);
            None
        }
    };
    
    loop {
        // Try to claim the next pending file from in-memory tracker
        let file_path = match file_tracker.claim_next_file() {
            Some(path) => path,
            None => {
                println!("[Worker {}] No more files to process, shutting down", worker_id);
                break;
            }
        };
        
        // Acquire semaphore permit (limits concurrent connections)
        let _permit = semaphore.acquire().await.unwrap();
        
        println!("[Worker {}] Processing file: {}", worker_id, file_path);
        
        // Ensure we have a valid SFTP session
        if sftp_session.is_none() {
            println!("[Worker {}] No active SFTP session, attempting to connect...", worker_id);
            match create_sftp_session(&cfg).await {
                Ok(session) => {
                    println!("[Worker {}] SFTP reconnection successful", worker_id);
                    sftp_session = Some(Arc::new(session));
                }
                Err(e) => {
                    let error_msg = format!("Failed to create SFTP session: {}", e);
                    eprintln!("[Worker {}] {}", worker_id, error_msg);
                    file_tracker.mark_failed(&file_path, &error_msg);
                    continue;
                }
            }
        }
        
        // Process the file with retry logic
        let mut cfg_file = cfg.clone();
        cfg_file.bank_remote_path = file_path.clone();
        
        let mut retry_count = 0;
        const MAX_RETRIES: usize = 2;
        
        loop {
            if let Some(ref sftp) = sftp_session {
                match upload_to_storage(worker_id, &cfg_file, Arc::clone(sftp), (*s3_client).clone()).await {
                    Ok(_) => {
                        println!("[Worker {}] Successfully uploaded: {}", worker_id, file_path);
                        file_tracker.mark_completed(&file_path);
                        break; // Success, move to next file
                    }
                    Err(e) => {
                        let error_msg = format!("{}", e);
                        eprintln!("[Worker {}] Failed to upload {} (attempt {}): {}", 
                                 worker_id, file_path, retry_count + 1, error_msg);
                        
                        // Check if error might be connection-related
                        let is_connection_error = error_msg.contains("Connection") 
                            || error_msg.contains("SFTP") 
                            || error_msg.contains("SSH")
                            || error_msg.contains("channel")
                            || error_msg.contains("session");
                        
                        if is_connection_error && retry_count < MAX_RETRIES {
                            println!("[Worker {}] Connection error detected, attempting to reconnect...", worker_id);
                            sftp_session = None; // Invalidate current session
                            
                            // Try to reconnect
                            match create_sftp_session(&cfg).await {
                                Ok(session) => {
                                    println!("[Worker {}] SFTP reconnection successful, retrying file...", worker_id);
                                    sftp_session = Some(Arc::new(session));
                                    retry_count += 1;
                                    continue; // Retry with new connection
                                }
                                Err(e) => {
                                    eprintln!("[Worker {}] SFTP reconnection failed: {}", worker_id, e);
                                    file_tracker.mark_failed(&file_path, &error_msg);
                                    break; // Give up on this file
                                }
                            }
                        } else {
                            // Non-connection error or max retries reached
                            file_tracker.mark_failed(&file_path, &error_msg);
                            break; // Move to next file
                        }
                    }
                }
            } else {
                // This shouldn't happen as we check above, but handle it anyway
                let error_msg = "No SFTP session available".to_string();
                eprintln!("[Worker {}] {}", worker_id, error_msg);
                file_tracker.mark_failed(&file_path, &error_msg);
                break;
            }
        }
        
        // Print current statistics
        let (pending, running, completed, failed) = file_tracker.get_stats();
        println!("[Worker {}] Progress: Pending={}, Running={}, Completed={}, Failed={}", 
                 worker_id, pending, running, completed, failed);
    }
    
    println!("[Worker {}] Shutting down", worker_id);
}

use ssh2::Sftp;

fn list_sftp_files_recursively(sftp: &Sftp, path: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();

    // Try reading directory
    let entries = match sftp.readdir(path) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("Cannot read {:?}: {}", path, err);
            return Ok(files);
        }
    };

    for (entry_path, stat) in entries {
        let file_name = entry_path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        if file_name.starts_with('.') {
            continue; // skip hidden files
        }

        // Construct full path as string
        let full_path = entry_path.to_string_lossy().to_string();

        if stat.is_dir() {
            // Recursive call for subfolder
            let mut nested = list_sftp_files_recursively(sftp, &entry_path)?;
            files.append(&mut nested);
        } else {
            // Add file path
            files.push(full_path);
        }
    }

    Ok(files)
}

// List all files in S3 bucket
async fn list_s3_files(s3_client: &S3Client, bucket: &str) -> Result<Vec<String>> {
    let mut files = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut request = s3_client.list_objects_v2().bucket(bucket);
        
        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let response = request.send().await.context("Failed to list S3 objects")?;

        for object in response.contents() {
            if let Some(key) = object.key() {
                files.push(key.to_string());
            }
        }

        if response.is_truncated() == Some(true) {
            continuation_token = response.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    Ok(files)
}

async fn create_sftp_session(cfg: &Config) -> Result<ssh2::Sftp> {
    // Connect to the SFTP server
    let tcp = TcpStream::connect(format!("{}:{}", cfg.bank_host, cfg.bank_port))
        .context("Failed to connect to SFTP server")?;
    let mut sess = Session::new().context("Failed to create SSH session")?;
    sess.set_tcp_stream(tcp);
    sess.handshake().context("SSH handshake failed")?;

    // Authenticate using private key
    if let Some(ref key_content) = cfg.bank_private_key_content {
        // Write the PEM key directly to temp file
        let temp_key_path = format!("/tmp/sftp_temp_key_{}.pem", process::id());
        
        // Format the key properly - replace spaces with newlines in the base64 content
        let formatted_key = if key_content.contains("-----BEGIN") && key_content.contains("-----END") {
            // Extract header, content, and footer
            let parts: Vec<&str> = key_content.split("-----").collect();
            if parts.len() >= 5 {
                let header = format!("-----{}-----", parts[1]);
                let footer = format!("-----{}-----", parts[3]);
                let content = parts[2].trim();
                
                // Split content into 64-character lines (standard PEM format)
                let mut formatted_content = String::new();
                for chunk in content.split_whitespace() {
                    formatted_content.push_str(chunk);
                }
                
                let mut lines = Vec::new();
                let chars: Vec<char> = formatted_content.chars().collect();
                for chunk in chars.chunks(64) {
                    lines.push(chunk.iter().collect::<String>());
                }
                
                format!("{}\n{}\n{}", header, lines.join("\n"), footer)
            } else {
                key_content.clone()
            }
        } else {
            key_content.clone()
        };
        
        fs::write(&temp_key_path, formatted_key)
            .context("Failed to write private key to temporary file")?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temp_key_path, fs::Permissions::from_mode(0o600))?;
        }

        // Authenticate using the temporary key file
        sess.userauth_pubkey_file(
            &cfg.bank_username,
            None,
            Path::new(&temp_key_path),
            None,
        ).context("SSH authentication failed")?;

        // Clean up the temporary file
        let _ = fs::remove_file(&temp_key_path);
    } else if let Some(ref path) = cfg.bank_private_key_path {
        sess.userauth_pubkey_file(&cfg.bank_username, None, Path::new(path), None)
            .context("SSH authentication failed")?;
    } else {
        return Err(anyhow::anyhow!("No private key provided"));
    }

    if !sess.authenticated() {
        return Err(anyhow::anyhow!("SSH authentication failed"));
    }

    // Open SFTP session
    let sftp = sess.sftp().context("Failed to open SFTP session")?;
    println!("✓ SFTP session established!");

    Ok(sftp)
}

async fn create_s3_client(cfg: &Config) -> Result<S3Client> {
    let region = Region::new(cfg.storage_region.clone());

    let base_config = aws_config::from_env()
        .region(region.clone())
        .load()
        .await;

    let credentials = Credentials::new(
        &cfg.storage_access_key,
        &cfg.storage_secret_key,
        None,
        None,
        "custom",
    );

    let credentials_provider = SharedCredentialsProvider::new(credentials);

    let s3_config = Builder::from(&base_config)
        .region(region)
        .endpoint_url(cfg.storage_endpoint.clone())
        .credentials_provider(credentials_provider)
        .build();

    let client = S3Client::from_conf(s3_config);

    Ok(client)
}

// --- Add this import at the top of your file ---
use tokio::sync::mpsc; // Import the channel

async fn upload_to_storage(
    worker_id: usize,
    cfg: &Config,
    sftp: Arc<ssh2::Sftp>,
    s3_client: S3Client,
) -> Result<()> {
    
    // 1. Initiate Upload
    let mpu = s3_client
        .create_multipart_upload()
        .bucket(cfg.storage_bucket.clone())
        .key(cfg.bank_remote_path.clone())
        .send()
        .await?;

    let upload_id = mpu.upload_id().ok_or_else(|| anyhow!("S3 response missing UploadId"))?;
    println!("[Worker {}] ({}) Initiated MPU. UploadId: {}", worker_id, cfg.bank_remote_path, upload_id);

    // This block ensures we abort the MPU on any error
    let transfer_result = async { 
        
        // --- NEW: Define the structure for our pipeline ---
        #[derive(Debug)]
        struct Chunk {
            part_number: i32,
            data: Bytes,
        }
        
        // --- NEW: Create a channel for pipelining ---
        // This creates a "pipeline" that can hold 2 chunks.
        // The reader can read ahead while the uploader is busy.
        const PIPELINE_BUFFER_SIZE: usize = 2;
        let (tx, mut rx) = mpsc::channel::<Result<Chunk>>(PIPELINE_BUFFER_SIZE);

        // --- NEW: Spawn the "Reader" task ---
        // This task will read from SFTP and send chunks to the `tx` side of the channel
        let cfg_clone = cfg.clone();
        let sftp_clone = Arc::clone(&sftp);
        let reader_handle = tokio::spawn(async move {
            // 3. Connect to SFTP (in a blocking task) - This is the *reader's* job
            let mut sftp_file = match tokio::task::spawn_blocking({
                let sftp_path = cfg_clone.bank_remote_path.clone();
                move || -> Result<File> {
                    sftp_clone.open(Path::new(&sftp_path)).context(format!("SFTP file open failed: {}", sftp_path))
                }
            }).await? {
                Ok(file) => file,
                Err(e) => {
                    // Send the error to the uploader and exit
                    let _ = tx.send(Err(e)).await;
                    return Ok(());
                }
            };

            let mut part_number = 1;

            // 4. Stream Parts - This is the *reader's* job
            loop {
                let (file_out, read_result) = tokio::task::spawn_blocking({
                    let mut sftp_file_ref = sftp_file;
                    let mut chunk_buffer = vec![0u8; cfg_clone.upload_chunk_size];
                    let mut bytes_in_buffer = 0;

                    move || -> (File, std::io::Result<(Vec<u8>, usize)>) {
                        loop {
                            let bytes_read = match sftp_file_ref.read(&mut chunk_buffer[bytes_in_buffer..]) {
                                Ok(0) => break,
                                Ok(n) => n,
                                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                                Err(e) => return (sftp_file_ref, Err(e)),
                            };
                            bytes_in_buffer += bytes_read;
                            if bytes_in_buffer == chunk_buffer.len() {
                                break;
                            }
                        }
                        (sftp_file_ref, Ok((chunk_buffer, bytes_in_buffer)))
                    }
                }).await?;

                sftp_file = file_out;
                
                match read_result {
                    Ok((mut buffer, bytes_read)) => {
                        if bytes_read == 0 {
                            break; // End of file, stop the reader loop
                        }
                        
                        buffer.truncate(bytes_read);
                        
                        let chunk = Chunk {
                            part_number,
                            data: Bytes::from(buffer),
                        };
                        
                        // Send the chunk to the uploader.
                        // If tx.send fails, the receiver (uploader) has crashed or stopped.
                        if tx.send(Ok(chunk)).await.is_err() {
                            println!("[Worker {}] ({}) Uploader stopped, shutting down reader.", worker_id, cfg_clone.bank_remote_path);
                            break;
                        }
                        
                        part_number += 1;
                    }
                    Err(e) => {
                        // Send the I/O error to the uploader
                        let _ = tx.send(Err(anyhow!(e).context("SFTP read error"))).await;
                        break;
                    }
                }
            }
            
            // Reader is done, sftp_file is dropped, tx is dropped.
            Ok::<(), anyhow::Error>(())
        });

        // --- MODIFIED: This is now the "Uploader" task ---
        // It receives chunks from the `rx` side of the channel and uploads them
        let mut completed_parts = Vec::new();

        while let Some(chunk_result) = rx.recv().await {
            let chunk = chunk_result.context("Reader task failed")?;
            
            println!("[Worker {}] ({}) Uploading part {}", worker_id, cfg.bank_remote_path, chunk.part_number);

            // 6. Upload Part
            let part_resp = s3_client
                .upload_part()
                .bucket(cfg.storage_bucket.clone())
                .key(cfg.bank_remote_path.clone())
                .upload_id(upload_id)
                .part_number(chunk.part_number)
                .body(ByteStream::from(chunk.data))
                .send()
                .await
                .context(format!("S3 UploadPart {} failed", chunk.part_number))?;
            
            let etag = part_resp.e_tag().ok_or_else(|| anyhow!("S3 response missing ETag for part {}", chunk.part_number))?;

            completed_parts.push(
                CompletedPart::builder()
                    .part_number(chunk.part_number)
                    .e_tag(etag)
                    .build(),
            );
        }
        
        // --- NEW: Wait for the reader to finish and check for panics/errors ---
        // `rx.recv()` returned None, so the reader is done.
        // We must `await` its handle to check if it panicked or returned an error.
        reader_handle.await?.context("Reader task failed")?;
        
        println!("[Worker {}] ({}) Reader and Uploader finished.", worker_id, cfg.bank_remote_path);

        // 8. Finalize Transfer
        if !completed_parts.is_empty() {
            println!("[Worker {}] ({}) Completing multipart upload...", worker_id, cfg.bank_remote_path);
            let mpu_completion = CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build();

            s3_client
                .complete_multipart_upload()
                .bucket(cfg.storage_bucket.clone())
                .key(cfg.bank_remote_path.clone())
                .upload_id(upload_id)
                .multipart_upload(mpu_completion)
                .send()
                .await
                .context("Failed to complete multipart upload")?;
        } else {
            println!("[Worker {}] ({}) File was empty, aborting MPU and creating empty object.", worker_id, cfg.bank_remote_path);
            
            s3_client
                .abort_multipart_upload()
                .bucket(cfg.storage_bucket.clone())
                .key(cfg.bank_remote_path.clone())
                .upload_id(upload_id)
                .send()
                .await
                .context("Failed to abort MPU for empty file")?;
            
            s3_client
                .put_object()
                .bucket(cfg.storage_bucket.clone())
                .key(cfg.bank_remote_path.clone())
                .body(ByteStream::from(Bytes::new()))
                .send()
                .await
                .context("Failed to upload 0-byte empty file")?;
        }

        Ok(())

    }.await;

    if let Err(e) = &transfer_result {
        eprintln!("[Worker {}] ({}) Aborting MPU due to error: {}", worker_id, cfg.bank_remote_path, e);
        if let Err(abort_err) = s3_client
            .abort_multipart_upload()
            .bucket(cfg.storage_bucket.clone())
            .key(cfg.bank_remote_path.clone())
            .upload_id(upload_id)
            .send()
            .await {
                eprintln!("[Worker {}] ({}) CRITICAL: Failed to abort MPU: {}", worker_id, cfg.bank_remote_path, abort_err);
            }
    }
    
    transfer_result
}
