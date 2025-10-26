#!/usr/bin/env python3
"""
csv_worker.py - Multi-threaded worker system for processing CSV and task files

This worker system processes files from a monitored directory, uploading them
to available servers with automatic health checking and load balancing.

Usage:
    python csv_worker.py                    # Run with 1 worker
    python csv_worker.py --workers 10       # Run with 10 workers
    python csv_worker.py --dir custom/      # Monitor custom directory
"""

import os
import re
import time
import glob
import json
import logging
import hashlib
import argparse
import threading
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any, Set
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum

import requests
from sqlalchemy import create_engine, text

# Import the CSV splitter module
from csv_splitter_module import CSVSplitter

# Import custom modules if available

# ============================================================================
# Configuration
# ============================================================================

@dataclass
class WorkerConfig:
    """Worker system configuration"""
    # Directories
    watch_directory: str = "loadingcsv"
    output_directory: str = "output"
    
    # Server settings
    health_check_url: str = "https://go3.aimachengine.com/health"
    upload_timeout: int = 6000
    request_timeout: int = 500
    
    # Database
    mysql_uri: str = "mysql+pymysql://admin:RQApoaNQ@mysql-199933-0.cloudclusters.net:10033/working_db"
    
    # Worker settings
    num_workers: int = 1
    max_file_checks: int = 50
    retry_delay: int = 5
    
    # Logging
    log_level: str = "DEBUG"
    log_file: str = "worker.log"
    log_format: str = "%(asctime)s [%(threadName)-10s] [%(levelname)s] %(message)s"


class FileType(Enum):
    """Supported file types"""
    CSV = ".csv"
    TASK = ".task"


class WorkerError(Exception):
    """Base exception for worker errors"""
    pass


class ServerError(WorkerError):
    """Server-related errors"""
    pass

# ============================================================================
# Server Management
# ============================================================================

class ServerManager:
    """Manages server health checks and allocation"""
    
    def __init__(self, config: WorkerConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.active_servers: Set[str] = set()
        self.lock = threading.Lock()
    


    
    def get_best_server(self) -> Optional[str]:
        """Get the best available healthy server URL not currently locked"""
        try:
            response = requests.get(
                self.config.health_check_url,
                timeout=self.config.request_timeout
            )

            response.raise_for_status()
            
            data = response.json()
            all_servers = data.get('all_servers', [])

            for server in all_servers:
                if server.get('health') == 'healthy':
                    server_url = server.get('url')
                    with self.lock:
                        if server_url in self.active_servers:
                            self.logger.debug(f"locked server: {server_url}")
                            continue
                        if server_url:
                            self.active_servers.add(server_url)
                            return server_url
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get best server: {e}")
            raise ServerError(f"Health check failed: {e}")





    def release_server(self, server_url: str):
        """Release a server back to the pool"""
        with self.lock:
            self.active_servers.discard(server_url)
            self.logger.debug(f"Released server: {server_url}")
    
    def check_table_mapping(self, server_url: str, file_path: str) -> str:
        """Get table name mapping for a CSV file"""
        try:
            # Read first line and compute hash
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline().rstrip('\r\n')
            
            header_hash = hashlib.sha256(first_line.encode('utf-8')).hexdigest()
            
            # Check with server
            url = f"{server_url.rstrip('/')}/check_test"
            response = requests.post(
                url,
                json={'HeaderHash': header_hash},
                timeout=self.config.request_timeout
            )
            response.raise_for_status()
            
            result = response.json()
            return result.get('table_name', 'unknown')
            
        except Exception as e:
            self.logger.error(f"Failed to get table mapping: {e}")
            return 'unknown'

# ============================================================================
# File Management
# ============================================================================

class FileManager:
    """Manages file discovery and tracking"""
    
    def __init__(self, config: WorkerConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.processed_files: Set[str] = set()
        self.lock = threading.Lock()
    
    def get_next_file(self) -> Optional[str]:
        """Get the next file to process"""
        with self.lock:
            pattern = os.path.join(self.config.watch_directory, "*")
            files = glob.glob(pattern)
            
            # Filter out already processed files
            available_files = [f for f in files if f not in self.processed_files]
            
            if not available_files:
                return None
            
            # Sort by modification time (most recent first)
            available_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            
            # Take the first available file (up to max_file_checks)
            for file_path in available_files[:self.config.max_file_checks]:
                self.processed_files.add(file_path)
                self.logger.info(f"Selected file: {file_path}")
                return file_path
            
            return None
    
    def release_file(self, file_path: str):
        """Release a file from processing"""
        with self.lock:
            self.processed_files.discard(file_path)
            self.logger.debug(f"Released file: {file_path}")
    
    def remove_file(self, file_path: str):
        """Remove a processed file"""
        try:
            os.remove(file_path)
            self.logger.info(f"Removed file: {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to remove file {file_path}: {e}")


# ============================================================================
# Upload Client
# ============================================================================

class UploadClient:
    """Handles file uploads to servers"""
    
    def __init__(self, server_url: str, config: WorkerConfig, logger: logging.Logger):
        self.server_url = server_url.rstrip('/')
        self.config = config
        self.logger = logger
    
    def upload_file(self, file_path: str, table_name: str) -> bool:
        """Upload a file to the server
        
        Args:
            file_path: Path to file to upload
            table_name: Target table name
            
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(file_path):
            self.logger.error(f"File does not exist: {file_path}")
            return False
        
        upload_url = f"{self.server_url}/upload"
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        self.logger.info(
            f"Uploading {Path(file_path).name} ({file_size_mb:.2f} MB) "
            f"to {upload_url} (table: {table_name})"
        )
        
        try:
            with open(file_path, 'rb') as f:
                files = {'file': f}
                data = {'tableName': table_name}
                
                response = requests.post(
                    upload_url,
                    files=files,
                    data=data,
                    stream=True,
                    timeout=self.config.upload_timeout
                )
                
                if response.status_code == 200:
                    return self._process_response(response)
                else:
                    self.logger.error(
                        f"Upload failed with status {response.status_code}: "
                        f"{response.text[:200]}"
                    )
                    return False
                    
        except Exception as e:
            self.logger.error(f"Upload error: {e}")
            return False
    
    def _process_response(self, response) -> bool:
        """Process streaming response from server"""
        self.logger.info("Upload successful, processing response...")
        
        try:
            lines_processed = 0
            response_buffer = []
            
            # Try to stream the response
            for line in response.iter_lines(decode_unicode=True, chunk_size=1024):
                if line and line.strip():
                    timestamp = time.strftime("%H:%M:%S")
                    self.logger.debug(f"[{timestamp}] Server: {line.strip()} From "+self.server_url )
                    response_buffer.append(line.strip())
                    lines_processed += 1
            
            # If no streaming data, try to get full response
            if lines_processed == 0:
                full_content = response.text
                if full_content.strip():
                    for line in full_content.split('\n'):
                        if line.strip():
                            self.logger.debug(f"Server: {line.strip()}")
                            lines_processed += 1
            
            self.logger.info(f"Processing complete. {lines_processed} lines received.")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing response: {e}")
            return False




# ============================================================================
# Worker
# ============================================================================

class Worker:
    """Individual worker thread"""
    
    def __init__(self, 
                 config: WorkerConfig,
                 file_manager: FileManager,
                 server_manager: ServerManager,
                 logger: logging.Logger,
                 worker_id: int = 0):
        self.config = config
        self.file_manager = file_manager
        self.server_manager = server_manager
        self.logger = logger
        self.worker_id = worker_id
        self.thread_id = None
        self.should_stop = False
    
    def run(self):
        """Run the worker in a continuous loop"""
        self.thread_id = threading.current_thread().ident
        self.logger.info(f"Worker {self.worker_id} (thread {self.thread_id}) started")
        
        while not self.should_stop:
            try:
                self._process_one_file()
            except Exception as e:
                self.logger.error(f"Worker {self.worker_id} unexpected error: {e}")
                time.sleep(self.config.retry_delay)
        
        self.logger.info(f"Worker {self.worker_id} stopped")
    
    def stop(self):
        """Signal the worker to stop"""
        self.should_stop = True
    
    def _process_one_file(self):
        """Process a single file"""
        # Get next file
        file_path = self.file_manager.get_next_file()
        if not file_path:
            self.logger.debug(f"Worker {self.worker_id}: No files available, sleeping...")
            time.sleep(self.config.retry_delay)
            return
        
        self.logger.info(f"Worker {self.worker_id} processing: {Path(file_path).name}")
        file_type = self._get_file_type(file_path)
        
        # Process based on file type
        try:
            if file_type == FileType.TASK:
                success = self._process_task(file_path)
            elif file_type == FileType.CSV:
                success = self._process_csv(file_path)
            else:
                self.logger.warning(f"Unknown file type: {file_path}")
                success = False
            
            # Handle result
            if success:
                self.file_manager.remove_file(file_path)
                self.logger.info(f"Worker {self.worker_id}: Successfully processed {Path(file_path).name}")
            else:
                self.file_manager.release_file(file_path)
                self.logger.warning(f"Worker {self.worker_id}: Failed to process {Path(file_path).name}")
                
        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} error processing {Path(file_path).name}: {e}")
            self.file_manager.release_file(file_path)
    
    def _get_file_type(self, file_path: str) -> Optional[FileType]:
        """Determine file type from extension"""
        ext = Path(file_path).suffix.lower()
        
        for file_type in FileType:
            if file_type.value == ext:
                return file_type
        return None
    
    def _process_csv(self, file_path: str) -> bool:
        """Process a CSV file"""
        server = None
        
        try:
            # Get server
            server = self.server_manager.get_best_server()
            if not server:
                self.logger.error("No healthy servers available")
                return False
            
            # Get table mapping
            table_name = self.server_manager.check_table_mapping(server, file_path)
            
            # Upload file
            client = UploadClient(server, self.config, self.logger)
            return client.upload_file(file_path, table_name)
            
        except ServerError:
            time.sleep(self.config.retry_delay)
            return False
        finally:
            if server:
                self.server_manager.release_server(server)
    
    def _process_task(self, file_path: str) -> bool:
        """Process a task file"""
        #server = None
        
        try:
            server = self.server_manager.get_best_server()
            if not server:
                self.logger.error("No healthy servers available")
                return False
            
            # Get table mapping
            
            # Upload file
            client = UploadClient(server, self.config, self.logger)
            return client.upload_file(file_path, "table_name")
                
        except ServerError:
            time.sleep(self.config.retry_delay)
            return False
        finally:
            self.server_manager.release_server(server)



# ============================================================================
# Worker Pool Manager
# ============================================================================

class WorkerPool:
    """Manages a pool of workers"""
    
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.logger = self._setup_logging()
        self.shutdown_requested = False
        
        # Initialize managers
        self.file_manager = FileManager(config, self.logger)
        self.server_manager = ServerManager(config, self.logger)
        
        # Create output directory
        Path(config.output_directory).mkdir(parents=True, exist_ok=True)
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger("WorkerPool")
        logger.setLevel(getattr(logging, self.config.log_level))
        
        formatter = logging.Formatter(self.config.log_format)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.FileHandler(self.config.log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def run(self):
        """Run the worker pool"""
        self.logger.info(f"Starting worker pool with {self.config.num_workers} workers")
        self.config.num_workers=int(self.config.num_workers)
        self.logger.info(f"Watching directory: {self.config.watch_directory}")
        
        # Ensure watch directory exists
        Path(self.config.watch_directory).mkdir(parents=True, exist_ok=True)
        
        # Track workers
        self.workers = []
        
        with ThreadPoolExecutor(max_workers=self.config.num_workers) as executor:
            futures = {}  # Map futures to workers
            
            # Start initial workers
            for i in range(self.config.num_workers):
                worker = Worker(
                    self.config,
                    self.file_manager,
                    self.server_manager,
                    self.logger,
                    worker_id=i
                )
                self.workers.append(worker)
                future = executor.submit(worker.run)
                futures[future] = worker
            
            self.logger.info(f"Started {len(futures)} workers")
            
            try:
                # Keep running until shutdown
                while not self.shutdown_requested:
                    # Just sleep and let workers run
                    time.sleep(1)
                    
                    # Check if any workers have died unexpectedly
                    dead_futures = []
                    for future in futures:
                        if future.done():
                            try:
                                future.result()  # This will raise any exceptions
                            except Exception as e:
                                self.logger.error(f"Worker died with exception: {e}")
                            dead_futures.append(future)
                    
                    # Restart any dead workers
                    for dead_future in dead_futures:
                        dead_worker = futures[dead_future]
                        del futures[dead_future]
                        self.workers.remove(dead_worker)
                        
                        # Start replacement worker
                        self.logger.info(f"Restarting dead worker {dead_worker.worker_id}")
                        new_worker = Worker(
                            self.config,
                            self.file_manager,
                            self.server_manager,
                            self.logger,
                            worker_id=dead_worker.worker_id
                        )
                        self.workers.append(new_worker)
                        new_future = executor.submit(new_worker.run)
                        futures[new_future] = new_worker
                        
            except KeyboardInterrupt:
                self.logger.info("Shutdown signal received")
                self.shutdown(executor, futures)
    
    def shutdown(self, executor: ThreadPoolExecutor, futures: dict):
        """Gracefully shutdown the worker pool"""
        self.logger.info("Initiating graceful shutdown...")
        self.shutdown_requested = True
        
        # Signal all workers to stop
        for worker in self.workers:
            worker.stop()
        
        # Wait for workers to complete
        self.logger.info("Waiting for workers to stop...")
        for future in futures:
            try:
                future.result(timeout=10)
            except Exception as e:
                self.logger.debug(f"Worker shutdown exception: {e}")
        
        self.logger.info("Worker pool shutdown complete")


# ============================================================================
# Command Line Interface
# ============================================================================

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Multi-threaded worker system for processing CSV and task files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           # Run with 1 worker
  %(prog)s --workers 10              # Run with 10 workers
  %(prog)s --dir custom/             # Monitor custom directory
  %(prog)s --verbose                 # Enable verbose logging
  %(prog)s --workers 5 --dir data/  # 5 workers monitoring 'data' directory
        """
    )
    
    parser.add_argument(
        '-w', '--workers',
        type=int,
        default=1,
        help='Number of worker threads (default: 1)'
    )
    
    parser.add_argument(
        '-d', '--dir',
        type=str,
        default='loadingcsv',
        help='Directory to monitor for files (default: loadingcsv)'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='output',
        help='Output directory for logs (default: output)'
    )
    
    parser.add_argument(
        '--health-url',
        type=str,
        default='https://go3.aimachengine.com/health',
        help='Server health check URL'
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=6000,
        help='Upload timeout in seconds (default: 6000)'
    )
    
    parser.add_argument(
        '--retry-delay',
        type=int,
        default=5,
        help='Retry delay in seconds (default: 5)'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        default='worker.log',
        help='Log file name (default: worker.log)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose (DEBUG) logging'
    )
    
    parser.add_argument(
        '--mysql-uri',
        type=str,
        help='MySQL connection URI (overrides default)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point for the worker system"""
    args = parse_arguments()
    
    # Build configuration from arguments INFO
    config = WorkerConfig(
        watch_directory=args.dir,
        output_directory=args.output,
        health_check_url=args.health_url,
        upload_timeout=args.timeout,
        retry_delay=args.retry_delay,
        num_workers=args.workers,
        log_file=args.log_file,
        log_level='DEBUG' if args.verbose else 'DEBUG'
    )
    
    # Override MySQL URI if provided
    if args.mysql_uri:
        config.mysql_uri = args.mysql_uri
    
    # Print startup information
    print("=" * 60)
    print("CSV Worker System")
    print("=" * 60)
    print(f"Workers: {config.num_workers}")
    print(f"Watch Directory: {config.watch_directory}")
    print(f"Output Directory: {config.output_directory}")
    print(f"Health Check URL: {config.health_check_url}")
    print(f"Log File: {config.log_file}")
    print(f"Log Level: {config.log_level}")
    print("=" * 60)
    print("Press Ctrl+C to stop\n")
    
    # Create and run worker pool
    pool = WorkerPool(config)
    
    try:
        pool.run()
    except KeyboardInterrupt:
        print("\nShutdown initiated...")
    except Exception as e:
        print(f"Fatal error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())