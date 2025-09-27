#!/usr/bin/env python3

"""
csv_worker_api.py - REST API service for managing CSV worker system

Provides HTTP endpoints to:
- Start/stop the worker system
- Monitor file processing status
- Track files in the watch directory
- Get system health and statistics
"""

from flask import Flask, request, jsonify, render_template,send_from_directory
from flask_cors import CORS
import threading
import time
import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import signal
import sys

# Import the existing worker system
from csv_worker_system import WorkerPool, WorkerConfig, FileManager

from flask_cors import CORS

app = Flask(__name__)

# Allow specific origins

@dataclass
class FileInfo:
    """Information about a file in the watch directory"""
    filename: str
    path: str
    size: int
    modified: str
    status: str  # 'pending', 'processing', 'completed', 'error'
    worker_id: Optional[int] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_msg: Optional[str] = None

@dataclass
class SystemStatus:
    """Current system status"""
    running: bool
    num_workers: int
    active_workers: int
    total_files_processed: int
    files_pending: int
    files_processing: int
    files_completed: int
    files_error: int
    uptime: str
    last_activity: Optional[str] = None

class CSVWorkerAPI:
    """REST API for CSV Worker System"""

    def __init__(self, config: WorkerConfig):
        self.app = Flask(__name__)
        CORS(self.app, origins=['*'])

        self.config = config
        self.worker_pool: Optional[WorkerPool] = None
        self.worker_thread: Optional[threading.Thread] = None
        self.files: Dict[str, FileInfo] = {}  # filename -> FileInfo
        self.system_stats = {
            'total_processed': 0,
            'total_errors': 0,
            'start_time': datetime.now().isoformat()
        }

        # Setup logging
        self.logger = self._setup_logging()

        # Setup routes
        self._setup_routes()

        # Start file monitoring
        self.file_monitor_thread = threading.Thread(target=self._monitor_files, daemon=True)
        self.file_monitor_thread.start()

    def _setup_logging(self) -> logging.Logger:
        """Setup logging for the API"""
        logger = logging.getLogger('CSVWorkerAPI')
        #logger.setLevel(logging.INFO)
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _setup_routes(self):
        """Setup Flask routes"""

        @self.app.route('/health', methods=['GET'])
        def health():
            """Health check endpoint"""
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'worker_running': self.worker_pool is not None
            })

        @self.app.route('/status', methods=['GET'])
        def get_status():
            """Get system status"""
            status = self._get_system_status()
            return jsonify(asdict(status))

        @self.app.route('/health-check', methods=['GET'])
        def check_external_health():
            """Check the external server health"""
            try:
                import requests
                health_url = self.config.health_check_url
                
                response = requests.get(health_url, timeout=10)
                response.raise_for_status()
                
                return jsonify({
                    'url': health_url,
                    'status': 'healthy',
                    'status_code': response.status_code,
                    'response': response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text[:500]
                })
                
            except Exception as e:
                return jsonify({
                    'url': health_url,
                    'status': 'error',
                    'error': str(e)
                }), 500


        @self.app.route('/start', methods=['POST'])
        def start_workers():
            """Start the worker system"""
            if self.worker_pool is not None:
                return jsonify({'error': 'Workers already running'}), 400

            try:
                # Parse request parameters
                data = request.get_json() or {}

                # Update config if parameters provided
                if 'num_workers' in data:
                    self.config.num_workers = data['num_workers']
                if 'watch_directory' in data:
                    self.config.watch_directory = data['watch_directory']

                # Start worker pool
                self.worker_pool = WorkerPool(self.config)
                self.worker_thread = threading.Thread(target=self.worker_pool.run, daemon=True)
                self.worker_thread.start()

                self.logger.info(f"Started {self.config.num_workers} workers")
                return jsonify({
                    'message': 'Workers started successfully',
                    'num_workers': self.config.num_workers,
                    'watch_directory': self.config.watch_directory
                })

            except Exception as e:
                self.logger.error(f"Failed to start workers: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/stop', methods=['POST'])
        def stop_workers():
            """Stop the worker system"""
            if self.worker_pool is None:
                return jsonify({'error': 'Workers not running'}), 400

            try:
                self.worker_pool.shutdown_requested = True
                self.worker_pool = None
                self.worker_thread = None

                self.logger.info("Workers stopped")
                return jsonify({'message': 'Workers stopped successfully'})

            except Exception as e:
                self.logger.error(f"Failed to stop workers: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/files', methods=['GET'])
        def list_files():
            """List all files in watch directory with status"""
            files_list = []
            for file_info in self.files.values():
                files_list.append(asdict(file_info))

            # Sort by modification time (newest first)
            files_list.sort(key=lambda x: x['modified'], reverse=True)

            return jsonify({
                'files': files_list,
                'total': len(files_list),
                'pending': len([f for f in self.files.values() if f.status == 'pending']),
                'processing': len([f for f in self.files.values() if f.status == 'processing']),
                'completed': len([f for f in self.files.values() if f.status == 'completed']),
                'error': len([f for f in self.files.values() if f.status == 'error'])
            })

        @self.app.route('/files/<filename>', methods=['GET'])
        def get_file_info(filename):
            """Get information about a specific file"""
            if filename not in self.files:
                return jsonify({'error': 'File not found'}), 404

            return jsonify(asdict(self.files[filename]))

        @self.app.route('/files/<filename>', methods=['DELETE'])
        def delete_file(filename):
            """Delete a file from the watch directory"""
            if filename not in self.files:
                return jsonify({'error': 'File not found'}), 404

            try:
                file_path = self.files[filename].path
                os.remove(file_path)
                del self.files[filename]

                self.logger.info(f"Deleted file: {filename}")
                return jsonify({'message': f'File {filename} deleted successfully'})

            except Exception as e:
                self.logger.error(f"Failed to delete file {filename}: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/upload', methods=['POST'])
        def upload_file():
            """Upload a file to the watch directory"""
            if 'file' not in request.files:
                return jsonify({'error': 'No file provided'}), 400

            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400

            try:
                # Ensure watch directory exists
                Path(self.config.watch_directory).mkdir(parents=True, exist_ok=True)

                # Save file
                file_path = os.path.join(self.config.watch_directory, file.filename)
                file.save(file_path)

                self.logger.info(f"Uploaded file: {file.filename}")
                return jsonify({
                    'message': f'File {file.filename} uploaded successfully',
                    'path': file_path
                })

            except Exception as e:
                self.logger.error(f"Failed to upload file: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/config', methods=['GET'])
        def get_config():
            """Get current configuration"""
            return jsonify({
                'watch_directory': self.config.watch_directory,
                'output_directory': self.config.output_directory,
                'num_workers': self.config.num_workers,
                'health_check_url': self.config.health_check_url,
                'upload_timeout': self.config.upload_timeout,
                'retry_delay': self.config.retry_delay,
                'log_level': self.config.log_level
            })

        @self.app.route('/config', methods=['PUT'])
        def update_config():
            """Update configuration"""
            data = request.get_json()
            if not data:
                return jsonify({'error': 'No configuration data provided'}), 400

            try:
                # Update allowed configuration fields
                allowed_fields = [
                    'watch_directory', 'output_directory', 'num_workers',
                    'upload_timeout', 'retry_delay', 'log_level'
                ]

                for field in allowed_fields:
                    if field in data:
                        setattr(self.config, field, data[field])

                self.logger.info("Configuration updated")
                return jsonify({'message': 'Configuration updated successfully'})

            except Exception as e:
                self.logger.error(f"Failed to update configuration: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/logs', methods=['GET'])
        def get_logs():
            """Get recent log entries"""
            try:
                log_lines = []
                if os.path.exists(self.config.log_file):
                    with open(self.config.log_file, 'r') as f:
                        lines = f.readlines()
                        # Get last 100 lines
                        log_lines = [line.strip() for line in lines[-1000:]]

                return jsonify({
                    'logs': log_lines,
                    'total_lines': len(log_lines)
                })

            except Exception as e:
                self.logger.error(f"Failed to read logs: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/')
        def dashboard():
            return render_template('dashboard.html')

    def _monitor_files(self):
        """Monitor files in the watch directory"""
        while True:
            try:
                self._scan_directory()
                time.sleep(5)  # Scan every 5 seconds
            except Exception as e:
                self.logger.error(f"Error monitoring files: {e}")
                time.sleep(10)

    def _scan_directory(self):
        """Scan watch directory for files"""
        try:
            watch_path = Path(self.config.watch_directory)
            if not watch_path.exists():
                return

            current_files = set()

            # Scan for all files
            for file_path in watch_path.iterdir():
                if file_path.is_file():
                    filename = file_path.name
                    current_files.add(filename)

                    # Add new files or update existing ones
                    if filename not in self.files:
                        file_info = FileInfo(
                            filename=filename,
                            path=str(file_path),
                            size=file_path.stat().st_size,
                            modified=datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
                            status='pending'
                        )
                        self.files[filename] = file_info
                        self.logger.debug(f"New file detected: {filename}")
                    else:
                        # Update file info if modified
                        existing = self.files[filename]
                        new_modified = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                        if existing.modified != new_modified:
                            existing.modified = new_modified
                            existing.size = file_path.stat().st_size
                            # Reset to pending if file was modified
                            if existing.status in ['completed', 'error']:
                                existing.status = 'pending'

            # Remove files that no longer exist
            existing_files = set(self.files.keys())
            for filename in existing_files - current_files:
                if self.files[filename].status != 'processing':
                    del self.files[filename]
                    self.logger.debug(f"File removed: {filename}")

        except Exception as e:
            self.logger.error(f"Error scanning directory: {e}")

    def _get_system_status(self) -> SystemStatus:
        """Get current system status"""
        start_time = datetime.fromisoformat(self.system_stats['start_time'])
        uptime = str(datetime.now() - start_time).split('.')[0]  # Remove microseconds

        return SystemStatus(
            running=self.worker_pool is not None,
            num_workers=self.config.num_workers,
            active_workers=len(getattr(self.worker_pool, 'workers', [])) if self.worker_pool else 0,
            total_files_processed=self.system_stats['total_processed'],
            files_pending=len([f for f in self.files.values() if f.status == 'pending']),
            files_processing=len([f for f in self.files.values() if f.status == 'processing']),
            files_completed=len([f for f in self.files.values() if f.status == 'completed']),
            files_error=len([f for f in self.files.values() if f.status == 'error']),
            uptime=uptime
        )

    def run(self, host='0.0.0.0', port=5000, debug=False):
        """Run the Flask application"""
        self.logger.info(f"Starting CSV Worker API on {host}:{port}")
        self.app.run(host=host, port=port, debug=debug, threaded=True)

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="CSV Worker REST API")
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5010, help='Port to bind to')
    parser.add_argument('--workers', type=int, default=1, help='Default number of workers')
    parser.add_argument('--watch-dir', default='loadingcsv', help='Directory to watch for files')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')

    args = parser.parse_args()

    # Create configuration
    config = WorkerConfig(
        num_workers=args.workers,
        watch_directory=args.watch_dir
    )

    # Create and run API
    api = CSVWorkerAPI(config)

    try:
        api.run(host=args.host, port=args.port, debug=args.debug)
    except KeyboardInterrupt:
        print("\nShutting down...")
        if api.worker_pool:
            api.worker_pool.shutdown_requested = True
        sys.exit(0)

if __name__ == "__main__":
    main()
