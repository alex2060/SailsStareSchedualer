#!/usr/bin/env python3
"""
csv_splitter.py - CSV file splitting utility

Standalone module for splitting CSV files into smaller chunks.
Can be imported as a module or run directly from command line.

Usage as script:
    python csv_splitter.py input.csv                    # Default 10 rows per chunk
    python csv_splitter.py input.csv --size 20          # 20 rows per chunk
    python csv_splitter.py input.csv --output chunks/   # Custom output directory
    python csv_splitter.py input.csv --sizes 2 5 20     # Multiple chunk sizes

Usage as module:
    from csv_splitter import CSVSplitter
    splitter = CSVSplitter()
    result = splitter.split_file('data.csv', chunk_size=100)
"""

import os
import csv
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class SplitResult:
    """Result of a CSV split operation"""
    input_file: str
    chunk_size: int
    total_rows: int
    num_chunks: int
    output_dir: str
    files: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'input_file': self.input_file,
            'chunk_size': self.chunk_size,
            'total_rows': self.total_rows,
            'num_chunks': self.num_chunks,
            'output_dir': self.output_dir,
            'files': self.files
        }


class CSVSplitter:
    """CSV file splitter with dialect detection and flexible options"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize the CSV splitter
        
        Args:
            logger: Optional logger instance. If None, creates a default logger.
        """
        self.logger = logger or self._setup_default_logger()
    
    def _setup_default_logger(self) -> logging.Logger:
        """Setup a default logger"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def detect_dialect(self, file_path: str, sample_size: int = 8192) -> csv.Dialect:
        """Detect CSV dialect (delimiter, quote char, etc.)
        
        Args:
            file_path: Path to CSV file
            sample_size: Number of bytes to sample for detection
            
        Returns:
            Detected CSV dialect
        """
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                sample = f.read(sample_size)
                dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t', '|'])
                self.logger.debug(f"Detected delimiter: '{dialect.delimiter}'")
                return dialect
        except (csv.Error, Exception) as e:
            self.logger.warning(f"Could not detect dialect: {e}. Using default (comma-delimited)")
            return csv.get_dialect('excel')
    
    def read_csv_info(self, file_path: str) -> Tuple[List[str], int, csv.Dialect]:
        """Read CSV file information without loading all data
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Tuple of (header, row_count, dialect)
        """
        dialect = self.detect_dialect(file_path)
        
        with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f, dialect)
            header = next(reader, [])
            row_count = sum(1 for _ in reader)
        
        return header, row_count, dialect
    
    def split_file(self, 
                   input_file: str,
                   chunk_size: int = 10,
                   output_dir: Optional[str] = None,
                   prefix: Optional[str] = None) -> SplitResult:
        """Split a CSV file into smaller chunks
        
        Args:
            input_file: Path to input CSV file
            chunk_size: Number of rows per chunk
            output_dir: Output directory (default: same as input file)
            prefix: Optional prefix for output files
            
        Returns:
            SplitResult object with details of the operation
        """
        # Validate input
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        if chunk_size < 1:
            raise ValueError(f"Chunk size must be at least 1, got {chunk_size}")
        
        # Setup paths
        input_path = Path(input_file)
        if output_dir is None:
            output_dir = input_path.parent
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Determine base name
        base_name = prefix if prefix else input_path.stem
        
        # Get file info
        self.logger.info(f"Analyzing {input_file}...")
        header, total_rows, dialect = self.read_csv_info(input_file)
        
        if total_rows == 0:
            self.logger.warning(f"File {input_file} has no data rows")
            return SplitResult(
                input_file=str(input_file),
                chunk_size=chunk_size,
                total_rows=0,
                num_chunks=0,
                output_dir=str(output_dir),
                files=[]
            )
        
        # Calculate chunks
        num_chunks = (total_rows + chunk_size - 1) // chunk_size
        
        self.logger.info(
            f"Splitting {input_file}: {total_rows} rows, "
            f"{len(header)} columns, {num_chunks} chunks of {chunk_size} rows"
        )
        
        # Create result object
        result = SplitResult(
            input_file=str(input_file),
            chunk_size=chunk_size,
            total_rows=total_rows,
            num_chunks=num_chunks,
            output_dir=str(output_dir),
            files=[]
        )
        
        # Process file in chunks
        with open(input_file, 'r', newline='', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f, dialect)
            _ = next(reader)  # Skip header
            
            for chunk_idx in range(num_chunks):
                # Generate output filename
                output_file = output_dir / f"{base_name}_chunk{chunk_size}_{chunk_idx + 1}.csv"
                
                # Read chunk rows
                chunk_rows = []
                for _ in range(chunk_size):
                    try:
                        chunk_rows.append(next(reader))
                    except StopIteration:
                        break
                
                # Write chunk
                with open(output_file, 'w', newline='', encoding='utf-8') as out_f:
                    writer = csv.writer(out_f, dialect)
                    writer.writerow(header)
                    writer.writerows(chunk_rows)
                
                # Record file info
                file_info = {
                    'filename': str(output_file),
                    'chunk_index': chunk_idx + 1,
                    'rows': len(chunk_rows),
                    'size_bytes': output_file.stat().st_size
                }
                result.files.append(file_info)
                
                self.logger.debug(f"Created chunk {chunk_idx + 1}/{num_chunks}: {output_file.name}")
        
        self.logger.info(f"Split complete: {num_chunks} files created in {output_dir}")
        return result
    
    def split_with_multiple_sizes(self,
                                  input_file: str,
                                  chunk_sizes: List[int],
                                  output_dir: Optional[str] = None) -> List[SplitResult]:
        """Split a file with multiple chunk sizes
        
        Args:
            input_file: Path to input CSV file
            chunk_sizes: List of chunk sizes to create
            output_dir: Output directory
            
        Returns:
            List of SplitResult objects
        """
        results = []
        
        for size in chunk_sizes:
            self.logger.info(f"Creating chunks of size {size}...")
            result = self.split_file(
                input_file=input_file,
                chunk_size=size,
                output_dir=output_dir,
                prefix=f"{Path(input_file).stem}_size{size}"
            )
            results.append(result)
        
        return results
    
    def get_header(self, file_path: str) -> List[str]:
        """Get just the header row from a CSV file
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            List of column names
        """
        try:
            dialect = self.detect_dialect(file_path)
            with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f, dialect)
                return next(reader, [])
        except Exception as e:
            self.logger.error(f"Error reading header from {file_path}: {e}")
            return []
    
    def get_row_count(self, file_path: str) -> int:
        """Get the number of data rows (excluding header) in a CSV file
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Number of data rows
        """
        try:
            with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as f:
                return sum(1 for _ in f) - 1  # Subtract 1 for header
        except Exception as e:
            self.logger.error(f"Error counting rows in {file_path}: {e}")
            return 0


def main():
    """Command-line interface for CSV splitter"""
    parser = argparse.ArgumentParser(
        description="Split CSV files into smaller chunks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s data.csv                        # Split into 10-row chunks
  %(prog)s data.csv --size 100             # Split into 100-row chunks
  %(prog)s data.csv --output chunks/       # Output to 'chunks' directory
  %(prog)s data.csv --sizes 10 50 100      # Create multiple chunk sizes
  %(prog)s data.csv --verbose              # Show detailed progress
        """
    )
    
    parser.add_argument(
        'input_file',
        help='CSV file to split'
    )
    
    parser.add_argument(
        '-s', '--size',
        type=int,
        default=10,
        help='Number of rows per chunk (default: 10)'
    )
    
    parser.add_argument(
        '-m', '--sizes',
        type=int,
        nargs='+',
        help='Multiple chunk sizes (overrides --size)'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Output directory (default: same as input file)'
    )
    
    parser.add_argument(
        '-p', '--prefix',
        help='Prefix for output files (default: input filename)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create splitter
    splitter = CSVSplitter()
    
    try:
        if args.sizes:
            # Multiple sizes specified
            results = splitter.split_with_multiple_sizes(
                input_file=args.input_file,
                chunk_sizes=args.sizes,
                output_dir=args.output
            )
            
            print("\nSplit Summary:")
            print("-" * 50)
            for result in results:
                print(f"Chunk size {result.chunk_size}: {result.num_chunks} files")
                total_size = sum(f['size_bytes'] for f in result.files)
                print(f"  Total size: {total_size / 1024:.1f} KB")
        else:
            # Single size
            result = splitter.split_file(
                input_file=args.input_file,
                chunk_size=args.size,
                output_dir=args.output,
                prefix=args.prefix
            )
            
            print("\nSplit Summary:")
            print("-" * 50)
            print(f"Input file: {result.input_file}")
            print(f"Total rows: {result.total_rows}")
            print(f"Chunk size: {result.chunk_size}")
            print(f"Files created: {result.num_chunks}")
            print(f"Output directory: {result.output_dir}")
            
            if args.verbose and result.files:
                print("\nCreated files:")
                for f in result.files:
                    print(f"  - {Path(f['filename']).name}: {f['rows']} rows, {f['size_bytes']} bytes")
    
    except Exception as e:
        logging.error(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())