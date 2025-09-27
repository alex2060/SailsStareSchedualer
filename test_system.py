#!/usr/bin/env python3

"""
test_system.py - Test script for CSV Worker System

This script tests the API endpoints and basic functionality.
"""

import requests
import time
import json
import os
from pathlib import Path

API_BASE = "http://localhost:5000"

def test_api_endpoint(endpoint, method='GET', data=None, description=""):
    """Test a single API endpoint"""
    print(f"Testing {method} {endpoint} - {description}")

    try:
        url = f"{API_BASE}{endpoint}"

        if method == 'GET':
            response = requests.get(url, timeout=10)
        elif method == 'POST':
            response = requests.post(url, json=data, timeout=10)
        elif method == 'DELETE':
            response = requests.delete(url, timeout=10)
        else:
            print(f"  ❌ Unsupported method: {method}")
            return False

        print(f"  Status: {response.status_code}")

        if response.status_code == 200:
            try:
                result = response.json()
                print(f"  ✅ Success: {json.dumps(result, indent=2)[:200]}...")
                return True
            except:
                print(f"  ✅ Success (non-JSON response)")
                return True
        else:
            print(f"  ❌ Failed: {response.text[:200]}")
            return False

    except requests.exceptions.ConnectionError:
        print(f"  ❌ Connection failed - Is the API service running?")
        return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def create_test_csv():
    """Create a test CSV file"""
    test_data = """name,email,age
John Doe,john@example.com,30
Jane Smith,jane@example.com,25
Bob Johnson,bob@example.com,35
Alice Brown,alice@example.com,28
"""

    # Ensure loadingcsv directory exists
    Path("loadingcsv").mkdir(exist_ok=True)

    with open("loadingcsv/test_data.csv", "w") as f:
        f.write(test_data)

    print("✅ Created test CSV file: loadingcsv/test_data.csv")

def run_tests():
    """Run comprehensive API tests"""
    print("="*60)
    print("CSV Worker System API Tests")
    print("="*60)

    # Test basic connectivity
    tests_passed = 0
    total_tests = 0

    # Health check
    total_tests += 1
    if test_api_endpoint("/health", description="Health check"):
        tests_passed += 1

    print("")

    # Status check
    total_tests += 1
    if test_api_endpoint("/status", description="System status"):
        tests_passed += 1

    print("")

    # Configuration
    total_tests += 1
    if test_api_endpoint("/config", description="Get configuration"):
        tests_passed += 1

    print("")

    # Files listing (should work even if empty)
    total_tests += 1
    if test_api_endpoint("/files", description="List files"):
        tests_passed += 1

    print("")

    # Create test file and upload it via file system
    create_test_csv()

    # Wait a moment for file to be detected
    print("Waiting 10 seconds for file detection...")
    time.sleep(10)

    # Check files again
    total_tests += 1
    if test_api_endpoint("/files", description="List files after adding test file"):
        tests_passed += 1

    print("")

    # Try to start workers
    total_tests += 1
    if test_api_endpoint("/start", method="POST", data={"num_workers": 1}, description="Start workers"):
        tests_passed += 1

    print("")

    # Wait a moment
    time.sleep(5)

    # Check status after starting
    total_tests += 1
    if test_api_endpoint("/status", description="Status after starting workers"):
        tests_passed += 1

    print("")

    # Check logs
    total_tests += 1
    if test_api_endpoint("/logs", description="Get recent logs"):
        tests_passed += 1

    print("")

    # Stop workers
    total_tests += 1
    if test_api_endpoint("/stop", method="POST", description="Stop workers"):
        tests_passed += 1

    print("")
    print("="*60)
    print(f"Test Results: {tests_passed}/{total_tests} tests passed")

    if tests_passed == total_tests:
        print("🎉 All tests passed! System is working correctly.")
    else:
        print(f"⚠️  {total_tests - tests_passed} tests failed. Check the API service.")

    print("="*60)

    return tests_passed == total_tests

def main():
    """Main test function"""
    print("Starting CSV Worker System API Tests...")
    print("Make sure the system is running with: ./start.sh")
    print("")

    # Check if API is reachable
    try:
        response = requests.get(f"{API_BASE}/health", timeout=5)
        if response.status_code != 200:
            print("❌ API service is not healthy. Please start the system first.")
            return 1
    except:
        print("❌ Cannot connect to API service. Please start the system first:")
        print("   ./start.sh")
        return 1

    # Run the tests
    success = run_tests()

    return 0 if success else 1

if __name__ == "__main__":
    exit(main())
