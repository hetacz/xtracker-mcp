"""Test the XTracker Polymarket API endpoint with comprehensive date combinations and CSV output."""
import csv
import json
import os
from datetime import datetime

import requests

# Create output directory for test results
OUTPUT_DIR = r"C:\kod\xt\downloads\polymarket_tests"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def save_response_to_file(response, test_name: str):
    """Save response to file with timestamp."""
    if response is None:
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save raw response
    raw_filename = os.path.join(OUTPUT_DIR, f"{test_name}_{timestamp}_raw.txt")
    with open(raw_filename, 'wb') as f:
        f.write(response.content)
    print(f"✓ Saved raw response to: {raw_filename}")

    # Save metadata
    meta_filename = os.path.join(OUTPUT_DIR, f"{test_name}_{timestamp}_meta.json")
    metadata = {
        "test_name": test_name,
        "timestamp": timestamp,
        "url": response.url,
        "status_code": response.status_code,
        "headers": dict(response.headers),
        "content_length": len(response.content),
        "content_type": response.headers.get('Content-Type', 'N/A')
    }
    with open(meta_filename, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"✓ Saved metadata to: {meta_filename}")

    # If JSON, save pretty-printed version
    content_type = response.headers.get('Content-Type', '')
    if 'json' in content_type.lower():
        try:
            data = response.json()
            json_filename = os.path.join(OUTPUT_DIR, f"{test_name}_{timestamp}_pretty.json")
            with open(json_filename, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"✓ Saved pretty JSON to: {json_filename}")
        except:
            pass


def convert_json_to_csv(response, test_name: str):
    """Convert JSON response to CSV and save."""
    if response is None:
        return

    try:
        content_type = response.headers.get('Content-Type', '')
        if 'json' in content_type.lower():
            data = response.json()
            if isinstance(data, dict) and 'data' in data:
                posts = data['data']
                if posts:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_filename = os.path.join(OUTPUT_DIR, f"{test_name}_{timestamp}.csv")

                    with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                        fieldnames = ['id', 'userId', 'platformId', 'content', 'createdAt', 'importedAt', 'metrics']
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()

                        for post in posts:
                            writer.writerow(
                                {
                                    'id': post.get('id', ''),
                                    'userId': post.get('userId', ''),
                                    'platformId': post.get('platformId', ''),
                                    'content': post.get('content', ''),
                                    'createdAt': post.get('createdAt', ''),
                                    'importedAt': post.get('importedAt', ''),
                                    'metrics': post.get('metrics', '')
                                })

                    print(f"✓ Saved CSV to: {csv_filename}")
                    return csv_filename
    except Exception as e:
        print(f"✗ Error converting to CSV: {e}")
        return None


def test_custom(test_num: int, params: dict, description: str):
    """Generic test function for custom date combinations."""
    print("\n\n" + "=" * 80)
    print(f"TEST {test_num}: {description}")
    print("=" * 80)

    url = "https://xtracker.polymarket.com/api/users/elonmusk/posts"

    # Build URL display
    if params:
        param_str = "&".join([f"{k}={v}" for k, v in params.items()])
        full_url = f"{url}?{param_str}"
    else:
        full_url = url

    print(f"URL: {full_url}\n")

    try:
        response = requests.get(url, params=params, timeout=60)
        print(f"✓ Status Code: {response.status_code}")
        print(f"✓ Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        print(f"✓ Response Length: {len(response.content)} bytes\n")

        # Check if JSON
        content_type = response.headers.get('Content-Type', '')
        if 'json' in content_type.lower():
            data = response.json()
            print(f"✓ Format: JSON")
            if isinstance(data, dict) and 'data' in data:
                posts = data['data']
                print(f"✓ Structure: Dictionary with 'data' array")
                print(f"✓ Data array length: {len(posts)} items")
                if posts:
                    first_date = posts[0].get('createdAt', 'N/A')
                    last_date = posts[-1].get('createdAt', 'N/A')
                    print(f"✓ Date range: {last_date} to {first_date}")
                    print(f"\nFirst item:")
                    print(json.dumps(posts[0], indent=2))
                    if len(posts) > 1:
                        print(f"\nLast item:")
                        print(json.dumps(posts[-1], indent=2))
        else:
            print(f"✓ Format: {content_type or 'Plain text/CSV'}")
            print(f"\nFirst 1000 characters:")
            print(response.text[:1000])

        # Save to file
        test_name = f"test{test_num}_{description.lower().replace(' ', '_').replace(':', '').replace('(', '').replace(')', '').replace('-', '')}"
        save_response_to_file(response, test_name)
        convert_json_to_csv(response, test_name)

        return response

    except Exception as e:
        print(f"✗ ERROR: {e}")
        return None


def main():
    """Run all tests with specified date combinations."""
    print("XTracker Polymarket API Endpoint Test - Comprehensive Date Testing")
    print("=" * 80)
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 80)
    print()

    results = []

    # Test 1: No query params
    print("Test Set: Complete Date Range Testing")
    print("-" * 80)
    print("Start Dates: 2024-08-08, 2025-03-03, 2025-11-12")
    print("End Date: 2025-11-19")
    print("-" * 80)

    # Test 1: No parameters
    r1 = test_custom(1, {}, "No query parameters")
    results.append(("Test 1: No params", r1))

    # Test 2a: Only startDate = 2024-08-08
    r2a = test_custom(2, {"startDate": "2024-08-08T00:00:00.000Z"}, "Only startDate (2024-08-08)")
    results.append(("Test 2a: startDate 2024-08-08", r2a))

    # Test 2b: Only startDate = 2025-03-03
    r2b = test_custom(3, {"startDate": "2025-03-03T00:00:00.000Z"}, "Only startDate (2025-03-03)")
    results.append(("Test 2b: startDate 2025-03-03", r2b))

    # Test 2c: Only startDate = 2025-11-12
    r2c = test_custom(4, {"startDate": "2025-11-12T00:00:00.000Z"}, "Only startDate (2025-11-12)")
    results.append(("Test 2c: startDate 2025-11-12", r2c))

    # Test 3: Only endDate = 2025-11-19
    r3 = test_custom(5, {"endDate": "2025-11-19T23:59:59.999Z"}, "Only endDate (2025-11-19)")
    results.append(("Test 3: endDate 2025-11-19", r3))

    # Test 4a: startDate 2024-08-08 + endDate 2025-11-19
    r4a = test_custom(
        6, {
            "startDate": "2024-08-08T00:00:00.000Z",
            "endDate": "2025-11-19T23:59:59.999Z"
        }, "Both dates (2024-08-08 to 2025-11-19)")
    results.append(("Test 4a: 2024-08-08 to 2025-11-19", r4a))

    # Test 4b: startDate 2025-03-03 + endDate 2025-11-19
    r4b = test_custom(
        7, {
            "startDate": "2025-03-03T00:00:00.000Z",
            "endDate": "2025-11-19T23:59:59.999Z"
        }, "Both dates (2025-03-03 to 2025-11-19)")
    results.append(("Test 4b: 2025-03-03 to 2025-11-19", r4b))

    # Test 4c: startDate 2025-11-12 + endDate 2025-11-19
    r4c = test_custom(
        8, {
            "startDate": "2025-11-12T00:00:00.000Z",
            "endDate": "2025-11-19T23:59:59.999Z"
        }, "Both dates (2025-11-12 to 2025-11-19)")
    results.append(("Test 4c: 2025-11-12 to 2025-11-19", r4c))

    # Summary
    print("\n\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    for name, response in results:
        if response:
            print(f"✓ {name}: {response.status_code} - {len(response.content)} bytes")
        else:
            print(f"✗ {name}: Failed")

    print(f"\n✓ All response data saved to: {OUTPUT_DIR}")
    print("✓ JSON files: *_raw.txt, *_meta.json, *_pretty.json")
    print("✓ CSV files: *.csv")
    print(f"✓ Total tests executed: {len(results)}")


if __name__ == "__main__":
    main()
