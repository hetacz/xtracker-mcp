"""Test the new XTracker Polymarket API endpoint with and without query parameters."""
import json
import os
from datetime import datetime

import requests

# Create output directory for test results
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "downloads", "polymarket_tests")
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


def test_endpoint_without_params():
    """Test endpoint without query parameters."""
    print("=" * 80)
    print("TEST 1: Request WITHOUT query parameters")
    print("=" * 80)

    url = "https://xtracker.polymarket.com/api/users/elonmusk/posts"
    print(f"URL: {url}\n")

    try:
        response = requests.get(url, timeout=30)
        print(f"✓ Status Code: {response.status_code}")
        print(f"✓ Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        print(f"✓ Response Length: {len(response.content)} bytes\n")

        # Check if JSON or CSV
        content_type = response.headers.get('Content-Type', '')
        if 'json' in content_type.lower():
            data = response.json()
            print(f"✓ Format: JSON")
            if isinstance(data, list):
                print(f"✓ Structure: List with {len(data)} items")
                if data:
                    print(f"✓ Sample item keys: {list(data[0].keys())}")
                    print(f"\nFirst item:")
                    print(json.dumps(data[0], indent=2))
                    if len(data) > 1:
                        print(f"\nLast item:")
                        print(json.dumps(data[-1], indent=2))
            elif isinstance(data, dict):
                print(f"✓ Structure: Dictionary")
                print(f"✓ Keys: {list(data.keys())}")
                print(json.dumps(data, indent=2)[:1000])
        else:
            print(f"✓ Format: {content_type or 'Plain text/CSV'}")
            print(f"\nFirst 1000 characters:")
            print(response.text[:1000])

        # Save to file
        save_response_to_file(response, "test1_no_params")

        return response

    except Exception as e:
        print(f"✗ ERROR: {e}")
        return None


def test_endpoint_only_start_date():
    """Test endpoint with only startDate parameter."""
    print("\n\n" + "=" * 80)
    print("TEST 2: Request WITH only startDate parameter")
    print("=" * 80)

    url = "https://xtracker.polymarket.com/api/users/elonmusk/posts"
    params = {
        "startDate": "2025-11-25T17:00:00.000Z"
    }
    full_url = f"{url}?startDate={params['startDate']}"
    print(f"URL: {full_url}\n")

    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"✓ Status Code: {response.status_code}")
        print(f"✓ Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        print(f"✓ Response Length: {len(response.content)} bytes\n")

        # Check if JSON or CSV
        content_type = response.headers.get('Content-Type', '')
        if 'json' in content_type.lower():
            data = response.json()
            print(f"✓ Format: JSON")
            if isinstance(data, list):
                print(f"✓ Structure: List with {len(data)} items")
                if data:
                    print(f"✓ Sample item keys: {list(data[0].keys())}")
                    print(f"\nFirst item:")
                    print(json.dumps(data[0], indent=2))
                    if len(data) > 1:
                        print(f"\nLast item:")
                        print(json.dumps(data[-1], indent=2))
            elif isinstance(data, dict):
                print(f"✓ Structure: Dictionary")
                print(f"✓ Keys: {list(data.keys())}")
                print(json.dumps(data, indent=2)[:1000])
        else:
            print(f"✓ Format: {content_type or 'Plain text/CSV'}")
            print(f"\nFirst 1000 characters:")
            print(response.text[:1000])

        # Save to file
        save_response_to_file(response, "test2_only_start_date")

        return response

    except Exception as e:
        print(f"✗ ERROR: {e}")
        return None


def test_endpoint_only_end_date():
    """Test endpoint with only endDate parameter."""
    print("\n\n" + "=" * 80)
    print("TEST 3: Request WITH only endDate parameter")
    print("=" * 80)

    url = "https://xtracker.polymarket.com/api/users/elonmusk/posts"
    params = {
        "endDate": "2025-12-02T17:00:59.000Z"
    }
    full_url = f"{url}?endDate={params['endDate']}"
    print(f"URL: {full_url}\n")

    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"✓ Status Code: {response.status_code}")
        print(f"✓ Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        print(f"✓ Response Length: {len(response.content)} bytes\n")

        # Check if JSON or CSV
        content_type = response.headers.get('Content-Type', '')
        if 'json' in content_type.lower():
            data = response.json()
            print(f"✓ Format: JSON")
            if isinstance(data, list):
                print(f"✓ Structure: List with {len(data)} items")
                if data:
                    print(f"✓ Sample item keys: {list(data[0].keys())}")
                    print(f"\nFirst item:")
                    print(json.dumps(data[0], indent=2))
                    if len(data) > 1:
                        print(f"\nLast item:")
                        print(json.dumps(data[-1], indent=2))
            elif isinstance(data, dict):
                print(f"✓ Structure: Dictionary")
                print(f"✓ Keys: {list(data.keys())}")
                print(json.dumps(data, indent=2)[:1000])
        else:
            print(f"✓ Format: {content_type or 'Plain text/CSV'}")
            print(f"\nFirst 1000 characters:")
            print(response.text[:1000])

        # Save to file
        save_response_to_file(response, "test3_only_end_date")

        return response

    except Exception as e:
        print(f"✗ ERROR: {e}")
        return None


def test_endpoint_with_params():
    """Test endpoint with both startDate and endDate parameters."""
    print("\n\n" + "=" * 80)
    print("TEST 4: Request WITH both startDate and endDate parameters (2025-11-25 to 2025-12-02)")
    print("=" * 80)

    url = "https://xtracker.polymarket.com/api/users/elonmusk/posts"
    params = {
        "startDate": "2025-11-25T17:00:00.000Z",
        "endDate": "2025-12-02T17:00:59.000Z"
    }
    full_url = f"{url}?startDate={params['startDate']}&endDate={params['endDate']}"
    print(f"URL: {full_url}\n")

    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"✓ Status Code: {response.status_code}")
        print(f"✓ Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        print(f"✓ Response Length: {len(response.content)} bytes\n")

        # Check if JSON or CSV
        content_type = response.headers.get('Content-Type', '')
        if 'json' in content_type.lower():
            data = response.json()
            print(f"✓ Format: JSON")
            if isinstance(data, list):
                print(f"✓ Structure: List with {len(data)} items")
                if data:
                    print(f"✓ Sample item keys: {list(data[0].keys())}")
                    print(f"\nFirst item:")
                    print(json.dumps(data[0], indent=2))
                    if len(data) > 1:
                        print(f"\nLast item:")
                        print(json.dumps(data[-1], indent=2))
            elif isinstance(data, dict):
                print(f"✓ Structure: Dictionary")
                print(f"✓ Keys: {list(data.keys())}")
                print(json.dumps(data, indent=2)[:1000])
        else:
            print(f"✓ Format: {content_type or 'Plain text/CSV'}")
            print(f"\nFirst 1000 characters:")
            print(response.text[:1000])

        # Save to file
        save_response_to_file(response, "test4_start_and_end_date")

        return response

    except Exception as e:
        print(f"✗ ERROR: {e}")
        return None


def test_endpoint_historical_start():
    """Test endpoint with historical startDate from 2020."""
    print("\n\n" + "=" * 80)
    print("TEST 5: Request WITH historical startDate (2020-01-01)")
    print("=" * 80)

    url = "https://xtracker.polymarket.com/api/users/elonmusk/posts"
    params = {
        "startDate": "2020-01-01T00:00:00.000Z"
    }
    full_url = f"{url}?startDate={params['startDate']}"
    print(f"URL: {full_url}\n")

    try:
        response = requests.get(url, params=params, timeout=60)  # Increased timeout for larger response
        print(f"✓ Status Code: {response.status_code}")
        print(f"✓ Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        print(f"✓ Response Length: {len(response.content)} bytes\n")

        # Check if JSON or CSV
        content_type = response.headers.get('Content-Type', '')
        if 'json' in content_type.lower():
            data = response.json()
            print(f"✓ Format: JSON")
            if isinstance(data, list):
                print(f"✓ Structure: List with {len(data)} items")
                if data:
                    print(f"✓ Sample item keys: {list(data[0].keys())}")
                    print(f"\nFirst item:")
                    print(json.dumps(data[0], indent=2))
                    if len(data) > 1:
                        print(f"\nLast item:")
                        print(json.dumps(data[-1], indent=2))
            elif isinstance(data, dict):
                print(f"✓ Structure: Dictionary")
                print(f"✓ Keys: {list(data.keys())}")

                # For dict with 'data' array, show count and date range
                if 'data' in data and isinstance(data['data'], list):
                    print(f"✓ Data array length: {len(data['data'])} items")
                    if data['data']:
                        first_date = data['data'][0].get('createdAt', 'N/A')
                        last_date = data['data'][-1].get('createdAt', 'N/A')
                        print(f"✓ Date range: {last_date} to {first_date}")
                        print(f"\nFirst item:")
                        print(json.dumps(data['data'][0], indent=2))
                        print(f"\nLast item:")
                        print(json.dumps(data['data'][-1], indent=2))
                else:
                    print(json.dumps(data, indent=2)[:1000])
        else:
            print(f"✓ Format: {content_type or 'Plain text/CSV'}")
            print(f"\nFirst 1000 characters:")
            print(response.text[:1000])

        # Save to file
        save_response_to_file(response, "test5_historical_2020")

        return response

    except Exception as e:
        print(f"✗ ERROR: {e}")
        return None


def main():
    """Run all tests."""
    print("XTracker Polymarket API Endpoint Test")
    print("=" * 80)
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 80)
    print()

    # Test 1: No query params
    response_no_params = test_endpoint_without_params()

    # Test 2: Only startDate
    response_only_start = test_endpoint_only_start_date()

    # Test 3: Only endDate
    response_only_end = test_endpoint_only_end_date()

    # Test 4: Both startDate and endDate
    response_with_params = test_endpoint_with_params()

    # Test 5: Historical startDate (2020-01-01)
    response_historical = test_endpoint_historical_start()

    # Summary
    print("\n\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if response_no_params:
        print(f"✓ Test 1 (no params): {response_no_params.status_code} - {len(response_no_params.content)} bytes")
    else:
        print(f"✗ Test 1 (no params): Failed")

    if response_only_start:
        print(
            f"✓ Test 2 (only startDate): {response_only_start.status_code} - {len(response_only_start.content)} bytes")
    else:
        print(f"✗ Test 2 (only startDate): Failed")

    if response_only_end:
        print(f"✓ Test 3 (only endDate): {response_only_end.status_code} - {len(response_only_end.content)} bytes")
    else:
        print(f"✗ Test 3 (only endDate): Failed")

    if response_with_params:
        print(f"✓ Test 4 (both params): {response_with_params.status_code} - {len(response_with_params.content)} bytes")
    else:
        print(f"✗ Test 4 (both params): Failed")

    if response_historical:
        print(
            f"✓ Test 5 (historical 2020): {response_historical.status_code} - {len(response_historical.content)} bytes")
    else:
        print(f"✗ Test 5 (historical 2020): Failed")

    print(f"\n✓ All response data saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
