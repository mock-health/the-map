"""Characterization tests for tools/resolve_endpoints_to_npd.py.

Synthetic inputs cover:
  - build_lookups: by_addr keyed on address_normalized; by_host keyed on hostname
  - resolve_one canonical_url hit → high confidence with NPI
  - resolve_one hostname_registered → low confidence with npi: None (invariant)
  - resolve_one miss → None
  - iter_fleet_endpoints: flattens clusters, carries cluster_id and org name
  - resolve_vendor summary arithmetic via per-endpoint resolve_one aggregation
"""
from __future__ import annotations

from tools import resolve_endpoints_to_npd as rn


def _build_npd_index(endpoints: list[dict]) -> dict:
    """Build a synthetic NPD index for testing."""
    return {"endpoints": endpoints}


def _build_fleet(endpoints: list[dict]) -> dict:
    """Build a synthetic fleet for testing."""
    return {
        "capstmt_shape_clusters": [
            {
                "cluster_id": "cluster-1",
                "endpoints": endpoints,
            },
        ],
    }


# --- build_lookups tests -------------------------------------------------------

def test_build_lookups_by_addr_keyed_on_address_normalized() -> None:
    """by_addr should be keyed on the address_normalized field."""
    npd = _build_npd_index([
        {
            "address_normalized": "https://fhir.example.org/r4",
            "address_raw": "https://fhir.example.org/R4/",
            "npd_endpoint_id": "e1",
            "npd_endpoint_name": "Example Hospital R4",
            "managing_org": {
                "npi": "1234567890",
                "name": "Example Hospital",
                "parent_npi": None,
                "parent_name": None,
                "address": {"state": "IA", "city": "Iowa City", "postalCode": "52240"},
            },
        },
    ])
    by_addr, _by_host = rn.build_lookups(npd)

    # Key should be normalized (lowercase)
    assert "https://fhir.example.org/r4" in by_addr
    assert by_addr["https://fhir.example.org/r4"]["npd_endpoint_id"] == "e1"


def test_build_lookups_by_host_keyed_on_lowercase_hostname() -> None:
    """by_host should be keyed on lowercase hostname of address_raw."""
    npd = _build_npd_index([
        {
            "address_raw": "https://Fhir.Example.ORG/r4/",
            "npd_endpoint_id": "e1",
            "managing_org": {"npi": "1234567890", "name": "Example", "address": {}},
        },
        {
            "address_raw": "https://fhir.example.org/r5/",
            "npd_endpoint_id": "e2",
            "managing_org": {"npi": "1234567891", "name": "Example2", "address": {}},
        },
    ])
    _by_addr, by_host = rn.build_lookups(npd)

    assert "fhir.example.org" in by_host
    assert len(by_host["fhir.example.org"]) == 2


def test_build_lookups_skips_empty_address() -> None:
    """Entries with empty address_normalized should not appear in by_addr."""
    npd = _build_npd_index([
        {
            "address_normalized": "",
            "address_raw": "https://example.org/r4",
            "npd_endpoint_id": "e1",
            "managing_org": {"npi": "1234567890", "name": "Example", "address": {}},
        },
        {
            "address_normalized": "https://fhir.example.org/r4",
            "address_raw": "https://fhir.example.org/r4/",
            "npd_endpoint_id": "e2",
            "managing_org": {"npi": "1234567891", "name": "Example2", "address": {}},
        },
    ])
    by_addr, _by_host = rn.build_lookups(npd)

    assert len(by_addr) == 1
    assert "https://fhir.example.org/r4" in by_addr


# --- resolve_one tests ----------------------------------------------------------

def test_resolve_one_canonical_url_hit() -> None:
    """canonical_url match should return high confidence with NPI propagated."""
    npd = _build_npd_index([
        {
            "address_normalized": "https://fhir.example.org/R4",
            "address_raw": "https://fhir.example.org/R4/",
            "npd_endpoint_id": "e1",
            "npd_endpoint_name": "Example Hospital R4",
            "managing_org": {
                "npi": "1234567890",
                "name": "Example Hospital",
                "parent_npi": "9876543210",
                "parent_name": "Example Health System",
                "address": {"state": "IA", "city": "Iowa City", "postalCode": "52240"},
            },
        },
        # Another endpoint on same host but different path (for hostname test)
        {
            "address_normalized": "https://fhir.example.org/R5",
            "address_raw": "https://fhir.example.org/R5",
            "npd_endpoint_id": "e2",
            "npd_endpoint_name": "Example Hospital R5",
            "managing_org": {
                "npi": "1234567891",
                "name": "Example Hospital",
                "parent_npi": "9876543210",
                "parent_name": "Example Health System",
                "address": {"state": "IA", "city": "Iowa City", "postalCode": "52240"},
            },
        },
    ])
    by_addr, by_host = rn.build_lookups(npd)

    # Same address_raw as the NPD entry (case matches)
    fleet_ep = {
        "endpoint_id": "fleet-ep-1",
        "address": "https://fhir.example.org/R4/",
        "managing_organization_name": "Example Hospital",
    }
    result = rn.resolve_one(fleet_ep, by_addr, by_host)

    assert result is not None
    assert result["match_strategy"] == "canonical_url"
    assert result["confidence"] == "high"
    assert result["npi"] == "1234567890"
    assert result["org_name_npd"] == "Example Hospital"
    assert result["parent_org_npi"] == "9876543210"
    assert result["parent_org_name"] == "Example Health System"
    assert result["state"] == "IA"
    assert result["city"] == "Iowa City"
    assert result["postal_code"] == "52240"


def test_resolve_one_hostname_registered_npi_null_invariant() -> None:
    """hostname_registered strategy must return npi: None even if the NPD entry has an NPI.
    This is the critical invariant: shared tenant hosts must not attribute an NPI."""
    npd = _build_npd_index([
        {
            "address_raw": "https://fhir.tenanthost.org/tenant1",
            "npd_endpoint_id": "e1",
            "managing_org": {
                "npi": "1111111111",
                "name": "Tenant 1 Hospital",
                "address": {"state": "CA", "city": "Los Angeles", "postalCode": "90001"},
            },
        },
        {
            "address_raw": "https://fhir.tenanthost.org/tenant2",
            "npd_endpoint_id": "e2",
            "managing_org": {
                "npi": "2222222222",
                "name": "Tenant 2 Clinic",
                "address": {"state": "CA", "city": "San Diego", "postalCode": "92101"},
            },
        },
    ])
    by_addr, by_host = rn.build_lookups(npd)

    # This endpoint has a different path but same hostname
    fleet_ep = {
        "endpoint_id": "fleet-ep-unknown",
        "address": "https://fhir.tenanthost.org/unknown-tenant",
        "managing_organization_name": "Unknown Tenant Org",
    }
    result = rn.resolve_one(fleet_ep, by_addr, by_host)

    assert result is not None
    assert result["match_strategy"] == "hostname_registered"
    assert result["confidence"] == "low"
    # Critical invariant: hostname match must NOT attribute NPI
    assert result["npi"] is None
    assert result["org_name_npd"] is None
    assert result["parent_org_npi"] is None
    assert result["parent_org_name"] is None
    assert result["state"] is None
    assert result["city"] is None
    assert result["postal_code"] is None
    # Should report how many endpoints share this host
    assert result["hostname_npd_endpoint_count"] == 2


def test_resolve_one_hostname_registered_single_entry() -> None:
    """hostname_registered on a host with exactly 1 NPD entry should still return npi: None."""
    npd = _build_npd_index([
        {
            "address_raw": "https://fhir.singlehost.org/endpoint",
            "npd_endpoint_id": "e1",
            "managing_org": {
                "npi": "3333333333",
                "name": "Single Host Org",
                "address": {"state": "NY", "city": "New York", "postalCode": "10001"},
            },
        },
    ])
    by_addr, by_host = rn.build_lookups(npd)

    fleet_ep = {
        "endpoint_id": "fleet-ep-x",
        "address": "https://fhir.singlehost.org/another-path",
        "managing_organization_name": "Some Tenant",
    }
    result = rn.resolve_one(fleet_ep, by_addr, by_host)

    assert result is not None
    assert result["match_strategy"] == "hostname_registered"
    assert result["confidence"] == "low"
    assert result["npi"] is None
    assert result["hostname_npd_endpoint_count"] == 1


def test_resolve_one_miss_unknown_host() -> None:
    """Unknown host should return None."""
    npd = _build_npd_index([
        {
            "address_raw": "https://known.host.org/r4",
            "npd_endpoint_id": "e1",
            "managing_org": {"npi": "1234567890", "name": "Known", "address": {}},
        },
    ])
    by_addr, by_host = rn.build_lookups(npd)

    fleet_ep = {
        "endpoint_id": "fleet-ep-unknown",
        "address": "https://unknown.host.org/r4",
        "managing_organization_name": "Unknown Org",
    }
    result = rn.resolve_one(fleet_ep, by_addr, by_host)

    assert result is None


def test_resolve_one_miss_empty_address() -> None:
    """Endpoint with empty address should return None."""
    npd = _build_npd_index([
        {
            "address_raw": "https://known.host.org/r4",
            "npd_endpoint_id": "e1",
            "managing_org": {"npi": "1234567890", "name": "Known", "address": {}},
        },
    ])
    by_addr, by_host = rn.build_lookups(npd)

    fleet_ep = {
        "endpoint_id": "fleet-ep-empty",
        "address": "",
        "managing_organization_name": "",
    }
    result = rn.resolve_one(fleet_ep, by_addr, by_host)

    assert result is None


# --- iter_fleet_endpoints tests -----------------------------------------------

def test_iter_fleet_endpoints_flattens_clusters() -> None:
    """Should flatten clusters and carry cluster_id and managing_organization_name."""
    fleet = {
        "capstmt_shape_clusters": [
            {
                "cluster_id": "cluster-1",
                "endpoints": [
                    {
                        "endpoint_id": "ep-1",
                        "address": "https://fhir.ep1.org/r4",
                        "managing_organization_name": "Org 1",
                    },
                    {
                        "endpoint_id": "ep-2",
                        "address": "https://fhir.ep2.org/r4",
                        "managing_organization_name": "Org 2",
                    },
                ],
            },
        ],
    }

    records = rn.iter_fleet_endpoints(fleet)

    assert len(records) == 2
    assert records[0]["endpoint_id"] == "ep-1"
    assert records[0]["cluster_id"] == "cluster-1"
    assert records[0]["managing_organization_name"] == "Org 1"
    assert records[1]["endpoint_id"] == "ep-2"
    assert records[1]["cluster_id"] == "cluster-1"
    assert records[1]["managing_organization_name"] == "Org 2"


def test_iter_fleet_endpoints_empty_clusters() -> None:
    """Empty capstmt_shape_clusters should return empty list."""
    fleet = {"capstmt_shape_clusters": []}
    assert rn.iter_fleet_endpoints(fleet) == []

    fleet_empty = {}
    assert rn.iter_fleet_endpoints(fleet_empty) == []


# --- resolve_vendor tests -------------------------------------------------------

def test_resolve_vendor_summary_arithmetic() -> None:
    """Test resolve_vendor summary arithmetic via direct resolution of endpoints."""
    # Create synthetic NPD index
    npd = _build_npd_index([
        {
            "address_normalized": "https://fhir.match1.org/r4",
            "address_raw": "https://fhir.match1.org/r4/",
            "npd_endpoint_id": "e1",
            "npd_endpoint_name": "Match1 Hospital",
            "managing_org": {"npi": "1234567890", "name": "Match1", "address": {"state": "CA"}},
        },
        {
            "address_raw": "https://fhir.tenant.org/tenant1",
            "npd_endpoint_id": "e2",
            "managing_org": {"npi": "1111111111", "name": "Tenant1", "address": {}},
        },
    ])

    # Build lookups from NPD
    by_addr, by_host = rn.build_lookups(npd)

    # Create synthetic fleet
    fleet = _build_fleet([
        {
            "endpoint_id": "ep-1",
            "address": "https://fhir.match1.org/r4/",
            "managing_organization_name": "Match1 Hospital",
        },
        {
            "endpoint_id": "ep-2",
            "address": "https://fhir.tenant.org/unknown",
            "managing_organization_name": "Some Tenant",
        },
        {
            "endpoint_id": "ep-3",
            "address": "https://fhir.missing.org/r4",
            "managing_organization_name": "Unknown Org",
        },
    ])

    endpoints = rn.iter_fleet_endpoints(fleet)

    matches = []
    for ep in endpoints:
        m = rn.resolve_one(ep, by_addr, by_host)
        if m:
            matches.append(m)

    assert len(matches) == 2  # ep-1 (canonical_url), ep-2 (hostname_registered)
    assert matches[0]["match_strategy"] == "canonical_url"
    assert matches[1]["match_strategy"] == "hostname_registered"
    assert matches[0]["npi"] == "1234567890"
    assert matches[1]["npi"] is None  # hostname match has no NPI

    # Verify counts match expected values
    matched = len(matches)
    matched_with_npi = sum(1 for m in matches if m.get("npi"))
    unmatched = len(endpoints) - matched
    by_strategy = {}
    for m in matches:
        by_strategy[m["match_strategy"]] = by_strategy.get(m["match_strategy"], 0) + 1

    assert matched == 2
    assert matched_with_npi == 1
    assert unmatched == 1
    assert by_strategy["canonical_url"] == 1
    assert by_strategy["hostname_registered"] == 1


# --- edge cases ----------------------------------------------------------------

def test_resolve_one_address_normalized_mismatch_does_not_match() -> None:
    """Address with different normalization should not match canonical_url."""
    npd = _build_npd_index([
        {
            "address_normalized": "https://fhir.example.org/r4",
            "address_raw": "https://fhir.example.org/r4/",
            "npd_endpoint_id": "e1",
            "managing_org": {"npi": "1234567890", "name": "Example", "address": {}},
        },
        # Add another endpoint on same host so hostname isn't triggered
        {
            "address_normalized": "https://fhir.example.org/r5",
            "address_raw": "https://fhir.example.org/r5",
            "npd_endpoint_id": "e2",
            "managing_org": {"npi": "1234567891", "name": "Example2", "address": {}},
        },
    ])
    by_addr, by_host = rn.build_lookups(npd)

    # Different URL path that doesn't exist in by_addr
    # and is not on a host with ONLY one endpoint
    fleet_ep = {
        "endpoint_id": "ep-diff",
        "address": "https://different.example.org/r4",
        "managing_organization_name": "Different Org",
    }
    result = rn.resolve_one(fleet_ep, by_addr, by_host)

    assert result is None


def test_build_lookups_last_entry_wins_for_duplicate_normalized_address() -> None:
    """When multiple NPD entries share the same normalized address, last entry wins."""
    npd = _build_npd_index([
        {
            "address_normalized": "https://fhir.example.org/r4",
            "address_raw": "https://fhir.example.org/r4/",
            "npd_endpoint_id": "e-first",
            "managing_org": {"npi": "1111111111", "name": "First", "address": {}},
        },
        {
            "address_normalized": "https://fhir.example.org/r4",
            "address_raw": "https://fhir.example.org/r4/v2",
            "npd_endpoint_id": "e-last",
            "managing_org": {"npi": "2222222222", "name": "Last", "address": {}},
        },
    ])
    by_addr, _by_host = rn.build_lookups(npd)

    # by_addr should have the last entry
    assert by_addr["https://fhir.example.org/r4"]["npd_endpoint_id"] == "e-last"
    assert by_addr["https://fhir.example.org/r4"]["managing_org"]["npi"] == "2222222222"
