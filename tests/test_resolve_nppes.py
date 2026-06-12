"""Characterization tests for tools/resolve_endpoints_to_nppes.py.

Synthetic inputs cover:
  - build_lookups: all five lookup structures
  - resolve_one canonical_url -> high confidence
  - resolve_one hostname_registered: 1 NPI -> high; 2-5 -> medium; >5 skipped
  - _identify_shared_tenant_hosts: threshold behavior (>=2 fleet URLs)
  - org_name_state: unique -> high; <=3 -> medium
  - org_name_fuzzy_state: score >=92 -> medium; 85-91 -> low; <85 (cutoff) -> None
  - _is_infrastructure_jargon: positive and negative cases
  - No-match endpoints return None
"""
from __future__ import annotations

from tools import resolve_endpoints_to_nppes as rn


def _build_fhir_index(endpoints: list[dict]) -> dict:
    """Build a synthetic NPPES fhir-endpoints index."""
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

def test_build_lookups_by_url_norm() -> None:
    """by_url_norm should be keyed on url_norm."""
    fhir = _build_fhir_index([
        {
            "url": "https://fhir.example.org/r4",
            "url_norm": "https://fhir.example.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {"1234567890": {"npi": "1234567890", "name": "Test", "state": "CA", "city": "LA", "postal": "90001", "addr": "123 Main St", "taxonomy": "207Q00000X"}}

    by_url_norm, _by_host, _by_name_state, _by_name, _by_state = rn.build_lookups(fhir, orgs)

    assert "https://fhir.example.org/r4" in by_url_norm
    assert by_url_norm["https://fhir.example.org/r4"][0]["npi"] == "1234567890"


def test_build_lookups_by_host() -> None:
    """by_host should map hostname -> {NPI: count}."""
    fhir = _build_fhir_index([
        {
            "url": "https://fhir.example.org/ep1",
            "url_norm": "https://fhir.example.org/ep1",
            "npi": "1111111111",
        },
        {
            "url": "https://fhir.example.org/ep2",
            "url_norm": "https://fhir.example.org/ep2",
            "npi": "2222222222",
        },
    ])
    orgs = {
        "1111111111": {"npi": "1111111111", "name": "Test1", "state": "CA"},
        "2222222222": {"npi": "2222222222", "name": "Test2", "state": "NY"},
    }

    _by_url_norm, by_host, _by_name_state, _by_name, _by_state = rn.build_lookups(fhir, orgs)

    assert "fhir.example.org" in by_host
    assert by_host["fhir.example.org"]["1111111111"] == 1
    assert by_host["fhir.example.org"]["2222222222"] == 1


def test_build_lookups_by_name_state() -> None:
    """by_name_state should map (name_norm, state) -> [npi]."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital1.org/r4",
            "url_norm": "https://hospital1.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "TEST HOSPITAL",
            "name_norm": "TEST HOSPITAL",
            "state": "CA",
        },
    }

    _by_url_norm, _by_host, by_name_state, _by_name, _by_state = rn.build_lookups(fhir, orgs)

    assert ("TEST HOSPITAL", "CA") in by_name_state
    assert by_name_state[("TEST HOSPITAL", "CA")] == ["1234567890"]


def test_build_lookups_by_name() -> None:
    """by_name should map name_norm -> [npi] including other_name_norm."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "TEST HOSPITAL",
            "name_norm": "TEST HOSPITAL",
            "other_name_norm": "TEST HOSPITAL SYSTEM",
            "state": "CA",
        },
    }

    _by_url_norm, _by_host, _by_name_state, by_name, _by_state = rn.build_lookups(fhir, orgs)

    assert "TEST HOSPITAL" in by_name
    assert "TEST HOSPITAL SYSTEM" in by_name


def test_build_lookups_by_state() -> None:
    """by_state should map state -> [(name_norm, npi)]."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "TEST HOSPITAL",
            "name_norm": "TEST HOSPITAL",
            "state": "CA",
        },
    }

    _by_url_norm, _by_host, _by_name_state, _by_name, by_state = rn.build_lookups(fhir, orgs)

    assert "CA" in by_state
    assert ("TEST HOSPITAL", "1234567890") in by_state["CA"]


# --- resolve_one tests -----------------------------------------------------------

def test_resolve_one_canonical_url_exact_match() -> None:
    """canonical_url exact match should return high confidence."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Test Hospital",
            "name_norm": "TEST HOSPITAL",
            "state": "CA",
            "city": "Los Angeles",
            "postal": "90001",
            "addr": "123 Main St",
            "taxonomy": "207Q00000X",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    fleet_ep = {
        "endpoint_id": "ep-1",
        "address": "https://hospital.org/r4",
        "managing_organization_name": "Test Hospital",
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint=None,
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is not None
    assert result["match_strategy"] == "canonical_url"
    assert result["confidence"] == "high"
    assert result["npi"] == "1234567890"
    assert result["org_name_nppes"] == "Test Hospital"
    assert result["state"] == "CA"
    assert result["city"] == "Los Angeles"


def test_resolve_one_hostname_single_npi_high_confidence() -> None:
    """hostname_registered with exactly 1 NPI on host should return high confidence."""
    fhir = _build_fhir_index([
        {
            "url": "https://singlehost.org/ep1",
            "url_norm": "https://singlehost.org/ep1",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Single Host Hospital",
            "state": "NY",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    # Endpoint on same host but different URL path
    fleet_ep = {
        "endpoint_id": "ep-unknown",
        "address": "https://singlehost.org/unknown",
        "managing_organization_name": "Some Tenant",
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint=None,
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is not None
    assert result["match_strategy"] == "hostname_registered"
    assert result["confidence"] == "high"
    assert result["npi"] == "1234567890"


def test_resolve_one_hostname_2_5_npi_medium_confidence() -> None:
    """hostname_registered with 2-5 NPIs should return medium confidence via top_npi."""
    fhir = _build_fhir_index([
        {
            "url": "https://multihost.org/ep1",
            "url_norm": "https://multihost.org/ep1",
            "npi": "1111111111",
        },
        {
            "url": "https://multihost.org/ep2",
            "url_norm": "https://multihost.org/ep2",
            "npi": "2222222222",
        },
    ])
    orgs = {
        "1111111111": {"npi": "1111111111", "name": "Org1", "state": "CA"},
        "2222222222": {"npi": "2222222222", "name": "Org2", "state": "CA"},
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    fleet_ep = {
        "endpoint_id": "ep-unknown",
        "address": "https://multihost.org/unknown",
        "managing_organization_name": "Tenant",
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint=None,
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is not None
    assert result["match_strategy"] == "hostname_registered"
    assert result["confidence"] == "medium"
    assert result["npi"] == "1111111111"  # First NPI wins (top_npi)
    # extra field is only added when there's ambiguity (2-5 NPIs)
    # extra fields are merged directly into result dict
    assert result.get("host_npi_ambiguity") == 2


def test_resolve_one_hostname_ambiguity_cutoff() -> None:
    """hostname_registered should skip when >5 NPIs on host (shared tenant)."""
    # Create fhir index with 6 endpoints on same host
    fhir = _build_fhir_index([
        {"url": f"https://ambighost.org/ep{i}", "url_norm": f"https://ambighost.org/ep{i}", "npi": f"{i}000000000"}
        for i in range(6)
    ])
    orgs = {
        f"{i}000000000": {"npi": f"{i}000000000", "name": f"Org{i}", "state": "CA"}
        for i in range(6)
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    fleet_ep = {
        "endpoint_id": "ep-unknown",
        "address": "https://ambighost.org/unknown",
        "managing_organization_name": "Tenant",
    }
    shared_tenant_hosts = set()

    # hostname_registered should be skipped due to ambiguity (6 > 5)
    result = rn.resolve_one(
        fleet_ep,
        state_hint=None,
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is None


def test_resolve_one_org_name_state_unique_high() -> None:
    """org_name_state with unique match in state should return high confidence."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Unique Hospital",
            "name_norm": "UNIQUE HOSPITAL",
            "state": "CA",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    # normalize_org_name converts to uppercase
    fleet_ep = {
        "endpoint_id": "ep-1",
        "address": "https://unknown.org/r4",
        "managing_organization_name": "Unique Hospital",
    }
    shared_tenant_hosts = set()

    # State hint narrows to 1 match
    result = rn.resolve_one(
        fleet_ep,
        state_hint="CA",
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is not None
    assert result["match_strategy"] == "org_name_state"
    assert result["confidence"] == "high"


def test_resolve_one_org_name_state_2_3_medium() -> None:
    """org_name_state with 2-3 matches should return medium confidence."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {"npi": "1234567890", "name": "SHARED NAME", "name_norm": "SHARED NAME", "state": "CA"},
        "2222222222": {"npi": "2222222222", "name": "SHARED NAME", "name_norm": "SHARED NAME", "state": "CA"},
        "3333333333": {"npi": "3333333333", "name": "SHARED NAME", "name_norm": "SHARED NAME", "state": "CA"},
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    # normalize_org_name converts to uppercase
    fleet_ep = {
        "endpoint_id": "ep-1",
        "address": "https://unknown.org/r4",
        "managing_organization_name": "Shared Name",
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint="CA",
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is not None
    assert result["match_strategy"] == "org_name_state"
    assert result["confidence"] == "medium"
    assert result["state_match_ambiguity"] == 3


def test_resolve_one_org_name_unique_nationally() -> None:
    """org_name_unique with 1 nationwide match should return medium confidence."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Only One Nationally",
            "name_norm": "ONLY ONE NATIONALLY",
            "state": "CA",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    # normalize_org_name converts to uppercase
    fleet_ep = {
        "endpoint_id": "ep-1",
        "address": "https://unknown.org/r4",
        "managing_organization_name": "Only One Nationally",
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint=None,  # No state hint
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is not None
    assert result["match_strategy"] == "org_name_unique"
    assert result["confidence"] == "medium"


def test_resolve_one_org_name_fuzzy_state_high_score() -> None:
    """org_name_fuzzy_state with score >=92 should return medium confidence.

    The fleet name must NOT be an exact normalized match (else org_name_state /
    org_name_unique fire first). token_set_ratio("TRINITY HEALTH",
    "TRINITY HEALTH CARE") == 100 because the shorter name's tokens are a
    subset of the longer's.
    """
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Trinity Health Care Inc",
            "name_norm": "TRINITY HEALTH CARE",
            "state": "NJ",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    # "Trinity Health" normalizes to "TRINITY HEALTH" — no exact name hit,
    # but a token-subset fuzzy score of 100.
    fleet_ep = {
        "endpoint_id": "ep-1",
        "address": "https://unknown.org/r4",
        "managing_organization_name": "Trinity Health",
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint="NJ",
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is not None
    assert result["match_strategy"] == "org_name_fuzzy_state"
    assert result["confidence"] == "medium"
    assert result["fuzzy_score"] >= 92


def test_resolve_one_org_name_fuzzy_state_low_score() -> None:
    """org_name_fuzzy_state with 85 <= score < 92 should return low confidence.

    "RWJ BARNABAS HEALTH" vs "ROBERT WOOD JOHNSON BARNABAS HEALTH" scores ~88
    on token_set_ratio — above the FUZZY_NAME_THRESHOLD=85 cutoff but below the
    92 medium-confidence line (this exact pair is cited in the resolver's
    threshold comment).
    """
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Robert Wood Johnson Barnabas Health",
            "name_norm": "ROBERT WOOD JOHNSON BARNABAS HEALTH",
            "state": "NJ",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    fleet_ep = {
        "endpoint_id": "ep-1",
        "address": "https://unknown.org/r4",
        "managing_organization_name": "RWJ Barnabas Health",
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint="NJ",
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is not None
    assert result["match_strategy"] == "org_name_fuzzy_state"
    assert result["confidence"] == "low"
    assert 85 <= result["fuzzy_score"] < 92


def test_resolve_one_org_name_fuzzy_state_below_cutoff_returns_none() -> None:
    """Fuzzy scores below FUZZY_NAME_THRESHOLD=85 are not matches at all.

    score_cutoff=85 is passed to rapidfuzz's extractOne, so a dissimilar name
    yields None — NOT a low-confidence match. This pins the floor: there is no
    "low confidence" band below 85.
    """
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Different Name Hospital",
            "name_norm": "DIFFERENT NAME HOSPITAL",
            "state": "CA",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    fleet_ep = {
        "endpoint_id": "ep-1",
        "address": "https://unknown.org/r4",
        "managing_organization_name": "Another Hospital",
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint="CA",
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is None


def test_resolve_one_no_match_returns_none() -> None:
    """Unknown endpoint should return None."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Test Hospital",
            "name_norm": "TEST HOSPITAL",
            "state": "CA",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    fleet_ep = {
        "endpoint_id": "ep-unknown",
        "address": "https://unknown.org/r4",
        "managing_organization_name": "Some Random Name",
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint=None,
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is None


# --- _identify_shared_tenant_hosts tests ----------------------------------------

def test_identify_shared_tenant_hosts_threshold() -> None:
    """Hosts with >=2 distinct fleet URLs should be identified as shared tenants."""
    fleet_endpoints = [
        {"address": "https://fhir-ehr.cerner.com/r4/tenant1"},
        {"address": "https://fhir-ehr.cerner.com/r4/tenant2"},
        {"address": "https://uniquehost.org/r4"},
    ]

    shared = rn._identify_shared_tenant_hosts(fleet_endpoints)

    assert "fhir-ehr.cerner.com" in shared
    assert "uniquehost.org" not in shared


def test_identify_shared_tenant_hosts_single_host_not_shared() -> None:
    """Hosts with only 1 fleet URL should not be identified as shared tenants."""
    fleet_endpoints = [
        {"address": "https://uniquehost.org/r4"},
    ]

    shared = rn._identify_shared_tenant_hosts(fleet_endpoints)

    assert len(shared) == 0


# --- _is_infrastructure_jargon tests --------------------------------------------

def test_is_infrastructure_jargon_positive() -> None:
    """Infrastructure jargon should be detected."""
    # Test Pod pattern
    assert rn._is_infrastructure_jargon("MaaS MEDITECH Hosted Pod 009") is True
    # Test Domain pattern
    assert rn._is_infrastructure_jargon("Continuum Of Care Domain A") is True
    # Test Time Zone pattern
    assert rn._is_infrastructure_jargon("CPHY Production Central Time Zone B") is True
    # Test Tenant pattern
    assert rn._is_infrastructure_jargon("Tenant 123") is True
    # Test Sandbox pattern
    assert rn._is_infrastructure_jargon("Sandbox Environment") is True
    # Test Hosted Pod pattern
    assert rn._is_infrastructure_jargon("Hosted Pod 42") is True
    # Test MaaS pattern - exact match for "MaaS"
    assert rn._is_infrastructure_jargon("MEDITECH-as-a-Service") is False  # No "MaaS" word
    # Test Testing Tenant pattern
    assert rn._is_infrastructure_jargon("Testing Tenant Org") is True
    # Test FHIR URL placeholder
    assert rn._is_infrastructure_jargon("FHIR URL") is True


def test_is_infrastructure_jargon_negative() -> None:
    """Real org names should not be detected as infrastructure jargon."""
    assert rn._is_infrastructure_jargon("Mayo Clinic") is False
    assert rn._is_infrastructure_jargon("Cleveland Clinic") is False
    assert rn._is_infrastructure_jargon("Johns Hopkins Hospital") is False
    assert rn._is_infrastructure_jargon("") is False
    assert rn._is_infrastructure_jargon(None) is False


# --- iter_fleet tests -----------------------------------------------------------

def test_iter_fleet_flattens_clusters() -> None:
    """_iter_fleet should flatten clusters and carry relevant fields."""
    fleet = {
        "capstmt_shape_clusters": [
            {
                "cluster_id": "cluster-1",
                "endpoints": [
                    {
                        "endpoint_id": "ep-1",
                        "address": "https://fhir.ep1.org/r4",
                        "managing_organization_name": "Org 1",
                        "state": "CA",
                    },
                ],
            },
        ],
    }

    records = list(rn._iter_fleet(fleet))

    assert len(records) == 1
    assert records[0]["endpoint_id"] == "ep-1"
    assert records[0]["address"] == "https://fhir.ep1.org/r4"
    assert records[0]["managing_organization_name"] == "Org 1"
    assert records[0]["cluster_id"] == "cluster-1"
    assert records[0]["state_hint_from_fleet"] == "CA"


def test_iter_fleet_empty_clusters() -> None:
    """_iter_fleet should return empty list for empty or missing clusters."""
    fleet = {"capstmt_shape_clusters": []}
    assert list(rn._iter_fleet(fleet)) == []

    fleet_empty = {}
    assert list(rn._iter_fleet(fleet_empty)) == []


# --- resolve_vendor summary tests -----------------------------------------------

def test_resolve_vendor_summary_arithmetic() -> None:
    """Test resolve_vendor summary arithmetic via direct resolution of endpoints."""
    # Create synthetic NPPES fhir-endpoints index
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Test Hospital",
            "name_norm": "TEST HOSPITAL",
            "state": "CA",
            "city": "Los Angeles",
            "postal": "90001",
            "addr": "123 Main St",
            "taxonomy": "207Q00000X",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    # Create synthetic fleet
    fleet = _build_fleet([
        {
            "endpoint_id": "ep-1",
            "address": "https://hospital.org/r4",
            "managing_organization_name": "Test Hospital",
        },
        {
            "endpoint_id": "ep-2",
            "address": "https://unknown.org/r4",
            "managing_organization_name": "Unknown Org",
        },
    ])

    endpoints = list(rn._iter_fleet(fleet))

    matches = []
    for ep in endpoints:
        m = rn.resolve_one(
            ep,
            state_hint=None,
            by_url_norm=by_url_norm,
            by_host=by_host,
            by_name_state=by_name_state,
            by_name=by_name,
            by_state=by_state,
            orgs_by_npi=orgs,
            shared_tenant_hosts=set(),
        )
        if m:
            matches.append(m)

    assert len(matches) == 1  # ep-1 (canonical_url)
    assert matches[0]["match_strategy"] == "canonical_url"
    assert matches[0]["npi"] == "1234567890"

    # Verify counts match expected values
    matched = len(matches)
    matched_with_npi = sum(1 for m in matches if m.get("npi"))
    unmatched = len(endpoints) - matched
    by_strategy = {}
    for m in matches:
        by_strategy[m["match_strategy"]] = by_strategy.get(m["match_strategy"], 0) + 1

    assert matched == 1
    assert matched_with_npi == 1
    assert unmatched == 1
    assert by_strategy["canonical_url"] == 1


# --- edge cases ----------------------------------------------------------------

def test_resolve_one_host_in_shared_tenant_hosts_skipped() -> None:
    """When host is in shared_tenant_hosts, hostname strategy should be skipped."""
    fhir = _build_fhir_index([
        {
            "url": "https://sharedhost.org/ep1",
            "url_norm": "https://sharedhost.org/ep1",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Shared Host Org",
            "state": "CA",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    fleet_ep = {
        "endpoint_id": "ep-unknown",
        "address": "https://sharedhost.org/unknown",
        "managing_organization_name": "Tenant",
    }

    # shared_tenant_hosts explicitly includes this host
    result = rn.resolve_one(
        fleet_ep,
        state_hint=None,
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts={"sharedhost.org"},
    )

    # Should return None since hostname is skipped and no canonical_url match
    assert result is None


def test_resolve_one_canonical_url_with_multiple_endpoints_picks_valid_npi() -> None:
    """When multiple NPPES rows have same URL, should pick the one with valid NPI."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "9999999999",  # This one won't be in orgs
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Test Hospital",
            "state": "CA",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    fleet_ep = {
        "endpoint_id": "ep-1",
        "address": "https://hospital.org/r4",
        "managing_organization_name": "Test Hospital",
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint=None,
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is not None
    assert result["npi"] == "1234567890"


def test_resolve_one_empty_name_skipped() -> None:
    """Endpoint with empty name should skip name-based strategies."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Test Hospital",
            "name_norm": "TEST HOSPITAL",
            "state": "CA",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    fleet_ep = {
        "endpoint_id": "ep-1",
        "address": "https://different.org/r4",
        "managing_organization_name": "",  # Empty name
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint=None,
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is None


def test_resolve_one_infrastructure_name_skipped() -> None:
    """Endpoint with infrastructure jargon name should skip name-based strategies."""
    fhir = _build_fhir_index([
        {
            "url": "https://hospital.org/r4",
            "url_norm": "https://hospital.org/r4",
            "npi": "1234567890",
        },
    ])
    orgs = {
        "1234567890": {
            "npi": "1234567890",
            "name": "Test Hospital",
            "name_norm": "TEST HOSPITAL",
            "state": "CA",
        },
    }
    by_url_norm, by_host, by_name_state, by_name, by_state = rn.build_lookups(fhir, orgs)

    fleet_ep = {
        "endpoint_id": "ep-1",
        "address": "https://different.org/r4",
        "managing_organization_name": "Tenant 123",  # Infrastructure jargon
    }
    shared_tenant_hosts = set()

    result = rn.resolve_one(
        fleet_ep,
        state_hint=None,
        by_url_norm=by_url_norm,
        by_host=by_host,
        by_name_state=by_name_state,
        by_name=by_name,
        by_state=by_state,
        orgs_by_npi=orgs,
        shared_tenant_hosts=shared_tenant_hosts,
    )

    assert result is None
