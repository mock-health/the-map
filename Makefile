# The Map — common developer tasks.
#
# `make help` to see all targets. Each target is a thin wrapper around the
# Python pipeline (Phases A-F) so contributors don't need to memorize CLI
# invocations.

# Prefer the project's .venv for local dev; fall back to whatever python is on
# PATH so CI runners (which install Python via setup-python and skip the venv)
# can `make export` without an extra activation step. Override with `PY=...`
# if you keep your interpreter somewhere unusual.
PY ?= $(shell test -x .venv/bin/python && echo .venv/bin/python || command -v python3 || echo python)
EHRS := $(notdir $(wildcard ehrs/*))
QUARTER := $(shell date +%Y)-q$(shell echo $$(( ($$(date +%-m) - 1) / 3 + 1 )))
EXPORT_DIR := dist/data/the-map

.PHONY: help install validate render render-all synthesize fetch-all-anonymous discover-luxera test lint typecheck export clean clean-render pos-index resolve-pos resolve-pos-llm

help:
	@echo "The Map — developer tasks"
	@echo ""
	@echo "  make install              install runtime + dev deps in .venv"
	@echo "  make validate [EHR=epic]  schema-validate one EHR or all"
	@echo "  make render [EHR=epic]    render HTML for one EHR (or all)"
	@echo "  make render-all           render HTML for every EHR"
	@echo "  make synthesize EHR=epic  print the synthesized matrix"
	@echo "  make fetch-all-anonymous  Phase A + Phase F.1 (no creds needed)"
	@echo "  make discover-luxera      diff Luxera FHIR Directory against official brands bundles, emit *-luxera-augmented-*.json supersets for harvest"
	@echo "  make pos-index            build data/cms-pos/hospitals-{date}.json from CMS POS CSV"
	@echo "  make resolve-pos [EHR=]   resolve FHIR Endpoints to CMS hospitals (one EHR or all)"
	@echo "  make resolve-pos-llm EHR= LLM disambiguation pass on top of resolve-pos (needs THE_MAP_ANTHROPIC_API_KEY)"
	@echo "  make test                 run pytest"
	@echo "  make lint                 ruff check"
	@echo "  make typecheck            mypy"
	@echo "  make export               build the consumer subset (see EXPORT.md)"
	@echo "  make clean-render         delete reports/<quarter>/html/"
	@echo ""
	@echo "Detected EHRs: $(EHRS)"
	@echo "Quarter: $(QUARTER)"

install:
	@test -d .venv || python3 -m venv .venv
	$(PY) -m pip install -e .[dev]

validate:
ifdef EHR
	$(PY) -m tools.validate $(EHR)
else
	$(PY) -m tools.validate
endif

synthesize:
ifndef EHR
	$(error 'make synthesize requires EHR=<ehr>')
endif
	$(PY) -m tools.synthesize $(EHR)

render:
ifdef EHR
	$(PY) -m tools.render_html $(EHR)
else
	$(PY) -m tools.render_html --all
endif

render-all:
	$(PY) -m tools.render_html --all

fetch-all-anonymous:
	@echo "Phase A: anonymous /metadata fetches"
	@for ehr in $(EHRS); do \
	    echo "  → $$ehr"; \
	    $(PY) -m tools.fetch_capability $$ehr || true; \
	done
	@echo "Phase F.1: anonymous brands-bundle harvest"
	$(PY) -m tools.fetch_brands --all

# Phase F.2 — discovery augmentation. Queries Luxera's FHIR Directory
# (https://fhir-api.luxera.io) for our three EHRs, diffs against the latest
# official brands bundles in tests/golden/cross-vendor/, and writes
# *-luxera-augmented-{date}.json supersets that tools.harvest_production_capstmts
# can consume. Discovery-only: we re-probe every endpoint ourselves; Luxera's
# pre-probed CapStmts are not mirrored. Set LUXERA_API_KEY only if Luxera
# requires one (anonymous works as of 2026-05-07).
discover-luxera:
	$(PY) -m tools.luxera_endpoint_discovery --all

# Phase G — geographic enrichment: join FHIR Endpoints to CMS hospitals.
# Set THE_MAP_POS_CSV to the path of POS_File_Hospital_Non_Hospital_Facilities_*.zip
# downloaded from data.cms.gov, or pass --input to build_pos_hospital_index.
pos-index:
	$(PY) -m tools.build_pos_hospital_index

resolve-pos:
ifdef EHR
	$(PY) -m tools.resolve_endpoints_to_pos $(EHR)
else
	$(PY) -m tools.resolve_endpoints_to_pos --all
endif

# LLM disambiguation pass (Phase G.2). Requires THE_MAP_ANTHROPIC_API_KEY.
# Writes data/hospital-overrides/{vendor}-pos.llm.json + .detail.json, then
# re-runs the resolver so the pos.json output reflects the LLM picks.
resolve-pos-llm:
ifndef EHR
	$(error 'make resolve-pos-llm requires EHR=<vendor> (cerner|meditech|epic)')
endif
	$(PY) -m tools.llm_disambiguate $(EHR) $(LIMIT_ARG)
	$(PY) -m tools.resolve_endpoints_to_pos $(EHR)

# Phase G.3 — CMS National Provider Directory ingest. NPD publishes a weekly
# bulk release at directory.cms.gov. fetch-npd downloads the 5 NDJSON.zst files
# we use (Endpoint, Organization, OrganizationAffiliation, Location,
# PractitionerRole) into $THE_MAP_CMS_NPD_DIR (default: ~/back/data/cms-npd/).
# Pass `make fetch-npd FETCH_ARGS="--all"` to also pull the 18GB Practitioner.
# build-npd-index streams those files into a per-FHIR-endpoint identity JSON
# under data/cms-npd/. resolve-npd matches each production_fleet endpoint
# against that index and writes data/hospital-overlays/{vendor}-npd.json — the
# NPI-side twin of resolve-pos's CCN overlay.
fetch-npd:
	$(PY) -m tools.fetch_cms_npd $(FETCH_ARGS)

build-npd-index:
	$(PY) -m tools.build_npd_endpoint_index $(NPD_ARGS)

resolve-npd:
ifdef EHR
	$(PY) -m tools.resolve_endpoints_to_npd $(EHR)
else
	$(PY) -m tools.resolve_endpoints_to_npd --all
endif

# Rebuild ehrs/{ehr}/production_fleet.json so it picks up the latest NPD+POS
# overlays as per-endpoint enrichment (npi/ccn/state/city/parent_org_*) and
# per-cluster aggregates (state_distribution, recognizable_members).
rebuild-fleet-npd:
	@for ehr in $(EHRS); do \
		echo "  → $$ehr"; \
		$(PY) -m tools.analyze_fleet_drift $$ehr; \
	done

# Cross-pipeline coverage report: per-vendor counts of endpoints resolved by
# NPD (NPI), by POS (CCN), by both, by neither. The (NPI ∩ CCN) intersection
# validates both pipelines; (NPI ⊕ CCN) flags vendor-specific gaps.
npd-coverage:
	$(PY) -m tools.report_npd_coverage

test:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check tools tests

typecheck:
	$(PY) -m mypy tools

# --- export: produce the consumer subset described in EXPORT.md ---
# Downstream consumers (the rendered site, partner integrations) sync this
# directory tree at a tagged release. Anything outside this allow-list is
# repository-internal (Python, fixtures, render output, internal docs) and
# must NOT be included in the export.
export:
	@rm -rf $(EXPORT_DIR)
	@mkdir -p $(EXPORT_DIR)
	@cp -R ehrs $(EXPORT_DIR)/ehrs
	@find $(EXPORT_DIR)/ehrs -type d -name '__pycache__' -exec rm -rf {} +
	@find $(EXPORT_DIR)/ehrs -type f ! -name '*.json' -delete
	@mkdir -p $(EXPORT_DIR)/us-core $(EXPORT_DIR)/uscdi $(EXPORT_DIR)/schema
	@cp us-core/us-core-6.1-baseline.json $(EXPORT_DIR)/us-core/
	@cp uscdi/uscdi-v3-baseline.json $(EXPORT_DIR)/uscdi/ 2>/dev/null || true
	@cp schema/overlay.schema.json $(EXPORT_DIR)/schema/
	@cp schema/production_fleet.schema.json $(EXPORT_DIR)/schema/
	@cp schema/hospital_resolution.schema.json $(EXPORT_DIR)/schema/ 2>/dev/null || true
	@if [ -d data/cms-pos ]; then \
		mkdir -p $(EXPORT_DIR)/data/cms-pos; \
		cp data/cms-pos/*.json $(EXPORT_DIR)/data/cms-pos/ 2>/dev/null || true; \
	fi
	@if [ -d data/hospital-overrides ]; then \
		mkdir -p $(EXPORT_DIR)/data/hospital-overrides; \
		cp data/hospital-overrides/*.json $(EXPORT_DIR)/data/hospital-overrides/ 2>/dev/null || true; \
	fi
	@if [ -d data/hospital-overlays ]; then \
		mkdir -p $(EXPORT_DIR)/data/hospital-overlays; \
		cp data/hospital-overlays/*.json $(EXPORT_DIR)/data/hospital-overlays/ 2>/dev/null || true; \
	fi
	@if [ -d data/cms-npd ]; then \
		mkdir -p $(EXPORT_DIR)/data/cms-npd; \
		cp data/cms-npd/*.json $(EXPORT_DIR)/data/cms-npd/ 2>/dev/null || true; \
		cp data/cms-npd/*.md $(EXPORT_DIR)/data/cms-npd/ 2>/dev/null || true; \
	fi
	@$(PY) -c "import json, pathlib; \
		root = pathlib.Path('$(EXPORT_DIR)'); \
		manifest = { \
			'export_version': '1', \
			'source_repo': 'github.com/mock-health/the-map', \
			'export_files': sorted(str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()), \
		}; \
		(root / 'MANIFEST.json').write_text(json.dumps(manifest, indent=2))"
	@echo "exported to $(EXPORT_DIR)/"
	@du -sh $(EXPORT_DIR)
	@echo "validating each emitted overlay against schema..."
	@$(PY) -c "import json, jsonschema, pathlib; \
		schema = json.loads(pathlib.Path('schema/overlay.schema.json').read_text()); \
		fail = False; \
		[print('OK', p.parent.name) or jsonschema.validate(json.loads(p.read_text()), schema) \
			for p in pathlib.Path('$(EXPORT_DIR)/ehrs').rglob('overlay.json')]; \
		print('export validates clean')"

clean-render:
	rm -rf reports/*/html/

clean: clean-render
	rm -rf $(EXPORT_DIR) .pytest_cache .mypy_cache .ruff_cache
