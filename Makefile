# Capital Bikeshare lakehouse. Run `make help` to list the targets.
#
# Settings are read in this order, later files overriding earlier ones:
#   config/project.env  project settings, committed
#   .env                your subscription, region and email, git-ignored
#   build/infra.env     outputs of the Bicep deployment, written by make infra-up

SHELL := /bin/bash
.DEFAULT_GOAL := help

include config/project.env
-include .env
-include build/infra.env
export

PYTHON ?= python3.12
VENV := .venv
BIN := $(VENV)/bin
INFRA_ENV := build/infra.env
DEPLOYMENT := $(NAME_PREFIX)-lakehouse
DATA_RG := rg-$(NAME_PREFIX)-data
PLATFORM_RG := rg-$(NAME_PREFIX)-platform

# Local Spark tests need Java 17. CI sets JAVA_HOME itself.
JAVA_HOME ?= $(shell brew --prefix openjdk@17 2>/dev/null)/libexec/openjdk.jdk/Contents/Home

# The Databricks CLI and SDK both read this profile.
DATABRICKS_CONFIG_PROFILE := $(DATABRICKS_PROFILE)

LANDING_VOLUME_PATH := /Volumes/$(CATALOG)/bronze/$(LANDING_VOLUME)

# Bundle variables (see databricks.yml)
BUNDLE_VAR_catalog := $(CATALOG)
BUNDLE_VAR_landing_volume_path := $(LANDING_VOLUME_PATH)
BUNDLE_VAR_trip_months := $(TRIP_MONTHS)
BUNDLE_VAR_trip_base_url := $(TRIP_BASE_URL)
BUNDLE_VAR_gbfs_discovery_url := $(GBFS_DISCOVERY_URL)
BUNDLE_VAR_gbfs_feeds := $(GBFS_FEEDS)
BUNDLE_VAR_gbfs_poll_minutes := $(GBFS_POLL_MINUTES)
BUNDLE_VAR_gbfs_schedule_status := $(GBFS_SCHEDULE_STATUS)
BUNDLE_VAR_pipeline_schedule_status := $(PIPELINE_SCHEDULE_STATUS)
BUNDLE_VAR_timezone := $(TIMEZONE)
BUNDLE_VAR_alert_email := $(ALERT_EMAIL)
BUNDLE_VAR_engineers_group := $(ENGINEERS_GROUP)

.PHONY: help setup lint format test bicep-check check-env check-azure check-databricks \
	preflight infra-up infra-outputs databricks-login ensure-databricks-login catalog-up \
	deploy run collect-now collect-pause collect-resume up down

help: ## List the targets
	@grep -E '^[a-zA-Z_-]+:.*## ' Makefile | awk 'BEGIN {FS = ":.*## "}; {printf "  %-24s %s\n", $$1, $$2}'

# ---------------------------------------------------------------- local

setup: ## Create the virtual environment and install the development tools
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --quiet --upgrade pip
	$(BIN)/pip install --quiet -e ".[dev]"
	@if [ -d .git ]; then $(BIN)/pre-commit install; fi

lint: ## Check formatting and lint rules
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

format: ## Format the code and apply safe lint fixes
	$(BIN)/ruff format .
	$(BIN)/ruff check --fix .

test: ## Run the unit and integration tests on local Spark
	$(BIN)/pytest

bicep-check: ## Compile and lint the Bicep templates
	az bicep build --file infra/bicep/main.bicep --stdout > /dev/null
	az bicep lint --file infra/bicep/main.bicep

# ---------------------------------------------------------------- checks

check-env:
	@test -f .env || { echo "Copy .env.example to .env and fill in your values."; exit 1; }
	@for v in AZURE_SUBSCRIPTION_ID AZURE_LOCATION NAME_PREFIX DATABRICKS_PROFILE ALERT_EMAIL; do \
		if [ -z "$${!v}" ]; then echo "Set $$v in .env"; exit 1; fi; \
	done

check-azure: check-env
	@az account show > /dev/null 2>&1 || { echo "Sign in to Azure first: az login"; exit 1; }
	@az account set --subscription "$(AZURE_SUBSCRIPTION_ID)"

check-databricks: check-env
	@databricks current-user me > /dev/null 2>&1 || \
		{ echo "Sign in to the workspace first: make databricks-login"; exit 1; }

preflight: check-azure ## Check the subscription, region and resource providers
	@az account show --query "{subscription:name, id:id, state:state, user:user.name}" -o table
	@az account list-locations --query "[?name=='$(AZURE_LOCATION)'].name" -o tsv | grep -q . || \
		{ echo "Unknown region $(AZURE_LOCATION)"; exit 1; }
	@for ns in Microsoft.Databricks Microsoft.Storage; do \
		state=$$(az provider show --namespace $$ns --query registrationState -o tsv); \
		echo "$$ns: $$state"; \
		if [ "$$state" != "Registered" ]; then az provider register --namespace $$ns --wait; fi; \
	done
	@echo "Preflight passed for $(AZURE_LOCATION)."

# ---------------------------------------------------------------- deployment

infra-up: check-azure ## Preview, then deploy the Azure resources with Bicep
	az deployment sub what-if --name $(DEPLOYMENT) --location $(AZURE_LOCATION) \
		--template-file infra/bicep/main.bicep \
		--parameters location=$(AZURE_LOCATION) namePrefix=$(NAME_PREFIX)
	az deployment sub create --name $(DEPLOYMENT) --location $(AZURE_LOCATION) \
		--template-file infra/bicep/main.bicep \
		--parameters location=$(AZURE_LOCATION) namePrefix=$(NAME_PREFIX) --output none
	@$(MAKE) --no-print-directory infra-outputs

infra-outputs: check-azure
	@mkdir -p build
	@az deployment sub show --name $(DEPLOYMENT) --query properties.outputs -o json \
		| $(BIN)/python scripts/infra_outputs.py > $(INFRA_ENV)
	@echo "Wrote $(INFRA_ENV)"

databricks-login: ## Sign in to the workspace and save the CLI profile
	@test -n "$(WORKSPACE_URL)" || { echo "Run make infra-up first."; exit 1; }
	databricks auth login --host https://$(WORKSPACE_URL) --profile $(DATABRICKS_PROFILE)

ensure-databricks-login:
	@databricks current-user me > /dev/null 2>&1 || $(MAKE) --no-print-directory databricks-login

catalog-up: check-databricks ## Create the Unity Catalog objects, groups and grants
	$(BIN)/python scripts/unity_catalog.py up

deploy: check-databricks ## Build the wheel, then validate and deploy the bundle
	databricks bundle validate
	databricks bundle deploy

run: check-databricks ## Run the lakehouse_pipeline job once
	databricks bundle run lakehouse_pipeline

collect-now: check-databricks ## Collect one GBFS snapshot now
	databricks bundle run gbfs_collection

collect-pause: ## Pause the GBFS schedule until the next plain make deploy
	@$(MAKE) --no-print-directory deploy GBFS_SCHEDULE_STATUS=PAUSED

collect-resume: ## Resume the GBFS schedule
	@$(MAKE) --no-print-directory deploy GBFS_SCHEDULE_STATUS=UNPAUSED

up: ## Build everything from nothing: Azure, Unity Catalog, bundle, first run
	@$(MAKE) --no-print-directory preflight
	@$(MAKE) --no-print-directory infra-up
	@$(MAKE) --no-print-directory ensure-databricks-login
	@$(MAKE) --no-print-directory catalog-up
	@$(MAKE) --no-print-directory deploy
	@$(MAKE) --no-print-directory collect-now
	@$(MAKE) --no-print-directory run

down: check-azure ## Remove everything except the collected data (CONFIRM_DATA_LOSS=yes removes that too)
	-databricks bundle destroy --auto-approve
	-$(BIN)/python scripts/unity_catalog.py down
	az group delete --name $(PLATFORM_RG) --yes
	@rm -f $(INFRA_ENV)
	@if [ "$(CONFIRM_DATA_LOSS)" = "yes" ]; then \
		az group delete --name $(DATA_RG) --yes; \
		echo "Deleted $(DATA_RG) and every collected file."; \
	else \
		echo "Kept $(DATA_RG). The landing files survive and the next make up reloads them."; \
	fi
