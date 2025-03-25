# Makefile

DEUS_LIB_PACKAGE_VERSION = 0.1.0
BUILD_NUMBER ?= $(shell date +'%Y%m%d%H%M') # local developer build
BUILD_NUMBER := $(or ${CODEBUILD_BUILD_NUMBER},${BUILD_NUMBER}) # CI/CD job in AWS CodeBuild has env var $CODEBUILD_BUILD_NUMBER
BUILD_NUMBER := $(strip ${BUILD_NUMBER})
DEUS_LIB_PACKAGE = deus_lib-${DEUS_LIB_PACKAGE_VERSION}-${BUILD_NUMBER}-py3-none-any.whl

.PHONY: build-wheel, deploy-bundle, deploy-bundle-customer, localbricks-custom, localbricks-default, run-job, run-tests

localbricks-default:
	docker-compose exec local_dev poetry run python /opt/deus_dev_env/localbricks/localbricks_factory.py

localbricks-custom:
	docker-compose exec local_dev poetry run python /opt/deus_dev_env/$(COMMAND)

build-wheel:
	@echo "Building package: ${DEUS_LIB_PACKAGE}" && \
	docker-compose exec local_dev sh -c "export DEUS_LIB_PACKAGE_VERSION=${DEUS_LIB_PACKAGE}" && \
	docker-compose exec local_dev sh -c "cd /opt/deus_dev_env && rm -rf dist/*" && \
	docker-compose exec local_dev sh -c "cd /opt/deus_dev_env && python setup.py bdist_wheel --dist-dir ./dist --build-number $(BUILD_NUMBER) && rm -rf build deus_lib.egg-info" && \
	docker-compose exec local_dev sh -c "poetry add dist/$(DEUS_LIB_PACKAGE) && poetry show deus-lib && poetry lock --no-update"

# Docs: https://docs.gcp.databricks.com/en/workflows/jobs/how-to/use-bundles-with-jobs.html#step-5-deploy-the-local-project-to-the-remote-workspace
deploy-bundle:
	@chmod +x deploy_bundle.sh && ./deploy_bundle.sh $(TARGET_ENV) $(DEUS_LIB_PACKAGE) 

# Example (only for DEV purposes): > make deploy-bundle-customer CUSTOMER_CODE=VS
deploy-bundle-customer:
	@chmod +x deploy_bundle.sh && ./deploy_bundle.sh dev $(DEUS_LIB_PACKAGE) $(CUSTOMER_CODE)

ci-pipeline-dev:
	@echo "Starting deployment for development..." && \
	$(MAKE) build-wheel && \
	$(MAKE) run-tests && \
	$(MAKE) deploy-bundle TARGET=dev

ci-pipeline-prod:
	@echo "Starting deployment for production..." && \
	@test $${CODEBUIUILD_ID?Please run this command only from CI/CD job} && \
	$(MAKE) build-wheel && \
	$(MAKE) run-tests && \
	$(MAKE) deploy-bundle TARGET=production

# Docs: https://docs.gcp.databricks.com/en/workflows/jobs/how-to/use-bundles-with-jobs.html#step-6-run-the-deployed-project
run-job:
	@if [ -z "$(customer_code)" ]; then \
		echo "Error: 'customer_code' is required"; \
		exit 1; \
	else \
		echo "Running job for customer $(customer_code)..."; \
		databricks bundle run $(customer_code)_job; \
	fi

destroy:
	docker-compose exec local_dev sh -c "databricks bundle destroy"

run-tests:
	@echo "Running tests..."
# 	pytest tests
