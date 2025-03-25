#!/bin/bash
restore_yaml() {
    local yaml_files=("$@")
    for yaml_file in "${yaml_files[@]}"; do
        echo "Restoring YAML file $yaml_file from backup..."
        mv "${yaml_file}.bak" "$yaml_file"
    done
    echo "Restoration complete."
}

deploy_jobs() {
    # https://docs.databricks.com/en/dev-tools/cli/bundle-commands.html#deploy-a-bundle
    var1="\"deus_lib_package_version=$DEUS_LIB_PACKAGE\""
    if [[ "$TARGET_ENV" == "prod" ]] 
    then
        databricks bundle deploy --debug -t $TARGET_ENV --var=$(var1)
    else
        docker-compose exec local_dev sh -c "databricks bundle deploy --debug -t $TARGET_ENV --var=$(var1)"
    fi
}

validate_bundle() {
    # Set environment variables to be available in databricks.yml
    # https://docs.databricks.com/en/dev-tools/bundles/settings.html#set-a-variables-value
    var1="\"deus_lib_package_version=$DEUS_LIB_PACKAGE\""
    docker-compose exec local_dev sh -c "databricks bundle validate --var=${var1}"
}

main() {
    CURRENT_DIR=$(pwd)
    yaml_files=(
        "$CURRENT_DIR/databricks.yml"
        "$CURRENT_DIR/workflow_definition/shared/job_libraries.yml"
    )
    TARGET_ENV=$1          # workspace env (dev/production)
    DEUS_LIB_PACKAGE=$2  # deus-lib package version reference
    CUSTOMER_CODE=$3       # isolate deployment for single customer
    
    # Set the trap to call restore_yaml for all YAML files when exiting
    trap 'restore_yaml "${yaml_files[@]}"' EXIT
    
    echo "Starting the deployment of databricks bundle for target env '${TARGET_ENV}'" && \
    
    # grabbing git branch info
    branch_name=$(git rev-parse --abbrev-ref HEAD) && \
    lib_yaml_file=${yaml_files[1]} && \
    sed -i.bak "s|{git_branch}|${branch_name}|g" ${lib_yaml_file} && \
    databricks_yaml_file=${yaml_files[0]} && \
    sed -i.bak "s|{git_branch}|${branch_name}|g" ${databricks_yaml_file} && \
    echo "Configuring bundle for git branch: '${branch_name}'" && \
    
    # warning
    if [[ "$TARGET_ENV" == "prod" && "$branch_name" != "main" ]]; then
        echo "ERROR: Production deployments can only be done from the main branch."
        exit 1
    fi

    echo "Installing/updating required dependencies..." && \
    docker-compose exec local_dev sh -c "pip install pydantic" && \
    docker-compose exec local_dev sh -c "pip install pyyaml" && \
    docker-compose exec local_dev sh -c "pip install GitPython" && \
    docker-compose exec local_dev sh -c "poetry update" && \

    echo "Exporting env vars for databricks bundle settings..." && \
    export TARGET_ENV DEUS_LIB_PACKAGE CUSTOMER_CODE && \
    echo "TARGET_ENV => $TARGET_ENV" && \
    echo "GIT_BRANCH => ${branch_name}" && \
    echo "DEUS_LIB_PACKAGE => $DEUS_LIB_PACKAGE" && \

    jobs_build_args="--target ${TARGET_ENV}"
    if [[ -n $CUSTOMER_CODE ]]; then
        echo "Isolated deployment for customer '${CUSTOMER_CODE}'" && \
        jobs_build_args="--target ${TARGET_ENV} --customer ${CUSTOMER_CODE}"
    fi
    echo "Job builder args: ${jobs_build_args}" && \

    echo "Packing inner jobs for layers..." && \
    docker-compose exec local_dev sh -c "cd /opt/deus_dev_env/workflow_definition && python3 job_helper/merge_jobs.py  ${jobs_build_args}" && \
    echo "Validating bundle..." && \
    validate_bundle && \
    echo "Deploying inner jobs..." && \
    deploy_jobs && \
    
    echo "Packaging outer job(s) composed of internal job(s) already deployed..." && \
    docker-compose exec local_dev sh -c "cd /opt/deus_dev_env/workflow_definition && python3 job_helper/outer_job_build.py  ${jobs_build_args}" && \
    echo "Validating bundle..." && \
    validate_bundle && \
    echo "Deploying outer jobs..." && \
    deploy_jobs && \

    echo "Deployment completed!";
}

main $1 $2 $3
