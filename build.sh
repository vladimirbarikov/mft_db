#!/bin/bash

# MFT (Material Flow Table) Project Build Script
# ========================
#
# DESCRIPTION:
#   Comprehensive build and deployment script for MFT project.
#   Handles sequential image pulling, local image building, and container creation
#   with dependency awareness and user confirmation at each step.
#
# USAGE:
#   ./build.sh [OPTIONS]
#
# OPTIONS:
#   --skip-pull           Skip pulling images from Docker registry
#   --skip-build          Skip building local images (mft-airflow, mft-api)
#   --skip-create         Skip container creation step
#   --create-only SERVICE Create only a specific service (e.g., --create-only mft-display-api)
#   --test-display        Test display-api service (health check and endpoint testing)
#   --yes, -y             Automatic yes to all prompts (non-interactive mode)
#   --help                Show this help message
#
# STAGES:
#   1. STAGE 1: Build local images
#      - Builds mft-airflow:2.9.3 from ./dockerfiles/Dockerfile.airflow
#      - Builds mft-api:3.12.3-slim from ./dockerfiles/Dockerfile.api
#      - Checks if images exist and asks for rebuild confirmation
#
#   2. STAGE 2: Sequential image pull from registry
#      - Pulls images in dependency order:
#        * Infrastructure: postgres, redis, db
#        * Exporters: postgres-exporter-airflow, postgres-exporter-mft, redis-exporter, statsd-exporter
#        * Monitoring: prometheus, grafana
#        * Security: clamav
#        * Additional: adminer, flower
#      - 5-minute timeout per image, continues on error
#      - Asks for confirmation if images already exist
#
#   3. STAGE 3: Sequential container creation
#      - Creates containers in dependency order with group confirmation:
#        
#        GROUP 1: Infrastructure (postgres, redis, db)
#        GROUP 2: Exporters (postgres-exporter-airflow, postgres-exporter-mft, redis-exporter, statsd-exporter)
#        GROUP 3: ClamAV (clamav)
#        GROUP 4: Monitoring (prometheus, grafana)
#        GROUP 5: Airflow Init (airflow-init)
#        GROUP 6: Airflow Core (airflow-webserver, airflow-scheduler, airflow-worker, airflow-triggerer)
#        GROUP 7: Additional Services (adminer, flower, airflow-cli)
#        GROUP 8: API Services (mft-upload-api, mft-display-api)
#        
#      - Special handling for mft-display-api:
#        * Prompts user to upload Excel file via mft-upload-api first
#        * Verifies data is in database before creating container
#      - Checks for existing containers and asks for recreation
#      - Shows container status after creation
#
# FEATURES:
#   - Color-coded output for better readability
#   - Comprehensive error handling with set -euo pipefail
#   - Prerequisite checks (Docker, Docker Compose, curl, disk space)
#   - .env file validation
#   - Execution time tracking
#   - Detailed statistics (pulled/skipped/failed, created/skipped/failed)
#   - Next steps guidance with service URLs and useful commands
#
# CONTAINER NAMES (from docker-compose.yml):
#   - postgres:              airflow_db
#   - redis:                 airflow_redis
#   - db:                    mft_db
#   - postgres-exporter-airflow: postgres_exporter_airflow
#   - postgres-exporter-mft:     postgres_exporter_mft
#   - redis-exporter:        redis_exporter
#   - statsd-exporter:       statsd-exporter
#   - prometheus:            prometheus
#   - grafana:               grafana
#   - clamav:                clamav
#   - adminer:               adminer
#   - flower:                airflow_flower
#   - airflow-init:          airflow_init
#   - airflow-webserver:     airflow_webserver
#   - airflow-scheduler:     airflow_scheduler
#   - airflow-worker:        airflow_worker
#   - airflow-triggerer:     airflow_triggerer
#   - airflow-cli:           airflow_cli (debug profile)
#   - mft-upload-api:        mft_upload_api
#   - mft-display-api:       mft_display_api
#
# SERVICE URLS:
#   - Airflow:     http://localhost:8080
#   - Upload API:  http://localhost:5002
#   - Display API: http://localhost:5003
#   - Grafana:     http://localhost:3000
#   - Prometheus:  http://localhost:9090
#   - Adminer:     http://localhost:8081
#   - Flower:      http://localhost:5555
#
# DEPENDENCIES:
#   - Docker (20.10+)
#   - Docker Compose (v2)
#   - curl (for API testing)
#   - At least 10GB free disk space
#   - .env file (optional, falls back to defaults)
#
# EXAMPLES:
#
#   1. Full build with interactive prompts (recommended for first time):
#      ./build.sh
#
#   2. Automatic mode (yes to all prompts):
#      ./build.sh -y
#
#   3. Skip image pull, only build local images and create containers:
#      ./build.sh --skip-pull
#
#   4. Skip build, only pull images and create containers:
#      ./build.sh --skip-build
#
#   5. Only create display-api container (with dependencies):
#      ./build.sh --create-only mft-display-api
#
#   6. Test display-api without rebuilding:
#      ./build.sh --test-display
#
#   7. Quick rebuild of everything (skip confirmations):
#      ./build.sh -y --skip-pull
#
#   8. Create only specific group (example for API services):
#      ./build.sh --create-only mft-upload-api
#      ./build.sh --create-only mft-display-api
#
# TROUBLESHOOTING:
#
#   If build fails:
#     1. Check Docker is running: docker ps
#     2. Check disk space: df -h
#     3. Check .env file exists: ls -la .env
#     4. Check Dockerfiles exist: ls -la dockerfiles/
#     5. View logs: docker-compose logs -f [service-name]
#
#   If display-api has no data:
#     1. Upload Excel file: curl -F 'file=@data.xlsx' http://localhost:5002/upload-mft-excel
#     2. Check database: docker exec -it mft_db psql -U mft_user -d mft_db -c "SELECT * FROM your_table;"
#     3. Recreate display-api: ./build.sh --create-only mft-display-api
#
#   To reset everything:
#     docker-compose down -v
#     ./build.sh
#
# EXIT CODES:
#   0 - All operations completed successfully
#   1 - Operations completed with warnings (some pulls/creations failed)
#   >1 - Fatal error (prerequisites missing, critical failure)
#
# AUTHOR:
#   MFT Project Team
#
# VERSION:
#   2.0.0 - Added sequential container creation with group confirmation
#         - Special handling for display-api data dependency
#         - Interactive prompts for each group
#         - Comprehensive testing mode

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Error handling - exit on error, undefined variable, pipe failure
set -euo pipefail

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}   MFT Project Build Script${NC}"
echo -e "${YELLOW}========================================${NC}"

# Function to check execution status
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN} $1 completed successfully${NC}"
    else
        echo -e "${RED} Error during: $1${NC}"
        exit 1
    fi
}

# Function to ask user for confirmation
ask_confirmation() {
    local prompt=$1
    local default=${2:-n}
    local answer

    while true; do
        if [ "$default" = "y" ]; then
            echo -e -n "${YELLOW}$prompt (Y/n): ${NC}"
        else
            echo -e -n "${YELLOW}$prompt (y/N): ${NC}"
        fi

        # Reading the response, checking for EOF
        if ! read -r answer; then
            # If a reading error has occurred (including Ctrl+D)
            echo -e "\n${RED}Input cancelled. Exiting...${NC}"
            exit 1
        fi

        # If the answer is empty (just press Enter)
        if [ -z "$answer" ]; then
            answer=$default
        fi

        case $answer in
            [Yy]* ) return 0 ;;
            [Nn]* ) return 1 ;;
            * ) echo -e "${RED}Please answer yes (y) or no (n)${NC}" ;;
        esac
    done
}

# Function to check if local images exist
check_local_images_exist() {
    local airflow_exists=false
    local api_exists=false

    if docker image inspect mft-airflow:2.9.3 &>/dev/null; then
        airflow_exists=true
    fi

    if docker image inspect mft-api:3.12.3-slim &>/dev/null; then
        api_exists=true
    fi

    if $airflow_exists && $api_exists; then
        return 0  # Both images exist
    else
        return 1  # Some images missing
    fi
}

# Function to build local images (FIRST)
build_local_images() {
    echo -e "\n${CYAN} Building local images${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    # Check if Dockerfiles exist
    if [ ! -f "./dockerfiles/Dockerfile.airflow" ]; then
        echo -e "${RED} Dockerfile.airflow not found!${NC}"
        exit 1
    fi

    if [ ! -f "./dockerfiles/Dockerfile.api" ]; then
        echo -e "${RED} Dockerfile.api not found!${NC}"
        exit 1
    fi

    # Check if images already exist
    local should_build=true

    if check_local_images_exist; then
        echo -e "${GREEN} Local images already exist:${NC}"
        docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}" | grep -E "mft-airflow|mft-api" || true
        
        if ask_confirmation "Do you want to rebuild the local images?" "n"; then
            echo -e "${YELLOW} Rebuilding local images...${NC}"
            should_build=true
        else
            echo -e "${YELLOW} Skipping local image build${NC}"
            should_build=false
        fi
    else
        echo -e "${YELLOW} Local images not found or incomplete, building required...${NC}"
        should_build=true
    fi

    if [ "$should_build" = true ]; then
        # Build Airflow image
        echo -e "${YELLOW}  Building mft-airflow:2.9.3...${NC}"
        if docker build --no-cache -t mft-airflow:2.9.3 -f ./dockerfiles/Dockerfile.airflow .; then
            echo -e "${GREEN}  Airflow image built successfully${NC}"
        else
            echo -e "${RED}  Failed to build Airflow image${NC}"
            exit 1
        fi

        # Build API image
        echo -e "${YELLOW}  Building mft-api:3.12.3-slim...${NC}"
        if docker build --no-cache -t mft-api:3.12.3-slim -f ./dockerfiles/Dockerfile.api .; then
            echo -e "${GREEN}  API image built successfully${NC}"
        else
            echo -e "${RED}  Failed to build API image${NC}"
            exit 1
        fi
    fi

    # Verify images were created
    echo -e "\n${GREEN} Current local images:${NC}"
    docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}" | grep -E "mft-airflow|mft-api" || echo -e "${YELLOW} No local images found${NC}"
}

# Function to check if registry images are already pulled
check_registry_images_pulled() {
    local services=("$@")
    local all_pulled=true
    local missing_images=()

    for service in "${services[@]}"; do
        # Get image name for service from docker-compose
        local image_name
        # Use docker compose config and search for the service section more precisely
        image_name=$(docker compose config | awk -v svc="$service" '
            $0 ~ "^  " svc ":" {found=1; next}
            found && /^    image:/ {print $2; exit}
            found && /^    [a-z]/ {found=0}
        ' | tr -d '"' || echo "")

        if [ -n "$image_name" ]; then
            if ! docker image inspect "$image_name" &>/dev/null; then
                all_pulled=false
                missing_images+=("$service ($image_name)")
            fi
        else
            # If it was not possible to get the image name, try an alternative method.
            image_name=$(docker compose config | grep -A 5 "^  $service:" | grep "image:" | head -1 | awk '{print $2}' | tr -d '"' || echo "")
            if [ -n "$image_name" ]; then
                if ! docker image inspect "$image_name" &>/dev/null; then
                    all_pulled=false
                    missing_images+=("$service ($image_name)")
                fi
            else
                echo -e "${YELLOW} Warning: Could not determine image for service $service${NC}"
            fi
        fi
    done

    if [ "$all_pulled" = true ]; then
        return 0  # All images exist
    else
        if [ ${#missing_images[@]} -gt 0 ]; then
            echo -e "${YELLOW} Missing images:${NC}"
            for img in "${missing_images[@]}"; do
                echo -e "  - $img"
            done
        fi
        return 1  # Some images missing
    fi
}

# Function for sequential image pull (THEN)
pull_images_sequentially() {
    echo -e "\n${BLUE} Sequential image pull from registry${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    # Array of services in correct order (excluding local ones)
    local services=(
        "postgres"
        "redis"
        "db"
        "postgres-exporter-airflow"
        "postgres-exporter-mft"
        "redis-exporter"
        "statsd-exporter"
        "prometheus"
        "grafana"
        "clamav"
        "adminer"
        "flower"
    )

    # Check if docker-compose.yml exists
    if [ ! -f "docker-compose.yml" ]; then
        echo -e "${RED} docker-compose.yml not found!${NC}"
        exit 1
    fi

    # Get list of available services from docker-compose.yml
    local available_services
    available_services=$(docker compose config --services 2>/dev/null || echo "")

    if [ -z "$available_services" ]; then
        echo -e "${RED} Failed to get services from docker-compose.yml${NC}"
        exit 1
    fi

    # Check if images already exist
    local should_pull=true

    if check_registry_images_pulled "${services[@]}"; then
        echo -e "${GREEN} All registry images already exist locally${NC}"
        
        if ask_confirmation "Do you want to pull the latest versions anyway?" "n"; then
            echo -e "${YELLOW} Pulling latest images...${NC}"
            should_pull=true
        else
            echo -e "${YELLOW} Skipping image pull${NC}"
            should_pull=false
        fi
    else
        echo -e "${YELLOW} Some images are missing, pulling required...${NC}"
        should_pull=true
    fi

    # Counters for statistics
    local pulled=0
    local skipped=0
    local failed=0

    if [ "$should_pull" = true ]; then
        # Iterate through all services in the correct order
        for service in "${services[@]}"; do
            # Check if service exists in compose file
            if echo "$available_services" | grep -q "^$service$"; then
                echo -e "${YELLOW}  Pulling $service from registry...${NC}"

                # Try to pull with 5 minute timeout, continue on error
                if timeout 300 docker compose pull "$service" 2>/dev/null; then
                    echo -e "${GREEN} Successfully pulled $service${NC}"
                    ((pulled++))
                else
                    local exit_code=$?
                    if [ $exit_code -eq 124 ]; then
                        echo -e "${RED} Timeout while pulling $service${NC}"
                    else
                        echo -e "${RED} Failed to pull $service${NC}"
                    fi
                    ((failed++))
                    # Continue with next service instead of exiting
                fi
                echo ""

                # Small pause between pulls
                sleep 2
            else
                echo -e "${YELLOW} Service $service not found in docker-compose.yml, skipping...${NC}\n"
                ((skipped++))
            fi
        done

        echo -e "${GREEN} Images pulled: $pulled | Skipped: $skipped | Failed: $failed${NC}"
    fi

    # Return non-zero if any pulls failed
    if [ $failed -gt 0 ]; then
        return 1
    fi
    return 0
}

# Function to create containers sequentially with user confirmation
create_containers_sequentially() {
    echo -e "\n${PURPLE} Creating containers sequentially${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    # Define services in dependency order based on docker-compose.yml
    # Разбиваем на группы с описаниями
    local group1_infra=("postgres" "redis" "db")
    local group2_exporters=("postgres-exporter-airflow" "postgres-exporter-mft" "redis-exporter" "statsd-exporter")
    local group3_clamav=("clamav")
    local group4_monitoring=("prometheus" "grafana")
    local group5_airflow_init=("airflow-init")
    local group6_airflow_core=("airflow-webserver" "airflow-scheduler" "airflow-worker" "airflow-triggerer")
    local group7_additional=("adminer" "flower" "airflow-cli")
    local group8_api=("mft-upload-api" "mft-display-api")

    # Get list of available services from docker-compose.yml
    local available_services
    available_services=$(docker compose config --services 2>/dev/null || echo "")

    if [ -z "$available_services" ]; then
        echo -e "${RED} Failed to get services from docker-compose.yml${NC}"
        exit 1
    fi

    # Counters for statistics
    local created=0
    local skipped=0
    local failed=0

    # Функция для создания группы контейнеров
    create_group() {
        local group_name=$1
        shift
        local group_services=("$@")
        
        echo -e "\n${CYAN} Group: $group_name${NC}"
        
        # Показываем сервисы в группе
        local group_list=()
        for service in "${group_services[@]}"; do
            if echo "$available_services" | grep -q "^$service$"; then
                group_list+=("$service")
            fi
        done

        if [ ${#group_list[@]} -eq 0 ]; then
            echo -e "${YELLOW} No services in this group found in docker-compose.yml${NC}"
            return
        fi

        echo -e "${YELLOW} Services in this group:${NC}"
        for service in "${group_list[@]}"; do
            echo -e "    - $service"
        done

        if ask_confirmation " Create this group?" "y"; then
            # Create containers in this group
            for service in "${group_list[@]}"; do
                echo -e "\n${BLUE} Creating container for: $service${NC}"

                # Специальная обработка для mft-display-api
                if [ "$service" = "mft-display-api" ]; then
                    echo -e "\n${PURPLE} IMPORTANT: Before creating mft-display-api container${NC}"
                    echo -e "${PURPLE} You need to upload an Excel file through mft-upload-api first${NC}"
                    echo -e "${PURPLE} The data from the Excel file must be processed and stored in the database${NC}"
                    echo -e "${PURPLE} Only then mft-display-api will have data to display${NC}"
                    echo ""

                    if ! ask_confirmation "Have you uploaded an Excel file and verified data is in the database?" "n"; then
                        echo -e "${YELLOW} Skipping mft-display-api creation. You can create it later with:${NC}"
                        echo -e "${YELLOW} ./build.sh --create-only mft-display-api${NC}"
                        ((skipped++))
                        continue
                    fi
                fi

                # Check if container already exists
                local container_name
                container_name=$(docker compose config | grep -A 5 "$service:" | grep "container_name:" | awk '{print $2}' || echo "$service")

                if docker ps -a --format '{{.Names}}' | grep -q "^$container_name$"; then
                    echo -e "${YELLOW} Container $container_name already exists${NC}"
                    
                    if ask_confirmation "Do you want to recreate $service?" "n"; then
                        echo -e "${YELLOW} Removing old container...${NC}"
                        docker compose rm -sf "$service" 2>/dev/null || true
                        echo -e "${YELLOW} Creating new container...${NC}"

                        if docker compose up -d --no-deps "$service"; then
                            echo -e "${GREEN} Container $service recreated successfully${NC}"
                            ((created++))
                        else
                            echo -e "${RED} Failed to recreate $service${NC}"
                            ((failed++))

                            if ! ask_confirmation "Continue with next service?" "y"; then
                                echo -e "${RED} Stopping group creation${NC}"
                                return 1
                            fi
                        fi
                    else
                        echo -e "${YELLOW} Keeping existing container${NC}"
                        ((skipped++))
                    fi
                else
                    echo -e "${YELLOW} Creating new container...${NC}"
                    
                    if docker compose up -d --no-deps "$service"; then
                        echo -e "${GREEN} Container $service created successfully${NC}"
                        ((created++))
                    else
                        echo -e "${RED} Failed to create $service${NC}"
                        ((failed++))

                        if ! ask_confirmation "Continue with next service?" "y"; then
                            echo -e "${RED} Stopping group creation${NC}"
                            return 1
                        fi
                    fi
                fi

                # Show container status
                local container_status
                container_status=$(docker compose ps "$service" --format json 2>/dev/null | grep -o '"Status":"[^"]*"' | cut -d'"' -f4 || echo "unknown")
                echo -e "Status: ${CYAN}$container_status${NC}"

                # Small pause between services
                sleep 2
            done
        else
            echo -e "${YELLOW} Skipping group: $group_name${NC}"
            for service in "${group_list[@]}"; do
                ((skipped++))
            done
        fi
        echo ""
        return 0
    }

    # Create groups in order
    create_group "Infrastructure (postgres, redis, db)" "${group1_infra[@]}"
    create_group "Exporters" "${group2_exporters[@]}"
    create_group "ClamAV" "${group3_clamav[@]}"
    create_group "Monitoring (prometheus, grafana)" "${group4_monitoring[@]}"
    create_group "Airflow Init" "${group5_airflow_init[@]}"
    create_group "Airflow Core (webserver, scheduler, worker, triggerer)" "${group6_airflow_core[@]}"
    create_group "Additional Services (adminer, flower, airflow-cli)" "${group7_additional[@]}"
    create_group "API Services (mft-upload-api, mft-display-api)" "${group8_api[@]}"

    echo -e "\n${GREEN} Container creation summary:${NC}"
    echo -e "  Created: $created | Skipped: $skipped | Failed: $failed"
    
    if [ $failed -gt 0 ]; then
        return 1
    fi
    return 0
}

# Function to test display-api specifically
test_display_api() {
    echo -e "\n${PURPLE} Testing mft-display-api service${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    # Check if display-api container is running
    if ! docker ps --format '{{.Names}}' | grep -q "^mft_display_api$"; then
        echo -e "${RED} Display API container (mft_display_api) is not running${NC}"

        if ask_confirmation "Do you want to create mft-display-api now?" "y"; then
            # Check dependencies
            echo -e "${YELLOW} Checking dependencies for mft-display-api...${NC}"

            local deps_ok=true
            for dep in db; do
                if ! docker ps --format '{{.Names}}' | grep -q "^mft_db$"; then
                    echo -e "${RED} Dependency $dep is not running${NC}"
                    deps_ok=false
                fi
            done

            if [ "$deps_ok" = false ]; then
                echo -e "${YELLOW} Dependencies missing, creating them first...${NC}"
                
                # Create db first
                if ! docker ps --format '{{.Names}}' | grep -q "^mft_db$"; then
                    echo -e "${YELLOW} Creating db container...${NC}"
                    docker compose up -d db
                fi

                # Wait for db to be healthy
                echo -e "${YELLOW} Waiting for db to be healthy...${NC}"
                local timeout=60
                local elapsed=0
                while [ $elapsed -lt $timeout ]; do
                    if docker compose ps db --format json | grep -q '"Health":"healthy"'; then
                        echo -e "${GREEN} db is healthy${NC}"
                        break
                    fi
                    sleep 5
                    elapsed=$((elapsed + 5))
                    echo -n "."
                done
                echo ""
            fi

            # Create display-api
            echo -e "${YELLOW} Creating mft-display-api container...${NC}"
            if docker compose up -d mft-display-api; then
                echo -e "${GREEN} Display API created successfully${NC}"
            else
                echo -e "${RED} Failed to create display-api${NC}"
                return 1
            fi
        else
            echo -e "${YELLOW} Skipping display-api creation${NC}"
            return 1
        fi
    fi

    # Test display-api health
    echo -e "\n${YELLOW} Testing mft-display-api health...${NC}"
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "http://localhost:5003/health" &>/dev/null; then
            echo -e "${GREEN} Display API is healthy!${NC}"
            break
        else
            echo -n "."
            sleep 2
            attempt=$((attempt + 1))
        fi
    done

    if [ $attempt -gt $max_attempts ]; then
        echo -e "\n${RED} Display API health check failed${NC}"
        echo -e "${YELLOW} Check logs: docker compose logs mft-display-api${NC}"
    fi

    # Show display-api logs
    echo -e "\n${YELLOW} Recent mft-display-api logs:${NC}"
    docker compose logs --tail=20 mft-display-api

    # Test actual endpoints
    echo -e "\n${YELLOW} Testing mft-display-api endpoints:${NC}"

    # Test main endpoint
    echo -e "\n${CYAN}GET /${NC}"
    curl -s "http://localhost:5003/" | head -5 || echo -e "${RED} Failed to connect${NC}"

    # Test health endpoint
    echo -e "\n${CYAN}GET /health${NC}"
    curl -s "http://localhost:5003/health" || echo -e "${RED} Failed to connect${NC}"
    
    echo ""
}

# Function to check required tools
check_prerequisites() {
    echo -e "\n${YELLOW} Checking required tools...${NC}"
    local missing_tools=0

    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${RED} Docker is not installed!${NC}"
        missing_tools=1
    else
        echo -e "${GREEN} Docker found: $(docker --version 2>/dev/null | head -1)${NC}"
    fi

    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        echo -e "${RED} Docker Compose is not installed!${NC}"
        missing_tools=1
    else
        echo -e "${GREEN} Docker Compose found${NC}"
    fi

    # Check curl (for API testing)
    if ! command -v curl &> /dev/null; then
        echo -e "${YELLOW} curl is not installed (needed for API testing)${NC}"
    fi

    # Check for .env file
    if [ ! -f ".env" ]; then
        echo -e "${YELLOW} .env file not found, will use .env.example if available${NC}"
    fi

    # Check disk space (at least 10GB free)
    local available_space
    available_space=$(df -BG . | awk 'NR==2 {print $4}' | sed 's/G//')
    if [ "$available_space" -lt 10 ]; then
        echo -e "${YELLOW} Warning: Less than 10GB disk space available (${available_space}GB)${NC}"
    fi
    
    if [ $missing_tools -eq 1 ]; then
        exit 1
    fi
}

# Function to show Available services and Useful commands
show_next_steps() {    
    echo -e "\n${PURPLE} Available services:${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "  Airflow:     ${GREEN}http://localhost:8080${NC} (admin/airflow)"
    echo -e "  Upload API:  ${GREEN}http://localhost:5002${NC}"
    echo -e "  Display API: ${GREEN}http://localhost:5003${NC}"
    echo -e "  Grafana:     ${GREEN}http://localhost:3000${NC} (admin/grafana)"
    echo -e "  Prometheus:  ${GREEN}http://localhost:9090${NC}"
    echo -e "  Adminer:     ${GREEN}http://localhost:8081${NC}"
    echo -e "  Flower:      ${GREEN}http://localhost:5555${NC}"

    echo -e "\n${PURPLE} Useful commands:${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "  View all containers (even stopped):  ${GREEN}docker-compose ps -a${NC}"
    echo -e "  View all logs:                       ${GREEN}docker-compose logs -f${NC}"
    echo -e "  View single service logs:            ${GREEN}docker-compose logs -f <service-name>${NC}"
    echo -e "  Test upload-api:                     ${GREEN}curl -F 'file=@data.xlsx' http://localhost:5002/upload-mft-excel${NC}"
    echo -e "  Test display-api:                    ${GREEN}curl http://localhost:5003/health${NC}"
    echo -e "  Enter upload-api:                     ${GREEN}docker exec -it mft_upload_api bash${NC}"
    echo -e "  Enter display-api:                    ${GREEN}docker exec -it mft_display_api bash${NC}"
    echo -e "  Enter airflow:                        ${GREEN}docker exec -it airflow_webserver bash${NC}"
    echo -e "  Enter airflow-cli:                     ${GREEN}docker compose run --rm airflow-cli${NC}"
    echo -e "  Enter database:                       ${GREEN}docker exec -it mft_db psql -U mft_user -d mft_db${NC}"
    echo -e "  Enter postgres:                       ${GREEN}docker exec -it airflow_db psql -U admin -d airflow${NC}"
    echo -e "  Enter redis:                          ${GREEN}docker exec -it airflow_redis redis-cli${NC}"
    echo -e "  Enter clamav:                         ${GREEN}docker exec -it clamav bash${NC}"
    echo -e "  Stop everything:                      ${GREEN}docker-compose down${NC}"
    echo -e "  Stop everything (with volumes):       ${GREEN}docker-compose down -v${NC}"
    echo -e "  Create single service:                ${GREEN}docker-compose up -d <service-name>${NC}"
    echo -e "  Rebuild single service:               ${GREEN}docker-compose up -d --build <service-name>${NC}"
    echo -e "  Check service status:                 ${GREEN}docker-compose ps <service-name>${NC}"
}

# If the --show-steps argument is passed, we show only the steps and exit
if [[ "${1:-}" == "--show-steps" ]]; then
    show_next_steps
    exit 0
fi

# Main script logic
main() {
    local exit_code=0

    # Track execution time
    local start_time
    start_time=$(date +%s)

    # Parse command line arguments
    local SKIP_PULL=false
    local SKIP_BUILD=false
    local SKIP_CREATE=false
    local SKIP_CONFIRM=false
    local TEST_DISPLAY=false
    local CREATE_ONLY=""

    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-pull)
                SKIP_PULL=true
                shift
                ;;
            --skip-build)
                SKIP_BUILD=true
                shift
                ;;
            --skip-create)
                SKIP_CREATE=true
                shift
                ;;
            --yes|-y)
                SKIP_CONFIRM=true
                shift
                ;;
            --test-display)
                TEST_DISPLAY=true
                shift
                ;;
            --create-only)
                CREATE_ONLY="$2"
                shift 2
                ;;
            --help)
                echo "Usage: $0 [options]"
                echo "Options:"
                echo "  --skip-pull           Skip pulling images from registry"
                echo "  --skip-build          Skip building local images"
                echo "  --skip-create         Skip container creation"
                echo "  --create-only SERVICE Create only specific service"
                echo "  --test-display        Test display-api service"
                echo "  --yes, -y             Automatic yes to all prompts"
                echo "  --help                Show this help message"
                exit 0
                ;;
            *)
                echo -e "${RED} Unknown option: $1${NC}"
                exit 1
                ;;
        esac
    done

    # Override confirmation if -y flag is used
    if [ "$SKIP_CONFIRM" = true ]; then
        # Create a wrapper function that always returns true
        ask_confirmation() { return 0; }
    fi

    # Check prerequisites
    check_prerequisites

    # Special mode: test display-api
    if [ "$TEST_DISPLAY" = true ]; then
        test_display_api
        exit $?
    fi

    # Special mode: create only specific service
    if [ -n "$CREATE_ONLY" ]; then
        echo -e "\n${CYAN} Creating single service: $CREATE_ONLY${NC}"
        docker compose up -d "$CREATE_ONLY"
        docker compose ps "$CREATE_ONLY"
        exit $?
    fi

    # Build local images (FIRST)
    if [ "$SKIP_BUILD" = false ]; then
        build_local_images
    else
        echo -e "\n${YELLOW} Image build skipped${NC}"
    fi

    # Sequential pull of remaining images (THEN)
    if [ "$SKIP_PULL" = false ]; then
        if pull_images_sequentially; then
            echo -e "${GREEN} All images pulled successfully${NC}"
        else
            echo -e "${YELLOW} Some images failed to pull, but continuing...${NC}"
            exit_code=1
        fi
    else
        echo -e "\n${YELLOW} Image pull skipped${NC}"
    fi

    # Create containers sequentially
    if [ "$SKIP_CREATE" = false ]; then
        if create_containers_sequentially; then
            echo -e "${GREEN} All containers created successfully${NC}"
        else
            echo -e "${YELLOW} Some containers failed to create, but continuing...${NC}"
            exit_code=1
        fi
    else
        echo -e "\n${YELLOW} Container creation skipped${NC}"
    fi

    # Calculate execution time
    local end_time
    end_time=$(date +%s)
    local duration=$((end_time - start_time))

    # Final message
    echo -e "\n${GREEN}========================================${NC}"
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN} All operations completed successfully!${NC}"
    else
        echo -e "${YELLOW} Operations completed with warnings!${NC}"
    fi
    echo -e "${GREEN} Execution time: ${duration} seconds${NC}"
    echo -e "${YELLOW}========================================${NC}"

    # Show next steps
    show_next_steps

    return $exit_code
}

# Run main function with command line arguments
main "$@"