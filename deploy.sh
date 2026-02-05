#!/usr/bin/env bash
# ==============================================================================
# deploy.sh
# Deployment script for SetStream
# ==============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
PROJECT_NAME="${PROJECT_NAME:-setstream}"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  SetStream Deployment${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed${NC}"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose is not installed${NC}"
    exit 1
fi

echo -e "${YELLOW}📦 Pulling latest images...${NC}"
docker-compose -f "$COMPOSE_FILE" pull

echo -e "${YELLOW}🏗️  Building containers...${NC}"
docker-compose -f "$COMPOSE_FILE" build

echo -e "${YELLOW}🛑 Stopping existing containers...${NC}"
docker-compose -f "$COMPOSE_FILE" down

echo -e "${YELLOW}🚀 Starting services...${NC}"
docker-compose -f "$COMPOSE_FILE" up -d

echo -e "${YELLOW}⏳ Waiting for services to be healthy...${NC}"
sleep 10

# Check service health
echo -e "${YELLOW}🏥 Checking service health...${NC}"

if docker-compose -f "$COMPOSE_FILE" ps | grep -q "Up (healthy)"; then
    echo -e "${GREEN}✅ Services are healthy${NC}"
else
    echo -e "${RED}⚠️  Some services may not be healthy${NC}"
    docker-compose -f "$COMPOSE_FILE" ps
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "📊 Services:"
echo "  - API:       http://localhost:8000"
echo "  - Dashboard: http://localhost:3838"
echo ""
echo "📝 Useful commands:"
echo "  - View logs:      docker-compose -f $COMPOSE_FILE logs -f"
echo "  - Run pipeline:   docker-compose -f $COMPOSE_FILE exec pipeline make run"
echo "  - Stop services:  docker-compose -f $COMPOSE_FILE down"
echo "  - Restart:        docker-compose -f $COMPOSE_FILE restart"
echo ""
