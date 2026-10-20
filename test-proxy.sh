#!/usr/bin/env bash
# Test script to verify proxy setup for MimoCode

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
INJECTOR_URL="http://localhost:8006"
API_PROXY_URL="http://localhost:3000"
TEST_KEY="sk-test-1234567890abcdef1234567890abcdef12345678"

echo "=== Proxy Test Suite ==="
echo ""

# Test 1: Health check on injector
echo -e "${YELLOW}Test 1: Health check on openrouter-proxy-injector (port 8006)${NC}"
if curl -sf --max-time 5 "$INJECTOR_URL/health" > /dev/null; then
    echo -e "${GREEN}✓ Health endpoint responding${NC}"
    curl -s "$INJECTOR_URL/health"
else
    echo -e "${RED}✗ Health endpoint failed${NC}"
fi
echo ""

# Test 2: Key status endpoint
echo -e "${YELLOW}Test 2: Key status endpoint${NC}"
if curl -sf --max-time 5 -H "APIKEY: openrouter_proxy_injector_api_key" "$INJECTOR_URL/key-status" | jq . > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Key status endpoint responding${NC}"
    curl -s -H "APIKEY: openrouter_proxy_injector_api_key" "$INJECTOR_URL/key-status" | jq .
else
    echo -e "${RED}✗ Key status endpoint failed${NC}"
fi
echo ""

# Test 3: Test API proxy (port 3000)
echo -e "${YELLOW}Test 3: Test openrouter-api-proxy (port 3000)${NC}"
if curl -sf --max-time 5 "$API_PROXY_URL/api/v1/chat/completions" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TEST_KEY" \
    -d '{"model":"openai/gpt-3.5-turbo","messages":[{"role":"user","content":"Hello"}]}' > /dev/null 2>&1; then
    echo -e "${GREEN}✓ API proxy responding${NC}"
    curl -s "$API_PROXY_URL/api/v1/chat/completions" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $TEST_KEY" \
        -d '{"model":"openai/gpt-3.5-turbo","messages":[{"role":"user","content":"Hello"}]}' | jq .
else
    echo -e "${RED}✗ API proxy failed${NC}"
    echo "Response:"
    curl -v "$API_PROXY_URL/api/v1/chat/completions" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $TEST_KEY" \
        -d '{"model":"openai/gpt-3.5-turbo","messages":[{"role":"user","content":"Hello"}]}' 2>&1 | tail -20
fi
echo ""

# Test 4: Test with MimoCode environment variables
echo -e "${YELLOW}Test 4: Test with REDUX proxy env vars (port 8006)${NC}"
export REDUX_HTTP_PROXY="$INJECTOR_URL"
export REDUX_HTTPS_PROXY="$INJECTOR_URL"
export REDUX_HTTP_NO_PROXY="localhost,127.0.0.1"

# Test through the proxy
if curl -sf --max-time 10 \
    --proxy "$REDUX_HTTP_PROXY" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TEST_KEY" \
    -d '{"model":"openai/gpt-3.5-turbo","messages":[{"role":"user","content":"Test via proxy"}]}' \
    "https://openrouter.ai/api/v1/chat/completions" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Proxy forwarding working${NC}"
else
    echo -e "${RED}✗ Proxy forwarding failed${NC}"
    curl -v --max-time 10 \
        --proxy "$REDUX_HTTP_PROXY" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $TEST_KEY" \
        -d '{"model":"openai/gpt-3.5-turbo","messages":[{"role":"user","content":"Test via proxy"}]}' \
        "https://openrouter.ai/api/v1/chat/completions" 2>&1 | tail -30
fi
echo ""

# Test 5: Docker container health
echo -e "${YELLOW}Test 5: Docker container status${NC}"
for container in "openrouter-proxy-injector-openrouter-proxy-injector-1" "openrouter-api-proxy-openrouter-proxy-1"; do
    if docker inspect "$container" --format '{{.State.Health.Status}}' 2>/dev/null | grep -q "healthy\|none"; then
        status=$(docker inspect "$container" --format '{{.State.Status}}')
        echo -e "${GREEN}✓ $container: $status${NC}"
    else
        echo -e "${RED}✗ $container: unhealthy or not found${NC}"
    fi
done
echo ""

# Test 6: Port listening
echo -e "${YELLOW}Test 6: Port listening verification${NC}"
for port in 8006 3000; do
    if ss -tln | grep -q ":$port "; then
        echo -e "${GREEN}✓ Port $port is listening${NC}"
    else
        echo -e "${RED}✗ Port $port is NOT listening${NC}"
    fi
done
echo ""

echo "=== Test Complete ==="
echo ""
echo "If tests pass, configure MimoCode with:"
echo "  export REDUX_HTTP_PROXY=\"$INJECTOR_URL\""
echo "  export REDUX_HTTPS_PROXY=\"$INJECTOR_URL\""
echo "  export REDUX_HTTP_NO_PROXY=\"localhost,127.0.0.1\""
echo "  mimo code <your-flags>"