#!/usr/bin/env bash
# proxy-setup.sh - Set up proxy environment variables for MimoCode
# Place this file in your project root and run: ./proxy-setup.sh

# Proxy server address (change as needed)
PROXY_HOST="localhost"
PROXY_PORT="8006"  # Adjust if your proxy uses a different port

# Export proxy variables
export REDUX_HTTP_PROXY="http://${PROXY_HOST}:${PROXY_PORT}"
export REDUX_HTTPS_PROXY="http://${PROXY_HOST}:${PROXY_PORT}"
export REDUX_HTTP_NO_PROXY="localhost,127.0.0.1,::1"

# Verify and display
echo "✅ Proxy configured:"
echo "   HTTP_PROXY:  $REDUX_HTTP_PROXY"
echo "   HTTPS_PROXY: $REDUX_HTTPS_PROXY"
echo "   NO_PROXY:    $REDUX_HTTP_NO_PROXY"

# Optionally run mimo code after setup
if [[ "$1" == "--run" ]]; then
    echo "🚀 Running mimo code..."
    exec mimo code "${@:2}"
fi
