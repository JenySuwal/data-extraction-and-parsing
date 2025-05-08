#!/bin/bash
set -euo pipefail

# Fix X11 permissions (must run before USER switch)
mkdir -p /tmp/.X11-unix
chmod 1777 /tmp/.X11-unix

# Initialize Xvfb for headless Chrome
export DISPLAY=:99
Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset >/dev/null 2>&1 &

# Wait for Xvfb to be ready
sleep 2

# Verify Chrome and Chromedriver are working (with --no-sandbox for Docker)
echo "Testing Chrome..."
google-chrome --no-sandbox --version || {
    echo "Chrome failed - checking dependencies..."
    ldd $(which google-chrome) | grep -i "not found"
    exit 1
}

chromedriver --version || {
    echo "Chromedriver failed to start"
    exit 1
}

# Start OpenVPN connection (if needed)
if [ -f "/etc/openvpn/client/config.ovpn" ]; then
    echo "Starting OpenVPN..."
    openvpn \
        --config /etc/openvpn/client/config.ovpn \
        --auth-user-pass /etc/openvpn/client/auth.txt \
        --log /tmp/openvpn.log \
        --daemon

    sleep 5  # Wait for VPN connection
    
    # Check OpenVPN logs for errors
    if ! tail -n 20 /var/log/openvpn.log | grep -i "Initialization Sequence Completed"; then
        echo "Error: OpenVPN failed to connect."
        tail -n 20 /var/log/openvpn.log
        exit 1
    fi

    # Verify VPN connection
    if ! curl --max-time 3 --retry 3 --retry-delay 1 ifconfig.io >/dev/null 2>&1; then
        echo "Warning: VPN connection check failed"
        exit 1
    fi
fi

# Network connectivity check
echo "Testing network..."
if curl --max-time 3 --retry 2 --retry-delay 1 http://google.com >/dev/null 2>&1; then
    echo "Network OK"
else
    echo "Warning: Network check failed (but continuing)"
fi

# Drop to appuser and run the main command
exec gosu appuser "$@"
