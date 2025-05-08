# syntax=docker/dockerfile:1.4
FROM python:3.12-slim as builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget unzip curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG CHROME_VERSION="116.0.5845.96"

# Install Chrome for Testing
RUN wget -q -O chrome-linux64.zip \
    "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${CHROME_VERSION}/linux64/chrome-linux64.zip" \
    && unzip chrome-linux64.zip -d /opt \
    && rm chrome-linux64.zip

# Install ChromeDriver
RUN wget -q -O chromedriver-linux64.zip \
    "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${CHROME_VERSION}/linux64/chromedriver-linux64.zip" \
    && unzip chromedriver-linux64.zip -d /opt \
    && rm chromedriver-linux64.zip

WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt

# --------- Runtime Image ---------
FROM python:3.12-slim

# Install all system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libnss3 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxrandr2 libgbm1 libpango-1.0-0 \
    libasound2 libwayland-client0 liblzo2-2 \
    libpkcs11-helper1 libcairo2 \
    xvfb iputils-ping curl wait-for-it \
    openvpn iptables net-tools gosu \
    && rm -rf /var/lib/apt/lists/*

# Copy venv, Chrome, Chromedriver from builder
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /opt/chrome-linux64 /opt/chrome
COPY --from=builder /opt/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver

# Create Chrome symlink
RUN ln -s /opt/chrome/chrome /usr/bin/google-chrome \
    && chmod +x /usr/bin/google-chrome \
    && chmod +x /usr/local/bin/chromedriver

# VPN Config
COPY vpn/config.ovpn /etc/openvpn/client/
COPY vpn/auth.txt /etc/openvpn/client/

# App files
WORKDIR /app
COPY . .

# App user
RUN groupadd -r appuser && useradd -r -g appuser appuser \
    && chown -R appuser:appuser /app /opt/venv

USER appuser
ENV PATH="/opt/venv/bin:$PATH"

# DISABLE healthcheck for now (unhealthy if app doesn't expose /health)
HEALTHCHECK NONE

# Entrypoint script
COPY --chown=appuser:appuser docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh
ENTRYPOINT ["/app/docker-entrypoint.sh"]


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
