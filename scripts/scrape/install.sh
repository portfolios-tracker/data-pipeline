#!/usr/bin/env bash
# install.sh
# ──────────────────────────────────────────────────────────────────────────────
# Sets up webclaw CLI + Python environment for news_extractor.py
# Tested on: Ubuntu 22.04 / 24.04, Debian, WSL2
#
# Usage:
#   chmod +x install.sh
#   ./install.sh
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()    { echo -e "${GREEN}[✓]${NC} $*"; }
warning() { echo -e "${YELLOW}[!]${NC} $*"; }
error()   { echo -e "${RED}[✗]${NC} $*"; exit 1; }

echo "======================================================"
echo "  webclaw + Python environment installer"
echo "======================================================"
echo ""

# ── 1. Rust / Cargo ───────────────────────────────────────────────────────────
if command -v cargo &>/dev/null; then
    info "Rust/Cargo already installed: $(cargo --version)"
else
    warning "Rust not found — installing via rustup…"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # Source cargo env for the rest of this script
    source "$HOME/.cargo/env"
    info "Rust installed: $(cargo --version)"
fi

# Ensure cargo bin is on PATH for this session
export PATH="$HOME/.cargo/bin:$PATH"

# ── 2. Build dependencies (Linux only) ────────────────────────────────────────
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    info "Installing system build dependencies…"
    sudo apt-get update -qq
    sudo apt-get install -y -qq \
        build-essential \
        pkg-config \
        libssl-dev \
        git \
        curl
fi

# ── 3. webclaw CLI ────────────────────────────────────────────────────────────
if command -v webclaw &>/dev/null; then
    info "webclaw CLI already installed: $(webclaw --version 2>/dev/null || echo 'version unknown')"
else
    info "Building webclaw CLI from source (this takes ~2–5 min)…"
    cargo install --git https://github.com/0xMassi/webclaw.git webclaw-cli
    info "webclaw CLI installed."
fi

# Verify
if ! command -v webclaw &>/dev/null; then
    warning "webclaw not found in PATH after install."
    warning "Add this to your ~/.bashrc or ~/.zshrc:"
    echo '    export PATH="$HOME/.cargo/bin:$PATH"'
    warning "Then run: source ~/.bashrc"
fi

# ── 4. Python check ───────────────────────────────────────────────────────────
if command -v python3 &>/dev/null; then
    PY_VER=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
    info "Python found: $PY_VER"
    # Warn if below 3.9 (required for dict | None type hints)
    python3 -c "import sys; sys.exit(0 if sys.version_info >= (3,9) else 1)" \
        || warning "Python 3.9+ recommended. You have $PY_VER — type hints may need adjusting."
else
    error "Python 3 not found. Install it with: sudo apt install python3 python3-pip"
fi

# ── 5. Python dependencies ────────────────────────────────────────────────────
info "Installing Python dependencies…"

# Prefer pip inside a venv if one exists, else use pip3
if [[ -f ".venv/bin/pip" ]]; then
    PIP=".venv/bin/pip"
    info "Using existing .venv"
elif python3 -m venv --help &>/dev/null; then
    info "Creating virtual environment (.venv)…"
    python3 -m venv .venv
    PIP=".venv/bin/pip"
    source .venv/bin/activate
else
    PIP="pip3"
    warning "venv not available — installing to user site-packages."
fi

$PIP install --quiet --upgrade pip
$PIP install --quiet requests beautifulsoup4
info "Python packages installed: requests, beautifulsoup4"

# ── 6. Smoke test ─────────────────────────────────────────────────────────────
echo ""
info "Running smoke test: webclaw https://example.com -f json"
if webclaw https://example.com -f json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print('  title:', d['metadata']['title'])" ; then
    info "Smoke test passed."
else
    warning "Smoke test failed — webclaw may need WEBCLAW_API_KEY for some sites."
    warning "For public sites it should work without a key."
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "======================================================"
info "Setup complete."
echo ""
echo "  To activate the venv (if created):"
echo "    source .venv/bin/activate"
echo ""
echo "  To run the scraper:"
echo "    python news_extractor.py"
echo "======================================================"
