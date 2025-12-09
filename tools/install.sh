#!/bin/sh
#
# D8 (Deckhouse CLI) Installer Script
#
# This script should be run via curl:
#   sh -c "$(curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)"
# or via wget:
#   sh -c "$(wget -qO- https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh)"
#
# Alternatively, download the script first and then run it:
#   curl -fsSL https://raw.githubusercontent.com/deckhouse/deckhouse-cli/main/tools/install.sh -o install.sh
#   sh install.sh
#
# Environment variables:
#   INSTALL_DIR - Installation directory (default: /usr/local/bin)
#   VERSION     - Version to install (default: latest)
#   REPO        - GitHub repository (default: deckhouse/deckhouse-cli)
#   FORCE       - Force reinstall if already exists (default: no)
#
# Command-line options:
#   --version <version>  - Install specific version
#   --force             - Force reinstall
#   --install-dir <dir> - Custom installation directory
#   --unattended        - Non-interactive mode
#
# Examples:
#   sh install.sh --version v1.0.0 --install-dir ~/bin
#   INSTALL_DIR=~/bin sh install.sh --force
#
set -e

# Default settings
REPO=${REPO:-deckhouse/deckhouse-cli}
VERSION=${VERSION:-latest}
INSTALL_DIR=${INSTALL_DIR:-/opt/deckhouse/bin}
FORCE=${FORCE:-no}
UNATTENDED=${UNATTENDED:-no}
BINARY_NAME="d8"

# Detect OS and architecture
detect_platform() {
  OS="$(uname -s)"
  ARCH="$(uname -m)"
  
  case "$OS" in
    Linux*)  OS="linux" ;;
    Darwin*) OS="darwin" ;;
    *)       
      fmt_error "Unsupported operating system: $OS"
      exit 1
      ;;
  esac
  
  case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    amd64)   ARCH="amd64" ;;
    arm64)   ARCH="arm64" ;;
    aarch64) ARCH="arm64" ;;
    *)       
      fmt_error "Unsupported architecture: $ARCH"
      exit 1
      ;;
  esac
  
  PLATFORM="${OS}-${ARCH}"
}


command_exists() {
  command -v "$@" >/dev/null 2>&1
}

user_can_sudo() {
  command_exists sudo || return 1
  ! LANG='' sudo -n -v 2>&1 | grep -q "may not run sudo"
}

check_dependencies() {
  missing=""
  
  if ! command_exists curl && ! command_exists wget; then
    missing="${missing}curl or wget, "
  fi
  
  if ! command_exists tar; then
    missing="${missing}tar, "
  fi
  
  if [ -n "$missing" ]; then
    fmt_error "Missing required dependencies: ${missing%,*}"
    fmt_error "Please install them and try again"
    exit 1
  fi
}

get_latest_version() {
  api_url="https://api.github.com/repos/${REPO}/releases/latest"
  version=""
  
  if command_exists curl; then
    version=$(curl -fsSL "$api_url" | grep '"tag_name":' | sed -E 's/.*"tag_name": "([^"]+)".*/\1/')
  elif command_exists wget; then
    version=$(wget -qO- "$api_url" | grep '"tag_name":' | sed -E 's/.*"tag_name": "([^"]+)".*/\1/')
  fi
  
  if [ -z "$version" ]; then
    fmt_error "Failed to fetch latest version"
    exit 1
  fi
  
  echo "$version"
}

download_file() {
  url="$1"
  output="$2"
  
  if command_exists curl; then
    curl -fsSL "$url" -o "$output"
  elif command_exists wget; then
    wget -qO "$output" "$url"
  else
    fmt_error "Neither curl nor wget is available"
    exit 1
  fi
}

# The [ -t 1 ] check only works when the function is not called from
# a subshell (like in `$(...)` or `(...)`, so this hack redefines the
# function at the top level to always return false when stdout is not
# a tty.
if [ -t 1 ]; then
  is_tty() {
    true
  }
else
  is_tty() {
    false
  }
fi

# This function uses the logic from supports-hyperlinks[1][2], which is
# made by Kat Marchán (@zkat) and licensed under the Apache License 2.0.
# [1] https://github.com/zkat/supports-hyperlinks
# [2] https://crates.io/crates/supports-hyperlinks
#
# Copyright (c) 2021 Kat Marchán
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
supports_hyperlinks() {
  # $FORCE_HYPERLINK must be set and be non-zero (this acts as a logic bypass)
  if [ -n "$FORCE_HYPERLINK" ]; then
    [ "$FORCE_HYPERLINK" != 0 ]
    return $?
  fi

  # If stdout is not a tty, it doesn't support hyperlinks
  is_tty || return 1

  # DomTerm terminal emulator (domterm.org)
  if [ -n "$DOMTERM" ]; then
    return 0
  fi

  # VTE-based terminals above v0.50 (Gnome Terminal, Guake, ROXTerm, etc)
  if [ -n "$VTE_VERSION" ]; then
    [ "$VTE_VERSION" -ge 5000 ]
    return $?
  fi

  # If $TERM_PROGRAM is set, these terminals support hyperlinks
  case "$TERM_PROGRAM" in
  Hyper|iTerm.app|terminology|WezTerm|vscode) return 0 ;;
  esac

  # These termcap entries support hyperlinks
  case "$TERM" in
  xterm-kitty|alacritty|alacritty-direct) return 0 ;;
  esac

  # xfce4-terminal supports hyperlinks
  if [ "$COLORTERM" = "xfce4-terminal" ]; then
    return 0
  fi

  # Windows Terminal also supports hyperlinks
  if [ -n "$WT_SESSION" ]; then
    return 0
  fi

  # Konsole supports hyperlinks, but it's an opt-in setting that can't be detected
  # https://github.com/ohmyzsh/ohmyzsh/issues/10964
  # if [ -n "$KONSOLE_VERSION" ]; then
  #   return 0
  # fi

  return 1
}

# Adapted from code and information by Anton Kochkov (@XVilka)
# Source: https://gist.github.com/XVilka/8346728
supports_truecolor() {
  case "$COLORTERM" in
  truecolor|24bit) return 0 ;;
  esac

  case "$TERM" in
  iterm           |\
  tmux-truecolor  |\
  linux-truecolor |\
  xterm-truecolor |\
  screen-truecolor) return 0 ;;
  esac

  return 1
}

fmt_link() {
  # $1: text, $2: url, $3: fallback mode
  if supports_hyperlinks; then
    printf '\033]8;;%s\033\\%s\033]8;;\033\\\n' "$2" "$1"
    return
  fi

  case "$3" in
  --text) printf '%s\n' "$1" ;;
  --url|*) fmt_underline "$2" ;;
  esac
}

fmt_underline() {
  is_tty && printf '\033[4m%s\033[24m\n' "$*" || printf '%s\n' "$*"
}

# shellcheck disable=SC2016 # backtick in single-quote
fmt_code() {
  is_tty && printf '`\033[2m%s\033[22m`\n' "$*" || printf '`%s`\n' "$*"
}

fmt_error() {
  printf '%sError: %s%s\n' "${FMT_BOLD}${FMT_RED}" "$*" "$FMT_RESET" >&2
}

fmt_info() {
  printf '%s%s%s\n' "${FMT_BLUE}" "$*" "$FMT_RESET"
}

fmt_success() {
  printf '%s%s%s\n' "${FMT_GREEN}" "$*" "$FMT_RESET"
}

fmt_warn() {
  printf '%sWarning: %s%s\n' "${FMT_YELLOW}" "$*" "$FMT_RESET"
}

setup_color() {
  # Only use colors if connected to a terminal
  if ! is_tty; then
    FMT_RAINBOW=""
    FMT_RED=""
    FMT_GREEN=""
    FMT_YELLOW=""
    FMT_BLUE=""
    FMT_BOLD=""
    FMT_RESET=""
    return
  fi

  # shellcheck disable=SC2034  # FMT_RAINBOW kept for potential future use
  if supports_truecolor; then
    FMT_RAINBOW="
      $(printf '\033[38;2;255;0;0m')
      $(printf '\033[38;2;255;97;0m')
      $(printf '\033[38;2;247;255;0m')
      $(printf '\033[38;2;0;255;30m')
      $(printf '\033[38;2;77;0;255m')
      $(printf '\033[38;2;168;0;255m')
      $(printf '\033[38;2;245;0;172m')
    "
  else
    FMT_RAINBOW="
      $(printf '\033[38;5;196m')
      $(printf '\033[38;5;202m')
      $(printf '\033[38;5;226m')
      $(printf '\033[38;5;082m')
      $(printf '\033[38;5;021m')
      $(printf '\033[38;5;093m')
      $(printf '\033[38;5;163m')
    "
  fi

  FMT_RED=$(printf '\033[31m')
  FMT_GREEN=$(printf '\033[32m')
  FMT_YELLOW=$(printf '\033[33m')
  FMT_BLUE=$(printf '\033[34m')
  FMT_BOLD=$(printf '\033[1m')
  FMT_RESET=$(printf '\033[0m')
}

install_d8() {
  install_version="$VERSION"
  
  # Get latest version if not specified
  if [ "$install_version" = "latest" ]; then
    fmt_info "Fetching latest version..."
    install_version=$(get_latest_version)
    fmt_success "Latest version: ${install_version}"
  fi
  
  # Version for filename (keep 'v' prefix as in releases)
  version_for_filename="$install_version"
  
  # Build download URL
  filename="${BINARY_NAME}-${version_for_filename}-${PLATFORM}.tar.gz"
  download_url="https://github.com/${REPO}/releases/download/${install_version}/${filename}"
  
  fmt_info "Downloading D8 ${install_version} for ${PLATFORM}..."
  
  # Create temporary directory
  tmp_dir=$(mktemp -d -t d8-install.XXXXXXXXXX)
  trap 'rm -rf "$tmp_dir"' EXIT
  
  tar_file="${tmp_dir}/${filename}"
  
  # Download the release
  if ! download_file "$download_url" "$tar_file"; then
    fmt_error "Failed to download D8 from ${download_url}"
    fmt_error "Please check if the version and platform are correct"
    exit 1
  fi
  
  fmt_success "Downloaded successfully"
  
  # Extract the tarball
    fmt_info "Extracting archive..."
    tar -xzf "$tar_file" -C "$tmp_dir"
    
    # Look for the binary (it's wrapped in a directory by goreleaser)
    archive_dir="$PLATFORM"
    binary_path="${tmp_dir}/${archive_dir}/bin/${BINARY_NAME}"
    if [ ! -f "$binary_path" ]; then
        fmt_error "Binary not found in archive at: ${binary_path}"
        exit 1
    fi  # Make binary executable
  chmod +x "$binary_path"
  
  # Install the binary
  install_binary "$binary_path"
  
  echo
}

install_binary() {
  binary_path="$1"
  # Normalize INSTALL_DIR by removing trailing slashes
  INSTALL_DIR="${INSTALL_DIR%/}"
  target_path="${INSTALL_DIR}/${BINARY_NAME}"
  
  # Check if binary already exists
  if [ -f "$target_path" ] && [ "$FORCE" != "yes" ]; then
    if [ "$UNATTENDED" = "yes" ]; then
      fmt_error "${BINARY_NAME} is already installed at ${target_path}"
      fmt_error "Use --force to reinstall"
      exit 1
    fi
    
    printf '%s%s is already installed at %s. Overwrite? [y/N]%s ' \
      "$FMT_YELLOW" "$BINARY_NAME" "$target_path" "$FMT_RESET"
    read -r opt
    case $opt in
      [Yy]*) ;;
      *) echo "Installation cancelled."; exit 0 ;;
    esac
  fi
  
  # Check if we need sudo to install
  if [ ! -w "$INSTALL_DIR" ]; then
    if user_can_sudo; then
      fmt_info "Installing ${BINARY_NAME} to ${target_path} (requires sudo)..."
      sudo mkdir -p "$INSTALL_DIR"
      sudo cp "$binary_path" "$target_path"
      sudo chmod +x "$target_path"
    else
      fmt_error "Cannot write to ${INSTALL_DIR} and sudo is not available"
      fmt_error "Please run with sudo or choose a different installation directory"
      exit 1
    fi
  else
    fmt_info "Installing ${BINARY_NAME} to ${target_path}..."
    mkdir -p "$INSTALL_DIR"
    cp "$binary_path" "$target_path"
    chmod +x "$target_path"
  fi
  
  fmt_success "${BINARY_NAME} installed successfully!"
}

verify_installation() {
  # Normalize INSTALL_DIR by removing trailing slashes
  INSTALL_DIR="${INSTALL_DIR%/}"
  target_path="${INSTALL_DIR}/${BINARY_NAME}"
  
  fmt_info "Verifying installation..."

  # Check if binary exists after installation 
  if [ ! -f "$target_path" ]; then
    fmt_error "Installation verification failed"
    exit 1
  fi
  
  # Try to get version directly or using absolute path if not in PATH
  if command_exists "$BINARY_NAME"; then
    version_output=$("$BINARY_NAME" --version 2>/dev/null || echo "")
    if [ -n "$version_output" ]; then
      fmt_success "✓ ${version_output}"
    fi
  elif [ -f "$target_path" ]; then
    version_output=$("$target_path" --version 2>/dev/null || echo "")
    if [ -n "$version_output" ]; then
      fmt_success "✓ ${version_output}"
    fi
  fi

  # Suggest adding INSTALL_DIR to PATH for the user
  if ! command_exists "$BINARY_NAME"; then
    echo ""
    fmt_warn "Note: ${INSTALL_DIR} is not in your PATH"
    echo "Add it to your PATH by adding this line to your shell profile:"
    echo ""
    echo "  ${FMT_BOLD}export PATH=\"${INSTALL_DIR}:\$PATH\"${FMT_RESET}"
    echo ""
  fi
}

print_success() {
  cat << EOF

${FMT_GREEN}${FMT_BOLD}
╔════════════════════════════════════════════════════╗
║                                                    ║
║                                                    ║
║           ▄     ▌ ▌           ▄▖▜ ▘                ║
║           ▌▌█▌▛▘▙▘▛▌▛▌▌▌▛▘█▌▄▖▌ ▐ ▌                ║
║           ▙▘▙▖▙▖▛▖▌▌▙▌▙▌▄▌▙▖  ▙▖▐▖▌                ║ 
║                                                    ║
║                 Deckhouse CLI                      ║
║            Successfully Installed!                 ║
║                                                    ║
╚════════════════════════════════════════════════════╝
${FMT_RESET}

${FMT_BOLD}Getting Started:${FMT_RESET}

  Run ${FMT_BOLD}${FMT_BLUE}d8 --help${FMT_RESET} to see available commands

${FMT_BOLD}Documentation:${FMT_RESET}

  • GitHub: $(fmt_link "https://github.com/${REPO}" "https://github.com/${REPO}")
  • Issues: $(fmt_link "https://github.com/${REPO}/issues" "https://github.com/${REPO}/issues")

${FMT_BOLD}Quick Commands:${FMT_RESET}

  ${FMT_BLUE}d8 status${FMT_RESET}     - Show cluster status
  ${FMT_BLUE}d8 mirror${FMT_RESET}     - Mirror Deckhouse modules
  ${FMT_BLUE}d8 version${FMT_RESET}    - Show version information

EOF
}

print_usage() {
  cat << EOF
${FMT_BOLD}Usage:${FMT_RESET} $0 [options]

${FMT_BOLD}Options:${FMT_RESET}
  --version <version>       Install specific version (default: latest)
  --install-dir <dir>       Installation directory (default: /usr/local/bin)
  --force                   Force reinstall if already exists
  --unattended              Non-interactive mode
  --help                    Show this help message

${FMT_BOLD}Environment Variables:${FMT_RESET}
  VERSION                   Version to install
  INSTALL_DIR               Installation directory
  REPO                      GitHub repository (default: deckhouse/deckhouse-cli)
  FORCE                     Force reinstall (yes/no)

${FMT_BOLD}Examples:${FMT_RESET}
  # Install latest version
  $0

  # Install specific version
  $0 --version v1.0.0

  # Install to custom directory
  $0 --install-dir ~/bin

  # Force reinstall
  $0 --force

EOF
}

main() {
  # Run as unattended if stdin is not a tty
  if [ ! -t 0 ]; then
    UNATTENDED=yes
  fi

  # Parse arguments
  while [ $# -gt 0 ]; do
    case $1 in
      --version)
        VERSION="$2"
        shift 2
        ;;
      --install-dir)
        INSTALL_DIR="$2"
        shift 2
        ;;
      --force)
        FORCE=yes
        shift
        ;;
      --unattended)
        UNATTENDED=yes
        shift
        ;;
      --help|-h)
        setup_color
        print_usage
        exit 0
        ;;
      *)
        echo "Unknown option: $1"
        print_usage
        exit 1
        ;;
    esac
  done

  setup_color
  
  printf '%s%sD8 (Deckhouse CLI) Installer%s\n' "${FMT_BOLD}" "${FMT_BLUE}" "${FMT_RESET}"
  echo ""

  # Check dependencies
  check_dependencies
  
  # Detect platform
  detect_platform
  fmt_success "Detected platform: ${PLATFORM}"
  echo ""

  # Install D8
  install_d8
  
  # Verify installation
  verify_installation
  
  # Print success message
  print_success
}

main "$@"