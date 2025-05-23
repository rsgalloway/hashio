# =============================================================================
# Project: Hashio - Custom file and directory checksum tool
# Makefile for building project executables on Linux and Windows (using Wine)
#
# Usage:
#   make           - Builds targets
#   make clean     - Removes all build artifacts
#   make install   - Installs the build artifacts using distman
#
# Requirements:
#   - Python and pip installed (Linux)
#   - Wine installed for Windows builds on Linux
#   - distman installed for installation (pip install distman)
# =============================================================================

# Define the installation command
BUILD_CMD := pip install -r requirements.txt -t build

# Target to build for Linux
build: clean
	$(BUILD_CMD)

# Combined target to build for both platforms
all: build

# Clean target to remove the build directory
clean:
	rm -rf build

# Install target to install the builds using distman
# using --force allows uncommitted changes to be disted
dryrun:
	distman --force --dryrun

# Install target to install the builds using distman
# using --force allows uncommitted changes to be disted
install: build
	distman --force --yes

# Phony targets
.PHONY: build dryrun install clean
