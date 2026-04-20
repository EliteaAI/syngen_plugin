# Syngen Plugin for ELITEA Platform

Synthetic data generation plugin for ELITEA platform using [EPAM Syngen](https://github.com/tdspora/syngen).

## Overview

This plugin provides tools for:
- **train_model**: Train a VAE model on sample data to learn data patterns
- **generate_data**: Generate synthetic data using a trained model

## Architecture

The plugin runs syngen in a Docker sidecar container due to Python version requirements (syngen requires Python 3.10/3.11, while the platform uses Python 3.12).

### Docker Images

Two Docker image options are supported:

1. **tdspora/syngen** (x86_64 only)
   - Official image from Docker Hub
   - Uses `python3 -m start --task=train/infer` command format
   - Set `command_format: docker` in config.yml

2. **syngen-arm64** (ARM64/Apple Silicon)
   - Custom image built from `docker/Dockerfile.arm64`
   - Uses `train`/`infer` CLI commands
   - Set `command_format: cli` in config.yml

## Local Development on Apple Silicon

The official `tdspora/syngen` image is x86_64 only and crashes on Apple Silicon due to TensorFlow's AVX requirements.

### Build ARM64 Image

```bash
cd syngen_plugin
docker build --platform linux/arm64 -t syngen-arm64 -f docker/Dockerfile.arm64 .
```

### Start Development Container

```bash
# Using docker-compose
docker-compose -f docker/docker-compose.arm64.yml up -d

# Or manually
docker run -d --name syngen_runner \
  -v syngen_workspace:/src/model_artifacts \
  syngen-arm64 "tail -f /dev/null"
```

### Configuration

Update `config.yml` for ARM64:

```yaml
docker:
  enabled: true
  container_name: syngen_runner
  syngen_artifacts_path: /src/model_artifacts
  command_format: cli  # Use CLI format for ARM64 image
```

## Production Deployment (x86_64)

For production on x86_64 servers, use the official image:

```yaml
# docker-compose.yml
services:
  syngen_runner:
    image: tdspora/syngen
    container_name: syngen_runner
    entrypoint: ["tail", "-f", "/dev/null"]
    volumes:
      - syngen_workspace:/src/model_artifacts
```

And set `command_format: docker` in config.yml.

## Volume Mapping

The plugin uses a shared volume for data exchange:
- Pylon container: `/data/syngen` → volume `syngen_workspace`
- Syngen container: `/src/model_artifacts` → volume `syngen_workspace`