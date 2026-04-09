# Integration Guide

This document describes how the data-pipeline repository integrates with other services.

## Service Identity

- Service name: data-pipeline
- Repository: portfolios-tracker/data-pipeline
- Runtime: Apache Airflow + Python DAG modules

## Upstream and Downstream Dependencies

- Core API (NestJS): triggered via protected batch endpoints
- Supabase: persistent storage for market/enrichment data
- External providers: vnstock, yfinance, CoinGecko, Gemini (feature-dependent)

## Core API Contract

Required environment variables:

- `NESTJS_API_URL`
- `DATA_PIPELINE_API_KEY`

Authentication header used by pipeline-triggered API calls:

- `X-Api-Key: <DATA_PIPELINE_API_KEY>`

## Change Management

1. Treat all exported tables and API-triggered payloads as versioned contracts.
2. Document schema-affecting changes with migration notes.
3. Coordinate contract changes with the core repository before merge.
