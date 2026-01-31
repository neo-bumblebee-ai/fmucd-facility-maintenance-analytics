# FMUCD Capstone Plan

## Domain
Facilities Management / Maintenance Operations

## Objective
Identify work orders at risk of becoming long-duration and disruptive,
and provide an operationally usable prioritization queue.

## Approach
- Use FMUCD operational + environmental data
- Model historical work order durations
- Predict risk using ML
- Rank risk using percentiles (not raw thresholds)

## Key Tables
- Silver:
  - `fact_work_orders`
  - `dim_building` (SCD-2)
  - `dim_system` (SCD-2)
- Gold:
  - `work_orders_enriched`
  - `high_duration_risk_queue_ranked`

## Why Percentile Bucketing
Raw ML probabilities were poorly calibrated.
Percentile ranking guarantees:
- Stable queue size
- Business-friendly prioritization
- Independence from model calibration drift

## Outcome
A production-style risk queue suitable for daily ops and leadership review.
