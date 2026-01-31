# Key Findings – FMUCD Capstone

## Data Scale
- Total work orders: ~2.6M
- Usable duration records: ~1.6M

## ML Results
- Model: Logistic Regression
- AUC: ~0.56
- Raw probabilities poorly calibrated

## Operational Insight
Percentile-based ranking produced:
- HIGH risk: ~1% (~26K work orders)
- MEDIUM risk: ~4% (~106K)
- LOW risk: ~95%

## Business Insights
- Certain systems consistently dominate HIGH-risk queues
- Buildings with high deferred maintenance correlate with risk
- Reactive maintenance (UPM) skews higher risk than PPM

## Recommendation
Use ML score as a **ranking signal**, not a binary decision.
Integrate risk queue into daily maintenance triage.