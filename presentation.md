---
marp: true
title: Workplace incidents analysis
paginate: true
---

# Workplace incidents analysis
## EU and US occupational accidents

![INSA](./images/logo-insa_0.png)

Loe Louis-Marie | Lancelot Tariot Camille | Nguyen Sang
INSA Lyon - Data Engineering

---

# Objective and data sources

- Build an end-to-end pipeline for occupational accident analytics
- Harmonize ARIA (France) and OSHA (USA) datasets
- Cover 2015-2025 cases plus 2009-2017 fatality summaries

---

# Pipeline overview

![Pipeline overview](./images/pipeline-overview.png)

- Airflow DAG chain: download -> clean -> prep -> star schema -> validation
- Staging with MongoDB for schema-safe ARIA ingestion
- Production analytics in Postgres (data_db)

---

# Star schema

![Star schema](./images/star-schema.png)

- Dimensions: date, country, industry, accident type, hazard, employer, fatality
- Fact table: standardized accidents with metrics

---

# Analytics and takeaways

- 5 automated queries: fatalities by year, by country, FR vs USA, top 10, last 10 years
- Results saved to data/analytics_results after validation tests
- Next steps: offline datasets, extra dimensions, Druid integration