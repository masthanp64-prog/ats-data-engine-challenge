# Automated Relational Data Engine — End-to-End ATS Pipelines

This project provides a robust, professional, deterministic synthetic data generator framework designed to simulate an enterprise Applicant Tracking System (ATS) environment. 

## Architectural Schema Overview
The generation framework manages a deep relational topography tracking job requests, candidate processing states, technical review matrices, interviewer block availability indices, panel evaluation logs, and onboarding financial structures. It maintains cross-table historical log tracking with strict integrity constraint checking algorithms.

## Injected Anomaly Monitoring (Isolated Ground Truth Layer)
To support testing and metrics compliance audits, this framework features a rule-based anomaly injection interface that handles real-world business exception paths. These edge cases are monitored separately inside `ground_truth.csv` to protect core machine learning models or verification engines from hidden data leakage:
* **INTERVIEW_UNAVAILABLE_INTERVIEWER**: Schedule blocks mapped against un-allocated panels.
* **DUPLICATE_APPLICATION**: Double submissions coming from individual unique candidates.
* **MISSING_FEEDBACK_STAGE_ADVANCED**: Workflow updates bypassing step validation thresholds.
* **OFFER_SALARY_OUT_OF_BAND**: Assigning financial packages violating max structural budget rules.
* **CLOSED_ROLE_ACTIVE_APPLICATIONS**: Ongoing evaluation pathways working on dead requisitions.

## Local Execution Instructions

### Installation Prerequisites
Verify that Python 3.8+ is configured alongside `pandas` and `numpy`:
```bash
python -m pip install pandas numpy
```

### Running the Data Engine Pipeline
Execute the deterministic processing layer from your project terminal root:
```bash
python generator.py
```
This routine triggers multi-run checksum verification logs, verifies database state validations, and exports clean seed tables to the `./output_data/` folder.
