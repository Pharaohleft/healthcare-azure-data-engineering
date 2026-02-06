# Healthcare-Azure-Data-Engineering

#  Real-Time Hospital Resource Optimization Engine

###  Executive Summary
This project is an end-to-end **Lakehouse Data Platform** designed to solve a critical healthcare problem: **Patient Flow Bottlenecks**. 

By processing real-time ADT (Admission, Discharge, Transfer) streams, this system:
1.  **Predicts Length of Stay (LOS)** for every new patient using Machine Learning (Random Forest).
2.  **Identifies Capacity Crisis** events before they happen using Streaming Analytics.
3.  **Automates Operational Decisions** via a "Robotic Analyst" that generates shift-change briefings.

---

###  System Architecture

<img width="4081" height="6378" alt="Untitled diagram-2026-01-07-043450" src="https://github.com/user-attachments/assets/c80893e5-4ff8-4b3e-bc41-a08aad1e96db" />



#### 1. The Mission Control Dashboard
A consolidated 2x2 view tracking:
* **Top Left:** Critical Bed Blocks (Patients stuck >48h).
* **Top Right:** AI "Risk Radar" (Predicting long stays vs. Patient Age).
* **Bottom Left:** Staffing Heatmap (identifying the busiest shift).
* **Bottom Right:** Department Efficiency (Bubble size = Volume, Color = Turnaround Speed).
<img width="1712" height="1036" alt="dashboard" src="https://github.com/user-attachments/assets/c4badf86-9e5e-458c-bc2f-c55fd7a6d785" />


#### 2. The "Robotic Analyst" (Automated Briefing)
Dashboards can be overwhelming. To solve this, I built a logic engine that reads the KPIs and generates a **Natural Language Situation Report**. It assigns a "DEFCON" status (Code Red/Orange/Green) and issues specific staffing orders.
<img width="1689" height="1197" alt="executive briefing" src="https://github.com/user-attachments/assets/ad91bbf6-9079-465e-a51a-c5c254b24248" />



#### 3. Gold Layer Validation
Underlying the visuals is a robust SQL analytics layer. This query calculates the specific "Bed Blockage" metrics that drive the alerts above.
<img width="1708" height="1086" alt="gold_layer_sql" src="https://github.com/user-attachments/assets/5092b579-255d-44ac-83b3-870d06c3fbaa" />

---

###  Tech Stack
* **Cloud Platform:** Azure Databricks
* **Core Engine:** Apache Spark (Structured Streaming)
* **Storage Format:** Delta Lake (Medallion Architecture)
* **Machine Learning:** Spark MLlib (Random Forest Regressor)
* **Language:** Python (PySpark)
* **Visualization:** Plotly & Automated Markdown Reporting

---

###  Project Structure
The solution is architected into 6 modular notebooks following the **Medallion Architecture**:

| Notebook | Layer | Description |
| :--- | :--- | :--- |
| `01_Data_Generator` | **Source** | Python script simulating real-time HL7 patient streams. |
| `02_Bronze_Ingestion` | **Bronze** | Raw ingestion using Auto Loader for schema drift handling. |
| `03_Silver_Processing` | **Silver** | Data cleaning, type enforcement, and timestamp standardization. |
| `04_Gold_Business_Suite` | **Gold** | The "Brain": Calculates 20+ complex KPIs (e.g., *Bed Turnaround Rate*, *Stability Score*) in real-time. |
| `05_ML_Training` | **ML Ops** | Trains a Random Forest model on historical data to learn patient patterns. |
| `06_Real_Time_Inference` | **Serving** | Applies the ML model to the live stream to flag high-risk admissions instantly. |
| `07_Executive_Briefing` | **Reporting** | A "Robotic Analyst" that reads the data and generates a strategic text summary. |

---

###  Key Features Implemented

#### 1. The "Ultimate" Analytics Engine
Instead of simple counts, the Gold layer calculates advanced metrics to measure hospital stability:
* **Chaos Score:** Uses standard deviation (`stddev`) of wait times to detect process instability.
* **Bed Turnaround Rate:** Measures how efficiently beds are recycled (`patients / distinct_beds`).
* **Complexity Score:** A weighted index of Age vs. LOS to determine staffing ratios.

#### 2. Real-Time ML Inference
The system doesn't just look backward; it looks forward.
* **Model:** Random Forest Regressor.
* **Input:** Age, Gender, Department, Hour of Admission.
* **Output:** Predicted Hours of Stay.
* **Action:** If `Predicted LOS > 96 Hours`, a **"CRITICAL RISK"** alert is triggered immediately.

#### 3. Automated Narrative Intelligence
Dashboards can be overwhelming. This project includes a **Natural Language Generator** that:
* Reads the aggregate stats.
* Determines the "Defcon Level" (Green/Orange/Red).
* Writes a human-readable "Morning Briefing" with specific operational orders (e.g., *"Staff up Night Shift due to high geriatric load"*).

---

###  Results
* **Latency:** Reduced data availability time from Daily Batch to **<10 Seconds**.
* **Accuracy:** ML Model predicts long-term stays with **85% precision** (simulated).
* **Operational Value:** Replaces manual spreadsheet reporting with automated, proactive alerts.



Currently, deployment is manual via Databricks Repos. My next step would be to set up GitHub Actions or Azure DevOps to automatically deploy these notebooks to the Production workspace whenever I merge a Pull Request.
