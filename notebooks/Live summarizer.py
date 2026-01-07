# Databricks notebook source
import pandas as pd
from IPython.display import display, Markdown
from pyspark.sql.functions import col

def generate_strategic_briefing():
    
    # Fetch the Gold Data
    df_kpi = spark.read.table("hive_metastore.default.gold_business_kpis").toPandas()
    
    # Fetch AI Predictions for the "Forward Looking" section
    df_preds = spark.read.table("hive_metastore.default.gold_patient_predictions") \
        .filter("risk_alert LIKE '%CRITICAL%'").toPandas()
    
    # --- 2. CALCULATE AGGREGATE METRICS ---
    total_admits = df_kpi['1_total_admissions'].sum()
    total_critical_stays = df_kpi['12_critical_stay_over_2_days'].sum()
    total_blocked_hours = total_critical_stays * 48 # Approx wasted capacity
    avg_complexity = df_kpi['14_complexity_score'].mean()
    
    # --- 3. BUILD THE REPORT ---
    md = "#  Strategic Operations Briefing\n"
    md += f"**Live Snapshot:** {pd.Timestamp.now().strftime('%A, %d %b %Y - %H:%M')}\n\n"
    
    # SECTION A: EXECUTIVE SUMMARY (The "Traffic Light")
    if total_critical_stays > 10:
        status = " CODE RED: CAPACITY CRISIS"
        advice = "Activate Surge Protocols immediately. Elective procedures should be put on hold."
    elif total_admits > 100:
        status = " CODE ORANGE: HIGH VOLUME"
        advice = "Staff up for incoming shift. Open overflow wings if available."
    else:
        status = " CODE GREEN: STABLE"
        advice = "Operations normal. Focus on clearing minor backlogs."
        
    md += f"### {status}\n"
    md += f"> **Strategic Order:** *{advice}*\n\n"
    md += "---\n"

    # SECTION B: RESOURCE BLEED (Financial/Capacity Impact)
    md += "###  Capacity & Waste Analysis\n"
    if total_critical_stays > 0:
        md += f"**CRITICAL ISSUE:** We currently have **{total_critical_stays} patients** stuck for >48 hours.\n"
        md += f"-  **Capacity Loss:** ~{total_blocked_hours} Bed-Hours are effectively blocked.\n"
        md += f"-  **Target:** Review the *{df_kpi.sort_values('12_critical_stay_over_2_days', ascending=False).iloc[0]['department']}* department first.\n"
    else:
        md += " Bed turnover is healthy. No significant long-term blocks detected.\n"
    md += "\n"

    # SECTION C: STAFFING & COMPLEXITY (The "Nurse Manager" View)
    md += "### Resource Allocation & Clinical Load & Complexity\n"
    
    # Check if patients are "Hard" (High Complexity Score)
    if avg_complexity > 50:
        md += f"**CRITICAL HIGH ACUITY WARNING:** Patient mix is unusually complex (Score: {avg_complexity:.1f}).\n"
        md += "- **Implication:** Nurses will need lower ratios (1:3 instead of 1:5).\n"
    else:
        md += "Patient acuity is standard. Standard staffing ratios apply.\n"
        
    # Check Shift Load
    night_load = df_kpi['4_night_shift_admits'].sum()
    if night_load > (total_admits * 0.4): # If >40% of admits are at night
        md += f"- **Staffing Alert:** Night shift is bearing **{night_load} admits** (Heavy Load). Consider swing-shift support.\n"
    
    md += "\n"

    # SECTION D: DEPARTMENT DEEP DIVE (The "Problem Child")
    md += "###  Department Performance Matrix\n"
    md += "| Department | Status | Chaos Score (StdDev) | Bottleneck? |\n"
    md += "| :--- | :--- | :--- | :--- |\n"
    
    for _, row in df_kpi.iterrows():
        dept = row['department']
        chaos = row['9_std_dev_los']
        stuck = row['12_critical_stay_over_2_days']
        
        # Determine "Stability"
        if chaos > 15: stability = "🌪️ Chaotic"
        else: stability = " Stable"
        
        # Determine "Bottleneck"
        if stuck > 2: bottleneck = f"⛔ {stuck} Stuck"
        else: bottleneck = "Flowing"
        
        # Status Flag
        if "CRITICAL" in row['Recommendation_Status']: status_icon = "🔴"
        elif "WARNING" in row['Recommendation_Status']: status_icon = "🟠"
        else: status_icon = "🟢"
        
        md += f"| **{dept}** | {status_icon} | {stability} ({chaos}h) | {bottleneck} |\n"

    # SECTION E: AI RADAR (The Future)
    md += "\n---\n###  AI Predictive Radar (Next 24h)\n"
    high_risk_count = len(df_preds)
    if high_risk_count > 0:
        md += f"**Heads Up:** The AI model has flagged **{high_risk_count} new admissions** as 'High Risk' for long stays.\n"
        md += "These patients are predicted to stay **>96 Hours**. Early discharge planning is recommended *now*.\n"
    else:
        md += "AI forecasts standard turnover for recent admissions.\n"

    # RENDER
    display(Markdown(md))

# --- RUN IT ---
generate_strategic_briefing()

# COMMAND ----------

import requests
import json

def send_to_slack(report_text):
   
    webhook_url = "https://hooks.slack.com/services/incompleteeee"
    
    # 2. FORMAT MESSAGE
    payload = {"text": report_text}
    
    # 3. SEND
    try:
        response = requests.post(webhook_url, json=payload) 
        print(" SENDING REPORT TO WEBHOOK...")
        print(" Message sent successfully (Simulated).")
    except Exception as e:
        print(f" Failed to send alert: {e}")

