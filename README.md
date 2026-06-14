
# Real-Time ICU Patient Heart Rate Monitoring — Spark Structured Streaming

**Course:** ENGR 5785G – Real-Time Data Analytics for IoT
**Assignment:** Real-Time Stream Processing
**Scenario:** B – Hospital Patient Monitoring (Tumbling 2-Minute Window)

## Overview

This project implements a Spark Structured Streaming pipeline that simulates real-time vital sign monitoring for ICU patients. It detects sustained abnormal heart rates (not single spikes) by computing average heart rate per patient over tumbling 2-minute windows and raising a clinical alert when a patient's average HR exceeds 100 bpm.

## Dataset

- Source: IoMT Health Monitoring dataset (Kaggle)
- 40 simulated patients, 240 readings each at 30-second intervals
- Fields: timestamp, patient_id, heart_rate, spo2, systolic_bp, diastolic_bp, body_temp
- Synthetic anomaly injection: every 4th patient has an elevated HR (102–130 bpm) during a sustained interval to trigger alerts across consecutive windows

## Pipeline Architecture

1. Data generation – Real patient baseline vitals are sampled from the source dataset and used to generate a time-series stream (stream_df), written out as CSV chunks (chunks/) to simulate incoming sensor data.
2. Streaming ingestion – spark.readStream watches the stream_source/ directory for new CSV files using a fixed schema.
3. Windowed aggregation – withWatermark("timestamp", "1 minute") combined with groupBy(window(timestamp, "2 minutes"), patient_id) computes avg_hr per patient per tumbling 2-minute window.
4. Alert filter – Rows where avg_hr > 100 are filtered into the alerts stream.
5. Output sink – foreachBatch prints a formatted clinical alert table to the console for each micro-batch.
6. Stream simulation – CSV chunks are copied into stream_source/ one at a time with delays to simulate live data arrival.

## Why Tumbling Window?

A tumbling 2-minute window was chosen because the goal is to detect sustained elevated heart rate rather than momentary spikes. Tumbling (non-overlapping) windows naturally segment the stream into discrete 2-minute clinical observation periods, so a patient's average HR is independently evaluated for each period. When a patient's average exceeds 100 bpm in two consecutive windows, this indicates a sustained condition rather than noise, which is clinically more meaningful than reacting to a single high reading.

## Where the Pipeline Requires State

State is required in the windowed aggregation step. Spark must maintain partial aggregation state (running sum and count of heart rate readings) for each (patient_id, window) key across multiple micro-batches, since readings for a given 2-minute window arrive incrementally over several streaming batches. The withWatermark clause bounds this state by telling Spark how long to wait (1 minute) for late data before finalizing and evicting a window's state. Without it, state would grow unboundedly as the stream runs.

## How to Run

### Prerequisites

- Python 3.x
- PySpark (pip install pyspark)
- Java 8/11 (required by Spark)

### Steps

1. Clone this repository:
   git clone YOUR_REPO_URL
   cd YOUR_REPO_NAME

2. Install dependencies:
   pip install pyspark pandas openpyxl

3. Place the source dataset patients_data_with_alerts.xlsx in the project root.

4. Run the notebook:
   Open ICU_Streaming.ipynb in Jupyter or Google Colab and run all cells in order.

5. The pipeline will:
   - Generate synthetic streaming data from the source dataset
   - Start the Spark Structured Streaming query
   - Begin copying CSV chunks into stream_source/ to simulate live ingestion
   - Print clinical alerts to the console as windows close and thresholds are exceeded

6. The stream runs for approximately 60–90 seconds and then completes automatically.

## Sample Alert Output

======================================================================
ICU HEART RATE MONITORING — CLINICAL ALERTS  (Batch 1)
Scenario B: Tumbling 2-min Window | Threshold: avg HR > 100 bpm
======================================================================
patient_id  window_start          window_end            avg_hr    status
----------------------------------------------------------------------
14          2026-06-13 08:10:00   2026-06-13 08:12:00   147.0     CLINICAL ALERT
14          2026-06-13 08:12:00   2026-06-13 08:14:00   147.5     CLINICAL ALERT
======================================================================
Alerts in this batch: 474
======================================================================

(See screenshots/alert_output.png for full console screenshot.)

![Alert Output 1](screenshot-1.png)
![Alert Output 2](screenshot-2.png)

## Project Structure

.
├── ICU_Streaming.ipynb              # Main notebook with pipeline code
├── patients_data_with_alerts.xlsx   # Source dataset
├── screenshots/
│   └── alert_output.png             # Screenshot of alert output firing
└── README.md

## Technical Requirements Checklist

- readStream using a watched directory (stream_source/)
- Window aggregation with withWatermark (tumbling 2-min window, 1-min watermark)
- Alert condition as filtered output stream (avg_hr > 100)
- Console output via foreachBatch
