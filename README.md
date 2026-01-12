# Academia Spark Text Analytics

## Overview

This project implements large-scale text analytics using **Apache Spark**, providing two parallel implementations based on:

- **RDD (Resilient Distributed Dataset)**
- **Spark DataFrame API**

The application processes a news text dataset, groups records by year, performs tokenisation and stop-word filtering, and extracts **Top-K keywords per year**.  
The primary goal of this project is to compare the design, expressiveness, and usability differences between low-level RDD transformations and high-level DataFrame operations in Spark.

---

## Repository Structure

Academia-Spark-Text-Analytics/
├─ src/
│ ├─ project2_rdd.py # RDD-based implementation
│ └─ project2_df.py # DataFrame-based implementation
├─ data/
│ └─ sample_abcnews.txt # Small sample dataset for demonstration
├─ .gitignore
└─ README.md

---

## Dataset

The original dataset is a large news corpus and is **not included** in this repository due to size and licensing constraints.

A small sample file (`sample_abcnews.txt`) is provided to:
- Demonstrate the expected input format
- Allow the application to be executed locally
- Verify the correctness of the processing pipeline

---

## Requirements

- Python **3.9+**
- Apache Spark **3.x**
- Java **8 or later**

Python dependency:
pyspark

---

## How to Run

Ensure that Apache Spark is installed and that `spark-submit` is available in your system `PATH`.

### RDD Implementation

```bash
spark-submit src/project2_rdd.py data/sample_abcnews.txt

### DataFrame Implementation

spark-submit src/project2_df.py data/sample_abcnews.txt
