## Repository Structure

```bash
Academia-Spark-Text-Analytics/
├─ src/
│  ├─ project2_rdd.py        # RDD-based implementation
│  └─ project2_df.py         # DataFrame-based implementation
├─ data/
│  └─ sample_abcnews.txt     # Small sample dataset for demonstration
├─ .gitignore
└─ README.md
```

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

```text
pyspark
```

---

## How to Run

Ensure that Apache Spark is installed and that `spark-submit` is available in your system `PATH`.

### RDD Implementation

```bash
spark-submit src/project2_rdd.py data/sample_abcnews.txt
```

### DataFrame Implementation

```bash
spark-submit src/project2_df.py data/sample_abcnews.txt
```
