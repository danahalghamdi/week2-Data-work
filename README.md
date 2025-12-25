## Exploratory Data Analysis (EDA)

### Overview
This repository contains an exploratory data analysis (EDA) notebook built on the **processed analytics dataset**.

The notebook loads the processed data, performs a quick audit, answers analytical questions, and exports figures to disk.

All analysis is reproducible by running the notebook from top to bottom.
---

## How to run the analysis

### 1. Setup environment
### Setup environment
Project dependencies are defined in `pyproject.toml`.
To install all required dependencies, run the following command from the project root:

```bash
pip install .
```
## 2. Open the EDA notebook
### Run the notebook

From the project root, run:

```bash
jupyter lab notebooks/eda.ipynb

```
## 3. Run the notebook
Run the notebook from top to bottom.
The notebook is organized into the following sections:

1. Setup & Imports
	-	Imports required libraries
	-	Defines file paths
	-	Creates the figures output directory
	-   Defines a helper function to save Plotly figures

2. Load Processed Data
	-	Loads the processed dataset from: `data/processed/analytics_table.parquet`
    -	Prints:
	    Number of rows and columns
	    Column data types
	    Missing value counts

3. Quick Audit
	-	Displays dataset structure using df.info()
	-   Shows summary statistics using df.describe()
	-	Confirms data readiness for analysi

4. Questions & Results
Question 1: Revenue by country
	-	Aggregates:
	-	Number of orders (n)
	-	Total revenue
	-	Average order value (AOV)
	-	Creates a sorted bar chart
	-	Exports figure: reports/figures/revenue_by_country.png
Question 2: Revenue trend (monthly)
	-	Aggregates:
	-   Monthly order count
	-	Monthly total revenue
	-	Creates a line chart ordered by month
	-	Exports figure: reports/figures/revenue_trend_monthly.png
Question 3: Order amount distribution (winsorized)
	-	Visualizes the distribution of amount_winsor
	-	Uses a histogram
	-   Exports figure: reports/figures/amount_hist_winsor.png


5. Bootstrap Comparison
	-   Compared refund rates between SA and AE using bootstrap.
    -   The estimated difference was small, with a 95% confidence interval including zero.
    -   No statistically significant difference was observed.


## 4. Output
All figures generated in the notebook are automatically saved to: `reports/figures/`
Expected files include:
	-	revenue_by_country.png
	-	revenue_trend_monthly.png
	-	amount_hist_winsor.png


-----
Notes
	-	The notebook is designed to run without manual edits.
	-	All analysis uses the processed analytics dataset.
	-	Results and figures are reproducible by rerunning the notebook.
