🧹 Real Estate Data Cleaning & Wrangling Project
Overview

This notebook demonstrates end-to-end data cleaning and preprocessing steps applied to a real estate dataset. The project highlights my ability to identify, diagnose, and handle data quality issues—including missing values, inconsistent formats, and outliers—preparing the dataset for further analysis or modeling. The process reflects practical, real-world data handling and follows current industry practices for effective data wrangling.

📌 Project Objective

To clean, standardize, and impute missing data from a real estate dataset with nearly 10,000 records, ensuring the data is accurate, complete, and usable for downstream analysis or predictive modeling.

🛠️ Tools & Libraries Used
Python
Pandas
NumPy
Matplotlib & Seaborn
Scikit-learn

🔍 Key Steps Performed
- Data Loading & Initial Inspection
- Loaded dataset and explored data types, missing value distributions, and column summaries.
- Identified key features with missing values and assessed their significance and impact.
- Missing Value Handling
- Dropped records with null values in AddressStreet, AddressCity, and AddressState due to <1% missing rate.
- Applied median imputation for Beds, Baths, and HouseArea based on distribution analysis and outlier filtering.
- Imputed "Unknown" or "Undisclosed" for categorical fields such as Lat/Long, PGAPt, Zestimate, BadgeInfo, and more.
- Outlier Detection & Handling
- Visualized distribution of Beds, Baths, and HouseArea to detect skewness and extreme values.
- Filtered extreme outliers and preserved the realistic range of housing attributes.
- Used logical ratios (e.g., Bath-to-Bedroom ratio) to guide accurate imputations.
  
Data Standardization
- Replaced inconsistent null types (NaN, None) with standardized values.
- Ensured uniform encoding across related features to support further modeling or visualization tasks.
- Feature Relationship Analysis
- Explored relationships between Beds, Baths, and HouseArea to drive better imputation logic.
- Visualized patterns and counts of imputed vs. original values.

📈 Outcome
The dataset was transformed into a well-structured and clean format, with:
Over 95% reduction in missing critical fields.
Accurate imputation based on domain logic and distribution analysis.
Clear documentation of every decision and process.

📁 Repository Structure

.
├── .ipynb — Jupyter notebook with full data wrangling workflow
├── README.md — Project overview and documentation
    data - sourced from https://www.zillow.com/homedetails/



💡 Skills Demonstrated
Practical data wrangling & preprocessing
Exploratory Data Analysis (EDA)
Handling missing data and outliers
Feature imputation logic
Visual data validation

📬 Contact
If you'd like to collaborate or learn more about my work in data science and AI engineering, feel free to reach out via LinkedIn or my portfolio site.
