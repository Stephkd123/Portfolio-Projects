🧹 <h2> Housing Price Prediction Using XGBoost and Spatial Feature Engineering </h2>

Overview

This notebook <b> -House_std.ipynb </b>. I Developed a machine learning pipeline for residential housing price prediction using XGBoost, spatial feature engineering, and structured preprocessing techniques. The project includes exploratory data analysis, missing value handling, feature engineering, model evaluation, and production-oriented regression workflows for real estate pricing.

🔍 Key Steps Performed
- Spatial feature engineering using latitude and longitude
- Price prediction using XGBoost Regressor
- Feature interaction engineering
- Missing data analysis and cleaning
- Model evaluation using R², MAE, and MSE
- Leakage-aware ML pipeline design
- Scalable preprocessing workflows
- Outlier Detection & Handling
  
Data Standardization
- Replaced inconsistent null types (NaN, None) with standardized values.
- Ensured uniform encoding across related features to support further modeling or visualization tasks.
- Feature Relationship Analysis
- Explored relationships between Beds, Baths, and HouseArea to drive better imputation logic.
- Visualized patterns and counts of imputed vs. original values.

📈 Outcome
<h3> Strong Technical Findings: </h3>
- Spatial features contributed significantly to pricing behavior
- Engineered ratio-based features improved predictive stability
- XGBoost handled nonlinear housing relationships effectively
- Log-transformed target improved model learning behavior

<h3>Modeling Findings: </h3>
- Initial extremely high R² scores suggested potential leakage or overfitting risks
- Refactored pipeline produced more realistic model behavior
- Final evaluation metrics indicated a stable baseline real estate prediction model. 

<h3>Business Insight: </h3>
The project demonstrates how machine learning can be used to:
- Estimate residential property values
- Identify pricing patterns
- Support valuation workflows
- Improve market analysis efficiency

📁 Repository Structure


├── house_std.ipynb — Jupyter notebook with full data wrangling workflow
├── README.md — Project overview and documentation
    data - sourced from https://www.zillow.com/homedetails/



💡 Technical Skills Demonstrated
- Python
- Pandas
- NumPy
- Scikit-learn
- XGBoost
- Feature Engineering
- Regression Modeling
- Spatial Data Modeling
- Data Visualization
- Model Evaluation
- ML Pipeline Design

📬 Contact
If you'd like to collaborate or learn more about my work in data science and AI engineering, feel free to reach out via LinkedIn or my portfolio site.
