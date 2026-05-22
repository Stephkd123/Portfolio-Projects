<h2> Housing Price Prediction Using XGBoost and Spatial Feature Engineering </h2>

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
* Replaced inconsistent null types (NaN, None) with standardized values.
* Ensured uniform encoding across related features to support further modeling or visualization tasks.
* Feature Relationship Analysis
* Explored relationships between Beds, Baths, and HouseArea to drive better imputation logic.
* Visualized patterns and counts of imputed vs. original values.

📈 Outcome
<h3> Strong Technical Findings: </h3>
* Spatial features contributed significantly to pricing behavior.
* Engineered ratio-based features improved predictive stability.
* XGBoost handled nonlinear housing relationships effectively.
* Log-transformed target improved model learning behavior.

<h3>Modeling Findings: </h3>
* Initial extremely high R² scores suggested potential leakage or overfitting risks.
* Refactored pipeline produced more realistic model behavior.
* Final evaluation metrics indicated a stable baseline real estate prediction model. 

<h3>Business Insight: </h3>
The project demonstrates how machine learning can be used to:
* Estimate residential property values.
* Identify pricing patterns.
* Support valuation workflows.
* Improve market analysis efficiency.

📁 Repository Structure
* house_std.ipynb — Jupyter notebook with full data wrangling workflow
* README.md — Project overview and documentation. <br>
*** data - sourced from https://www.zillow.com/homedetails/

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

💡 Next steps for the project
- Improve Location Intelligence
Add:
ZIP code clustering
neighborhood segmentation
school district data
distance-to-city-center features
- Add Market Context Features
Include:
nearby average sale price
local market trends
property age
renovation status
- Improve Model Validation
Implement:
K-Fold Cross Validation
SHAP interpretability
feature importance ranking
residual error analysis

<h2> Music Recommendation and Acoustic Similarity Clustering Using Unsupervised Machine Learning </h2>

Overview
Developed an unsupervised machine learning workflow for music clustering and acoustic similarity analysis using audio features and exploratory analysis techniques. The project investigates how songs can be grouped based on musical characteristics to support recommendation system design and playlist intelligence.

🔍 Key Steps Performed
- Music feature preprocessing
- Audio feature engineering
- Unsupervised clustering workflows
- Cluster validation using silhouette score
- Exploratory music analytics
- Dimensionality reduction and similarity analysis
- Recommendation-oriented ML thinking
  
Work Flow included:
* Audio feature preprocessing.
* Exploratory analysis of music attributes.
* Handling missing and low-quality records.
* Clustering model experimentation.
* Cluster quality evaluation using silhouette scoring.
* Recommendation-oriented similarity analysis.

📈 Outcome
<h3> Strong Technical Findings: </h3>
* Spatial features contributed significantly to pricing behavior.
* Engineered ratio-based features improved predictive stability.
* XGBoost handled nonlinear housing relationships effectively.
* Log-transformed target improved model learning behavior.

<h3>Modeling Findings: </h3>
* Audio-based clustering produced overlapping music groups.
* Low silhouette score indicated weak natural separation between songs.
* Results reflected realistic modern music overlap across genres and production styles. 

<h3>Recommendation-System Insight: </h3>
The project demonstrated that:
* Music similarity is highly nonlinear.
* Audio features alone may not fully separate genres.
* Clustering can still support recommendation systems as a feature layer.

<h3>Industry-Relevant Insight: </h3>
The project demonstrated that:
* Recommendation systems rely on multiple similarity signals.
* Clustering is one component of recommendation architecture.
* Embeddings and listener behavior would improve future performance.

📁 Repository Structure
* house_std.ipynb — Jupyter notebook with full data wrangling workflow
* README.md — Project overview and documentation. <br>
*** data - sourced from https://www.zillow.com/homedetails/


💡 Technical Skills Demonstrated
- Python
- Pandas
- NumPy
- Scikit-learn
- Clustering Algorithms
- Unsupervised Learning
- Audio Feature Analysis
- Recommendation System Thinking
- Data Cleaning
- Exploratory Data Analysis
- Cluster Evaluation

💡 Next steps for the project
- Improve Feature Representation
Add:
Spotify audio embeddings
Genre encoding
Artist-level similarity features
Release year trends.
- Recommendation System Layer
Build:
nearest-neighbor recommendation engine
playlist continuation model
similarity ranking pipeline

📬 Contact
If you'd like to collaborate or learn more about my work in data science and AI engineering, feel free to reach out via LinkedIn or my portfolio site.
