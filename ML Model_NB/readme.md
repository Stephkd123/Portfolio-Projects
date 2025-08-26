# **Machine Learning models**

This folder contains the different machine learning models I have worked on, also updating this folder often with 
practical models I continously use to improve my knowledge on data science. The notebooks in this folder cover
industry stabdard data ETL processes, ensuring data quality for Knowledge discovery and modeling

## current uploaded notebooks
- Decision Tree model on Titanic data
- Regression Tree model on Patient Heart data
- Clustering analysis using pyclustering & Kmeans</b> on business data - representing company financials, employment, equity, assets, and sales.

### Clustering Analysis
This project explored clustering on imputed financial and business metrics, comparing different distance metrics and cluster counts to derive meaningful groupings. After several data preprocessing and improvement activities (feature imputation, normalization, and categorical handling), we evaluated clustering outcomes using K-Means.

🔑 Key Steps

#### Feature Engineering & Imputation
Missing financial attributes (e.g., PROFIT, SALES, EQUITY, ASSETS) were imputed using decision tree strategies to preserve distribution and variance.

#### Clustering with Different Distances
Chi-Square (5 clusters) → Captures categorical/normalized relationships effectively.
Manhattan (7 clusters) → Adds robustness for high-dimensional sparse features.

#### Visualization & Outlier Analysis
Cluster assignments were validated using Chi-Square significance plots and pie chart distributions.
Outlier detection ensured clusters represented true segmentations rather than noise.

#### Derivatives & Insights

Cluster Profiles → Each group aligns with distinct financial health indicators (e.g., profitability vs. asset-heavy companies).
Feature Importance → Key variables such as NETPROF, EQUITY, and SALES drove the clustering separation.<br>
Business Use-Case → Enables targeted customer strategies, risk modeling, and resource allocation.
