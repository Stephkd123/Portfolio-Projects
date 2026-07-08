# Sentiment analysis on 515K Hotel Reviews data in Europe
Data was scrapped from Booking.com, a public source and CC0 licenses included.
Exploring the dataset in detail and then use NLP techniques to gain new insights about hotels. 

## Dataset
- **Source**: [FIFA players dataset](https://www.kaggle.com/datasets/jiashenliu/515k-hotel-reviews-data-in-europe?resource=download) (`Hotel_Review.csv`)
- **515,738 Hotel_Reviews × 17 columns**: Hotel_Address, Additional_Number_of_Scoring, Review_Date, Average_Score, Hotel_Name, Reviewer_Nationality, Negative_Review,Review_Total_Negative_Word_Counts, Total_Number_of_Reviews, Positive_Review, Review_Total_Positive_Word_Counts, Total_Number_of_Reviews_Reviewer_Has_Given, Reviewer_Score, Tags, days_since_review, lat and lng. 

## Project Structure
```
FIFA Player Statistics Analysis/
├── hotel_revs_analysis.ipynb        # Overview: structure, missing, age/overall/value distributions
├── sentiment_analysis.ipynb   # Best players, age & rating dists, nationalities, value drivers
├── sent_model.ipynb   # Best players, age & rating dists, nationalities, value drivers
├── requirements.txt · README.md
└── (Data) Hotel_Reviews.csv . Hotel_Summary.csv . Hotel_Reviews_Filtered.csv . Hotel_Reviews_Final.csv . Hotel_Reviews_NLP2.csv
```
## Key Findings:

Some columns are filled with useless information, others seem incorrect. If they are correct, it's unclear how they were calculated, and answers cannot be independently verified solely my calculations.

All figures produced by executing the notebooks — not assumed.
- **515,738 Hotel_Reviews, 227 reviewer Nationalities,  UK with the hgihest reviewer Nationality** — USA, Australia and Ireland make up the 2nd, 3rd and 4th most reviewer nationalities.
- **Top-Reviewed Hotels by country: UK - Britannia International Hotel Canary Wharf (3833 revs), USA - Hotel Esther (423 revs), Ireland - Copthorne Tara Hotel London Kensington (239 revs).**
- **Barcelona, Spain | London, United Kingdom | Milan, Italy | Paris, France | Vienna, Austria | Amsterdam, Netherlands** After exploration it was found only hotels in these cities and countries were in the data set.
- **55242 unique Tags for customer hotel stay information** but after further analysis, identified the most important and frequent tags, applicable across all hotel types, as the following: **Leisure trip, Couple, Solo traveler, Business trip, Group combined with Travelers with friends, Family with young children, Family with older children, With a pet**
- After Final Data Processing and Feature Engineering - **NLTK sentiment analyzer** used for Reviews sentiment scoring.

## Tech Stack
- pandas, numpy, matplotlib, seaborn, Natural Language ToolKit (NLTK), SKlearn.

- Check CSV files file path when getting started. 


