# Electrical power plant performance prediction
This repository contains code and analysis for predicting the electrical power output of a power plant based on four main input parameters: ambient temperature, atmospheric pressure, relative humidity, and exhaust steam pressure. The project involves exploratory data analysis (EDA), regression modeling, and neural network implementation.

## Project overview
This project involves fetching taxi trip data and weather data from two separate web APIs, processing the data using pandas, and creating visualizations to identify patterns and insights. The primary goal is to understand how various factors, including weather, affect taxi trips in Chicago. The data collection and storage are managed using AWS Lambda functions and stored in an S3 bucket.
![image](https://github.com/user-attachments/assets/eab9d5ad-fa58-4e33-845f-083a0bdd6a7e)


## Dataset
The dataset used in this project contains records of the above-mentioned parameters and the corresponding electrical power output. There are two dataset: tran and test dataset.

## Exploratory Data Analysis (EDA)
Exploratory Data Analysis (EDA)
The EDA is performed using Python libraries like pandas, seaborn, and matplotlib. The key steps in the EDA include:
__Data Cleaning:__ Checking for missing values, handling outliers, and data type conversions.
__Descriptive Statistics:__ Summary statistics to understand the distribution of each variable.
__Visualization:__ Plotting relationships between the input parameters and the target variable using scatter plots, histograms, and heatmaps.
EDA helps in understanding the data, identifying trends, and preparing the data for modeling.

## Prediction models
### Regression models
Regression techniques are applied using the scikit-learn and statsmodels libraries. Various regression models are implemented to predict the power output, including:
__-Linear Regression__
__-Polynomial Regression__
__-Ridge Regression__
__-Lasso Regression__
__-kNN regression__
Model performance is evaluated using metrics such as Mean Squared Error (MSE) and R-squared (R²).

### Neural network model
A neural network model is implemented using the TensorFlow library. The neural network is designed with:
    -Input layer corresponding to the four input parameters.
    -Hidden layers with varying number of neurons and activation functions.
    -Output layer predicting the electrical power output.
The model is trained using backpropagation and optimized using an appropriate optimizer. The model's performance is evaluated on a test set using metrics like MSE and R².

## Conclusion
The results from both regression and neural network models are compared. Key findings include:
    -Which model performed the best in terms of prediction accuracy.
    -The effect of each input parameter on the electrical power output.
The comparison helps in understanding the trade-offs between simpler models and more complex neural network models.



    


    

