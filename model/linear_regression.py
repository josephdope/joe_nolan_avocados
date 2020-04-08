import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from sklearn import linear_model
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

df = pd.read_csv('../data/avocado_modeling_file.csv')
df = df.drop('Date', axis = 1).select_dtypes('number')
target = 'AveragePrice'
X = df.drop(target, inplace = False, axis = 1)
y = df[target]



X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2,
                                    random_state = 12)

regressor = LinearRegression()
regressor.fit(X_train, y_train)

y_pred = regressor.predict(X_test)

