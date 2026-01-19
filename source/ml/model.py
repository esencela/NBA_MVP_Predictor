import lightgbm as lgb
import pickle

class LGBMModel():

    def __init__(self):
        self.classifier = lgb.LGBMClassifier(
            objective='binary',
            boosting_type='gbdt',
            random_state=42,
            num_leaves=50,
            max_depth=10,
            learning_rate=0.1,
            n_estimators=600,
            subsample=1,
            colsample_bytree=0.95
        )

        self.regressor = lgb.LGBMRegressor(
            objective="regression",
            random_state=42,
            num_leaves=40,
            max_depth=-1,
            learning_rate=0.01,
            n_estimators=500,
            subsample=0.7,
            colsample_bytree=0.7
        )

    
    def fit(self, X_class, y_class, X_regr, y_regr):
        self.classifier.fit(X_class, y_class)
        self.regressor.fit(X_regr, y_regr)

    
    def predict(self, X):
        yhat_class = self.classifier.predict(X)
        yhat = self.regressor * yhat_class

        return yhat
    

    def save(self, path: str):
        with open(path, 'wb') as file:
            pickle.dump(self, file)

    
    @staticmethod
    def load(path: str):
        with open(path, 'rb') as file:
            model = pickle.load(file)
        
        return model