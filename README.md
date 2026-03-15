# NBA MVP Prediction Pipeline

End-to-end data engineering and machine learning pipeline that predicts the NBA MVP for the current season using historic and current player and team stats. 

The project ingests player and team stats, transforms and loads into **PostgreSQL**, transforming features to train a **LightGBM** model, and serves results via a **Streamlit** dashboard.

The pipeline is orchestrated using **Apache Airflow**, with containerised services using **Docker**.

