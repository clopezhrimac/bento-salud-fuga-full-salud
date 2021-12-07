from . import config
from .src import utils
from .src import transformers as T

from aiutils.constants import c
from aiutils import aiwr
from aiutils.aimisc import data_report

# import fire
import json
import os

import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import lightgbm as lgb

# predict_params = config.predict_pipeline
version = config.PACKAGE_VERSION

def get_source(period):
    period = utils.parse_period(period)
    query = utils.QUERY_GET_PREDICT_SOURCE_TEMPLATE.format(period=period)
    df_bq = aiwr.create_table_from_query(query)
    df = aiwr.read_dataset('bq://' + df_bq)
    return df, df_bq
    
def enrich(entity_source, feature_ref, online=False):
    df_enrich, df_enrich_bq = utils.enrich(entity_source, feature_ref, online)
    return df_enrich, df_enrich_bq
    
def preprocess_and_predict(df, artifact_path, targets,
                           df_report_path='docs/predict_report.html', df_transform_report_path='docs/predict_tranform_report.html'):
    for target in targets:
        path_pickle = '{}/{}/{}/preprocess.pkl'.format(artifact_path, target, version)
        path_model = '{}/{}/{}/model.pkl'.format(artifact_path, target, version)
        preprocess = pd.read_pickle(path_pickle)
        model = pd.read_pickle(path_model)
        df_transform = preprocess.transform(df)
        
        # Data Reports
        data_report(df, framework='sweetviz', filepath=df_report_path)
        data_report(df_transform, framework='sweetviz', filepath=df_transform_report_path)
        
        idx = (df[target] == 1)
        df.loc[idx, target] = model.predict(df_transform.loc[idx, model.feature_name()])
        df['model_version'] = version
    return df

def save_predictions(df, period, extra_columns, targets):
    entity = 'cliente_poliza'
    period = utils.parse_period(period)
    for target in targets:
        idx = df[target].notnull()
        df2 = df.loc[idx, extra_columns + [target]]
        df2['event_timestamp'] = df2['event_timestamp'].dt.date
        
        table_name = 'predictions.{}__{}'.format(entity, target)
        
        if aiwr.exist_bq_table(table_name):
            query = f"""
            DELETE FROM {table_name}
            WHERE event_timestamp = PARSE_DATE('%Y%m', '{period}');
            """
            job = c.bq_client.query(query)
            job.result()
        
        table_schema = [{'name': 'event_timestamp', 'type': 'DATE'}]
        table_saved = aiwr.save_dataset(df2, 'bq://' + table_name,
                                        if_exists='append', table_schema=table_schema)
        return table_saved

# def run(
#     version=predict_params['version'],
#     period=predict_params['period'],
#     feature_ref=predict_params['feature_ref'],
#     extra_columns=predict_params['extra_columns'],
#     artifact_path=predict_params['artifact_path'],
#     register=predict_params['register'],
# ):
#     pass
    
# def predict(df, artifact_path, target, version):
#     path_pickle = '{}{}/{}/model.pkl'.format(artifact_path, target, version)
#     preprocess = pd.read_pickle(path_pickle)
#     idx = df[target] == 1
    
    
#     pass
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    