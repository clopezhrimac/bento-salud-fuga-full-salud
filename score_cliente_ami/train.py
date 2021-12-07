from . import config
from .src import utils
from .src import transformers as T

from aiutils.constants import c
from aiutils import aiwr
from aiutils.aimisc import data_report

import fire
import json
import os

import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, roc_curve, confusion_matrix
import lightgbm as lgb

# train_params = config.train_pipeline
version = config.PACKAGE_VERSION

def target_engineering(period):
    query = utils.QUERY_TRAIN_TARGET_ENGINEERING_TEMPLATE.format(period=period)
    df_bq = aiwr.create_table_from_query(query)
    df = aiwr.read_dataset('bq://' + df_bq)
    return df, df_bq

def enrich(entity_source, feature_ref):
    df_enrich, df_enrich_bq = utils.enrich(entity_source, feature_ref)
    return df_enrich, df_enrich_bq

def create_target_dataset(df_enrich_bq, target, targets):
    targets = ', '.join(targets)
    query = utils.QUERY_TARGET_DATASET_TEMPLATE.format(table_enrich=df_enrich_bq,
                                                       target=target,
                                                       targets=targets)
    df_target = aiwr.read_dataset('bq://' + query)    
    return df_target

def preprocess(df):
    print('#'*50)
#     df = aiwr.read_dataset('bq://' + dataset)
    preprocess = Pipeline(
        steps=[
            ('dtypes', T.Dtypes(utils.EXCLUDE)),
            ('cleaning', T.Cleaning),
            ('category_type', T.ObjectToCategory(utils.EXCLUDE)),
#             ('labelencoding', T.MultipleLabelEncoder(exclude=encoding_exclude))
        ], verbose=True)
    df_transform = preprocess.fit_transform(df)    
    return df_transform, preprocess

def train(df, target, params):
    cutoff = 0.5
    features = [col for col in df.columns if col not in utils.EXCLUDE + [target]]
    X = df[features]
    y = df[target]

    idx_train = df['split'] == 'TRAIN'
    idx_test = df['split'] == 'TEST'
    idx_valid = df['split'] == 'VALIDATE'

    X_train, X_test, X_valid = X[idx_train], X[idx_test], X[idx_valid]
    y_train, y_test, y_valid = y[idx_train], y[idx_test], y[idx_valid]

    train_set = lgb.Dataset(X_train, y_train)
    validation_sets = lgb.Dataset(X_valid, y_valid, reference=train_set)

    model = lgb.train(    
        params,
        train_set,
        valid_sets=validation_sets,
        num_boost_round=100,
        verbose_eval=50)

    y_predict = model.predict(X_test)
    auc_test = roc_auc_score(y_test, y_predict)
    print('AUC Test:', auc_test)
    
    metrics = {
        'auc': auc_test,
    }
    metadata = {
        'target': target,
        'framework': 'lgbm',
        'cutoff': cutoff,
        'feature_importance': utils.get_feature_importance(model).__str__()
    }
    data_metrics = {
        'y_test': y_test,
        'y_predict': y_predict,
        'y_predict_label': (y_predict > cutoff) * 1,
    }
    return model, metrics, metadata, data_metrics

def register_report(df, target, filename, to_gcs=True):
    # TODO !!!
    df['event_timestamp'] = df['event_timestamp'].dt.strftime('%Y-%m-%d')
    df_train = df[df['split'] == 'TRAIN']
    df_test = df[df['split'] == 'TEST']
    data_report(
        df_train,
        framework='sweetviz',
        target_feat=target,
        df_compare=df_test,
        filepath=filename
    )
    if to_gcs:
        artifact_path = '{}/{}/{}/'.format(config.get_artifacts_uri(), target, version)
        artifact_path_file = '{}{}'.format(artifact_path, filename)
        os.system(f'gsutil cp "{filename}" "{artifact_path}"')
        return artifact_path_file
    else:
        return filename

def register_dataset(df_train, target):
    print(type(df_train), target, c.BQ_TRAINING_SET)
    table_name = '{}.{}'.format(c.BQ_TRAINING_SET, target)
    aiwr.save_dataset(df_train, 'bq://' + table_name, if_exists='replace')
    
    dataset = c.vertex_client.TabularDataset.create(
        display_name=target,
        bq_source="bq://"+table_name,
    )
    dataset.wait()

    print(f'\tDataset: "{dataset.display_name}"')
    print(f'\tname: "{dataset.resource_name}"')
    return table_name, dataset.resource_name

def register_model(model, preprocess, target):
#     artifact_path = '{}/MODELS/{}/{}/'.format(c.GS_ARTIFACT_STORE, target, version)
    model_path = '{}/{}/{}/'.format(config.get_models_uri(), target, version)
    pd.to_pickle(model, model_path + 'model.pkl')
    pd.to_pickle(preprocess, model_path + 'preprocess.pkl')
    return model_path

# def run(
#     version=train_params['version'],
#     period=train_params['period'],
#     feature_ref=train_params['feature_ref'],
#     model_params=train_params['model_params'],
#     register=train_params['register']
#     ):
    
#     df, df_bq = target_engineering(period)
    
#     df_enrich, df_enrich_bq = enrich(
#         entity_source=df_bq,
#         feature_ref=feature_ref
#     )
    
#     TARGETS = [
#         'per_fuga_amipreferente_cls', 'per_fuga_amiredmedica_cls', 'per_fuga_amifullsalud_cls',
#         'per_fuga_amipreferencial_cls'
#     ]
#     for target in TARGETS:
#         df_target = create_target_dataset(
#             df_enrich_bq,
#             target,
#             TARGETS
#         )
#         df_transform, preprocess_obj = preprocess(
#             df_target
#         )
#         model, metrics = train(
#             df_target,
#             target,
#             model_params
#         )

if __name__ == '__main__':
    fire.Fire()

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    