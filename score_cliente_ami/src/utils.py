# from aiutils.constants import c
from aiutils import aiwr

# import pandas as pd

# from sklearn.pipeline import Pipeline
# from src import transformers as T


###########################################################################
#################################  Utils  #################################
###########################################################################

# def _create_raw_training_set(table_enrich, target, destination):

QUERY_TARGET_DATASET_TEMPLATE = """
SELECT
  CASE(ABS(MOD(FARM_FINGERPRINT(CONCAT(event_timestamp, cuc, id_poliza)), 10)))
    WHEN 9 THEN 'TEST'
    WHEN 8 THEN 'VALIDATE'
    ELSE 'TRAIN' END AS split,
  CAST({target} AS INTEGER) AS {target},
  * EXCEPT({targets})
FROM `{table_enrich}`
WHERE {target} IS NOT NULL
"""

QUERY_TRAIN_TARGET_ENGINEERING_TEMPLATE = """
WITH
tabla_base AS (
  SELECT DISTINCT
    periodo,
    cuc,
    pol_id_poliza AS id_poliza,
    FIRST_VALUE(pol_fec_inicio_vigencia)
      OVER (PARTITION BY cuc, pol_id_poliza ORDER BY periodo ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS pol_fec_inicio_vigencia,
    CASE
      WHEN cod_producto_origen IN ('4038', '4054', '4099') THEN 'PREFERENTE'
      WHEN cod_producto_origen IN ('4004', '4096', '4050', '4097', '4057', '4082', '4083', '4116') THEN 'REDMEDICA'
      WHEN cod_producto_origen IN ('4011', '4098', '4052') THEN 'FULLSALUD'
      WHEN cod_producto_origen IN ('4012', '4049') THEN 'PREFERENCIAL'
    END AS tipo_prod
  FROM `rs-nprd-dlk-ia-dev-poc-55a3.anl_persona.cliente_persona_detalle`
  WHERE cod_producto_origen IN (
      '4038', '4054', '4099', -- PREFERENTE
      '4004', '4096', '4050', '4097', '4057', '4082', '4083', '4116', -- REDMEDICA
      '4011', '4098', '4052', -- FULLSALUD
      '4012', '4049' -- PREFERENCIAL
      )
    AND asg_tipo_afiliado = 'T'
),
tabla_target0 AS (
  SELECT DISTINCT
    DATE_ADD(pol_fec_inicio_vigencia,
             INTERVAL div(date_diff(periodo, pol_fec_inicio_vigencia, MONTH), 12) + 1 YEAR) AS event_timestamp,
    DATE_ADD(
      DATE_ADD(pol_fec_inicio_vigencia,
               INTERVAL div(date_diff(periodo, pol_fec_inicio_vigencia, MONTH), 12) + 1 YEAR),
       INTERVAL 1 MONTH) AS next_period,
    cuc,
    id_poliza,
    tipo_prod
  FROM tabla_base
  where periodo >= pol_fec_inicio_vigencia
),
tabla_target1 AS (
  SELECT DISTINCT
    A.tipo_prod,
    A.cuc,
    A.id_poliza,
    A.event_timestamp,
    0 AS flag_fuga
  FROM tabla_target0 A
  JOIN tabla_base B ON A.tipo_prod = B.tipo_prod
                    AND A.cuc = B.cuc
                    AND A.id_poliza = B.id_poliza
                    AND A.next_period  = B.periodo
                    #BETWEEN mes_inicio_vigencia_afiliado AND mes_fin_vigencia_afiliado
  )

SELECT
  a.event_timestamp,
  a.cuc,
  a.id_poliza,
  CASE WHEN a.tipo_prod = 'PREFERENTE' THEN IFNULL(b.flag_fuga, 1) END AS per_fuga_amipreferente_cls,
  CASE WHEN a.tipo_prod = 'REDMEDICA' THEN IFNULL(b.flag_fuga, 1) END AS per_fuga_amiredmedica_cls,
  CASE WHEN a.tipo_prod = 'FULLSALUD' THEN IFNULL(b.flag_fuga, 1) END AS per_fuga_amifullsalud_cls,
  CASE WHEN a.tipo_prod = 'PREFERENCIAL' THEN IFNULL(b.flag_fuga, 1) END AS per_fuga_amipreferencial_cls
FROM tabla_target0 a
LEFT JOIN tabla_target1 b ON A.tipo_prod = b.tipo_prod
                        AND a.cuc = b.cuc
                        AND a.id_poliza = b.id_poliza
                        AND a.event_timestamp = b.event_timestamp
WHERE a.event_timestamp < PARSE_DATE('%Y%m', '{period}')
--   A.id_poliza = 'AX-5991308'
-- ORDER BY cuc, event_timestamp
"""

QUERY_GET_PREDICT_SOURCE_TEMPLATE = """
WITH
tabla_base AS (
  SELECT DISTINCT
    periodo,
    cuc,
    pol_id_poliza AS id_poliza,
    FIRST_VALUE(pol_fec_inicio_vigencia)
      OVER (PARTITION BY cuc, pol_id_poliza ORDER BY periodo ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS pol_fec_inicio_vigencia,
    CASE
      WHEN cod_producto_origen IN ('4038', '4054', '4099') THEN 'PREFERENTE'
      WHEN cod_producto_origen IN ('4004', '4096', '4050', '4097', '4057', '4082', '4083', '4116') THEN 'REDMEDICA'
      WHEN cod_producto_origen IN ('4011', '4098', '4052') THEN 'FULLSALUD'
      WHEN cod_producto_origen IN ('4012', '4049') THEN 'PREFERENCIAL'
    END AS tipo_prod
  FROM `rs-nprd-dlk-ia-dev-poc-55a3.anl_persona.cliente_persona_detalle`
  WHERE cod_producto_origen IN (
      '4038', '4054', '4099', -- PREFERENTE
      '4004', '4096', '4050', '4097', '4057', '4082', '4083', '4116', -- REDMEDICA
      '4011', '4098', '4052', -- FULLSALUD
      '4012', '4049' -- PREFERENCIAL
      )
    AND asg_tipo_afiliado = 'T'
),
tabla_target0 AS (
  SELECT DISTINCT
    DATE_ADD(pol_fec_inicio_vigencia,
             INTERVAL div(date_diff(periodo, pol_fec_inicio_vigencia, MONTH), 12) + 1 YEAR) AS event_timestamp,
    DATE_ADD(
      DATE_ADD(pol_fec_inicio_vigencia,
               INTERVAL div(date_diff(periodo, pol_fec_inicio_vigencia, MONTH), 12) + 1 YEAR),
       INTERVAL 1 MONTH) AS next_period,
    cuc,
    id_poliza,
    tipo_prod
  FROM tabla_base
  where periodo >= pol_fec_inicio_vigencia
),
tabla_target1 AS (
  SELECT DISTINCT
    A.tipo_prod,
    A.cuc,
    A.id_poliza,
    A.event_timestamp,
    0 AS flag_fuga
  FROM tabla_target0 A
  JOIN tabla_base B ON A.tipo_prod = B.tipo_prod
                    AND A.cuc = B.cuc
                    AND A.id_poliza = B.id_poliza
                    AND A.next_period  = B.periodo
                    #BETWEEN mes_inicio_vigencia_afiliado AND mes_fin_vigencia_afiliado
  )

SELECT
  DISTINCT
  PARSE_DATE('%Y%m', '{period}') AS event_timestamp,
  current_datetime() as upload_timestamp,
  a.cuc,
  a.id_poliza,
  CASE WHEN a.tipo_prod = 'PREFERENTE' THEN 1 END AS per_fuga_amipreferente_cls,
  CASE WHEN a.tipo_prod = 'REDMEDICA' THEN 1 END AS per_fuga_amiredmedica_cls,
  CASE WHEN a.tipo_prod = 'FULLSALUD' THEN 1 END AS per_fuga_amifullsalud_cls,
  CASE WHEN a.tipo_prod = 'PREFERENCIAL' THEN 1 END AS per_fuga_amipreferencial_cls
FROM tabla_target0 a
LEFT JOIN tabla_target1 b ON A.tipo_prod = b.tipo_prod
                        AND a.cuc = b.cuc
                        AND a.id_poliza = b.id_poliza
                        AND a.event_timestamp = b.event_timestamp
WHERE a.event_timestamp >= PARSE_DATE('%Y%m', '{period}')
--   A.id_poliza = 'AX-5991308'
-- ORDER BY cuc, event_timestamp
"""

EXCLUDE = [
    'split', 'event_timestamp', 'cuc', 'id_poliza',
    'per_fuga_amipreferente_cls', 'per_fuga_amiredmedica_cls',
    'per_fuga_amifullsalud_cls', 'per_fuga_amipreferencial_cls'
]

def enrich(entity_source, feature_ref, online=False):
    if isinstance(feature_ref, str):
        feature_ref = eval(feature_ref)
    if not online:
        df_enrich_bq = aiwr.get_historical_features(entity_source, feature_ref)
        df_enrich = aiwr.read_dataset('bq://' + df_enrich_bq)
    else:
        df_enrich_bq = None
        df_enrich = aiwr.get_online_features(entity_source, feature_ref)
    return df_enrich, df_enrich_bq

def parse_period(period=None):
    if period.__str__() in ['None', '']:
        from datetime import datetime
        period = datetime.today().strftime('%Y%m')
    return period
    
def get_feature_importance(model):
    # Works for LGBM models
    return dict(sorted(zip(model.feature_name(), model.feature_importance('gain')),
                       key=lambda item: item[1],
                       reverse=True
                      )
               )











#     query=QUERY_TEMPLATE.format(table_enrich=table_enrich, target=target)
#     df = aiwr.read_dataset(query)
    
# #     table_name = aiwr.create_table_from_query(
# #         query=QUERY_TEMPLATE.format(table_enrich=table_enrich, target=target),
# #         table_destination='{}.{}'.format(destination, target),
# #         #'training_set.target1',
# #         mode='TRUNCATE'
# #     )
#     return df


# ###########################################################################
# ################################  Pipeline  ###############################
# ###########################################################################

# def target_engineering():
#     query = """
#     SELECT
#       event_timestamp,
#       cuc,
#       id_poliza,
#       CASE WHEN RAND() < 0.4 THEN CAST(RAND() AS INT64) END AS target1,
#       CASE WHEN RAND() < 0.4 THEN CAST(RAND() AS INT64) END AS target2,
#       CASE WHEN RAND() < 0.4 THEN CAST(RAND() AS INT64) END AS target3,
#       CASE WHEN RAND() < 0.4 THEN CAST(RAND() AS INT64) END AS target4
#     FROM featurestore.cli_pol__siniestros
#     WHERE RAND() < 0.01
#     """
#     df_bq = aiwr.create_table_from_query(query)
#     df = aiwr.read_dataset('bq://' + df_bq)
#     return df, df_bq
# #     # Some preprocessing to DataFrame
# #     pass
# #     table_target_engineering = aiwr.save_dataset(df, 'bq')
# #     return table_target_engineering

# def enrich(entity_source, feature_ref, online=False):
#     if isinstance(feature_ref, str):
#         feature_ref = eval(feature_ref)
#     if not online:
#         df_enrich_bq = aiwr.get_historical_features(entity_source, feature_ref)
#         df_enrich = aiwr.read_dataset('bq://' + df_enrich_bq)
#     else:
#         df_enrich_bq = None
#         df_enrich = aiwr.get_online_features(entity_source, feature_ref)
#     return df_enrich, df_enrich_bq

# def create_target_dataset(df_enrich_bq, target):
#     query = QUERY_TEMPLATE.format(table_enrich=df_enrich_bq, target=target)
#     df_target = aiwr.read_dataset('bq://' + query)
    
# #     if register_dataset:
# #         dataset = c.vertex.TabularDataset.create(
# #             display_name=target,
# #             bq_source= 'bq://' + table_name
# #         )
# #         dataset.wait()
    
#     return df_target

# def fit_transform(df):
#     print('#'*50)
# #     df = aiwr.read_dataset('bq://' + dataset)
#     encoding_exclude = ['split', 'event_timestamp', 'cuc', 'id_poliza']
#     preprocess = Pipeline(
#         steps=[
#             ('dtypes', T.Dtypes()),
#             ('cleaning', T.Cleaning),
#             ('labelencoding', T.MultipleLabelEncoder(exclude=encoding_exclude))
#         ], verbose=True)
#     df_transform = preprocess.fit_transform(df)
    
#     return df_transform, preprocess

# #     #Save
# #     pd.to_pickle(preprocess, path_pickle)    
# #     table_save = aiwr.save_dataset(df_transform, 'bq://' +  table_save, if_exists='replace')
# #     return table_save

# def transform(df, target, path_pickle=None):
#     print('#'*50)
#     if path_pickle is None:
#         path_pickle = '{}/tmp/{}/preprocess.pkl'.format(c.GCS_ARTIFACT_STORE, target)
#     preprocess = pd.read_pickle(path_pickle)
    
# #     if online:
# #         pass
# #     else:
# #         pass
#     df_transform = preprocess.transform(df)
#     return df_transform
    
    

# # def create_dataset(table_save):
# #     pass

# def model_train():
#     pass

# def model_predict():
#     pass

# def pipeline_train():
#     pass

# def pipeline_predict():
#     pass

# def test_train_pipeline():
#     pass

