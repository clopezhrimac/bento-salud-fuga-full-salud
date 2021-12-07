import os
from .__metadata__ import (
    __URL__, __DESCRIPTION__, __BUSINESS__, __MLE__, __MLE_EMAIL__, __DS__, __DS_EMAIL__
)
__INFERENCE_TYPE__ = 'batch' # batch/online

################################ PACKAGE ################################
PACKAGE_NAME = 'score_cliente_ami'
PACKAGE_VERSION = '0.1.0'

PACKAGE_FILENAME = f'{PACKAGE_NAME}-{PACKAGE_VERSION}.tar.gz'

def get_package_uri(dir_path=False):
    GS_ARTIFACT_STORE = os.environ['GS_ARTIFACT_STORE']
    if dir_path:
        return '{}/PACKAGES/{}'.format(GS_ARTIFACT_STORE, PACKAGE_NAME)
    else:
        return '{}/PACKAGES/{}/{}'.format(GS_ARTIFACT_STORE, PACKAGE_NAME, PACKAGE_FILENAME)
        
def get_pipeline_uri():
    GS_ARTIFACT_STORE = os.environ['GS_ARTIFACT_STORE']
    return '{}/PIPELINES/{}/{}'.format(GS_ARTIFACT_STORE, PACKAGE_NAME, PACKAGE_VERSION)

def get_models_uri():
    GS_ARTIFACT_STORE = os.environ['GS_ARTIFACT_STORE']
    return '{}/MODELS'.format(GS_ARTIFACT_STORE)

def get_artifacts_uri():
    GS_ARTIFACT_STORE = os.environ['GS_ARTIFACT_STORE']
    return '{}/ARTIFACTS'.format(GS_ARTIFACT_STORE)

############################### ENVIORMENT ##############################
_envs = [
#     'ENV',
    'GOOGLE_CLOUD_PROJECT',
    'LOCATION',
#     'GS_ADS',
#     'GS_EDS',
    'GS_ARTIFACT_STORE',
#     'GS_LOG',
#     'SERVICE_ACCOUNT',
    'SECRET_GH_TOKEN',
#     'GH_TOKEN',
    'BQ_MODEL_GOVERNANCE',
    'BQ_FEATURE_STORE'
]

############################# SHARED PARAMS #############################
_features = [
    "cli_pol:benef_ambulatorio",
    "cli_pol:cie10_cap_cap21_servicios_salud",
    "cli_pol:flag_cuidate",
    "cli_pol:flag_maternidad",
    "cli_pol:flag_mesvig_ocur_1",
    "cli_pol:flag_mesvig_ocur_2",
    "cli_pol:flag_mesvig_ocur_3",
    "cli_pol:grupo_benef_ambulatorio",
    "cli_pol:mesvig_ocur_0",
    "cli_pol:mesvig_ocur_1",
    "cli_pol:mesvig_ocur_2",
    "cli_pol:mesvig_ocur_7",
    "cli_pol:mesvig_ocur_8",
    "cli_pol:monto_usd_beneficio",
    "cli_pol:monto_usd_beneficio_sin_igv",
    "cli_pol:monto_usd_coaseguro",
    "cli_pol:monto_usd_deducible",
    "cli_pol:monto_usd_saldo_acargo_afiliado",
    "cli_pol:monto_usd_total_gastos_presentados",
    "cli_pol:mto_bsigv_benef_ambulatorio",
    "cli_pol:mto_bsigv_cie10_cap_cap18_sintomas_anormales",
    "cli_pol:mto_bsigv_cie10_cap_cap19_traumatismos_envenenam",
    "cli_pol:mto_bsigv_cie10_cap_otros",
    "cli_pol:mto_bsigv_especilidad_ad_traumatologia",
    "cli_pol:mto_bsigv_flag_cond_riesgo_covid",
    "cli_pol:mto_bsigv_flag_cuidate",
    "cli_pol:mto_bsigv_flag_enf_recurrentes",
    "cli_pol:mto_bsigv_flag_maternidad",
    "cli_pol:mto_bsigv_flag_prevencion",
    "cli_pol:mto_bsigv_grupo_benef_ambulatorio",
    "cli_pol:mto_bsigv_grupo_benef_extra_hospitalaria",
    "cli_pol:mto_bsigv_grupo_benef_hospitalaria",
    "cli_pol:mto_bsigv_grupo_benef_otros",
    "cli_pol:mto_bsigv_mesvig_ocur_0",
    "cli_pol:mto_bsigv_mesvig_ocur_1",
    "cli_pol:mto_bsigv_mesvig_ocur_2",
    "cli_pol:mto_bsigv_mesvig_ocur_4",
    "cli_pol:mto_bsigv_mesvig_ocur_5",
    "cli_pol:mto_bsigv_mesvig_ocur_8",
    "cli_pol:mto_bsigv_prov_otros",
    "cli_pol:mto_bsigv_riesgo_leve",
    "cli_pol:mto_bsigv_riesgo_severo",
    "cli_pol:mto_bsigv_siniestro_diasem_0",
    "cli_pol:mto_bsigv_siniestro_diasem_1",
    "cli_pol:mto_bsigv_siniestro_diasem_2",
    "cli_pol:mto_bsigv_siniestro_diasem_3",
    "cli_pol:mto_bsigv_siniestro_diasem_4",
    "cli_pol:mto_bsigv_siniestro_diasem_5",
    "cli_pol:mto_bsigv_siniestro_mesyear_1",
    "cli_pol:mto_bsigv_siniestro_mesyear_10",
    "cli_pol:mto_bsigv_siniestro_mesyear_2",
    "cli_pol:mto_bsigv_siniestro_mesyear_3",
    "cli_pol:mto_bsigv_siniestro_mesyear_4",
    "cli_pol:mto_bsigv_siniestro_mesyear_5",
    "cli_pol:mto_bsigv_siniestro_mesyear_6",
    "cli_pol:mto_bsigv_siniestro_mesyear_8",
    "cli_pol:mto_bsigv_siniestro_mesyear_9",
    "cli_pol:mto_bsigv_tipo_proveedor_clinicas_hosp_consultorio",
    "cli_pol:mto_bsigv_tipo_proveedor_farmacias",
    "cli_pol:mto_bsigv_tipo_proveedor_operador_extranjero",
    "cli_pol:mto_bsigv_tipo_proveedor_otros",
    "cli_pol:tipo_proveedor_clinicas_hosp_consultorio",
    "persona:ingreso_prom",
    "persona:linea_tcmax",
    "persona:linea_tcmax_12",
    "persona:linea_tcmax_24",
    "persona:num_tc_sbs_6",
    "persona:saldo_pp_sbs",
    "persona:saldo_tc_sbs_12",
    "persona:saldo_tc_sbs_6",
    "persona:cono_agrup_nuevo",
    "persona:distrito",
    "persona:nse_ubigeo",
    "persona:ubigeo",
    "persona:estado_civil",
    "persona:grado_instruccion",
    "persona:sexo"
  ]

############################### PIPELINES ###############################
train_pipeline = {
    "period": "202105",
    "feature_ref": _features,
    "model_params": {
        "task": "train",
        "boosting_type": "gbdt",
        "objective": "binary",
        "metric": "auc",
        "learning_rate": 0.05,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.7,
        "bagging_freq": 10,
        "verbose": -1,
        "max_depth": 4,
        "num_leaves": 128,
        "max_bin": 512
    },
    "register": True
}

predict_pipeline = {
    "period": None,
    "feature_ref": _features,
    "extra_columns": [
        "event_timestamp",
        "upload_timestamp",
        "cuc",
        "id_poliza",
        "model_version"
    ],
    "artifact_path": None, # get_models_uri(),
    "register": True
}

# ######################### Init Code Pipelines ###########################
# extra_code = f"""
# gsutil cp "gs://rs-nprd-dlk-ia-dev-poc-55a3-artfsto/AIUTILS/ml-aiutils-release-2.0.zip" .;
# unzip "ml-aiutils-release-2.0.zip";
# pip install "ml-aiutils-release-2.0/";

# import os
# c.init(config.enviorment[env], config.PACKAGE_NAME, config.PACKAGE_VERSION)
# os.system('gsutil cp "{c.PACKAGE_URI}" .; pip install {c.PACKAGE_FILENAME};')
# """
