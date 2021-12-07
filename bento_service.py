import bentoml

from bentoml.frameworks.lightgbm import LightGBMModelArtifact
from bentoml.service.artifacts.common import PickleArtifact
from bentoml.adapters import DataframeInput

from input_df_config import COLUMNS_NAME, COLUMNS_DTYPES

import score_cliente_ami
from score_cliente_ami.predict import preprocess_and_predict


@bentoml.env(requirements_txt_file='requirements.txt')
@bentoml.artifacts([LightGBMModelArtifact('model'), PickleArtifact('preprocessing_obj')])
class LgbModelService(bentoml.BentoService):

    @bentoml.api(input=DataframeInput(type='frame',
                                      orient='records',
                                      columns=COLUMNS_NAME,
                                      dtype=COLUMNS_DTYPES), batch=True)
    def predict(self, df):
        features = self.artifacts.model.feature_name()
        df_transform = self.artifacts.preprocessing_obj.transform(df)
        df_final = df_transform[features]
        results = self.artifacts.model.predict(df_final)
        return list(results)
