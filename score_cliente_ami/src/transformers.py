from sklearn.base import BaseEstimator, TransformerMixin

from sklearn.preprocessing import FunctionTransformer
# from sklearn.preprocessing import LabelEncoder

from aiutils import aiwr
from aiutils.transformers import MultipleLabelEncoder


##########################################################
##################### PreProcessors ######################
##########################################################

def _cleaning(X):
    X = X.copy()
    X['distrito'].fillna('LIMA', inplace=True)
    X['distrito'] = aiwr.clean_characters(X['distrito'])

    fill = {
        'riesgo_leve_sum': 0,
        'riesgo_moderado_sum': 0,
        'riesgo_severo_sum': 0,
        'siniestros_pasados_max': 0,
        'siniestros_periodo_max': 0
    }
    for col in X.columns:
        if col.startswith('monto'):
            fill[col] = 0
    X.fillna(value=fill, inplace=True)
    return X


Cleaning = FunctionTransformer(_cleaning)


class Dtypes(BaseEstimator, TransformerMixin):
    def __init__(self, exclude=None):
        self.exclude = exclude
        self.dtypes = None
        self.dict_dtypes = {}

    def fit(self, X, y=None):
        self.dtypes = X.dtypes
        self.dtypes = self.dtypes[~self.dtypes.index.isin(self.exclude)]
        self.dict_dtypes = aiwr.dict_dtypes(X)
        return self

    def transform(self, X):
        X = X.copy()
        for col, dtype in self.dtypes.iteritems():
            if col in X.columns:
                try:
                    X[col] = X[col].astype(dtype)
                except:
                    pass
        return X


class ObjectToCategory(BaseEstimator, TransformerMixin):
    def __init__(self, exclude):
        self.exclude = exclude
        self.cat_cols = None

    def fit(self, X, y=None):
        self.cat_cols = [x for x in aiwr.dict_dtypes(X)['cat']
                         if x not in self.exclude]
        return self

    def transform(self, X):
        X = X.copy()
        for col in self.cat_cols:
            X[col] = X[col].astype('category')
        return X

# Preprocess = Pipeline(
#     steps=[
#         ('dtypes', Dtypes()),
#         ('cleaning', , FunctionTransformer(_cleaning)),
#         ('labelencoding', MultipleLabelEncoder())        
#     ], verbose=True)

# pl = Pipeline(memory=None,
#     steps=[
#         ('dtypes', Dtypes),
#         ('cleaning', Cleaning),
#         ('multiple_label_encoder', MultipleLabelEncoder),
#         ('encode', get_encoded_text),
#         ('selector', get_numeric_data),
#         ('model', model)
#     ], verbose=True)
