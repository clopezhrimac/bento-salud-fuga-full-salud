import pandas as pd

from bento_service import LgbModelService

MODEL_PATH = './artifacts/model.pkl'
PREPROCESS_OBJ_PATH = './artifacts/preprocess.pkl'
SAVE_PATH = './packs/'


def main():
    # Read artifacts to package
    model = pd.read_pickle(MODEL_PATH)
    preprocess = pd.read_pickle(PREPROCESS_OBJ_PATH)

    # Package service
    svc = LgbModelService()
    svc.pack('model', model)
    svc.pack('preprocessing_obj', preprocess)
    svc.save_to_dir(path=SAVE_PATH)


if __name__ == '__main__':
    main()
