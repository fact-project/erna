import pandas as pd
import config
import click
from sklearn import cross_validation
# from sklearn import linear_model
from sklearn2pmml import sklearn2pmml
from sklearn import metrics
from tqdm import tqdm
from os import path

scale = 1.3
import matplotlib
matplotlib.rcParams['text.usetex'] = True
matplotlib.rcParams['text.latex.unicode'] = True
matplotlib.rcParams['font.size']  = 11*scale
matplotlib.rcParams['legend.fontsize']  = 10*scale
matplotlib.rcParams['xtick.labelsize']  = 9*scale
matplotlib.rcParams['ytick.labelsize']  = 9*scale
matplotlib.rcParams['axes.labelsize']  = 'large'
matplotlib.rcParams['text.latex.preamble'] = [
    r'\usepackage{siunitx}',   # i need upright \micro symbols, but you need...
    r'\sisetup{detect-all}',   # ...this to force siunitx to actually use your fonts
    r'\usepackage{Fira Sans}',    # set the normal font here
    r'\usepackage{sansmath}',  # load up the sansmath so that math -> helvet
    r'\sansmath']  # <- tricky! -- gotta actually tell tex to use!

matplotlib.use('Agg')

import matplotlib.pyplot as plt
plt.style.use('ggplot')

import numpy as np
from sklearn_pandas import DataFrameMapper



def write_data(df, file_path, hdf_key='table'):
    name, extension =  path.splitext(file_path)
    if extension in ['.hdf', '.hdf5', '.h5']:
        df.to_hdf(file_path, key=hdf_key)
    elif extension == '.json':
        df.to_json(file_path)
    else:
        print('cannot write tabular data with extension {}'.format(extension))

@click.command()
@click.argument('gamma_path', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('prediction_path', type=click.Path(exists=False, dir_okay=False, file_okay=True) )
@click.argument('model_path', type=click.Path(exists=False, dir_okay=False, file_okay=True) )
@click.argument('importances_path', type=click.Path(exists=False, dir_okay=False, file_okay=True) )
@click.option('--n_sample','-s', default=-1,  help='Number of data rows to sample. -1 for all rows.')
@click.option('--n_cv','-c', default=5,  help='Number of CV folds to perform')
@click.option('--bins','-b', default=100,  help='number of bins in ocrrelation plot.')
@click.option('--perform_kde','-kde', is_flag=True,  help='Flags whether KDE should be performed.')
@click.option('--save','-sm', is_flag=True,  help='Flags whether model should be save to pmml.')
def main(gamma_path,prediction_path,  model_path, importances_path,  n_sample, n_cv,  bins, perform_kde, save):
    '''
    Train a RF regressor and write the model to OUT in pmml format.
    '''
    print("Loading data")

    df = pd.read_hdf(gamma_path, key='table')

    if n_sample > 0:
        df = df.sample(n_sample)

    if config.query:
        print('Quering with string: {}'.format(config.query))
        df = df.query(config.query)

    df_train = df[config.training_variables]
    df_train = df_train.dropna(axis=0, how='any')

    df_target = df['MCorsikaEvtHeader.fTotalEnergy']
    df_target.name = 'true_energy'
    df_target = df_target[df_train.index]
    # embed()
    X_train, X_test, y_train, y_test = cross_validation.train_test_split(df_train, df_target, test_size=0.2)

    rf = config.learner

    print('Used learner: {}'.format(rf))

    print('Starting {} fold cross validation... '.format(n_cv) )
    # embed()

    # labels = []
    # predictions =[]

    scores = []
    cv_predictions = []

    cv = cross_validation.KFold(len(y_train), n_folds=n_cv, shuffle=True)
    for fold, (train, test) in tqdm(enumerate(cv)):
        # embed()
        # select data
        cv_x_train, cv_x_test = X_train.values[train], X_train.values[test]
        cv_y_train, cv_y_test = y_train.values[train], y_train.values[test]
        # fit and predict
        # embed()
        rf.fit(cv_x_train, cv_y_train)
        cv_y_prediciton = rf.predict(cv_x_test)

        #calcualte r2 score
        scores.append(metrics.r2_score(cv_y_test, cv_y_prediciton))

        cv_predictions.append(pd.DataFrame({'label':cv_y_test, 'label_prediction':cv_y_prediciton, 'cv_fold':fold}))



    predictions_df = pd.concat(cv_predictions,ignore_index=True)

    print('writing predictions from cross validation')
    predictions_df.to_hdf(prediction_path, key='table')

    scores = np.array(scores)
    print("Cross validated R^2 scores: {}".format(scores))
    print("Mean R^2 score from CV: %0.2f (+/- %0.2f)" % (scores.mean(), scores.std()))


    print("Building new model on complete data set...")
    # rf = ensemble.ExtraTreesRegressor(n_estimators=n_trees,max_features="sqrt", oob_score=True, n_jobs=n_jobs, max_depth=max_depth)
    rf.fit(X_train, y_train)
    print("Score on complete data set: {}".format(rf.score(X_test, y_test)))


    print("Saving importances")
    importances = pd.DataFrame(rf.feature_importances_, index=df_train.columns, columns=['importance'])
    write_data(importances, importances_path)

    if save:
        print("writing model to {}".format(model_path))
        mapper = DataFrameMapper([
                                (list(df_train.columns), None),
                                ('estimated_energy', None)
                        ])
        sklearn2pmml(rf, mapper,  model_path)

if __name__ == '__main__':
    main()
