import pandas as pd
import click
from sklearn import ensemble
from sklearn import cross_validation
# from sklearn import linear_model
from sklearn2pmml import sklearn2pmml

import matplotlib
matplotlib.rcParams['text.usetex'] = True
matplotlib.rcParams['text.latex.unicode'] = True
matplotlib.rcParams['text.latex.preamble'] = [
   r'\usepackage{siunitx}',   # i need upright \micro symbols, but you need...
   r'\sisetup{detect-all}',   # ...this to force siunitx to actually use your fonts
   r'\usepackage{helvet}',    # set the normal font here
   r'\usepackage{sansmath}',  # load up the sansmath so that math -> helvet
   r'\sansmath']  # <- tricky! -- gotta actually tell tex to use!

matplotlib.use('Agg')

import matplotlib.pyplot as plt
plt.style.use('ggplot')

import numpy as np
from sklearn_pandas import DataFrameMapper
from scipy import stats

training_variables = ['ConcCore',
 'Concentration_onePixel',
 'Concentration_twoPixel',
 'Leakage',
 'Leakage2',
 'Size',
 'Slope_long',
 'Slope_spread',
 'Slope_spread_weighted',
 'Slope_trans',
 'Distance',
 'Theta',
 'Timespread',
 'Timespread_weighted',
 'Width',
 'arrTimePosShower_kurtosis',
 'arrTimePosShower_max',
 'arrTimePosShower_mean',
 'arrTimePosShower_min',
 'arrTimePosShower_skewness',
 'arrTimePosShower_variance',
 'arrTimeShower_kurtosis',
 'arrTimeShower_max',
 'arrTimeShower_mean',
 'arrTimeShower_min',
 'arrTimeShower_skewness',
 'arrTimeShower_variance',
 'concCOG',
 'm3l',
 'm3t',
 'maxPosShower_kurtosis',
 'maxPosShower_max',
 'maxPosShower_mean',
 'maxPosShower_min',
 'maxPosShower_skewness',
 'maxPosShower_variance',
 'maxSlopesPosShower_kurtosis',
 'maxSlopesPosShower_max',
 'maxSlopesPosShower_mean',
 'maxSlopesPosShower_min',
 'maxSlopesPosShower_skewness',
 'maxSlopesPosShower_variance',
 'maxSlopesShower_kurtosis',
 'maxSlopesShower_max',
 'maxSlopesShower_mean',
 'maxSlopesShower_min',
 'maxSlopesShower_skewness',
 'maxSlopesShower_variance',
 'numIslands',
 'numPixelInShower',
 'phChargeShower_kurtosis',
 'phChargeShower_max',
 'phChargeShower_mean',
 'phChargeShower_min',
 'phChargeShower_skewness',
 'phChargeShower_variance',
 'photonchargeMean'
 ]

def plot_importances(rf, features, path):
    importances = pd.DataFrame(rf.feature_importances_, index=features, columns=['importance'])
    importances = importances.sort_values(by='importance', ascending=True)
    ax = importances.plot(
     kind='barh',
     color='#2775d0'
    )
    ax.set_xlabel(u'feature importance')
    ax.get_yaxis().grid(None)
    return plt.gcf()

@click.command()
@click.argument('path', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True) )
@click.option('--n_trees','-n', default=100,  help='Number of trees to train.')
@click.option('--n_jobs','-j', default=1,  help='Number of trees to train.')
@click.option('--n_sample','-s', default=-1,  help='Number of data rows to sample. -1 for all rows.')
@click.option('--n_cv','-c', default=5,  help='Number of CV folds to perform')
@click.option('--bins','-b', default=100,  help='number of bins in ocrrelation plot.')
@click.option('--max_depth','-d', default=None, type=click.INT, help='Maximum depth of the trees in the forest.')
@click.option('--save','-sm', is_flag=True,  help='Flags whether model should be save to pmml.')
def main(path, out, n_trees, n_jobs,n_sample, n_cv,  bins,  max_depth, save):
    '''
    Train a RF regressor and write the model to OUT in pmml format.
    '''
    print("Loading data")

    df = pd.read_hdf(path, key='table')

    if n_sample > 0:
        df = df.sample(n_sample)


    df_train = df[training_variables]
    df_train = df_train.dropna(axis=0, how='any')

    df_target = df['MCorsikaEvtHeader.fTotalEnergy']
    df_target.name = 'estimated_energy'
    df_target = df_target[df_train.index]

    X_train, X_test, y_train, y_test = cross_validation.train_test_split(df_train, df_target, test_size=0.2)

    rf = ensemble.ExtraTreesRegressor(n_estimators=n_trees,max_features="sqrt", n_jobs=n_jobs, max_depth=max_depth)
    # rf = linear_model.Ridge()
    print("Training classifier in a {} fold CV...".format(n_cv))
    scores = cross_validation.cross_val_score(rf, X_train, y_train, cv=n_cv)

    print("Cross validated R^2 scores: {}".format(scores))
    print("Mean R^2 score : %0.2f (+/- %0.2f)" % (scores.mean(), scores.std()))

    print("Building new model on complete data set...")
    # rf = ensemble.ExtraTreesRegressor(n_estimators=n_trees,max_features="sqrt", oob_score=True, n_jobs=n_jobs, max_depth=max_depth)
    rf.fit(X_train, y_train)
    print("Score on complete data set: {}".format(rf.score(X_test, y_test)))
    #
    # importances = pd.DataFrame(rf.feature_importances_, index=df_train.columns, columns=['importance'])
    # importances = importances.sort_values(by='importance', ascending=True)
    # print('Plotting importances to importances.pdf')
    # ax = importances.plot(
    #     kind='barh',
    #     color='#2775d0'
    # )
    # ax.set_xlabel(u'feature importance')
    # ax.get_yaxis().grid(None)
    # plt.tight_layout()
    # plt.savefig('importances.pdf')

    print('Plotting correlation to correlation.pdf')
    fig = plt.figure()
    prediction = rf.predict(X_test)
    log_true_energy = np.log10(y_test)
    log_estimated_energy = np.log10(prediction)
    _, _, _, im = plt.hist2d(log_true_energy, log_estimated_energy, range=[[log_true_energy.min(), log_true_energy.max()], [log_true_energy.min(), log_true_energy.max()]], bins=bins, normed=False, cmin=0, cmap='inferno')

    fig.colorbar(im)
    plt.xlabel(r'Logarithm of Simulated Energy $E_{\text{MC}}$ in $\si{\GeV}$')
    plt.ylabel(r'Logarithm of Estimated Energy $E_{\text{EST}}$ in $\si{\GeV})$')

    plt.tight_layout()
    plt.savefig('correlation.pdf')
    # # embed()
    print("plot histogram of true energy")
    fig = plt.figure()
    plt.hist(np.log10(df_target.values), bins=bins, normed=True)

    plt.yscale('log')
    plt.xlabel(r'monte carlo energy $E_{\text{MC}}$ in $\log(\si{\GeV})$'  )
    plt.ylabel('normed frequency')
    plt.tight_layout()
    plt.savefig('true_energy_histogram.pdf')


    print("Saving prediction to prediction.json")
    result = pd.DataFrame()
    result['Estimated Energy'] = prediction
    result['True Energy'] = y_test

    result.to_json('prediction.json')


    print("Performing KDE")
    plt.figure()

    kde = stats.gaussian_kde(np.vstack([y_test, prediction]), bw_method=0.05)
    l = np.logspace(np.log10(y_train.min()), np.log10(y_train.max()), 170)
    X, Y  = np.meshgrid(l, l)

    positions = np.vstack([X.ravel(), Y.ravel()])
    result = kde.pdf(positions)

    z = np.reshape(result.T, X.shape)
    plt.pcolormesh(X, Y , z, cmap='plasma')
    plt.xlabel(r'monte carlo energy $E_{\text{MC}}$ in $\si{\GeV}$'  )
    plt.ylabel(r'estimated energy $E_{\text{EST}}$ in $\si{\GeV}$')
    plt.yscale('log')
    plt.xscale('log')
    plt.xticks(10**np.arange(2.5, 5.0, 0.5 ), np.arange(2.5, 5.0, 0.5 ).astype(str) )
    plt.yticks(10**np.arange(2.5, 5.0, 0.5 ), np.arange(2.5, 5.0, 0.5 ).astype(str) )
    plt.xlim(y_train.min(), y_train.max())
    plt.ylim(y_train.min(), y_train.max())
    plt.savefig('energy_kde.png', dpi=150)

    if save:
        print("writing model to {}".format(out))
        mapper = DataFrameMapper([
                                (list(df_train.columns), None),
                                ('estimated_energy', None)
                        ])
        sklearn2pmml(rf, mapper,  out)

if __name__ == '__main__':
    main()
