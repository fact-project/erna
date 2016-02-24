import pandas as pd
import click
from sklearn import ensemble
from sklearn import cross_validation
# from sklearn import linear_model
from sklearn2pmml import sklearn2pmml
from sklearn import metrics
from tqdm import tqdm
# from  matplotlib.ticker import LogLocator
# from  matplotlib.ticker import LogFormatter
from IPython import embed
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
from scipy import stats
colors = ['#3ca3ec', '#F16745', '#FFC65D', '#7BC8A4', '#4CC3D9', '#93648D','#6a6493', '#73e3d2']
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
 # 'Distance',
 # 'Theta',
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



def bias_resolution(true_energy, estimated_energy, bin_edges):
    df = pd.DataFrame({'relative_difference':(estimated_energy - true_energy)/(true_energy), 'true_energy':true_energy, 'estimated_energy': estimated_energy})

    sample_groups  = df.groupby(np.digitize(np.log10(df.true_energy), bin_edges, right=True))
    # np.unique()
    bin_width = bin_edges[1] - bin_edges[0]
    bin_center = bin_edges[1:] - 0.5 * bin_width

    bias = np.full(len(bin_center), np.nan)
    resolution = np.full(len(bin_center), np.nan)
    # embed()
    for (name, samples), center in zip(sample_groups, bin_center):
        # print('fitting group {}'.format(name))
        # print(samples)
        scale, loc = fit_for_resolution(samples.relative_difference, center)
        bias[name-1] = scale
        resolution[name-1] = loc

    return bias, resolution




def fit_for_resolution(samples, expected_center):
    #center the samples
    # centered_samples = samples.copy() - expected_center
    loc, scale  = stats.norm.fit(samples)
    return loc, scale


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
@click.option('--query','-q', default=None,  help='Query to apply to the data e.g : \"(Leakage < 0.0) & (Size < 10000)\"')
@click.option('--save','-sm', is_flag=True,  help='Flags whether model should be save to pmml.')
def main(path, out, n_trees, n_jobs,n_sample, n_cv,  bins,  max_depth, query, save):
    '''
    Train a RF regressor and write the model to OUT in pmml format.
    '''
    print("Loading data")

    df = pd.read_hdf(path, key='table')

    if n_sample > 0:
        df = df.sample(n_sample)

    if query:
        print('Quering with string: {}'.format(query))
        df = df.query(query)

    df_train = df[training_variables]
    df_train = df_train.dropna(axis=0, how='any')

    df_target = df['MCorsikaEvtHeader.fTotalEnergy']
    df_target.name = 'true_energy'
    df_target = df_target[df_train.index]

    X_train, X_test, y_train, y_test = cross_validation.train_test_split(df_train, df_target, test_size=0.2)

    rf = ensemble.ExtraTreesRegressor(n_estimators=n_trees,max_features="sqrt", n_jobs=n_jobs, max_depth=max_depth)
    # rf = linear_model.Ridge()
    # print("Training classifier in a {} fold CV...".format(n_cv))
    # scores = cross_validation.cross_val_score(rf, X_train, y_train, cv=n_cv)

    print('Starting {} fold cross validation... '.format(n_cv) )
    # embed()
    cv = cross_validation.KFold(len(y_train), n_folds=n_cv, shuffle=True)
    # labels = []
    # predictions =[]
    resolution_bins = 20
    min_energy = np.log10(df_target.min())
    max_energy = np.log10(df_target.max())
    resolution_bin_edges = np.linspace(min_energy, max_energy, resolution_bins)
    resolution_bin_width = resolution_bin_edges[1] - resolution_bin_edges[0]

    scores = []
    bias = []
    resolution = []
    for train, test in tqdm(cv):
        # embed()
        # select data
        cv_x_train, cv_x_test = X_train.values[train], X_train.values[test]
        cv_y_train, cv_y_test = y_train.values[train], y_train.values[test]
        # fit and predict
        rf.fit(cv_x_train, cv_y_train)
        cv_y_prediciton = rf.predict(cv_x_test)

        #calcualte r2 score
        scores.append(metrics.r2_score(cv_y_test, cv_y_prediciton))

        #calucate bias and resolution
        # log_true_energy = np.log10(cv_y_test)
        # log_estimated_energy = np.log10(cv_y_prediciton)
        b, r = bias_resolution(cv_y_test, cv_y_prediciton, resolution_bin_edges)
        bias.append(b)
        resolution.append(r)


    scores = np.array(scores)
    print("Cross validated R^2 scores: {}".format(scores))
    print("Mean R^2 score : %0.2f (+/- %0.2f)" % (scores.mean(), scores.std()))

    print('plotting bias and resolution')
    plt.figure()
    bias = np.array([list(b) for b in bias])
    bias = pd.DataFrame(bias)

    resolution = np.array([list(b) for b in resolution])
    resolution = pd.DataFrame(resolution)

    height = 0.01
    heights = np.full_like(resolution.mean(), height)

    plt.bar(resolution_bin_edges[:-1], heights, width=resolution_bin_width, yerr=bias.std().values, linewidth=0,  label="bias", bottom=bias.mean().values, color=colors[1], ecolor=colors[1])

    plt.bar(resolution_bin_edges[:-1], heights, width=resolution_bin_width, yerr=resolution.std().values,   linewidth=0, label="resolution", bottom=resolution.mean().values, color=colors[0], ecolor=colors[0])

    plt.xlabel('Simulated Energy $\log_{10}(E_{\\text{MC}} / \si{\GeV})$')
    plt.legend(bbox_to_anchor=(0.0, 1.03, 1, .2), loc='lower center', ncol=2, borderaxespad=0., fancybox=True, framealpha=0.0)

    plt.savefig('bias_resolution.pdf')


    print("Building new model on complete data set...")
    # rf = ensemble.ExtraTreesRegressor(n_estimators=n_trees,max_features="sqrt", oob_score=True, n_jobs=n_jobs, max_depth=max_depth)
    rf.fit(X_train, y_train)
    print("Score on complete data set: {}".format(rf.score(X_test, y_test)))


    print('Plotting correlation to correlation.pdf')
    prediction = rf.predict(X_test)
    log_true_energy = np.log10(y_test)
    log_estimated_energy = np.log10(prediction)
    # bin_edges = np.linspace(min_energy, max_energy, bins)

    fig, ax = plt.subplots()
    _, _, _, im = ax.hist2d(log_true_energy, log_estimated_energy, range=[[log_true_energy.min(), log_true_energy.max()], [log_true_energy.min(), log_true_energy.max()]], bins=bins, norm=matplotlib.colors.LogNorm(), cmin=0, cmap='inferno')
    fig.colorbar(im)

    ax.set_xlim(log_true_energy.min(), log_true_energy.max())
    ax.set_ylim(log_true_energy.min(), log_true_energy.max())
    ax.set_xlabel('Simulated Energy $\log_{10}(E_{\\text{MC}} / \si{\GeV})$')
    ax.set_ylabel('Estimated Energy $\log_{10}(E_{\\text{EST}} / \si{\GeV})$')

    plt.tight_layout()
    plt.savefig('correlation.pdf')


    # embed()
    print("plot histogram of true energy")
    fig = plt.figure()
    plt.hist(np.log10(df_target.values), bins=bins, normed=True)

    plt.yscale('log')
    plt.xlabel('monte carlo energy $E_{\\text{MC}}$ in $\si{\GeV}$'  )
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
    plt.pcolormesh(X, Y , z, cmap='inferno')
    plt.xlabel('monte carlo energy $E_{\\text{MC}}$ in $\si{\GeV}$'  )
    plt.ylabel('estimated energy $E_{\\text{EST}}$ in $\si{\GeV}$')
    plt.yscale('log')
    plt.xscale('log')
    # plt.xticks(10**np.arange(2.5, 5.0, 0.5 ), np.arange(2.5, 5.0, 0.5 ).astype(str) )
    # plt.yticks(10**np.arange(2.5, 5.0, 0.5 ), np.arange(2.5, 5.0, 0.5 ).astype(str) )
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
