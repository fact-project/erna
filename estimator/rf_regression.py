import pandas as pd
import click
from sklearn import ensemble

import matplotlib.pyplot as plt
plt.style.use('ggplot')
# from IPython import embed
import numpy as np

training_variables = ['ConcCore', 'Concentration_onePixel', 'Concentration_twoPixel','Leakage',
        'Leakage2',  'Size', 'Slope_long', 'Slope_spread', 'Slope_spread_weighted',
       'Slope_trans','Timespread',
       'Timespread_weighted', 'Width', 'ZdPointing',
       'arrTimePosShower_kurtosis', 'arrTimePosShower_max',
       'arrTimePosShower_mean', 'arrTimePosShower_min',
       'arrTimePosShower_skewness', 'arrTimePosShower_variance',
       'arrTimeShower_kurtosis', 'arrTimeShower_max', 'arrTimeShower_mean',
       'arrTimeShower_min', 'arrTimeShower_skewness',
       'arrTimeShower_variance', 'arrivalTimeMean', 'concCOG', 'm3l',
       'm3t', 'maxPosShower_kurtosis', 'maxPosShower_max',
       'maxPosShower_mean', 'maxPosShower_min', 'maxPosShower_skewness',
       'maxPosShower_variance', 'maxSlopesPosShower_kurtosis',
       'maxSlopesPosShower_max', 'maxSlopesPosShower_mean',
       'maxSlopesPosShower_min', 'maxSlopesPosShower_skewness',
       'maxSlopesPosShower_variance', 'maxSlopesShower_kurtosis',
       'maxSlopesShower_max', 'maxSlopesShower_mean',
       'maxSlopesShower_min', 'maxSlopesShower_skewness',
       'maxSlopesShower_variance', 'numIslands', 'numPixelInShower',
       'phChargeShower_kurtosis', 'phChargeShower_max',
       'phChargeShower_mean', 'phChargeShower_min',
       'phChargeShower_skewness', 'phChargeShower_variance',
       'photonchargeMean']

@click.command()
@click.argument('path', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.option('--n_trees','-n', default=100,  help='Number of trees to train.')
@click.option('--n_jobs','-j', default=1,  help='Number of trees to train.')
@click.option('--n_sample','-s', default=-1,  help='Number of data rows to sample. -1 for all rows.')
@click.option('--bins','-b', default=100,  help='number of bins in ocrrelation plot.')
@click.option('--log','-l', is_flag=True,  help='Flags whether a log function should be applied to the true energie and size before training stuff.')
def main(path, n_trees, n_jobs,n_sample, bins, log):
    print("Loading data")

    df = pd.read_hdf(path, key='table')

    if n_sample > 0:
        df = df.sample(n_sample)

    df_target = df['MCorsikaEvtHeader.fTotalEnergy']
    df_train = df[training_variables]
    df_train = df_train.dropna(axis=0, how='any')
    df_target = df_target[df_train.index]

    if log:
        df_target = df_target.apply(np.log10)
        df_train['Size'] = df_train['Size'].apply(np.log10)

    rf = ensemble.RandomForestRegressor(n_estimators=n_trees,max_features="sqrt", oob_score=True, n_jobs=n_jobs)
    print("Training classifier")
    rf.fit(df_train.values, df_target)
    print("Scoring classifier")
    s = rf.score(df_train, df_target)

    print("Score R^2 (a value of 1.0 would be the best): {}".format(s))
    importances = pd.DataFrame(rf.feature_importances_, index=df_train.columns, columns=['importance'])
    importances = importances.sort_values(by='importance', ascending=True)

    # print(importances)
    print('Plotting importances to importances.pdf')
    ax = importances.plot(
        kind='barh',
        color='#2775d0'
    )
    ax.set_xlabel(u'feature importance')
    ax.get_yaxis().grid(None)
    plt.tight_layout()
    plt.savefig('importances.pdf')

    print('Plotting correlation to correlation.pdf')
    fig = plt.figure()
    prediction = rf.predict(df_train)
    _, _, _, im = plt.hist2d(np.log10(df_target), np.log10(prediction), bins=bins, normed=True, cmin=1, cmap='inferno')
    fig.colorbar(im)
    plt.xlabel('logarithm of true energy')
    plt.ylabel('logarithm of estimated energy')
    plt.tight_layout()
    plt.savefig('correlation.pdf')

    print("Saving prediction to prediction.json")
    result = pd.DataFrame()
    result['Estimated Energy'] = prediction
    result['True Energy'] = df_target.values

    result.to_json('prediction.json')

if __name__ == '__main__':
    main()
