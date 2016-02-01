import pandas as pd
import click
from sklearn import ensemble
from sklearn import cross_validation
from sklearn2pmml import sklearn2pmml
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes
from mpl_toolkits.axes_grid1.inset_locator import mark_inset
from tqdm import tqdm
from sklearn.naive_bayes import GaussianNB
from sklearn import metrics
from functools import partial
import os

import matplotlib.pyplot as plt
plt.style.use('ggplot')
from IPython import embed
import numpy as np
from sklearn_pandas import DataFrameMapper

training_variables = ['ConcCore', 'Concentration_onePixel', 'Concentration_twoPixel','Leakage',
        'Leakage2',  'Size', 'Slope_long', 'Slope_spread', 'Slope_spread_weighted',
       'Slope_trans','Timespread','Theta',
       'Timespread_weighted', 'Width',
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

def calculate_metric_for_confidence_cuts(predictions, metric, confidence_bins):
    n_folds = len(predictions)
    # plot stuff with confidence cuts
    metric_values = np.zeros((n_folds, confidence_bins))
    for fold, (test, prediction, probas) in enumerate(predictions):
        for c_bin, cut in enumerate(np.linspace(0, 1, confidence_bins)):
            cutted_prediction = prediction.copy()
            cutted_prediction[probas < cut] = 0
            cutted_prediction[probas >= cut] = 1
            m_value = metric(test, cutted_prediction)
            # embed()
            metric_values[fold][c_bin] = m_value
    return metric_values

def plot_metric_vs_confidence(metric_values, label='Some metric', axis = None, color='#cc4368'):
    fig = None
    if not axis:
        fig, axis= plt.subplots(1)
    # embed()
    acc_mean = np.mean(metric_values, axis=0)
    acc_err = np.std(metric_values, axis=0)

    b = np.linspace(0, 1, len(acc_mean))
    axis.plot(b, acc_mean, 'r+', label=label, color=color)
    axis.fill_between(
        b, acc_mean + acc_err * 0.5, acc_mean - acc_err*0.5,
        facecolor='gray', alpha=0.4,
    )
    axis.legend(loc='best', fancybox=True, framealpha=0.5)
    axis.set_xlabel('prediction threshold')

    return fig, axis


def plot_roc_curves(labels_predictions, ax=None, title=None):
        # plot roc aucs
    # fig = None
    if not ax:
        fig, ax = plt.subplots(1)
    axins = zoomed_inset_axes(ax, 2.5, loc=1)
    for test, prediction, proba in labels_predictions:
        fpr, tpr, thresholds = metrics.roc_curve(
            test, proba
        )
        ax.plot(fpr, tpr, linestyle='-', color='k', alpha=0.3)
        axins.plot(fpr, tpr, linestyle='-', color='k', alpha=0.3)

    ax.set_xlabel('False Positiv Rate')
    ax.set_ylabel('True Positiv Rate')
    ax.set_ylim(0, 1)
    ax.set_xlim(0, 1)
    axins.set_xlim(0, 0.15)
    axins.set_ylim(0.8, 1.0)
    axins.set_xticks([0.0, 0.05, 0.1, 0.15])
    axins.set_yticks([0.8, 0.85, 0.9, 0.95, 1.0])
    mark_inset(ax, axins, loc1=2, loc2=3, fc='none', ec='0.8')
    if not title:
        ax.set_title('RoC curves for the classifier', y=1.03)
    if title:
        ax.set_title(title, y=1.03)
    return fig, ax

def plot_q_values(labels_predictions,confidence_bins, ax=None):
    '''plot q values.'''
    if not ax:
        fig, ax = plt.subplots(1)

    matrices = np.zeros((len(labels_predictions), confidence_bins, 2, 2))
    for fold, (test, prediction, probas) in enumerate(labels_predictions):
        for i, cut in enumerate(np.linspace(0, 1, confidence_bins)):
            cutted_prediction = prediction.copy()
            cutted_prediction[probas < cut] = 0
            cutted_prediction[probas >= cut] = 1
            confusion = metrics.confusion_matrix(test, cutted_prediction)
            matrices[fold][i] = confusion

    b = np.linspace(0, 1, confidence_bins)

    tps = matrices[:, :, 0, 0]
    # fns = matrices[:, :, 0, 1]
    # fps = matrices[:, :, 1, 0]
    tns = matrices[:, :, 1, 1]

    q_mean = np.mean(tps / np.sqrt(tns), axis=0)
    q_err = np.std(tps / np.sqrt(tns), axis=0)
    e_mean = np.mean(tps / np.sqrt(tns + tps), axis=0)
    e_err = np.std(tps / np.sqrt(tns + tps), axis=0)

    ax.plot(b, q_mean, 'b+', label=r'$\frac{tps}{\sqrt{tns}}$')
    ax.fill_between(
        b, q_mean + q_err*0.5, q_mean - q_err*0.5, facecolor='gray', alpha=0.4
    )
    ax.plot(
        b, e_mean,
        color='#58BADB', linestyle='', marker='+',
        label=r'$\frac{tps}{\sqrt{tns + tps}}$',
    )
    ax.fill_between(
        b, e_mean + e_err*0.5, e_mean - e_err*0.5, facecolor='gray', alpha=0.4
    )
    ax.legend(loc='best', fancybox=True, framealpha=0.5)
    ax.set_xlabel('prediction threshold')
    return fig, ax


def print_performance(
        roc_aucs,
        confusion_matrices,
        precisions,
        recalls,
        f_scores,
        ):

    tp = confusion_matrices[:, 0, 0]
    fn = confusion_matrices[:, 0, 1]
    fp = confusion_matrices[:, 1, 0]
    tn = confusion_matrices[:, 1, 1]

    print('''Confusion Matrix:
        {:>8.2f} +- {:>8.2f}  {:>8.2f} +- {:>8.2f}
        {:>8.2f} +- {:>8.2f}  {:>8.2f} +- {:>8.2f}
    '''.format(
        tp.mean(), tp.std(),
        fn.mean(), fn.std(),
        fp.mean(), fp.std(),
        tn.mean(), tn.std(),
    ))

    fpr = fp / (fp + tn)
    relative_error = (fpr.std() / fpr.mean()) * 100
    print('Mean False Positive Rate: ')
    print('{:.5f} +- {:.5f} (+- {:.1f} %)'.format(
        fpr.mean(), fpr.std(), relative_error
    ))

    print('Mean area under ROC curve: ')
    relative_error = (roc_aucs.std() / roc_aucs.mean()) * 100
    print('{:.5f} +- {:.5f} (+- {:.1f} %)'.format(
        roc_aucs.mean(), roc_aucs.std(), relative_error
    ))

    print('Mean recall:')
    relative_error = (recalls.std() / recalls.mean()) * 100
    print('{:.5f} +- {:.5f} (+- {:.1f} %)'.format(
        recalls.mean(), recalls.std(), relative_error
    ))

    print('Mean fscore:')
    relative_error = (f_scores.std() / f_scores.mean()) * 100
    print('{:.5f} +- {:.5f} (+- {:.1f} %)'.format(
        f_scores.mean(), f_scores.std(), relative_error
    ))


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
@click.argument('gamma_path', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('proton_path', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('out', type=click.Path(exists=False, dir_okay=False, file_okay=True) )
@click.option('--n_trees','-n', default=100,  help='Number of trees to train.')
@click.option('--n_jobs','-j', default=1,  help='Number of trees to train.')
@click.option('--n_sample','-s', default=-1,  help='Number of data rows to sample. -1 for all rows.')
@click.option('--n_cv','-c', default=5,  help='Number of CV folds to perform')
@click.option('--n_bins','-b', default=50,  help='Number of bins to plot for performance plots')
def main(gamma_path, proton_path, out, n_trees, n_jobs,n_sample, n_cv, n_bins):
    '''
    Train a RF classifier and write the model to OUT in pmml format.
    '''
    print("Loading data")

    df_gamma = pd.read_hdf(gamma_path, key='table')
    df_gamma['label_text'] = 'gamma'
    df_gamma['label'] = 1
    df_proton = pd.read_hdf(proton_path, key='table')
    df_proton['label_text'] = 'proton'
    df_proton['label'] = 0

    if n_sample > 0:
        df_gamma = df_gamma.sample(n_sample)
        df_proton = df_proton.sample(n_sample)

    # embed()
    print('Training classifier with {} protons and {} gammas'.format(len(df_proton), len(df_gamma)))
    df_full = pd.concat([df_proton, df_gamma], ignore_index=True).dropna(axis=0, how='any')
    df_training = df_full[training_variables]
    df_label = df_full['label']

    #create classifier
    rf = ensemble.RandomForestClassifier(n_estimators=n_trees, max_features="sqrt", n_jobs=n_jobs)

    #save predictions for each cv iteration
    labels_predictions = []
    # iterate over test and training sets
    X =  df_training.values
    y = df_label.values
    print('Starting {} fold cross validation... '.format(n_cv) )
    cv = cross_validation.StratifiedKFold(y, n_folds=n_cv)
    for train, test in tqdm(cv):
        # select data
        xtrain, xtest = X[train], X[test]
        ytrain, ytest = y[train], y[test]
        # fit and predict
        rf.fit(xtrain, ytrain)
        y_probas = rf.predict_proba(xtest)[:, 1]
        y_prediction = rf.predict(xtest)
        labels_predictions.append((ytest, y_prediction, y_probas))


    print('Creating plots...')
    b = 1/10
    f_beta= partial(metrics.fbeta_score, beta=b)
    betas = calculate_metric_for_confidence_cuts(labels_predictions, f_beta, n_bins)
    fig, ax = plot_metric_vs_confidence(betas, label='f score with beta = {}'.format(b))
    fig.savefig('fbeta.pdf')


    aucs = calculate_metric_for_confidence_cuts(labels_predictions, metrics.roc_auc_score, n_bins)
    fig, ax = plot_metric_vs_confidence(aucs, label='roc auc')
    fig.savefig('roc_auc.pdf')

    recall = calculate_metric_for_confidence_cuts(labels_predictions, metrics.recall_score, n_bins)
    fig, ax = plot_metric_vs_confidence(recall, label='recall')
    precision = calculate_metric_for_confidence_cuts(labels_predictions, metrics.precision_score, n_bins)
    _, ax = plot_metric_vs_confidence(precision, label='precision', axis=ax, color='#3c84d7')
    fig.savefig('recall_precision.pdf')

    fig, ax = plot_q_values(labels_predictions, n_bins)
    fig.savefig('q_values.pdf')

    fig, ax = plot_roc_curves(labels_predictions)
    fig.savefig('roc_curves.pdf')



    # y_test = labels_predictions[0][0]
    # y_pred = labels_predictions[0][1]
    # report = metrics.classification_report(y_test, y_pred)
    # print(report)
    # roc_aucs, fig = classifier_crossval_performance(df_training.values, df_label.values, classifier=rf, n_folds=n_cv, bins=n_bins)

    # p, ext = os.path.splitext(out)
    # plot_path = '{}_crossval_performance{}'.format(p, '.pdf')
    # print('Saving plot to {}'.format(plot_path))
    # fig.savefig(plot_path)



    print("Writing model to {} ...".format(out))
    mapper = DataFrameMapper([
                            (list(df_training.columns), None),
                            ('label', None)
                    ])
    sklearn2pmml(rf, mapper,  out)

if __name__ == '__main__':
    main()
