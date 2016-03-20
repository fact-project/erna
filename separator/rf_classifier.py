import pandas as pd
import click
from sklearn import cross_validation
from sklearn2pmml import sklearn2pmml
from tqdm import tqdm


from sklearn_pandas import DataFrameMapper
from sklearn.externals import joblib
from os import path
import json
import config


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




def read_data(file_path, hdf_key='table'):

    name, extension =  path.splitext(file_path)
    if extension in ['.hdf', '.hdf5', '.h5']:
        return pd.read_hdf(file_path, key=hdf_key)
    if extension == '.json':
        with open(file_path, 'r') as j:
            d = json.load(j)
            return pd.DataFrame(d)


def write_data(df, file_path, hdf_key='table'):
    name, extension =  path.splitext(file_path)
    if extension in ['.hdf', '.hdf5', '.h5']:
        df.to_hdf(file_path, key=hdf_key)
    elif extension == '.json':
        df.to_json(file_path)
    else:
        print('cannot write tabular data with extension {}'.format(extension))

def check_extension(file_path, allowed_extensions= ['.hdf', '.hdf5', '.h5', 'json']):
    p, extension = path.splitext(file_path)
    if not extension in allowed_extensions:
        print('Extension {} not allowed here.'.format(extension))

@click.command()
@click.argument('gamma_path', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('proton_path', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('prediction_path', type=click.Path(exists=False, dir_okay=False, file_okay=True) )
@click.argument('model_path', type=click.Path(exists=False, dir_okay=False, file_okay=True) )
@click.argument('importances_path', type=click.Path(exists=False, dir_okay=False, file_okay=True) )
@click.option('--n_sample','-s', default=-1,  help='Number of data rows to sample. -1 for all rows.')
@click.option('--n_cv','-c', default=5,  help='Number of CV folds to perform')
def main(gamma_path, proton_path, prediction_path, model_path, importances_path,  n_sample,  n_cv):
    '''
    Train a RF classifier and write the model to OUT in pmml or pickle format.
    '''
    map(check_extension, [gamma_path, proton_path, prediction_path, importances_path])
    check_extension(model_path, allowed_extensions=['.pmml', '.pkl'])

    print("Loading data")
    df_gamma = read_data(gamma_path)
    df_proton = read_data(proton_path)
    # embed()
    # df_gamma = pd.read_hdf(gamma_path, key='table')
    # df_proton = pd.read_hdf(proton_path, key='table')
    query = config.query
    if query:
        print('Quering with string: {}'.format(query))
        df_gamma = df_gamma.query(query)
        df_proton = df_proton.query(query)

    df_gamma['label_text'] = 'gamma'
    df_gamma['label'] = 1
    df_proton['label_text'] = 'proton'
    df_proton['label'] = 0

    if n_sample > 0:
        df_gamma = df_gamma.sample(n_sample)
        df_proton = df_proton.sample(n_sample)


    print('Training classifier with {} protons and {} gammas'.format(len(df_proton), len(df_gamma)))

    df_full = pd.concat([df_proton, df_gamma], ignore_index=True).dropna(axis=0, how='any')
    df_training = df_full[config.training_variables]
    df_label = df_full['label']

    classifier = config.learner

    #save prediction_path for each cv iteration
    cv_predictions = []
    # iterate over test and training sets
    X =  df_training.values
    y = df_label.values
    print('Starting {} fold cross validation... '.format(n_cv) )
    cv = cross_validation.StratifiedKFold(y, n_folds=n_cv)


    for fold, (train, test) in enumerate(tqdm(cv)):
        # select data
        xtrain, xtest = X[train], X[test]
        ytrain, ytest = y[train], y[test]
        # fit and predict
        classifier.fit(xtrain, ytrain)

        y_probas = classifier.predict_proba(xtest)[:, 1]
        y_prediction = classifier.predict(xtest)
        cv_predictions.append(pd.DataFrame({'label':ytest, 'label_prediction':y_prediction, 'probabilities':y_probas, 'cv_fold':fold}))
        #labels_predictions.append([ytest, y_prediction, y_probas])


    predictions_df = pd.concat(cv_predictions,ignore_index=True)
    print('writing predictions from cross validation')
    write_data(predictions_df, prediction_path)

    print("Training model on complete dataset")
    classifier.fit(X,y)

    print("Saving importances")
    importances = pd.DataFrame(classifier.feature_importances_, index=df_training.columns, columns=['importance'])
    write_data(importances, importances_path)

    # print("Pickling model to {} ...".format(model_path))

    p, extension = path.splitext(model_path)
    if (extension == '.pmml'):
        print("Pickling model to {} ...".format(model_path))
        # joblib.dump(rf, mode, compress = 4)
        mapper = DataFrameMapper([
                                (list(df_training.columns), None),
                                ('label', None)
                        ])

        # joblib.dump(mapper, out, compress = 4)
        sklearn2pmml(classifier, mapper,  model_path)

        joblib.dump(classifier,p + '.pkl', compress = 4)
    else:
        joblib.dump(classifier, model_path, compress = 4)

    # print('Adding data information to pmml...')
    # ET.register_namespace('',"http://www.dmg.org/PMML-4_2")
    # xml_tree = ET.parse('rf.pmml')
    # root = xml_tree.getroot()
    # header = root.findall('{http://www.dmg.org/PMML-4_2}Header')[0]
    # newNode = ET.Element('Description')
    # newNode.text = 'Data was queried with {} and contained {} gammas and {} protons'.format(query, len(df_gamma), len(df_proton))
    # header.append(newNode)
    # xml_tree.write(out,
    #        xml_declaration=True,encoding='utf-8',
    #        method='xml')
    #




if __name__ == '__main__':
    main()
