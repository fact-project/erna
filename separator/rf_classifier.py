import pandas as pd
import click
from sklearn import cross_validation
from sklearn2pmml import sklearn2pmml
from tqdm import tqdm
import numpy as np
from sklearn import metrics

from sklearn_pandas import DataFrameMapper
from sklearn.externals import joblib
from os import path
import json
import yaml


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
@click.argument('configuration_path', type=click.Path(exists=True, dir_okay=False, file_okay=True) , help='Path to the config yaml file')
@click.argument('prediction_path', type=click.Path(exists=False, dir_okay=False, file_okay=True) , help='Path to where the CV predictions are saved')
@click.argument('model_path', type=click.Path(exists=False, dir_okay=False, file_okay=True) , help='Save location of the  model will be saved. ' +
        'Allowed extensions are .pkl and .pmml. If extension is .pmml, then both pmml and pkl file will be saved')
@click.argument('importances_path', type=click.Path(exists=False, dir_okay=False, file_okay=True) , help='Path to where the importances are saved')
def main(configuration_path, prediction_path, model_path, importances_path):
    '''
    Train a classifier on signal and background monte carlo data and write the model to MODEL_PATH in pmml or pickle format.
    '''

    check_extension(prediction_path)
    check_extension(importances_path)
    check_extension(model_path, allowed_extensions=['.pmml', '.pkl'])

    #load configuartion stuff
    with open(configuration_path) as f:
        config = yaml.load(f)

    sample = config['sample']
    query = config['query']
    num_cross_validations = config['n_cv']

    print("Loading data")
    df_gamma = read_data(config['signal_path'])
    df_proton = read_data(config['background_path'])

    if query:
        print('Quering with string: {}'.format(query))
        df_gamma = df_gamma.query(query)
        df_proton = df_proton.query(query)

    df_gamma['label_text'] = 'signal'
    df_gamma['label'] = 1
    df_proton['label_text'] = 'background'
    df_proton['label'] = 0


    if sample > 0:
        df_gamma = df_gamma.sample(sample)
        df_proton = df_proton.sample(sample)


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
    print('Starting {} fold cross validation... '.format(num_cross_validations) )
    cv = cross_validation.StratifiedKFold(y, n_folds=num_cross_validations)


    aucs =  []
    for fold, (train, test) in enumerate(tqdm(cv)):
        # select data
        xtrain, xtest = X[train], X[test]
        ytrain, ytest = y[train], y[test]
        # fit and predict
        classifier.fit(xtrain, ytrain)

        y_probas = classifier.predict_proba(xtest)[:, 1]
        y_prediction = classifier.predict(xtest)
        cv_predictions.append(pd.DataFrame({'label':ytest, 'label_prediction':y_prediction, 'probabilities':y_probas, 'cv_fold':fold}))
        aucs.append(metrics.roc_auc_score(ytest, y_prediction))
        #labels_predictions.append([ytest, y_prediction, y_probas])


    print('Mean AUC ROC : {}'.format(np.array(aucs).mean()))

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
