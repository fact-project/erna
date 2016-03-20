import pandas as pd
import click

from sklearn.externals import joblib
from os import path
import json
import config

def write_data(df, file_path, hdf_key='table'):
    name, extension =  path.splitext(file_path)
    if extension in ['.hdf', '.hdf5', '.h5']:
        df.to_hdf(file_path, key=hdf_key)
    elif extension == '.json':
        df.to_json(file_path)
    else:
        print('cannot write tabular data with extension {}'.format(extension))

def read_data(file_path, hdf_key='table'):
    name, extension =  path.splitext(file_path)
    if extension in ['.hdf', '.hdf5', '.h5']:
        return pd.read_hdf(file_path, key=hdf_key)
    if extension == '.json':
        with open(file_path, 'r') as j:
            d = json.load(j)
            return pd.DataFrame(d)

def check_extension(file_path, allowed_extensions= ['.hdf', '.hdf5', '.h5', 'json']):
    p, extension = path.splitext(file_path)
    if not extension in allowed_extensions:
        print('Extension {} not allowed here.'.format(extension))

@click.command()
@click.argument('data_path', type=click.Path(exists=True, dir_okay=False, file_okay=True) )
@click.argument('model_path', type=click.Path(exists=True, dir_okay=False, file_okay=True) )
@click.argument('output_path', type=click.Path(exists=False, dir_okay=False, file_okay=True) )
def main(data_path, model_path, output_path):
    '''
    Apply loaded model to data
    '''
    check_extension(output_path, allowed_extensions=['.csv'])

    model = joblib.load(model_path)
    df_data = read_data(data_path)[config.training_variables].dropna()
    prediction = model.predict_proba(df_data)
    df_data['signal_prediction'] = prediction[:,1]

    thetas = df_data['Theta']
    df_data['background_prediction'] = 0
    for key in ['Theta_Off_1', 'Theta_Off_2', 'Theta_Off_3', 'Theta_Off_4', 'Theta_Off_5']:
        df_data['Theta'] = df_data[key]
        prediction = model.predict_proba(df_data)[:,1]
        mask = (prediction > df_data['background_prediction'])
        df_data['background_prediction'] = prediction[mask]
        df_data['background_theta'] = df_data['Theta'][mask]

    df_data['Theta'] = thetas

    out = pd.DataFrame(df_data)
    out.to_csv(output_path, index_label='index')


if __name__ == '__main__':
    main()
