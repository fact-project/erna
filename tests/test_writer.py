import pandas as pd
from fact.io import read_h5py
import tempfile
import numpy as np

np.random.seed(0)
n_rows = 100
n_dfs = 10


def random_df(n_rows):
    return pd.DataFrame({
        'date': np.random.uniform(1.5e9, 1.55e9, n_rows).astype('datetime64[ns]'),
        'x': np.random.normal(size=n_rows),
        'n': np.random.poisson(20, size=n_rows),
    })


def test_jsonl():
    from erna.io import Writer

    with tempfile.NamedTemporaryFile(prefix='erna_test_', suffix='.jsonl') as f:
        with Writer(f.name) as writer:
            assert writer.fmt == 'jsonl'
            for i in range(n_dfs):
                writer.append(random_df(n_rows))

        df = pd.read_json(f.name, lines=True)
        assert len(df) == n_rows * n_dfs


def test_csv():
    from erna.io import Writer

    with tempfile.NamedTemporaryFile(prefix='erna_test_', suffix='.csv') as f:
        with Writer(f.name) as writer:
            assert writer.fmt == 'csv'
            for i in range(n_dfs):
                writer.append(random_df(n_rows))

        df = pd.read_csv(f.name)
        assert len(df) == n_rows * n_dfs


def test_hdf5():
    from erna.io import Writer

    with tempfile.NamedTemporaryFile(prefix='erna_test_', suffix='.hdf5') as f:
        with Writer(f.name) as writer:
            assert writer.fmt == 'hdf5'
            for i in range(n_dfs):
                writer.append(random_df(n_rows))

        df = read_h5py(f.name, key='events')
        assert len(df) == n_rows * n_dfs
