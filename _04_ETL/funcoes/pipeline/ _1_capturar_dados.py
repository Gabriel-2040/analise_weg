import requests
import pandas as pd
import json

def _captura_conta_dados(ti):
    urls = {
        1: 'https://api.mziq.com/mzfilemanager/v2/d/50c1bd3e-8ac6-42d9-884f-b9d69f690602/1e5cf2d6-2d39-de14-189a-6b11350afaac?origin=1',
        2: 'https://api.mziq.com/mzfilemanager/v2/d/50c1bd3e-8ac6-42d9-884f-b9d69f690602/b7bb518f-e7a7-a9e3-7942-7bece84d759e?origin=1',
        3: 'https://api.mziq.com/mzfilemanager/v2/d/50c1bd3e-8ac6-42d9-884f-b9d69f690602/b7bb518f-e7a7-a9e3-7942-7bece84d759e?origin=1'
    }

    for i, url in urls.items():
        response = requests.get(url)
        df = pd.DataFrame(json.loads(response.content))
        qtd = len(df.index)
        ti.xcom_push(key=f'qtd_{i}', value=qtd)
