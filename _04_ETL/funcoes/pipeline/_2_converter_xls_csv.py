import pandas as pd
import os
import sys
from pathlib import Path  # Melhor forma para trabalhar com caminhos
from unidecode import unidecode

def _converter_xls_csv(pasta_origem, pasta_destino): 
    for arquivo in os.listdir(pasta_origem):
        if arquivo.endswith('.xlsx'):
            entrada = os.path.join(pasta_origem, arquivo)
            saida = os.path.join(pasta_destino, arquivo.replace('.xlsx', '.csv'))
            try:
                df = pd.read_excel(entrada, header=1, engine='openpyxl')
                df.to_csv(saida, index=False)
                print(f"Convertido: {arquivo}")
            except Exception as e:
                print(f"Erro ao converter {arquivo}: {e}")