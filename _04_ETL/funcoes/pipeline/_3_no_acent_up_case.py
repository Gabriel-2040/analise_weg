# tudo sem acento e caixa alta

import pandas as pd
import os
import sys
from pathlib import Path  # Melhor forma para trabalhar com caminhos
from unidecode import unidecode

def _upper_noacent():
    for arquivo in os.listdir(pivotados):
        if arquivo.endswith('.csv'):
            entrada = os.path.join(pivotados, arquivo)
            saida = os.path.join(tratados, arquivo)
            try:
                df = pd.read_csv(entrada, encoding='utf-8')
                # Normaliza os nomes das colunas para maiúsculas e sem acentos
                df.columns = [unidecode(str(col).upper()) for col in df.columns]
                df = df.dropna()

                # Converte todos os valores de string para maiúsculas e sem acentos
                for col in df.select_dtypes(include='object').columns:
                    df[col] = df[col].apply(
                        lambda val: unidecode(str(val).strip().upper()) if pd.notnull(val) else val
                    )

                df.to_csv(saida, index=False, encoding='utf-8')
                print(f"Convertido: {arquivo}")
            except Exception as e:
                print(f"Erro ao converter {arquivo}: {e}")