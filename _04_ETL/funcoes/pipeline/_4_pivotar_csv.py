import pandas as pd
import os
import sys
from pathlib import Path  # Melhor forma para trabalhar com caminhos
from unidecode import unidecode


def _pivotar_csv(input_folder, output_folder):
    """Processa todos os arquivos CSV na pasta de entrada e salva na pasta de saída"""
    # Garante que a pasta de saída existe
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    for arquivo in os.listdir(input_folder):
        if arquivo.endswith('.csv') or arquivo.endswith('.xlsx'):
            try:
                nome = os.path.splitext(arquivo)[0]
                file_path = os.path.join(input_folder, arquivo)
                #trasnformar arquivo para csv e retirar a prima linha
                if arquivo.endswith('.xlsx'):
                    df = pd.read_excel(file_path, header=1)
                    #cria um  nome temporário para o CSV
                    temp_csv_path = os.path.join(input_folder, f"temp_{nome}.csv")
                    df.to_csv(temp_csv_path, index=False, encoding='utf-8')
                    file_path = temp_csv_path
                # Ler o CSV
                df = pd.read_csv(file_path, encoding='utf-8')
                
                # Transformar de wide para long
                df_long = df.melt(
                    id_vars=['Código da Conta','Descrição da Conta'],
                    var_name='data',
                    value_name='valor'
                )
           
                # Tratamento de nulos
                df_long = df_long.assign(valor =lambda x: x['valor'].fillna(0))
                                        
                # Ordenação
                df_long = df_long.sort_values(['Código da Conta','data'])
                
                # Salvar arquivo processado
                output_file = f"{nome}_rotacionado.csv"
                output_path = os.path.join(output_folder, output_file)
                df_long.to_csv(output_path, index=False, encoding='utf-8')
                
                print(f"Arquivo {arquivo} processado com sucesso! Salvo como {output_file}")
                print(df_long.head())
                
            except Exception as e:
                print(f"Erro ao processar o arquivo {arquivo}: {str(e)}")

