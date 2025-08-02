import pdfplumber
import pandas as pd

# Caminho para o PDF baixado da Weg, por exemplo
caminho_pdf = "weg_relatorio_trimestral.pdf"

dados_extraidos = []

with pdfplumber.open(caminho_pdf) as pdf:
    for pagina in pdf.pages:
        # Tenta extrair tabelas da página
        tabelas = pagina.extract_tables()
        for tabela in tabelas:
            df = pd.DataFrame(tabela[1:], columns=tabela[0])
            dados_extraidos.append(df)

# Concatenar todos os DataFrames encontrados
resultado = pd.concat(dados_extraidos, ignore_index=True)

# Exibir ou salvar
print(resultado.head())
resultado.to_csv("weg_dados_financeiros.csv", index=False)
