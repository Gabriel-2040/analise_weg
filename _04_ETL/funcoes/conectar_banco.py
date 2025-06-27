import psycopg2

def conectar_banco():
    #estabelecer conexão com banco postgres
    try:
        conexao = psycopg2.connect(
            host = "localhost",
            port = 5433,
            database = "analise_weg",
            user = "postgres",
            password = "1234",
            options='-c client_encoding=LATIN1'
        )
        cursor = conexao.cursor()
        print("Conexão estabelecida com sucesso!")
        return conexao, cursor
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None, None

# if __name__ == "__main__":
#     conectar_banco()
#     if conectar_banco():
#         print("Conexão estabelecida com sucesso!")
#     else:
#         print("Falha ao estabelecer conexão com o banco de dados.")