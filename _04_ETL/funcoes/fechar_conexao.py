def fechar_conexao(conexao, cursor):
    #encerrar conexão com banco postgres
    try:
        if conexao:
            conexao.commit()  # Commit any pending transaction
            cursor.close()    # Close the cursor
            conexao.close()   # Close the connection
            print("Conexão fechada com sucesso!")
    except psycopg2.Error as e:
        print(f"Erro ao fechar a conexão com o banco de dados: {e}")

if __name__ == "__main__":
   fechar_conexao(None, None)
   if fechar_conexao(None, None):
       print("Conexão fechada com sucesso!")
   else:
       print("Falha ao fechar a conexão com o banco de dados.")