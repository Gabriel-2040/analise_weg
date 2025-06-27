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