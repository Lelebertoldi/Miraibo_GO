import pandas as pd

csv = "D:/GitHub/MiraiboGO/album/miraibos.csv"
salvar_parquet = "D:/GitHub/MiraiboGO/breed/breed.parquet"


# NÃO CHAMAR DIRETAMENTE
# Processa um unico personagem do csv iterando em todo o arquivo
def processar_personagens(csv_file, id_inicial):
    # Lê o CSV em um DataFrame
    df = pd.read_csv(csv_file)
    
    # Lista para armazenar o resultado
    resultado = []
    
    # Itera em cada linha do DataFrame
    for index_1, row_1 in df.iterrows():
        # Pula a linha se o nome estiver vazio
        if pd.isna(row_1['nome']):
            continue
        
        # Itera novamente para criar as combinações (incluindo o próprio id)
        for index_2, row_2 in df.iterrows():
            # Pula se o nome estiver vazio na segunda iteração
            if pd.isna(row_2['nome']):
                continue
            
            # Cria o dicionário de resultados
            linha_resultado = {
                'id_1': row_1['id'],
                'nome_1': row_1['nome'],
                'id_2': row_2['id'],
                'nome_2': row_2['nome'],
                'id_resultado': 'não catalogado',
                'nome_resultado': 'não catalogado'
            }
            
            # Adiciona a linha à lista de resultados
            resultado.append(linha_resultado)
    
    # Retorna o resultado salvo em uma variável
    return resultado

# Exemplo de uso:
# resultado = processar_personagens('personagens.csv', '39')
# O resultado será uma lista de dicionários


# Processa um unico personagem do csv iterando em todo o arquivo, ignorando linhas ja existentes,
# fazer append para um unico personagem ou chamar na proxima funçao para multiplos personagens
def adicionar_personagem_no_parquet(csv_file, personagem_id, caminho_parquet):
    # Lê o arquivo Parquet existente para verificar as combinações
    try:
        df_existente = pd.read_parquet(caminho_parquet)
    except FileNotFoundError:
        df_existente = pd.DataFrame(columns=['id_1', 'nome_1', 'id_2', 'nome_2', 'id_resultado', 'nome_resultado'])
    
    # Processa o novo personagem
    novo_resultado = processar_personagens(csv_file, personagem_id)
    
    if not novo_resultado:
        print("Nenhum dado a adicionar.")
        return df_existente
    
    # Converte o novo resultado em DataFrame
    df_novo_resultado = pd.DataFrame(novo_resultado)
    
    # Verifica se as combinações já existem no arquivo Parquet
    df_novo_resultado['combination'] = df_novo_resultado['id_1'].astype(str) + '_' + df_novo_resultado['id_2'].astype(str)
    df_existente['combination'] = df_existente['id_1'].astype(str) + '_' + df_existente['id_2'].astype(str)
    
    # Filtra novas combinações que não existem no DataFrame existente
    novas_combinacoes = df_novo_resultado[~df_novo_resultado['combination'].isin(df_existente['combination'])]
    
    if not novas_combinacoes.empty:
        # Faz concat das novas combinações com o DataFrame existente
        df_final = pd.concat([df_existente, novas_combinacoes], ignore_index=True)
        print(f"Novas combinações de {personagem_id} prontas para serem salvas.")
    else:
        print(f"Nenhuma nova combinação para {personagem_id} foi encontrada.")
        df_final = df_existente
    
    # Remove a coluna temporária de combinação
    df_final.drop(columns=['combination'], inplace=True)
    
    return df_final



# Itera a função anterior em todo o csv
def iterar_todos_personagens_parquet(csv_file, caminho_parquet):
    # Lê o CSV em um DataFrame
    df = pd.read_csv(csv_file)
    
    # Lista para armazenar todos os resultados combinados
    todos_resultados = []
    
    # Itera sobre cada linha do DataFrame para pegar o ID e Nome de cada personagem
    for index, row in df.iterrows():
        # Pula a linha se o nome estiver vazio
        if pd.isna(row['nome']):
            continue
        
        # Chama a função adicionar_personagem_no_parquet para este id
        resultados = adicionar_personagem_no_parquet(csv_file, row['id'], caminho_parquet)
        
        # Adiciona o resultado para este id à lista total
        todos_resultados.append(resultados)
    
    # Retorna todos os resultados juntos
    return todos_resultados



# Salva em um parquet
def salvar_dados_em_parquet(dados, caminho_parquet):
    # Cria um DataFrame a partir dos dados fornecidos
    df_novos_dados = pd.concat(dados, ignore_index=True)
    
    # Tenta ler os dados existentes no arquivo Parquet
    try:
        df_existente = pd.read_parquet(caminho_parquet)
        # Concatena os dados novos com os existentes
        df_final = pd.concat([df_existente, df_novos_dados], ignore_index=True)
    except FileNotFoundError:
        # Se o arquivo não existir, usa apenas os novos dados
        df_final = df_novos_dados
    
    # Salva o DataFrame final no arquivo Parquet
    df_final.to_parquet(caminho_parquet, index=False)
    print(f"Dados salvos no arquivo {caminho_parquet} com append.")


# Atualiza id_resultado e nome_resultado qdo pais forem iguais
def atualizar_resultados_pais_iguais(caminho_parquet):
    # Lê o arquivo Parquet existente
    try:
        df = pd.read_parquet(caminho_parquet)
    except FileNotFoundError:
        print(f"O arquivo {caminho_parquet} não foi encontrado.")
        return
    
    # Atualiza id_resultado e nome_resultado
    mask = df['id_1'] == df['id_2']
    df.loc[mask, 'id_resultado'] = df.loc[mask, 'id_1']
    df.loc[mask, 'nome_resultado'] = df.loc[mask, 'nome_1']
    
    # Salva as alterações de volta ao arquivo Parquet
    df.to_parquet(caminho_parquet, index=False)
    print(f"Resultados atualizados no arquivo {caminho_parquet}.")



# dados_resultados = iterar_todos_personagens_parquet(csv, salvar_parquet)
# salvar_dados_em_parquet(dados_resultados, salvar_parquet)
# atualizar_resultados_pais_iguais(salvar_parquet)



# Remove linhas inteiras duplicadas
def remover_duplicatas_inteiras(caminho_arquivo, caminho_saida):

    try:
        # Carregar o DataFrame a partir do arquivo Parquet
        df = pd.read_parquet(caminho_arquivo)

        # Remover duplicatas inteiras
        df_sem_duplicatas = df.drop_duplicates()

        # Salvar o DataFrame sem duplicatas
        df_sem_duplicatas.to_parquet(caminho_saida)
        print(f"Duplicatas removidas e arquivo salvo como '{caminho_saida}'.")
    
    except Exception as e:
        print(f"Ocorreu um erro: {e}")


remover_duplicatas_inteiras(salvar_parquet, salvar_parquet)
