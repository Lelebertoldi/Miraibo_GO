from flask import Flask, request, jsonify, render_template
import pandas as pd

app = Flask(__name__)

# Defina o caminho para o arquivo Parquet
caminho_arquivo = 'data/breed.parquet'

def pesquisar_breed_por_id(caminho_arquivo, id_resultado):
    try:
        df = pd.read_parquet(caminho_arquivo)
        id_resultado = str(id_resultado)
        # Verifica se o 'id_resultado' existe em qualquer coluna
        resultado = df[df['id_resultado'] == id_resultado]

        if not resultado.empty:
            return resultado  # Retorna todas as colunas se resultados encontrados
        else:
            return pd.DataFrame()  # Retorna um DataFrame vazio se não encontrar resultados
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return pd.DataFrame()

def pesquisar_breed_por_nome(caminho_arquivo, nome_resultado):
    try:
        df = pd.read_parquet(caminho_arquivo)
        # Verifica se o 'nome_resultado' existe em qualquer coluna
        resultado = df[df['nome_resultado'] == nome_resultado]

        if not resultado.empty:
            return resultado  # Retorna todas as colunas se resultados encontrados
        else:
            return pd.DataFrame()  # Retorna um DataFrame vazio se não encontrar resultados
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return pd.DataFrame()


@app.route('/')
def index():
    return render_template('index.html')

# Rota de pesquisa
@app.route('/search_by_id', methods=['GET'])
def search_by_id():
    id_resultado = request.args.get('id')
    
    if id_resultado is None:
        return jsonify({"error": "ID de pesquisa não fornecido"}), 400
    
    resultado = pesquisar_breed_por_id(caminho_arquivo, id_resultado)
    
    if resultado.empty:
        return jsonify({"message": f"Nenhum breed encontrado para o ID: {id_resultado}"}), 404
    else:
        # Converte para JSON com orient="records" para obter uma estrutura clara
        return jsonify({"resultados": resultado.to_dict(orient="records")})


@app.route('/search_by_name', methods=['GET'])
def search_by_name():
    nome_resultado = request.args.get('name')
    
    if nome_resultado is None:
        return jsonify({"error": "Nome de pesquisa não fornecido"}), 400
    
    resultado = pesquisar_breed_por_nome(caminho_arquivo, nome_resultado)
    
    if resultado.empty:
        return jsonify({"message": f"Nenhum breed encontrado para o nome: {nome_resultado}"}), 404
    else:
        # Converte para JSON com orient="records" para obter uma estrutura clara
        return jsonify({"resultados": resultado.to_dict(orient="records")})

if __name__ == '__main__':
    app.run(debug=True)
