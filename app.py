from flask import Flask, render_template, jsonify
import pandas as pd
import glob
import os
import json

app = Flask(__name__)

@app.route('/')
def index():
    """Renderiza a página principal"""
    return render_template('index.html')

@app.route('/api/latest')
def get_latest_data():
    """Retorna os dados mais recentes processados pelo Spark"""
    try:
        # Encontrar os arquivos mais recentes na pasta processed
        processed_dirs = glob.glob('results/processed/processed_*')
        if not processed_dirs:
            return jsonify({"error": "Nenhum dado processado encontrado", "data": []})
        
        # Ordenar por data de modificação (mais recente primeiro)
        latest_dir = max(processed_dirs, key=os.path.getmtime)
        
        # Encontrar o arquivo CSV dentro do diretório
        csv_files = glob.glob(f"{latest_dir}/*.csv")
        if not csv_files:
            return jsonify({"error": "Nenhum arquivo CSV encontrado", "data": []})
        
        latest_file = csv_files[0]  # Pegar o primeiro arquivo CSV
        
        # Ler o arquivo CSV
        df = pd.read_csv(latest_file)
        
        # Converter para lista de dicionários
        data = df.to_dict(orient='records')
        
        return jsonify(data)
    
    except Exception as e:
        print(f"Erro na API /api/latest: {e}")
        return jsonify({"error": str(e), "data": []})

@app.route('/api/alerts')
def get_alerts():
    """Retorna os alertas mais recentes"""
    try:
        # Encontrar os arquivos de alerta
        alert_dirs = glob.glob('results/alerts/alerts_*')
        if not alert_dirs:
            print("aqui")
            return jsonify([])
        
        # Ordenar por data de modificação (mais recente primeiro)
        latest_dir = max(alert_dirs, key=os.path.getmtime)
        
        # Encontrar o arquivo CSV dentro do diretório
        csv_files = glob.glob(f"{latest_dir}/*.csv")
        if not csv_files:
            return jsonify([])

        
        latest_file = csv_files[0]  # Pegar o primeiro arquivo CSV
        
        # Ler o arquivo CSV
        df = pd.read_csv(latest_file)
        
        # Converter para lista de dicionários
        alerts = df.to_dict(orient='records')
        
        return jsonify(alerts)
    
    except Exception as e:
        print(f"Erro na API /api/alerts: {e}")
        return jsonify([])

@app.route('/api/test')
def test_api():
    """Endpoint de teste para verificar se a API está funcionando"""
    return jsonify({
        "status": "OK",
        "message": "API funcionando corretamente",
        "processed_files": len(glob.glob('results/processed/processed_*')),
        "alert_files": len(glob.glob('results/alerts/alerts_*'))
    })

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
