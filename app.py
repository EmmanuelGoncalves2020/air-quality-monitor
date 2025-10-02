from flask import Flask, render_template, jsonify, request, send_file
import pandas as pd
import glob
import os
import json
from datetime import datetime, timedelta
import io
import random
import math

app = Flask(__name__)

@app.route('/')
def index():
    """Renderiza a página principal"""
    return render_template('index.html')

@app.route('/api/latest')
def get_latest_data():
    """Retorna os dados mais recentes processados pelo Spark"""
    try:
        processed_dirs = glob.glob('results/processed/processed_*')
        if not processed_dirs:
            return jsonify({"error": "Nenhum dado processado encontrado", "data": []})
        
        latest_dir = max(processed_dirs, key=os.path.getmtime)
        csv_files = glob.glob(f"{latest_dir}/*.csv")
        if not csv_files:
            return jsonify({"error": "Nenhum arquivo CSV encontrado", "data": []})
        
        latest_file = csv_files[0]
        df = pd.read_csv(latest_file)
        
        # Adicionar tendências simuladas
        data = df.to_dict(orient='records')
        for station in data:
            # Simular tendência (melhorando, piorando, estável)
            station['trend'] = random.choice(['up', 'down', 'stable'])
            station['trend_percentage'] = round(random.uniform(-15, 15), 1)
            
            # Calcular AQI simplificado
            pm25 = float(station.get('avg_PM2_5', 0))
            station['aqi'] = calculate_aqi(pm25)
        
        return jsonify(data)
    
    except Exception as e:
        print(f"Erro na API /api/latest: {e}")
        return jsonify({"error": str(e), "data": []})

def calculate_aqi(pm25):
    """Calcula AQI simplificado baseado no PM2.5"""
    if pm25 <= 12:
        return {"value": int(pm25 * 4.17), "category": "Bom", "color": "#28a745"}
    elif pm25 <= 35.4:
        return {"value": int(51 + (pm25 - 12.1) * 2.1), "category": "Moderado", "color": "#ffc107"}
    elif pm25 <= 55.4:
        return {"value": int(101 + (pm25 - 35.5) * 2.5), "category": "Insalubre para Grupos Sensíveis", "color": "#fd7e14"}
    elif pm25 <= 150.4:
        return {"value": int(151 + (pm25 - 55.5) * 1.05), "category": "Insalubre", "color": "#dc3545"}
    else:
        return {"value": int(201 + (pm25 - 150.5) * 0.67), "category": "Muito Insalubre", "color": "#6f42c1"}

@app.route('/api/alerts')
def get_alerts():
    """Retorna os alertas mais recentes"""
    try:
        alert_dirs = glob.glob('results/alerts/alerts_*')
        if not alert_dirs:
            return jsonify([])
        
        latest_dir = max(alert_dirs, key=os.path.getmtime)
        csv_files = glob.glob(f"{latest_dir}/*.csv")
        if not csv_files:
            return jsonify([])
        
        latest_file = csv_files[0]
        df = pd.read_csv(latest_file)
        alerts = df.to_dict(orient='records')
        
        return jsonify(alerts)
    
    except Exception as e:
        print(f"Erro na API /api/alerts: {e}")
        return jsonify([])

@app.route('/api/cities')
def get_cities():
    """Retorna lista de cidades disponíveis"""
    try:
        processed_dirs = glob.glob('results/processed/processed_*')
        if not processed_dirs:
            return jsonify([])
        
        latest_dir = max(processed_dirs, key=os.path.getmtime)
        csv_files = glob.glob(f"{latest_dir}/*.csv")
        if not csv_files:
            return jsonify([])
        
        latest_file = csv_files[0]
        df = pd.read_csv(latest_file)
        cities = df['city'].unique().tolist() if 'city' in df.columns else []
        
        return jsonify(cities)
    
    except Exception as e:
        print(f"Erro na API /api/cities: {e}")
        return jsonify([])

@app.route('/api/data/filter')
def get_filtered_data():
    """Retorna dados filtrados por cidade"""
    try:
        city = request.args.get('city', '')
        search = request.args.get('search', '')
        
        processed_dirs = glob.glob('results/processed/processed_*')
        if not processed_dirs:
            return jsonify({"error": "Nenhum dado processado encontrado", "data": []})
        
        latest_dir = max(processed_dirs, key=os.path.getmtime)
        csv_files = glob.glob(f"{latest_dir}/*.csv")
        if not csv_files:
            return jsonify({"error": "Nenhum arquivo CSV encontrado", "data": []})
        
        latest_file = csv_files[0]
        df = pd.read_csv(latest_file)
        
        # Filtrar por cidade se especificada
        if city and city != 'all':
            df = df[df['city'] == city]
        
        # Filtrar por busca se especificada
        if search:
            df = df[df['station_name'].str.contains(search, case=False, na=False)]
        
        data = df.to_dict(orient='records')
        
        # Adicionar tendências e AQI
        for station in data:
            station['trend'] = random.choice(['up', 'down', 'stable'])
            station['trend_percentage'] = round(random.uniform(-15, 15), 1)
            pm25 = float(station.get('avg_PM2_5', 0))
            station['aqi'] = calculate_aqi(pm25)
        
        return jsonify(data)
    
    except Exception as e:
        print(f"Erro na API /api/data/filter: {e}")
        return jsonify({"error": str(e), "data": []})

@app.route('/api/statistics/city')
def get_city_statistics():
    """Retorna estatísticas por cidade"""
    try:
        processed_dirs = glob.glob('results/processed/processed_*')
        if not processed_dirs:
            return jsonify({})
        
        latest_dir = max(processed_dirs, key=os.path.getmtime)
        csv_files = glob.glob(f"{latest_dir}/*.csv")
        if not csv_files:
            return jsonify({})
        
        latest_file = csv_files[0]
        df = pd.read_csv(latest_file)
        
        city_stats = {}
        for city in df['city'].unique():
            city_data = df[df['city'] == city]
            avg_pm25 = float(city_data['avg_PM2_5'].mean()) if 'avg_PM2_5' in city_data.columns else 0
            
            city_stats[city] = {
                'stations': len(city_data),
                'avg_pm25': avg_pm25,
                'avg_co2': float(city_data['avg_CO2'].mean()) if 'avg_CO2' in city_data.columns else 0,
                'avg_o3': float(city_data['avg_O3'].mean()) if 'avg_O3' in city_data.columns else 0,
                'max_pm25': float(city_data['avg_PM2_5'].max()) if 'avg_PM2_5' in city_data.columns else 0,
                'min_pm25': float(city_data['avg_PM2_5'].min()) if 'avg_PM2_5' in city_data.columns else 0,
                'aqi': calculate_aqi(avg_pm25),
                'who_compliance': 'Sim' if avg_pm25 <= 15 else 'Não',  # Limite OMS para PM2.5
                'trend': random.choice(['improving', 'worsening', 'stable'])
            }
        
        return jsonify(city_stats)
    
    except Exception as e:
        print(f"Erro na API /api/statistics/city: {e}")
        return jsonify({})

@app.route('/api/ranking')
def get_city_ranking():
    """Retorna ranking de cidades por qualidade do ar"""
    try:
        processed_dirs = glob.glob('results/processed/processed_*')
        if not processed_dirs:
            return jsonify({"best": [], "worst": []})
        
        latest_dir = max(processed_dirs, key=os.path.getmtime)
        csv_files = glob.glob(f"{latest_dir}/*.csv")
        if not csv_files:
            return jsonify({"best": [], "worst": []})
        
        latest_file = csv_files[0]
        df = pd.read_csv(latest_file)
        
        # Calcular média por cidade
        city_averages = []
        for city in df['city'].unique():
            city_data = df[df['city'] == city]
            avg_pm25 = float(city_data['avg_PM2_5'].mean()) if 'avg_PM2_5' in city_data.columns else 0
            
            city_averages.append({
                'city': city,
                'avg_pm25': avg_pm25,
                'aqi': calculate_aqi(avg_pm25),
                'stations': len(city_data)
            })
        
        # Ordenar por PM2.5 (menor = melhor)
        city_averages.sort(key=lambda x: x['avg_pm25'])
        
        return jsonify({
            "best": city_averages[:3],  # 3 melhores
            "worst": city_averages[-3:][::-1]  # 3 piores (invertido)
        })
    
    except Exception as e:
        print(f"Erro na API /api/ranking: {e}")
        return jsonify({"best": [], "worst": []})

@app.route('/api/history')
def get_history():
    """Retorna histórico de dados para gráficos com variações reais"""
    try:
        # Gerar dados históricos mais realistas
        history_data = []
        base_time = datetime.now()
        
        # Valores base
        base_pm25 = 25
        base_co2 = 400
        base_o3 = 45
        
        for i in range(24):  # Últimas 24 horas
            time_point = base_time - timedelta(hours=23-i)
            
            # Simular padrões realistas (pior durante rush hours, melhor de madrugada)
            hour = time_point.hour
            
            # Fator de tráfego (pior às 8h, 18h)
            traffic_factor = 1 + 0.3 * math.sin((hour - 6) * math.pi / 12) if 6 <= hour <= 20 else 0.7
            
            # Variação aleatória
            random_factor = 1 + random.uniform(-0.2, 0.2)
            
            # Tendência geral (melhora ao longo do dia)
            trend_factor = 1 - (i * 0.01)
            
            pm25_value = base_pm25 * traffic_factor * random_factor * trend_factor
            co2_value = base_co2 * traffic_factor * random_factor
            o3_value = base_o3 * (1.2 - traffic_factor * 0.3) * random_factor  # O3 inverso ao tráfego
            
            history_data.append({
                'timestamp': time_point.strftime('%H:%M'),
                'full_timestamp': time_point.isoformat(),
                'avg_pm25': round(max(5, pm25_value), 1),
                'avg_co2': round(max(350, co2_value), 1),
                'avg_o3': round(max(20, o3_value), 1)
            })
        
        return jsonify(history_data)
    
    except Exception as e:
        print(f"Erro na API /api/history: {e}")
        return jsonify([])

@app.route('/api/environmental-goals')
def get_environmental_goals():
    """Retorna progresso em relação às metas ambientais"""
    try:
        processed_dirs = glob.glob('results/processed/processed_*')
        if not processed_dirs:
            return jsonify({})
        
        latest_dir = max(processed_dirs, key=os.path.getmtime)
        csv_files = glob.glob(f"{latest_dir}/*.csv")
        if not csv_files:
            return jsonify({})
        
        latest_file = csv_files[0]
        df = pd.read_csv(latest_file)
        
        # Calcular médias globais
        avg_pm25 = float(df['avg_PM2_5'].mean()) if 'avg_PM2_5' in df.columns else 0
        avg_co2 = float(df['avg_CO2'].mean()) if 'avg_CO2' in df.columns else 0
        avg_o3 = float(df['avg_O3'].mean()) if 'avg_O3' in df.columns else 0
        
        # Metas da OMS e outras organizações
        goals = {
            'pm25': {
                'current': avg_pm25,
                'target': 15,  # OMS 2021
                'unit': 'μg/m³',
                'progress': min(100, max(0, (15 - avg_pm25) / 15 * 100)),
                'status': 'achieved' if avg_pm25 <= 15 else 'in_progress'
            },
            'co2': {
                'current': avg_co2,
                'target': 350,  # Pré-industrial + margem
                'unit': 'ppm',
                'progress': min(100, max(0, (450 - avg_co2) / 100 * 100)),  # Meta até 450ppm
                'status': 'in_progress' if avg_co2 <= 450 else 'critical'
            },
            'o3': {
                'current': avg_o3,
                'target': 60,  # Limite recomendado
                'unit': 'μg/m³',
                'progress': min(100, max(0, (60 - avg_o3) / 60 * 100)),
                'status': 'achieved' if avg_o3 <= 60 else 'in_progress'
            }
        }
        
        return jsonify(goals)
    
    except Exception as e:
        print(f"Erro na API /api/environmental-goals: {e}")
        return jsonify({})

@app.route('/api/report/download')
def download_report():
    """Gera e baixa relatório em CSV"""
    try:
        format_type = request.args.get('format', 'csv')
        os.makedirs('reports', exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        processed_dirs = glob.glob('results/processed/processed_*')
        if not processed_dirs:
            return jsonify({"error": "Nenhum dado disponível para relatório"})
        
        latest_dir = max(processed_dirs, key=os.path.getmtime)
        csv_files = glob.glob(f"{latest_dir}/*.csv")
        if not csv_files:
            return jsonify({"error": "Nenhum arquivo CSV encontrado"})
        
        latest_file = csv_files[0]
        df = pd.read_csv(latest_file)
        
        # Adicionar informações do relatório
        df['data_relatorio'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['sistema'] = 'Monitor de Qualidade do Ar - Apache Spark'
        
        if format_type == 'json':
            report_filename = f"reports/relatorio_qualidade_ar_{timestamp}.json"
            df.to_json(report_filename, orient='records', indent=2)
            return send_file(report_filename, as_attachment=True, download_name=f"relatorio_qualidade_ar_{timestamp}.json")
        else:
            report_filename = f"reports/relatorio_qualidade_ar_{timestamp}.csv"
            df.to_csv(report_filename, index=False, encoding='utf-8-sig')
            return send_file(report_filename, as_attachment=True, download_name=f"relatorio_qualidade_ar_{timestamp}.csv")
    
    except Exception as e:
        print(f"Erro ao gerar relatório: {e}")
        return jsonify({"error": f"Erro ao gerar relatório: {str(e)}"})

@app.route('/api/test')
def test_api():
    """Endpoint de teste para verificar se a API está funcionando"""
    return jsonify({
        "status": "OK",
        "message": "API funcionando corretamente",
        "processed_files": len(glob.glob('results/processed/processed_*')),
        "alert_files": len(glob.glob('results/alerts/alerts_*')),
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
