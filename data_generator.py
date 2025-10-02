import pandas as pd
import numpy as np
import time
import os
from datetime import datetime

# Criar diretório para dados se não existir
os.makedirs('data', exist_ok=True)

# Estações de monitoramento simuladas
stations = [
    # ---------------- RIO DE JANEIRO ----------------
    {"id": "RJ001", "name": "Centro", "lat": -22.9068, "lon": -43.1729, "city": "Rio de Janeiro"},
    {"id": "RJ002", "name": "Copacabana", "lat": -22.9687, "lon": -43.1869, "city": "Rio de Janeiro"},
    {"id": "RJ003", "name": "Tijuca", "lat": -22.9309, "lon": -43.2376, "city": "Rio de Janeiro"},
    {"id": "RJ004", "name": "Nova Iguaçu", "lat": -22.7598, "lon": -43.4516, "city": "Nova Iguaçu"},
    {"id": "RJ005", "name": "Niterói", "lat": -22.8832, "lon": -43.1034, "city": "Niterói"},
    {"id": "RJ006", "name": "Petrópolis", "lat": -22.5050, "lon": -43.1786, "city": "Petrópolis"},
    {"id": "RJ007", "name": "Campos dos Goytacazes", "lat": -21.7622, "lon": -41.3181, "city": "Campos dos Goytacazes"},
    {"id": "RJ008", "name": "Volta Redonda", "lat": -22.5202, "lon": -44.0996, "city": "Volta Redonda"},
    
    # ---------------- SÃO PAULO ----------------
    {"id": "SP001", "name": "Paulista", "lat": -23.5614, "lon": -46.6559, "city": "São Paulo"},
    {"id": "SP002", "name": "Pinheiros", "lat": -23.5610, "lon": -46.7025, "city": "São Paulo"},
    {"id": "SP003", "name": "Ibirapuera", "lat": -23.5874, "lon": -46.6576, "city": "São Paulo"},
    {"id": "SP004", "name": "Campinas", "lat": -22.9099, "lon": -47.0626, "city": "Campinas"},
    {"id": "SP005", "name": "Santos", "lat": -23.9540, "lon": -46.3336, "city": "Santos"},
    {"id": "SP006", "name": "São José dos Campos", "lat": -23.2237, "lon": -45.9009, "city": "São José dos Campos"},
    {"id": "SP007", "name": "Sorocaba", "lat": -23.5015, "lon": -47.4526, "city": "Sorocaba"},
    {"id": "SP008", "name": "Ribeirão Preto", "lat": -21.1786, "lon": -47.8066, "city": "Ribeirão Preto"},

    # ---------------- MINAS GERAIS ----------------
    {"id": "MG001", "name": "Savassi", "lat": -19.9350, "lon": -43.9336, "city": "Belo Horizonte"},
    {"id": "MG002", "name": "Pampulha", "lat": -19.8522, "lon": -43.9766, "city": "Belo Horizonte"},
    {"id": "MG003", "name": "Uberlândia", "lat": -18.9188, "lon": -48.2768, "city": "Uberlândia"},
    {"id": "MG004", "name": "Juiz de Fora", "lat": -21.7622, "lon": -43.3434, "city": "Juiz de Fora"},
    {"id": "MG005", "name": "Montes Claros", "lat": -16.7282, "lon": -43.8578, "city": "Montes Claros"},
    {"id": "MG006", "name": "Uberaba", "lat": -19.7477, "lon": -47.9310, "city": "Uberaba"},
    {"id": "MG007", "name": "Ouro Preto", "lat": -20.3856, "lon": -43.5036, "city": "Ouro Preto"},

    # ---------------- ESPÍRITO SANTO ----------------
    {"id": "ES001", "name": "Vitória", "lat": -20.3155, "lon": -40.3128, "city": "Vitória"},
    {"id": "ES002", "name": "Vila Velha", "lat": -20.3417, "lon": -40.2875, "city": "Vila Velha"},
    {"id": "ES003", "name": "Serra", "lat": -20.1211, "lon": -40.3074, "city": "Serra"},
    {"id": "ES004", "name": "Cariacica", "lat": -20.2637, "lon": -40.4165, "city": "Cariacica"},
    {"id": "ES005", "name": "Guarapari", "lat": -20.6771, "lon": -40.5093, "city": "Guarapari"}
]




# Limites para alertas
PM2_5_ALERT = 50.0
CO2_ALERT = 450.0
O3_ALERT = 80.0

def generate_data():
    """Gera dados simulados de qualidade do ar"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = []
    
    for station in stations:
        # Gerar valores aleatórios para os poluentes
        co2 = np.random.uniform(380, 420)
        pm2_5 = np.random.uniform(15, 35)
        o3 = np.random.uniform(30, 60)
        
        # Simular picos ocasionalmente (10% de chance)
        if np.random.random() < 0.1:
            co2 += np.random.uniform(50, 150)
            pm2_5 += np.random.uniform(20, 60)
            o3 += np.random.uniform(10, 30)
        
        data.append({
            "timestamp": timestamp,
            "station_id": station["id"],
            "station_name": station["name"],
            "latitude": station["lat"],
            "longitude": station["lon"],
            "city": station["city"],
            "CO2": round(co2, 2),
            "PM2_5": round(pm2_5, 2),
            "O3": round(o3, 2)
        })
    
    return pd.DataFrame(data)

def main():
    """Função principal que gera dados continuamente"""
    print("Iniciando gerador de dados simulados...")
    
    try:
        while True:
            # Gerar dados
            df = generate_data()
            
            # Salvar em um arquivo CSV com timestamp
            filename = f"data/air_quality_{int(time.time())}.csv"
            df.to_csv(filename, index=False)
            print(f"Dados salvos em {filename}")
            
            # Aguardar antes da próxima geração
            time.sleep(5)  # 5 segundos entre cada geração
            
    except KeyboardInterrupt:
        print("\nGerador de dados interrompido.")

if __name__ == "__main__":
    main()
