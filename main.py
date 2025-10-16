import requests
import pandas as pd
import time
import random
import os

# --- CONFIGURAÇÕES GLOBAIS E CHAVE DE API ---

# O Google Places API requer uma chave de API. 
# VAMOS CARREGAR A CHAVE DIRETAMENTE DO AMBIENTE (Próxima etapa!)
GOOGLE_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY") 

# Endpoint base da API de busca
PLACE_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# Nichos de busca (termos otimizados para a API do Google)
NICHOS_PLACES = {
    "Futebol": ["clubes de futebol", "academias de futebol"],
    "Fitness": ["ginásios", "personal trainer studios"],
    "Medicina": ["clínicas médicas", "consultórios dentários", "fisioterapeutas"]
}

# Localização de foco (Portugal)
LOCAIS = ["Lisboa, Portugal", "Porto, Portugal", "Coimbra, Portugal"] # Foco em grandes centros

# --- FUNÇÕES AUXILIARES ---

def pausa_aleatoria():
    """Pausa o script por um tempo aleatório para evitar sobrecarga (1 a 3 segundos)."""
    sleep_time = random.uniform(1, 3)
    time.sleep(sleep_time)

# --- FUNÇÕES PRINCIPAIS DO BOT (GOOGLE API) ---

def buscar_places_api(query, location_bias, categoria):
    """
    Busca locais usando o endpoint 'textsearch' da Google Places API.
    """
    
    if not GOOGLE_API_KEY:
        print("⚠️ ERRO: Chave de API não definida. Não é possível conectar ao Google.")
        return []

    pausa_aleatoria()
    
    params = {
        'query': f"{query} em {location_bias}",
        'key': GOOGLE_API_KEY,
    }
    
    print(f"\n[BUSCANDO GOOGLE PLACES] Categoria: {categoria} | Termo: {query} | Local: {location_bias}")
    
    try:
        response = requests.get(PLACE_SEARCH_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get('status') == 'OK':
            # Se a busca inicial funcionar, buscamos os detalhes para extrair o máximo de dados
            return buscar_detalhes_de_locais(data.get('results', []), categoria)
        else:
            print(f"⚠️ [ERRO GOOGLE API] Status: {data.get('status')} | Mensagem: {data.get('error_message', 'Erro desconhecido')}")
            return []

    except requests.exceptions.RequestException as e:
        print(f"❌ [ERRO DE REQUISIÇÃO] Falha na API: {e}")
        return []

def buscar_detalhes_de_locais(lugares, categoria):
    """Para cada local encontrado, busca os detalhes (telefone, website, etc.)."""
    dados_coletados = []
    
    for lugar in lugares:
        place_id = lugar['place_id']
        params_details = {
            'place_id': place_id,
            'fields': 'name,formatted_address,website,formatted_phone_number,url', # Campos públicos relevantes
            'key': GOOGLE_API_KEY
        }
        
        pausa_aleatoria() # Pausa entre cada chamada de detalhe
        
        try:
            response = requests.get(PLACE_DETAILS_URL, params=params_details, timeout=15)
            response.raise_for_status()
            detalhes = response.json().get('result', {})
            
            # Estruturação dos Dados
            dados_coletados.append({
                "Nome": detalhes.get('name', 'N/A'),
                "Categoria": categoria,
                "Endereço": detalhes.get('formatted_address', 'N/A'),
                "Telefone": detalhes.get('formatted_phone_number', 'N/A'),
                "Email": "N/A (Google não fornece e-mail diretamente)",
                "Website": detalhes.get('website', 'N/A'),
                "Origem": "Google Places API"
            })
            
        except Exception as e:
            continue
            
    return dados_coletados

def salvar_dados(dados):
    """Salva a lista de dicionários em um arquivo CSV usando Pandas."""
    if not dados:
        print("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"prospeccao_clientes_google_places_{timestamp}.csv"
    try:
        df.to_csv(nome_arquivo, index=False, encoding='utf-8')
        print(f"\n✅ SUCESSO: {len(df)} contatos salvos em '{nome_arquivo}'")
    except Exception as e:
        print(f"❌ [ERRO DE GRAVAÇÃO] Falha ao salvar CSV: {e}")

def main_google_places():
    """Função principal que orquestra a busca via Google Places API."""
    print("🤖 Iniciando Bot de Prospecção (Google Places API)...")
    
    if not GOOGLE_API_KEY:
        return

    contatos_totais = []

    for local in LOCAIS:
        for categoria, termos in NICHOS_PLACES.items():
            for termo in termos:
                novos_contatos = buscar_places_api(termo, local, categoria)
                contatos_totais.extend(novos_contatos)
            
    salvar_dados(contatos_totais)
    print("🏁 Bot de Prospecção Finalizado.")


if __name__ == "__main__":
    main_google_places()