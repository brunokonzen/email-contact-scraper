#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
social_media_scraper.py

Focado em usar a busca do Google para encontrar e-mails em perfis de redes sociais
(LinkedIn, Instagram) com base em palavras-chave e localização, evitando e-mails duplicados.

Autor: Gemini (Senior Python Developer Persona)
Versão: 2.1 (Focada e Refinada)
"""

import argparse
import logging
import re
import sys
import time
from datetime import datetime

# --- Instalação de Dependências ---
try:
    import pandas as pd
    import requests
    from googlesearch import search
    from bs4 import BeautifulSoup
except ImportError:
    print("Erro: Bibliotecas necessárias não encontradas.")
    print("Por favor, execute: pip install pandas requests beautifulsoup4 googlesearch-python")
    sys.exit(1)

# Regex para encontrar e-mails em textos.
EMAIL_REGEX = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)


# --- Seção de Personalização da Busca (ALTERE AQUI!) ---
KEYWORDS = [
    # Fitness & Desporto
    "personal trainer", "instrutor de fitness", "treinador pessoal", "coach fitness",
    "academia de musculação", "ginásio", "crossfit box", "estúdio de pilates",
    "aula de grupo fitness", "preparador físico", "gymshark", "fitness",
    "personal trainer", "academia", "crossfit", "musculação",
    "treinador", "fit", "fisiculturismo", "yoga",
    "pilates", "nutrição", "suplementação", "gym",
    "treino em casa", "fitness girls",
    
    # Futebol
    "treinador de futebol", "director desportivo", "scout de futebol", "olheiro",
    "academia de futebol", "clube desportivo", "formação de futebol", "agente desportivo",
    "futebol juvenil", "futebol profissional", "futebol", "liga",
    "primeira liga", "futebol profissional", "treinador de futebol",
    "academia de futebol", "scout de futebol", "futebol juvenil",
    "benfica", "porto", "sporting", "futebol feminino", "futsal",
    "agente de futebol", "olheiro de futebol", "formação de futebol",
    
    # Saúde & Bem-estar
    "médico", "fisioterapeuta", "esteticista", "dentista", "nutricionista",
    "clínica de estética", "spa", "wellness center", "dermatologista",
    "medicina estética", "fisioterapia", "estética facial", "tratamentos corporais",
    "médico", "fisioterapia", "esteticista", "dentista",
    "clínica estética", "spa", "wellness", "saúde",
    "nutricionista", "psicologia", "ortomolecular",
    "medicina estética", "fisioterapeuta", "dermatologista",
    
    # LinkedIn Specific
    "personal trainer",
    "treinador de futebol", 
    "director desportivo",
    "médico",
    "fisioterapeuta",
    "esteticista",
    "nutricionista",
    "clínica de estética",
    "academia de futebol",
    "ginásio",
    "clube desportivo",
    "médico dentista",
    
    # Regional Variations (sem Portugal)
    "personal trainer lisboa", "personal trainer porto", "personal trainer algarve",
    "treinador futebol lisboa", "treinador futebol porto",
    "clínica estética lisboa", "clínica estética porto", "spa algarve",
    "médico lisboa", "médico porto", "fisioterapeuta lisboa", "fisioterapeuta porto",
    
    # Termos em Inglês (comuns nas redes sociais)
    "fitness coach", "football coach", "sports director", "football scout",
    "medical doctor", "physiotherapist", "aesthetic clinic", "beauty spa"
]
]
REGION = "Portugal"
SITES_TO_SEARCH = ["linkedin.com/in/", "instagram.com"]
COMMON_EMAIL_DOMAINS = ["@gmail.com", "@hotmail.com", "@sapo.pt", "@outlook.pt", "@protonmail.com"]
# --- Fim da Personalização ---


def load_existing_emails(csv_path: str) -> set:
    """
    Carrega e-mails de um arquivo CSV existente para evitar duplicatas.
    """
    try:
        df = pd.read_csv(csv_path)
        # Procura em múltiplas colunas possíveis
        email_columns = [col for col in ['email', 'Emails_Encontrados'] if col in df.columns]
        if not email_columns:
            logging.warning(f"Nenhuma coluna de e-mail encontrada em {csv_path}. Nenhum e-mail será ignorado.")
            return set()
        
        existing_emails = set()
        for col in email_columns:
            # Converte para string, remove NaNs, converte para minúsculas
            emails_in_col = df[col].dropna().astype(str).str.lower()
            # Lida com células que podem ter múltiplos e-mails separados por vírgula
            for item in emails_in_col:
                existing_emails.update(e.strip() for e in item.split(','))

        logging.info(f"{len(existing_emails)} e-mails existentes únicos carregados de '{csv_path}'.")
        return existing_emails
    except FileNotFoundError:
        logging.warning(f"Arquivo de e-mails existentes '{csv_path}' não encontrado. Nenhum e-mail será ignorado.")
        return set()
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo CSV existente: {e}")
        return set()

def build_search_queries(keywords: list, region: str, sites: list, email_domains: list) -> list:
    """
    Cria uma lista de strings de busca otimizadas para o Google.
    """
    queries = []
    email_search_part = " OR ".join([f'"{domain}"' for domain in email_domains])
    
    for site in sites:
        for keyword in keywords:
            # Query para buscar e-mails diretamente nos resultados da busca
            query1 = f'site:{site} "{keyword}" "{region}" AND ({email_search_part})'
            # Query mais genérica para buscar páginas de contato
            query2 = f'site:{site} "{keyword}" "{region}" AND ("email" OR "contato" OR "contact")'
            queries.append(query1)
            queries.append(query2)
            
    logging.info(f"{len(queries)} buscas diferentes serão realizadas.")
    return list(set(queries)) # Remove queries duplicadas

def find_emails_on_page(url: str) -> set:
    """
    Visita uma URL e extrai todos os e-mails que encontrar no conteúdo HTML da página.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            logging.warning(f"Falha ao acessar {url} - Status: {response.status_code}")
            return set()

        soup = BeautifulSoup(response.text, 'html.parser')
        text = soup.get_text()
        
        # Procura também por links "mailto:"
        mailto_links = {a['href'].replace('mailto:', '').split('?')[0] for a in soup.find_all('a', href=True) if a['href'].startswith('mailto:')}
        
        emails_in_text = set(email.lower() for email in EMAIL_REGEX.findall(text))
        
        return emails_in_text.union(mailto_links)
        
    except requests.RequestException as e:
        logging.error(f"Erro de rede ao visitar {url}: {e}")
        return set()

def main():
    parser = argparse.ArgumentParser(description="Busca novos e-mails em redes sociais via Google.")
    parser.add_argument("--existing", required=True, help="Caminho do CSV com e-mails que devem ser ignorados.")
    parser.add_argument("--output", default="novos_emails_sociais.csv", help="Nome do arquivo CSV de saída.")
    args = parser.parse_args()

    existing_emails = load_existing_emails(args.existing)
    search_queries = build_search_queries(KEYWORDS, REGION, SITES_TO_SEARCH, COMMON_EMAIL_DOMAINS)
    
    all_new_emails = set()

    for query in search_queries:
        logging.info(f"--- Executando busca: '{query}' ---")
        try:
            # O pause=2.0 é um delay entre as buscas para não sobrecarregar o Google
            search_results = search(query, tld="pt", lang="pt", num=10, stop=10, pause=2.0)
            
            urls_to_visit = list(search_results)
            if not urls_to_visit:
                logging.info("  -> Nenhum resultado encontrado no Google para esta busca.")
                continue

            for url in urls_to_visit:
                logging.info(f"  Analisando URL: {url}")
                emails_from_page = find_emails_on_page(url)
                
                # Compara os e-mails encontrados com a lista de existentes E com os que já achamos nesta sessão
                newly_found = emails_from_page - existing_emails - all_new_emails
                
                if newly_found:
                    for email in newly_found:
                        logging.info(f"  ✨ NOVO E-MAIL ENCONTRADO: {email}")
                        all_new_emails.add(email)
                else:
                    logging.info("  -> Nenhum e-mail *novo* encontrado nesta página.")
                
                time.sleep(random.uniform(1, 2)) # Pausa entre a visita a cada página

        except Exception as e:
            logging.error(f"  Ocorreu um erro durante a busca no Google. Pode ser um bloqueio temporário. Erro: {e}")
            logging.info("  Aguardando 2 minutos antes de continuar para evitar bloqueio...")
            time.sleep(120)

    if not all_new_emails:
        logging.info("\nBusca concluída. Nenhum e-mail novo foi encontrado desta vez.")
        return

    logging.info(f"\nBusca concluída! Total de {len(all_new_emails)} e-mails novos encontrados.")
    df_new = pd.DataFrame(sorted(list(all_new_emails)), columns=['email'])
    df_new['found_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_new.to_csv(args.output, index=False)
    logging.info(f"Resultados salvos com sucesso em '{args.output}'")


if __name__ == "__main__":
    main()