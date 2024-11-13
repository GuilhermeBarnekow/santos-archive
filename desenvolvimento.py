import asyncio
import aiohttp
import pandas as pd
import re
import unicodedata
import os
import json
import logging
from dotenv import load_dotenv
from fuzzywuzzy import process
from aiohttp import ClientSession, ClientResponseError
from asyncio import Semaphore, Lock
from math import radians, cos, sin, asin, sqrt
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
CUSTOM_SEARCH_ENGINE_ID = os.getenv('CUSTOM_SEARCH_ENGINE_ID')

if not GOOGLE_API_KEY:
    raise ValueError("A variável de ambiente 'GOOGLE_API_KEY' não está definida.")
if not CUSTOM_SEARCH_ENGINE_ID:
    logging.warning("A variável de ambiente 'CUSTOM_SEARCH_ENGINE_ID' não está definida. Continuando sem links de redes sociais.")

GOOGLE_PLACES_SEARCH_URL = 'https://maps.googleapis.com/maps/api/place/textsearch/json'
GOOGLE_PLACES_DETAILS_URL = 'https://maps.googleapis.com/maps/api/place/details/json'
GOOGLE_CUSTOM_SEARCH_URL = 'https://www.googleapis.com/customsearch/v1'

# Limite de concorrência para evitar sobrecarregar as APIs
SEM = Semaphore(20)  

CIDADES_E_BAIRROS = {
    "São Paulo": [
        "Jardim Paulista",
        "Pinheiros",
        "Vila Mariana",
        "Moema",
        "Brooklin",
        "Itaim Bibi",
        "Morumbi",
        "Vila Prudente",
        "Santana",
        "Brooklin Novo",
    ],
    "Campinas": [
        "Cidade Universitária",
        "Jardim Paulista",
        "Barão Geraldo",
        "Parque Prado",
        "Nova Campinas",
        "Sousas",
        "Cambuí",
    ],
    "Ribeirão Preto": [
        "Centro",
        "Jardim Universitário",
        "Jardim Catarina",
        "Jardim América",
        "Vila Formosa",
        "Jardim Petrópolis",
    ],
}

KEYWORDS = ["farmácia", "drogaria", "drugstore", "farmácia de manipulação"]

PROCESSED_PLACE_IDS_FILE = 'processed_place_ids.json'

def normalize_name(name):
    if not name:
        return ""
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    name = re.sub(r'\b(Ltda\.|Ltda|EIRELI|S\/A|SA|Limited|Ltd)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Raio da Terra em quilômetros
    return c * r

async def fetch_json(session: ClientSession, url: str, params: dict, retries=3, backoff_factor=0.5):
    async with SEM:
        for attempt in range(retries):
            try:
                async with session.get(url, params=params, timeout=10) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return data
            except ClientResponseError as e:
                logging.error(f"Erro na requisição para {url}: {e.status} - {e.message}")
                if e.status in [429, 500, 502, 503, 504]:
                    # Retentativa para erros que podem ser temporários
                    wait_time = backoff_factor * (2 ** attempt)
                    logging.info(f"Retentando em {wait_time} segundos...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    break
            except asyncio.TimeoutError:
                logging.error(f"Timeout na requisição para {url}")
                wait_time = backoff_factor * (2 ** attempt)
                logging.info(f"Retentando em {wait_time} segundos...")
                await asyncio.sleep(wait_time)
                continue
            except Exception as e:
                logging.error(f"Erro inesperado ao acessar {url}: {e}")
                wait_time = backoff_factor * (2 ** attempt)
                logging.info(f"Retentando em {wait_time} segundos...")
                await asyncio.sleep(wait_time)
                continue
        return {}

async def search_pharmacies_by_bairro(session: ClientSession, bairro: str):
    all_results = []
    for keyword in KEYWORDS:
        logging.info(f"Buscando por '{keyword}' em {bairro}")
        params = {
            'query': f'{keyword} em {bairro}',
            'key': GOOGLE_API_KEY,
            'type': 'pharmacy',
            'rankby': 'prominence'
        }
        data = await fetch_json(session, GOOGLE_PLACES_SEARCH_URL, params)
        if not data:
            continue
        if data.get('status') not in ['OK', 'ZERO_RESULTS']:
            logging.warning(f"Status inesperado na resposta: {data.get('status')} para a busca '{keyword}' em {bairro}")
        results = data.get('results', [])
        logging.info(f"Encontrados {len(results)} '{keyword}' farmácias em {bairro}")
        all_results.extend(results)
        if 'next_page_token' in data:
            next_page_token = data['next_page_token']
            await asyncio.sleep(2)  # Necessário esperar alguns segundos antes de usar o next_page_token
            params_next = {
                'pagetoken': next_page_token,
                'key': GOOGLE_API_KEY
            }
            data_next = await fetch_json(session, GOOGLE_PLACES_SEARCH_URL, params_next)
            if data_next and data_next.get('status') == 'OK':
                results_next = data_next.get('results', [])
                logging.info(f"Encontrados {len(results_next)} resultados adicionais para '{keyword}' em {bairro}")
                all_results.extend(results_next)
    return all_results

async def get_company_details(session: ClientSession, place_id: str):
    params = {
        'place_id': place_id,
        'fields': 'name,formatted_address,rating,formatted_phone_number,types,geometry/location,user_ratings_total',
        'key': GOOGLE_API_KEY
    }
    data = await fetch_json(session, GOOGLE_PLACES_DETAILS_URL, params)
    if 'result' not in data:
        logging.warning(f"Detalhes não encontrados para place_id: {place_id}")
        return {}
    if data.get('status') != 'OK':
        logging.warning(f"Status inesperado nos detalhes: {data.get('status')}")
    logging.info(f"Detalhes coletados para {data['result'].get('name', 'N/A')}")
    return data['result']

async def get_social_media_links(session: ClientSession, company_name: str, city='São Paulo'):
    if not CUSTOM_SEARCH_ENGINE_ID:
        logging.warning("Custom Search Engine ID não está configurado.")
        return []
    
    company_name_clean = normalize_name(company_name)
    query = f'"{company_name_clean}" {city} site:facebook.com OR site:instagram.com'
    params = {
        'key': GOOGLE_API_KEY,  
        'cx': CUSTOM_SEARCH_ENGINE_ID,
        'q': query,
        'num': 5
    }
    data = await fetch_json(session, GOOGLE_CUSTOM_SEARCH_URL, params)
    if not data or 'items' not in data:
        logging.info(f"Nenhum link de redes sociais encontrado para {company_name}.")
        return []
    links = [item['link'] for item in data.get('items', [])]
    logging.info(f"Links de redes sociais encontrados para {company_name}: {links}")
    return links

def parse_address(formatted_address):
    address_components = {}

    try:
        parts = formatted_address.split(',')
        address_components['route'] = parts[0].strip()
        address_components['neighborhood'] = parts[1].strip()
        address_components['locality'] = parts[2].strip()
    except IndexError:
        pass
    return address_components

def classify_company_size(user_ratings_total):
    if user_ratings_total is None or user_ratings_total == 0:
        return 'Pequena'
    elif user_ratings_total >= 100:
        return 'Grande'
    elif user_ratings_total >= 20:
        return 'Média'
    else:
        return 'Pequena'

def infer_cuisine(types):

    return 'Outros'

async def process_pharmacy(session: ClientSession, place_id: str, city: str):
    details = await get_company_details(session, place_id)
    if not details:
        return None
    
    company_name = details.get('name', 'N/A')
    logging.info(f"Processando farmácia: {company_name}")
    
    social_links = await get_social_media_links(session, company_name, city) if CUSTOM_SEARCH_ENGINE_ID else []
    formatted_address = details.get('formatted_address', '')
    address_components = parse_address(formatted_address)
    
    types = details.get('types', [])

    category = 'Farmácia'  
    
    pharmacy_data = {
        'PlaceID': place_id,  
        'Name': company_name,
        'Address': formatted_address,
        'Neighborhood': address_components.get('neighborhood', 'N/A'),
        'Street': address_components.get('route', 'N/A'),
        'City': address_components.get('locality', 'N/A'),
        'Rating': details.get('rating', 0),
        'UserRatingsTotal': details.get('user_ratings_total', 0),  
        'Phone': details.get('formatted_phone_number', 'N/A'),
        'Types': ', '.join(types) if isinstance(types, list) else types,
        'Category': category,
        'Latitude': details.get('geometry', {}).get('location', {}).get('lat'),
        'Longitude': details.get('geometry', {}).get('location', {}).get('lng'),
        'SocialLinks': social_links
    }
    return pharmacy_data

async def load_processed_place_ids():
    if os.path.exists(PROCESSED_PLACE_IDS_FILE):
        with open(PROCESSED_PLACE_IDS_FILE, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    return set()

async def save_processed_place_ids(place_ids):
    with open(PROCESSED_PLACE_IDS_FILE, 'w', encoding='utf-8') as f:
        json.dump(list(place_ids), f, ensure_ascii=False, indent=4)

async def collect_data():
    all_companies = {}
    processed_place_ids = await load_processed_place_ids()
    lock = Lock()
    
    async with ClientSession() as session:
        for city, bairros in CIDADES_E_BAIRROS.items():
            logging.info(f"Iniciando busca na cidade: {city}")
            all_results_city = []
            for bairro in bairros:
                results = await search_pharmacies_by_bairro(session, bairro)
                all_results_city.extend(results)
            
            place_ids = set()
            for place in all_results_city:
                pid = place['place_id']
                if pid not in processed_place_ids:
                    place_ids.add(pid)
            
            logging.info(f"Total único de farmácias encontradas em {city}: {len(place_ids)}")
            
            if not place_ids:
                logging.info(f"Nenhuma farmácia nova encontrada em {city}.")
                continue
            
            detail_tasks = []
            for place_id in place_ids:
                detail_tasks.append(process_pharmacy(session, place_id, city))
            
            company_details = await asyncio.gather(*detail_tasks)
            for company in company_details:
                if company:
                    if city not in all_companies:
                        all_companies[city] = []
                    all_companies[city].append(company)
                    processed_place_ids.add(company['PlaceID'])  
            
            async with lock:
                await save_processed_place_ids(processed_place_ids)
    
    return all_companies

def main():
    logging.info("Coletando dados das farmácias...")
    all_companies = asyncio.run(collect_data())
    total_farmacias = sum(len(farmacias) for farmacias in all_companies.values())
    logging.info(f"Total de farmácias coletadas: {total_farmacias}")
    
    if not all_companies:
        logging.warning("Nenhuma farmácia coletada.")
        return
    
    for city, farmacias in all_companies.items():
        df = pd.DataFrame(farmacias).drop_duplicates(subset=['Name', 'Address'])
        logging.info(f"Salvando dados para a cidade: {city} com {len(df)} farmácias.")
        
        df['CompanySize'] = df['UserRatingsTotal'].apply(classify_company_size)
        
        df = df[[
            'PlaceID',
            'Name',
            'Address',
            'Neighborhood',
            'Street',
            'City',
            'Rating',
            'UserRatingsTotal',
            'Phone',
            'Types',
            'Category',
            'Latitude',
            'Longitude',
            'SocialLinks',
            'CompanySize'
        ]]
        
        df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
        df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
        
        initial_count = len(df)
        df = df.dropna(subset=['Latitude', 'Longitude'])
        removed_count = initial_count - len(df)
        logging.info(f"Removidos {removed_count} farmácias sem coordenadas válidas na cidade: {city}.")
        
        output_json = df.to_json(orient='records', force_ascii=False, indent=4)
        filename = f'restaurantes_{normalize_name(city)}.json'.replace(" ", "_")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(output_json)
        
        logging.info(f"Dados limpos salvos no arquivo '{filename}'.")
    
    logging.info("Coleta de dados concluída.")

if __name__ == '__main__':
    main()
