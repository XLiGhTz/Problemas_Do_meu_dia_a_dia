import requests
import pandas as pd
import pymysql
import logging
import random
from datetime import datetime
import cv2
import numpy as np
from io import BytesIO
import time
from math import ceil

# Configurações
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações do MySQL
DB_CONFIG = {
    "host": "teu endpoint", #aqui eu uso os servicos da amazon
    "user": "Seu usuario aqui",
    "password": "Senha",
    "database": "Nome do Database",
    "connect_timeout": 10
}

# Chave da API Pixabay
API_KEY = "49996174-f87b881065bc02044f6759117"
TOTAL_FOTOS = 400  # Meta de 400 fotos
FOTOS_POR_PAGINA = 200  # Máximo permitido por requisição

def get_image_color_palette(image_url):
    """Analisa a imagem e retorna a paleta de cores predominantes"""
    try:
        response = requests.get(image_url, timeout=10)
        img_array = np.frombuffer(response.content, np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        
        img = cv2.resize(img, (100, 100))
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        pixels = img.reshape(-1, 3)
        
        colors, counts = np.unique(pixels, axis=0, return_counts=True)
        top_colors = colors[np.argsort(-counts)][:3]
        
        return [f"#{r:02x}{g:02x}{b:02x}" for r, g, b in top_colors]
    
    except Exception as e:
        logger.warning(f"Erro ao analisar cores: {e}")
        return ["#FFFFFF", "#CCCCCC", "#999999"]

def get_pixabay_photos():
    """Obtém 400 fotos da API Pixabay com paginação"""
    all_photos = []
    paginas = ceil(TOTAL_FOTOS / FOTOS_POR_PAGINA)
    
    for pagina in range(1, paginas + 1):
        try:
            url = f"https://pixabay.com/api/?key={API_KEY}&q=nature&page={pagina}&per_page={FOTOS_POR_PAGINA}&image_type=photo"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            if not data.get('hits'):
                logger.warning(f"Página {pagina} sem resultados")
                break
                
            all_photos.extend(data['hits'])
            logger.info(f"Página {pagina}: {len(data['hits'])} fotos (Total: {len(all_photos)})")
            
            time.sleep(0.5)  # Respeita o rate limit
            
            if len(all_photos) >= TOTAL_FOTOS:
                break
                
        except Exception as e:
            logger.error(f"Erro na página {pagina}: {e}")
            time.sleep(5)
    
    return all_photos[:TOTAL_FOTOS]

def process_photos(photos):
    """Processa os dados com tratamento robusto de erros"""
    dados = []
    for idx, photo in enumerate(photos):
        try:
            # Processamento seguro das tags
            tags = photo.get('tags', '')
            titulo = tags.split(',')[0][:255] if tags else "Sem título"
            
            # Paleta de cores com fallback
            palette = get_image_color_palette(photo.get('webformatURL', ''))
            
            # Data de captura segura
            data_captura = None
            if 'dateTaken' in photo:
                try:
                    data_captura = datetime.strptime(photo['dateTaken'], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    pass
            
            dados.append({
                'titulo': titulo,
                'fotografo': photo.get('user', 'Desconhecido')[:255],
                'visualizacoes': photo.get('views', 0),
                'downloads': photo.get('downloads', 0),
                'likes': photo.get('likes', 0),
                'comentarios': photo.get('comments', 0),
                'tipo': photo.get('type', 'foto'),
                'resolucao': f"{photo.get('imageWidth', 0)}x{photo.get('imageHeight', 0)}",
                'cor_primaria': palette[0],
                'cor_secundaria': palette[1],
                'cor_terciaria': palette[2],
                'url_imagem': photo.get('webformatURL', ''),
                'url_pagina': photo.get('pageURL', ''),
                'plataforma': "Pixabay",
                'tags': ','.join(tags.split(',')[:3])[:255] if tags else '',
                'camera': photo.get('userImageURL', '').split('/')[-2] if 'userImageURL' in photo else 'Desconhecida',
                'pais': photo.get('country', ''),
                'cidade': photo.get('city', ''),
                'data_captura': data_captura
            })
            
            if (idx + 1) % 50 == 0:
                logger.info(f"Processadas {idx + 1} fotos")
                
        except Exception as e:
            logger.error(f"Erro na foto {idx}: {e}")
            continue
                
    return pd.DataFrame(dados)

def create_table(cursor):
    """Cria tabela otimizada para 400 registros"""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fotos_400 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        titulo VARCHAR(255),
        fotografo VARCHAR(255),
        visualizacoes INT,
        downloads INT,
        likes INT,
        comentarios INT,
        tipo ENUM('foto', 'ilustracao', 'vetor'),
        resolucao VARCHAR(20),
        cor_primaria VARCHAR(7),
        cor_secundaria VARCHAR(7),
        cor_terciaria VARCHAR(7),
        url_imagem VARCHAR(512),
        url_pagina VARCHAR(512),
        plataforma VARCHAR(50),
        tags VARCHAR(255),
        camera VARCHAR(100),
        pais VARCHAR(100),
        cidade VARCHAR(100),
        data_captura DATETIME NULL,
        data_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FULLTEXT(titulo, tags),
        INDEX idx_fotografo (fotografo),
        INDEX idx_data (data_captura)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

def save_to_mysql(df):
    """Salva os dados em lotes otimizados"""
    if df.empty:
        logger.warning("Irmao deu errado!")
        return False
    
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        create_table(cursor)
        
        sql = """
        INSERT INTO fotos_400 
        (titulo, fotografo, visualizacoes, downloads, likes, comentarios, 
         tipo, resolucao, cor_primaria, cor_secundaria, cor_terciaria,
         url_imagem, url_pagina, plataforma, tags, camera, pais, cidade, data_captura)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Lotes de 40 registros (ajustado para 400 fotos)
        batch_size = 40
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            cursor.executemany(sql, [tuple(row) for row in batch.values])
            conn.commit()
            logger.info(f"Lote {i//batch_size + 1}/10 inserido")
            time.sleep(0.1)
        
        return True
        
    except pymysql.Error as e:
        logger.error(f"Erro MySQL: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()

if __name__ == "__main__":
    logger.info(f" Ja foram capturadas {TOTAL_FOTOS} fotos...")
    
    photos = get_pixabay_photos()
    if photos:
        logger.info(f"✅ {len(photos)} fotos obtidas da API")
        df = process_photos(photos)
        
        logger.info(f"📊 {len(df)} registros processados")
        logger.info("📝 Amostra:\n" + df.head(2).to_string(index=False))
        
        if save_to_mysql(df):
            logger.info(f"💾 {len(df)} Tudo certo capitao!")
        else:
            logger.error("❌ Vish, deu ruim")
    else:
        logger.error("❌ Mano, deu erro, vamos tentar denovo")