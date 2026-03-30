import os
import time
import json
import tweepy
from supabase import create_client, Client
import logging

# ================== CONFIGURAÇÕES ==================
X_BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAANiR8gEAAAAAdta99ULxZosQ2%2BQ6bI06hkYCokM%3Dz1CVilTZStkeSUvf25zO49m7zTcgwPlLhz8bX7QsI8RrDbX4dY"

SUPABASE_URL = "https://mncnvmalmxpbiojcqqjo.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1uY252bWFsbXhwYmlvamNxcWpvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ4OTE1MTQsImV4cCI6MjA5MDQ2NzUxNH0.lBgvhixDr98ne1HuqeY0YFzXRyAb2yoRRH1Fn74Z0vk"

# ================== CONEXÃO SUPABASE ==================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def classify_product(text: str) -> str:
    text_lower = text.lower()
    if "rexona" in text_lower or "nunca te abandona" in text_lower or "eterno convocado" in text_lower:
        return "rexona"
    elif "dove men" in text_lower or "dove men care" in text_lower or "dove men+cuidado" in text_lower:
        return "dove_men"
    elif "dove" in text_lower:
        return "dove"
    return "outro"

def save_tweet(tweet):
    try:
        data = {
            "tweet_id": tweet.id,
            "text": tweet.text,
            "author_id": tweet.author_id,
            "author_username": tweet.author_id,  # Vamos melhorar depois se quiser
            "created_at": tweet.created_at,
            "product": classify_product(tweet.text),
            "likes": tweet.public_metrics.get("like_count", 0),
            "retweets": tweet.public_metrics.get("retweet_count", 0),
            "impressions": tweet.public_metrics.get("impression_count", 0),
            "url": f"https://x.com/i/web/status/{tweet.id}",
            "raw_data": tweet.data
        }
        
        supabase.table("tweets_copa").upsert(data, on_conflict="tweet_id").execute()
        logging.info(f"Tweet salvo: {tweet.id} | Produto: {data['product']}")
        
    except Exception as e:
        logging.error(f"Erro ao salvar tweet {tweet.id}: {e}")

# ================== STREAMING ==================
class MyStream(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        save_tweet(tweet)
    
    def on_error(self, status_code):
        logging.error(f"Erro no stream: {status_code}")
        if status_code == 420:
            return False  # Disconnect and reconnect
        return True

    def on_connection_error(self, error):
        logging.error(f"Erro de conexão: {error}")
        time.sleep(10)
        return True

# ================== INICIAR ==================
if __name__ == "__main__":
    print("🚀 Iniciando Worker de Social Listening - Unilever Copa 2026")
    
    stream = MyStream(X_BEARER_TOKEN)
    
    # Adiciona a regra que você já criou
    rules = stream.get_rules()
    if rules.data:
        print(f"Regras existentes: {len(rules.data)}")
    
    print("✅ Conectando ao Filtered Stream...")
    stream.filter(tweet_fields=["created_at", "author_id", "public_metrics", "lang"])