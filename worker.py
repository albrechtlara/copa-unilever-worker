import time
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from supabase import create_client, Client
import tweepy

# ================== CREDENCIAIS ==================
X_BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAPmh8gEAAAAAjuhObuQ4i2GweJu1HC8kwtGn%2B7I%3DpvAoJHS1n7WWQSCXqyqKhV9kuZIroEXEGmDzM6drhVcRQJerae"

SUPABASE_URL = "https://mncnvmalmxpbiojcqqjo.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1uY252bWFsbXhwYmlvamNxcWpvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ4OTE1MTQsImV4cCI6MjA5MDQ2NzUxNH0.lBgvhixDr98ne1HuqeY0YFzXRyAb2yoRRH1Fn74Z0vk"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def classify_product(text: str) -> str:
    text_lower = text.lower()
    if any(word in text_lower for word in ["rexona", "nunca te abandona", "eterno convocado", "eternoconvocado"]):
        return "rexona"
    elif any(word in text_lower for word in ["dove men", "dove men care", "dove men+cuidado"]):
        return "dove_men"
    elif "dove" in text_lower:
        return "dove"
    return "outro"

def save_tweet(tweet):
    try:
        # Horário convertido para Brasília
        created_at_br = tweet.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("America/Sao_Paulo"))
        
        # Tenta pegar username de várias formas
        author_username = None
        if hasattr(tweet, 'author_username') and tweet.author_username:
            author_username = tweet.author_username
        elif hasattr(tweet, 'data') and isinstance(tweet.data, dict):
            author_username = tweet.data.get('author_username')

        data = {
            "tweet_id": str(tweet.id),
            "text": tweet.text,
            "author_id": str(tweet.author_id),
            "author_username": author_username,
            "created_at": created_at_br.isoformat(),
            "product": classify_product(tweet.text),
            "likes": tweet.public_metrics.get("like_count", 0),
            "retweets": tweet.public_metrics.get("retweet_count", 0),
            "replies": tweet.public_metrics.get("reply_count", 0),
            "quotes": tweet.public_metrics.get("quote_count", 0),
            "bookmarks": tweet.public_metrics.get("bookmark_count", 0),
            "impressions": tweet.public_metrics.get("impression_count", 0),
            "url": f"https://x.com/i/web/status/{tweet.id}",
            "raw_data": tweet.data
        }
        
        supabase.table("tweets_copa").upsert(data, on_conflict="tweet_id").execute()
        
        logging.info(f"✅ Salvo | {data['product']} | Likes: {data['likes']} | RT: {data['retweets']} | User: {author_username or 'NULL'} | {created_at_br.strftime('%H:%M')}")

    except Exception as e:
        logging.error(f"Erro ao salvar tweet {getattr(tweet, 'id', 'unknown')}: {e}")

class MyStream(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        save_tweet(tweet)
    
    def on_error(self, status_code):
        logging.error(f"Erro no stream: {status_code}")
        return True

    def on_connection_error(self, error):
        logging.error(f"Erro de conexão: {error}")
        time.sleep(15)
        return True

if __name__ == "__main__":
    print("🚀 Iniciando Worker Social Listening - Unilever Copa 2026")
    print("📡 Conectando ao Filtered Stream da X...")

    stream = MyStream(X_BEARER_TOKEN)
    print("✅ Aguardando tweets em tempo real...")
    stream.filter(
        tweet_fields=["created_at", "author_id", "public_metrics", "lang"]
    )
