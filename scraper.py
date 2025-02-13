!pip install requests beautifulsoup4 pandas tqdm numpy python-dateutil
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date, timedelta
import time
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
from dateutil.relativedelta import relativedelta
import json
import re

# ======== CONFIGURATION =======
START_YEAR = 2000
END_YEAR = 2002
MAX_WORKERS = 20  # Increased parallelism
BATCH_SIZE = 50   # Memory-optimized batch processing
OUTPUT_DIR = "news_articles"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}
# ===============================

class FastNewsScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.date_chunks = self.create_date_chunks()
        self.article_cache = []
        self.error_log = set()
        
    def create_date_chunks(self):
        """Divide dates into RAM-friendly chunks using numpy"""
        dates = pd.date_range(f'{START_YEAR}-01-01', f'{END_YEAR}-12-31', freq='D').date.tolist()
        return np.array_split(dates, len(dates) // BATCH_SIZE + 1)
    
    def process_date_chunk(self, date_chunk):
        """Process dates in parallel with connection reuse"""
        chunk_results = []
        for single_date in date_chunk:
            if single_date in self.error_log:
                continue
                
            try:
                article_urls = self.fetch_archive_links(single_date)
                articles = self.parallel_article_processing(article_urls, single_date)
                chunk_results.extend(articles)
            except Exception as e:
                self.error_log.add(single_date)
                print(f"Error in {single_date}: {str(e)}")
        
        return chunk_results
    
    def fetch_archive_links(self, single_date):
        """Optimized URL fetcher with connection pooling"""
        year = single_date.year
        month = single_date.month
        day = single_date.day
        starttime = (single_date - date(1900, 1, 1)).days + 1
        
        try:
            with self.session.get(
                f"https://timesofindia.indiatimes.com/{year}/{month}/{day}/archivelist/year-{year},month-{month},starttime-{starttime}.cms",
                timeout=10
            ) as response:
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    return [
                        f"https://timesofindia.indiatimes.com{a['href']}"
                        for a in soup.select('table.content a[href*="/articleshow/"]')
                    ]
                return []
        except:
            return []
    
    def parallel_article_processing(self, urls, current_date):
        """Massively parallel article processing with RAM optimization"""
        def process_article(url):
            try:
                with self.session.get(url, timeout=15) as response:
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        headline = soup.find('h1', class_=re.compile(r'(_23498|HNMDR)'))
                        content = soup.find('div', class_=re.compile(r'(_3YYSt|article_content)'))
                        
                        return {
                            'date': current_date.strftime('%Y-%m-%d'),
                            'headline': headline.get_text().strip() if headline else '',
                            'content': ' '.join(content.stripped_strings) if content else '',
                            'url': url
                        }
            except:
                return None
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_article, url) for url in urls]
            return [f.result() for f in futures if f.result()]

    def save_batch(self, batch_data):
        """Optimized batch saving with pandas compression"""
        if not batch_data:
            return
            
        df = pd.DataFrame(batch_data)
        dates = df['date'].unique()
        
        for d in dates:
            daily_df = df[df['date'] == d]
            filename = f"{OUTPUT_DIR}/{d}.csv"
            
            # Append to existing file if needed
            if os.path.exists(filename):
                existing = pd.read_csv(filename)
                combined = pd.concat([existing, daily_df]).drop_duplicates('url')
                combined.to_csv(filename, index=False)
            else:
                daily_df.to_csv(filename, index=False)

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    scraper = FastNewsScraper()
    
    with tqdm(total=len(scraper.date_chunks), desc="Processing Batches") as pbar:
        for chunk in scraper.date_chunks:
            batch_data = scraper.process_date_chunk(chunk)
            scraper.save_batch(batch_data)
            pbar.update(1)
            
            # Memory management
            del batch_data
            if MAX_WORKERS > 15:
                time.sleep(1)  # Prevent OOM

    # Final compression
    !zip -9 -r {OUTPUT_DIR}.zip {OUTPUT_DIR}
    files.download(f"{OUTPUT_DIR}.zip")

if __name__ == "__main__":
    main()
