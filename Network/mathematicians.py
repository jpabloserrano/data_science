import asyncio
import time
import aiohttp
import nest_asyncio
from aiohttp.client import ClientSession
import pandas as pd
import unicodedata
import re
import pickle
import json
from tqdm import tqdm

nest_asyncio.apply()
lock = asyncio.Lock()

def clean(text):
    normalized_text = unicodedata.normalize('NFKD', text)
    cleaned_text = re.sub(r'[^A-Za-z0-9\s.,]', '', normalized_text)
    return cleaned_text

async def download_link(ID: int, session: aiohttp.ClientSession, authors: list[dict], lock: asyncio.Lock, retries: int = 3):
    default = {'id': ID, 'profileName': '/, /', 'earliestPublicationYear': float('nan'),
               'totalPublications': float('nan'), 'totalCitations': float('nan'), 'coAuthors': None,
               'pubClassificationCounts': {'publicationsByPrimaryClassification': []}}
    url = f'https://mathscinet.ams.org/mathscinet/api/public/authors/{ID}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'DNT': '1'  # Do Not Track request header
    }
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    r = await response.text()
                    try:
                        author = json.loads(r) or default
                    except json.JSONDecodeError:
                        author = default
                elif response.status == 429:
                    retry_after = response.headers.get('Retry-After')
                    await asyncio.sleep(int(retry_after) if retry_after else 2 ** attempt)
                    continue
                else:
                    author = default
            break
        except aiohttp.ClientError as e:
            print(f'Network error while fetching id {ID}: {e}')
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                author = default
    
    async with lock:
        authors.append(author)
    return author

async def download_batch(ids: list[int], session: aiohttp.ClientSession, authors: list[dict], lock: asyncio.Lock):
    tasks = [download_link(ID=ID, session=session, authors=authors, lock=lock) for ID in ids]
    await asyncio.gather(*tasks, return_exceptions=True)

async def download_all(ids: list[int], batch_size: int = 100, concurrency_limit: int = 100):
    authors = []
    lock = asyncio.Lock()
    my_conn = aiohttp.TCPConnector(limit=concurrency_limit)
    async with aiohttp.ClientSession(connector=my_conn) as session:
        for i in tqdm(range(0, len(ids), batch_size), desc='Downloading batches'):
            batch_ids = ids[i:i + batch_size]
            await download_batch(batch_ids, session, authors, lock)
    return authors

def fetch_authors(first: bool = False, batch: int = 100):
    path_csv = r'../mathematitians.csv'
    path_coauthors = r'../coauthors.pkl'
    path_areas = r'../areas.pkl'
    mathids = [i for i in range(1,100000000)]
    if first:
        df = pd.DataFrame(columns=['MRAuthorID','First Name','Last Name','First Publication','Publications','Citations',
                                   'Most Published Area'])
        coauthors = dict()
        areas = dict()
    else:
        df = pd.read_csv(path_csv)
        with open(path_coauthors, 'rb') as f:
            coauthors = pickle.load(f)
        with open(path_areas, 'rb') as f:
            areas = pickle.load(f)
    print(f'\n{len(df)} authors have been fetched. Remaining {len(mathids)-len(df)} authors\n')
    start = time.time()
    ids = mathids[len(df):len(df)+batch]
    authors = asyncio.run(download_all(ids))
    for author in authors:
        split = clean(author['profileName']).split(', ')
        if len(split) >= 2:
            last, first = split[0], split[1]
        elif len(split) == 1:
            last, first = split[0], ''
        if author['pubClassificationCounts']['publicationsByPrimaryClassification'] != []:
            area = author['pubClassificationCounts']['publicationsByPrimaryClassification'][0]['description']
            code_area = int(author['pubClassificationCounts']['publicationsByPrimaryClassification'][0]['code'])
        else:
            area = ''
            code_area = -1
        df.loc[len(df)] = [author['id'], first, last, author['earliestPublicationYear'],
                            author['totalPublications'], author['totalCitations'], area]
        coauthors[author['id']] = [coauthor['authorId'] for coauthor in author['coAuthors']] if author['coAuthors'] else []
        areas[author['id']] = code_area
    assert len(ids) == len(authors), f'Lengths: {len(authors), len(ids)}'
    end = time.time()
    print(f'\nDownload {len(ids)} links in {int(end - start)} seconds\n')
    print(len(df[df.isnull().any(axis=1)].index), 'indices with null value')
    df.to_csv(path_csv, index=False)
    with open(path_coauthors, 'wb') as file:
        pickle.dump(coauthors, file=file)
    with open(path_areas, 'wb') as file:
        pickle.dump(areas, file=file)
    print(f'\nData saved at {path_csv} and {path_coauthors} and {path_areas}\n')
fetch_authors(first=False, batch=100)