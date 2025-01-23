import asyncio
import aiohttp # type: ignore
from bs4 import BeautifulSoup # type: ignore
import pickle as pkl
import os
import time
from tqdm import tqdm  # type: ignore

async def fetch(session, id):
    url = f'https://www.mathgenealogy.org/id.php?id={id}'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    scinet = soup.find('p', style='text-align: center; margin-top: 0; margin-bottom: 0px; font-size: small').find('a', href=True)
                    if scinet:
                        return (int(scinet.get('href').split('/')[-1]), id)
                    else:
                        return None
                else:
                    #print(f"Error fetching id {id}: HTTP Status {response.status}")
                    return None
    except Exception as e:
        #print(f"Error fetching id {id}: {e}")
        return None

async def process_batch(start_id:int,end_id:int,batch_size:int):
    async with aiohttp.ClientSession() as session:
        tasks = []
        sciNetToGen = {}
        for id in range(start_id, end_id + 1):
            tasks.append(fetch(session, id))
            if len(tasks) == batch_size:
                results = await asyncio.gather(*tasks)
                for result in results:
                    if result:
                        sciNetToGen[result[0]] = result[1]
                tasks = []
        if tasks:
            results = await asyncio.gather(*tasks)
            for result in results:
                if result:
                    sciNetToGen[result[0]] = result[1]
        return sciNetToGen
        
def main():
    path_pkl = r"C:\Users\wamjs\OneDrive\Documentos\Pavo\Coding\mathscinet\gen3.pkl"
    if not os.path.exists(path_pkl):
        sciNetToGen = {}
    else:
        with open(path_pkl, 'rb') as f:
            sciNetToGen = pkl.load(f)
    max_id = max(list(sciNetToGen.values())) if list(sciNetToGen.keys()) != [] else 1
    batch=100
    for i in tqdm(range(max_id, 315371, batch), desc="Processing..."):  # The value 315371 was obtained from https://www.mathgenealogy.org/search.php
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        new_gen = loop.run_until_complete(process_batch(i,i+batch,batch))
        with open(path_pkl, 'wb') as f:
            pkl.dump({**sciNetToGen, **new_gen}, f)
        time.sleep(1)
        with open(path_pkl, 'rb') as f:
            sciNetToGen = pkl.load(f)
        if i%1000 == 0:
            print(f'Batch {i}: {abs(len(sciNetToGen)-i)} non processed IDs')

if __name__ == '__main__':
    main()