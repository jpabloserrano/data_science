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
                    students = soup.find('table',style="margin-left: auto; margin-right: auto")
                    if students:
                        return (id,len(students.find_all('tr')[1:]))
                    else:
                        return (id,0)
                else:
                    #print(f"Error fetching id {id}: HTTP Status {response.status}")
                    return None
    except Exception as e:
        #print(f"Error fetching id {id}: {e}")
        return None

async def process_batch(start_id:int,end_id:int,batch_size:int):
    async with aiohttp.ClientSession() as session:
        tasks = []
        gen = {}
        for id in range(start_id, end_id + 1):
            tasks.append(fetch(session, id))
            if len(tasks) == batch_size:
                results = await asyncio.gather(*tasks)
                for result in results:
                    if result:
                        gen[result[0]] = result[1]
                tasks = []
        if tasks:
            results = await asyncio.gather(*tasks)
            for result in results:
                if result:
                    gen[result[0]] = result[1]
        return gen
        
def main():
    path_pkl = r"C:\Users\wamjs\OneDrive\Documentos\Coding\mathscinet\gen2.pkl"
    if not os.path.exists(path_pkl):
        gen = {}
    else:
        with open(path_pkl, 'rb') as f:
            gen = pkl.load(f)
    max_id = max(list(gen.keys())) if list(gen.keys()) != [] else 1
    batch=100
    for i in tqdm(range(max_id, 315371, batch), desc="Processing..."):  # The value 315371 was obtained from https://www.mathgenealogy.org/search.php
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        new_gen = loop.run_until_complete(process_batch(i,i+batch,batch))
        with open(path_pkl, 'wb') as f:
            pkl.dump({**gen, **new_gen}, f)
        time.sleep(1)
        with open(path_pkl, 'rb') as f:
            gen = pkl.load(f)
        if i%1000 == 0:
            print(f'Batch {i}: {abs(len(gen)-i)} non processed IDs')

if __name__ == '__main__':
    main()