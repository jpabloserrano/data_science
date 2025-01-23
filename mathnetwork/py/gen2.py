import asyncio
import aiohttp # type: ignore
import pandas as pd # type: ignore
from bs4 import BeautifulSoup # type: ignore
import pickle as pkl
import os
import time
from tqdm import tqdm  # type: ignore
from tqdm import tqdm # type: ignore

async def fetch(session, id):
    url = f'https://www.mathgenealogy.org/id.php?id={id}'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    student_name = soup.find('h2', style="text-align: center; margin-bottom: 0.5ex; margin-top: 1ex").text.strip()

                    advisor_tags = soup.find_all('p', style="text-align: center; line-height: 2.75ex")
                    advs_names = []
                    advs_id = []

                    for adv in advisor_tags[0].find_all('a', href=True):
                        advs_names.append(adv.get_text(strip=True))
                        advs_id.append(int(adv['href'].split('=')[-1]))
                    return (id, student_name, ', '.join(advs_names), advs_id)
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
        df = pd.DataFrame(columns=['student','advisor(s)'])
        for id in range(start_id, end_id + 1):
            tasks.append(fetch(session, id))

            if len(tasks) == batch_size:
                results = await asyncio.gather(*tasks)
                for result in results:
                    if result:
                        id, student_name, advs_names, advs_id = result
                        gen[id] = advs_id
                        df.loc[len(df)] = [student_name, advs_names]
                tasks = []
        if tasks:
            results = await asyncio.gather(*tasks)
            for result in results:
                if result:
                    id, student_name, advs_names, advs_id = result
                    gen[id] = advs_id
                    df.loc[len(df)] = [student_name, advs_names]
        
        return gen, df
        
def main():
    path_csv = r"C:\Users\wamjs\OneDrive\Documentos\Coding\mathscinet\gen.csv"
    path_pkl = r"C:\Users\wamjs\OneDrive\Documentos\Coding\mathscinet\gen.pkl"
    assert os.path.exists(path_csv), "No csv file"
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
        new_gen, df = loop.run_until_complete(process_batch(i,i+batch,batch))
        pd.concat([pd.read_csv(path_csv), df], ignore_index=True).to_csv(path_csv, index=False)
        with open(path_pkl, 'wb') as f:
            pkl.dump({**gen, **new_gen}, f)
        time.sleep(1)
        with open(path_pkl, 'rb') as f:
            gen = pkl.load(f)
        if i%1000 == 0:
            print(f'Batch {i}: {abs(len(gen)-i)} non processed IDs')

if __name__ == '__main__':
    main()
