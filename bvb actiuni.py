import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# Lista de User-Agent pentru a evita blocarea
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.4472.124 Safari/537.36'
]

def get_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(user_agents)
    })
    return session

def fetch_url(session, url, retries=5):
    for i in range(retries):
        try:
            # Delay aleator 100 - 500 ms
            time.sleep(random.uniform(0.1, 0.5))
            response = session.get(url, timeout=10)
            if response.status_code == 200:
                return response
            elif response.status_code in [500, 502, 503, 504]:
                time.sleep(2 ** i)  # Exponential backoff
            else:
                response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            time.sleep(2 ** i)
    return None

def extrage_detalii_actiune(simbol):
    session = get_session()
    url = f"https://bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s={simbol}"
    response = fetch_url(session, url)
    if response:
        soup = BeautifulSoup(response.content, 'html.parser')
        detalii = {}

        def extrage_tabel(tabel_id):
            tabel = soup.find('table', id=tabel_id)
            if tabel:
                for tr in tabel.find_all('tr'):
                    cells = tr.find_all('td')
                    if len(cells) == 2:
                        cheie = cells[0].get_text().strip()
                        valoare = cells[1].get_text().strip()
                        if not any(cheie.startswith(prefix) for prefix in ["Dividend", "Capitalizare", "Prospect", "Numar total actiuni"]):
                            detalii[cheie] = valoare

        extrage_tabel('ctl00_body_ctl02_PricesControl_dvCPrices')
        extrage_tabel('ctl00_body_ctl02_IndicatorsControl_dvIndicators')
        extrage_tabel('dvInfo')

        return detalii
    return {}

def extrage_continut_tabel(url):
    session = get_session()
    response = fetch_url(session, url)
    if response:
        soup = BeautifulSoup(response.content, 'html.parser')
        tabel = soup.find('table')
        if tabel:
            headers = [th.get_text().strip() for th in tabel.find_all('th')]
            headers[0] = "Simbol"
            headers.insert(1, "ISIN")
            
            randuri = []
            simboluri = []

            tbody = tabel.find('tbody')
            if not tbody:
                print("Nu s-a gasit tag-ul <tbody>")
                return None
            
            for tr in tbody.find_all('tr'):
                cells = tr.find_all('td')
                if cells:
                    simbol_span = cells[0].find('span')
                    if simbol_span:
                        simbol_a = simbol_span.find('a')
                    else:
                        simbol_a = cells[0].find('a')
                    
                    simbol_b = simbol_a.find('b')
                    simbol = simbol_b.get_text().strip()
                        
                    isin_p = cells[0].find('p')
                    isin = isin_p.get_text().strip() if isin_p else ''
                    rand = [simbol, isin] + [td.get_text().strip() for td in cells[1:]]
                    randuri.append(rand)
                    simboluri.append(simbol)

            df = pd.DataFrame(randuri, columns=headers)

            detalii_list = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(extrage_detalii_actiune, simbol) for simbol in simboluri if simbol]
                for future in as_completed(futures):
                    detalii_list.append(future.result())

            detalii_df = pd.DataFrame(detalii_list)
            
            full_df = pd.concat([df, detalii_df], axis=1)
            return full_df
        else:
            print("Nu exista tabele pe pagina")
            return None
    else:
        print("Nu am putut obtine pagina")
        return None

url = 'https://bvb.ro/FinancialInstruments/Markets/Shares'
tabel_df = extrage_continut_tabel(url)

if tabel_df is not None:
    current_date = datetime.now().strftime("%d.%m.%Y")
    nume_fisier = f"Date BVB {current_date}.xlsx"
    
    cale_desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
    save_dir = os.path.join(cale_desktop, 'Actiuni BVB')
    os.makedirs(save_dir, exist_ok=True)
    
    cale_fisier = os.path.join(save_dir, nume_fisier)
    tabel_df.to_excel(cale_fisier, index=False)
    print(f"Datele au fost salvate in: {cale_fisier}")
else:
    print("Nu am extras date.")
