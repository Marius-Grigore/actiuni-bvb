import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import re


# Imita un browser pentru a nu mi se mai inchide conexiunea
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}) 


# Simbolurile actiunilor sunt parametrii query pentru link-uri
def extrage_detalii_actiune(simbol):
    url = f"https://bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s={simbol}"
    try:
        raspuns = session.get(url, timeout=10)
        raspuns.raise_for_status()
        soup = BeautifulSoup(raspuns.content, 'html.parser')
        detalii = {}
        tabel = soup.find('table', id='ctl00_body_ctl02_IndicatorsControl_dvIndicators')
        if tabel:
            for tr in tabel.find_all('tr'):
                cells = tr.find_all('td')
                if len(cells) == 2:
                    cheie = cells[0].get_text().strip()
                    # Daca primul td de pe rand incepe cu "Dividend" sau cu "Capitalizare", sari peste acel rand (datele nu sunt in linie)
                    if not any(cheie.startswith(prefix) for prefix in ["Dividend", "Capitalizare"]):
                        valoare = cells[1].get_text().strip()
                        detalii[cheie] = valoare
        return detalii
    except requests.exceptions.RequestException as e:
        print(f"Eroare: {e}")
    return {}

def extrage_continut_tabel(url):
    try:
        raspuns = requests.get(url)
        if raspuns.status_code == 200:
            soup = BeautifulSoup(raspuns.content, 'html.parser')
            tabel = soup.find('table')
            if tabel:
                headers = [th.get_text().strip() for th in tabel.find_all('th')]
                headers[0] = "Simbol"
                headers.insert(1, "ISIN")
                
                randuri = []
                simboluri = []
                for tr in tabel.find_all('tr'):
                    cells = tr.find_all('td')
                    if cells:
                        simbol_isin = cells[0].get_text().strip()
                        match = re.search(r'RO|NL|CY|AT', simbol_isin)
                        if match:
                            isin_start_index = match.start()
                            simbol = simbol_isin[:isin_start_index]
                            isin = simbol_isin[isin_start_index:]
                        else:
                            simbol = simbol_isin
                            isin = ""
                        rand = [simbol, isin] + [td.get_text().strip() for td in cells[1:]]
                        randuri.append(rand)
                        simboluri.append(simbol)

                df = pd.DataFrame(randuri, columns=headers)
                

                detalii_list = [extrage_detalii_actiune(simbol) for simbol in simboluri]
                detalii_df = pd.DataFrame(detalii_list)
                

                full_df = pd.concat([df, detalii_df], axis=1)
                return full_df
            else:
                print("Nu exista tabele pe pagina")
                return None
        else:
            print(f"Nu am putut obtine pagina - status code: {raspuns.status_code}")
            return None
    except Exception as e:
        print(f"Eroare: {str(e)}")
        return None


url = 'https://bvb.ro/FinancialInstruments/Markets/Shares'


tabel_df = extrage_continut_tabel(url)


if tabel_df is not None:
    current_date = datetime.now().strftime("%d.%m.%Y")
    
    nume_fisier = f"Date BVB {current_date}.xlsx"
    
    save_dir = r'C:\Users\MARIUS\Desktop\BVB actiuni'
    
    cale_fisier = os.path.join(save_dir, nume_fisier)
    tabel_df.to_excel(cale_fisier, index=False)
    
    print(f"Datele au fost salvate in: {cale_fisier}")
else:
    print("Nu am extras date.")
