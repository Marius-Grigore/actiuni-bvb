import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import re

# Simbolurile actiunilor sunt parametrii query pentru link-uri
def extrage_detalii_actiune(simbol):
    url = f"https://bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s={simbol}"
    raspuns = requests.get(url)
    if raspuns.status_code == 200:
        soup = BeautifulSoup(raspuns.content, 'html.parser')
        detalii = {}

        tabel = soup.find('table', id='ct100_body_ct102_IndicatorsControl_dvIndicators')
        if tabel:
            for tr in tabel.find_all('tr'):
                cells = tr.find_all('td')
                if len(cells) == 2:  # Daca randul are exact doua celule
                    cheie = cells[0].get_text().strip()
                    valoare = cells[1].get_text().strip()
                    detalii[cheie] = valoare
        return detalii
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
                        match = re.search(r'RO', simbol_isin)
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
