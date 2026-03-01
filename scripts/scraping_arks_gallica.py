
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import ast
import json
import urllib.request, urllib.error, urllib.parse

if __name__ == "__main__": #permet de se charger uniquement si on l'active dans la ligne de commande mais qd fonctionne en tant que module ne s'activera pas

    print('lol')

    headers= {"User-Agent": "Mozilla/5.0"}
    cookie = {"JSESSIONID": "914B3F24C87905DA2C84EFE48D3F7093"}
    start_date = 1883
    end_date = 1889

    chemin1 = '/Users/mathieu/Documents/memoire/code_memoire/arks_revues.json'
    with open(chemin1, mode='r', encoding='utf-8') as f : 
        dico_revues = ast.literal_eval(f.read())

    dico_arks = { "numeros" : [],"urls" : [], "first_page": [], "last_page": []}
    for revue in dico_revues : 
        url_revue = dico_revues[revue]
        for i in tqdm(range(start_date, end_date)):
            for j in tqdm(range(1, 13)): #éventuellement raffiner avec boucle sur les jours du mois
                url = url_revue+f"/date{i}{j:02d}01"
                response = requests.get(url, headers=headers, cookies=cookie)
                print(response.url)
                if response.status_code == 200 and response.url[-5:] == ".item" :
                    dico_arks["urls"].append(response.url[23:-5])
                    dico_arks["numeros"].append(revue+url[-8:])

    with open("arks_numeros.json", "w", encoding="utf-8") as out: 
        json.dump(dico_arks, out)