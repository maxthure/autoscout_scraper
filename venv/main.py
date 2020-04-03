from bs4 import BeautifulSoup, SoupStrainer  # HTML parsing
import urllib.request  # aufrufen von URLs
from time import sleep  # damit legen wir den Scraper schlafen
import json  # lesen und schreiben von JSON-Dateien
from datetime import datetime  # um den Daten Timestamps zu geben
import re  # regular expressions
import os  # Dateipfade erstellen und lesen
import pandas as pd  # Datenanalyse und -manipulation
from pathlib import Path
import sqlalchemy


folders = [Path("out/data/visited/"), Path("out/data/autos/")]

for folder in folders:
    if not os.path.isdir(folder):
        os.makedirs(folder)
        print(folder, "erstellt.")
    else:
        print(folder, "existiert bereits")

path_to_visited_urls = "out/data/visited/visited_urls.json"

if not os.path.isfile(path_to_visited_urls):
    with open(path_to_visited_urls, "w") as file:
        json.dump([], file)

countries = {"Deutschland": "D"}

car_counter = 1
cycle_counter = 0

while True:
    with open(path_to_visited_urls) as file:
        visited_urls = json.load(file)

    if len(visited_urls) > 100000:
        visited_urls = []

    multiple_cars_dict = {}

    cycle_counter += 1

    for country in countries:

        car_URLs = []
        car_URLs_unique = []
        #Suche nach Hersteller + Modell
        make = 'BMW'
        model = '120'
        for page in range(1, 2):
            try:
                url = 'https://www.autoscout24.de/lst/'+make+'/' + model +'?sort=age&desc=1&ustate=N%2CU&size=5&page=' + str(page) + '&cy=' + countries[country] + '&atype=C&'
                only_a_tags = SoupStrainer("a")
                soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml', parse_only=only_a_tags)
                for link in soup.find_all("a"):
                    if r"/angebote/" in str(link.get("href")):
                        car_URLs.append(link.get("href"))

                car_URLs_unique = [car for car in list(set(car_URLs)) if car not in visited_urls]
                print(f'Lauf {cycle_counter} | {country} | Seite {page} | {len(car_URLs_unique)} neue URLs')

            except Exception as e:
                print("Übersicht: " + str(e) + " " * 50)
                pass


        if len(car_URLs_unique) > 0:
            for URL in car_URLs_unique:
                print(f'Lauf {cycle_counter} | {country} | Auto {car_counter}' + ' ' * 50)
                try:
                    car_counter += 1
                    car_dict = {}
                    car_dict["country"] = country
                    car_dict["date"] = str(datetime.now())
                    car = BeautifulSoup(urllib.request.urlopen('https://www.autoscout24.de' + URL).read(), 'lxml')

                    for key, value in zip(car.find_all("dt"), car.find_all("dd")):
                        car_dict[key.text.replace("\n", "")] = value.text.replace("\n", "")
                    car_dict["haendler"] = car.find("div", attrs={"class": "cldt-vendor-contact-box",
                                                                  "data-vendor-type": "dealer"}) != None
                    car_dict["privat"] = car.find("div", attrs={"class": "cldt-vendor-contact-box",
                                                                "data-vendor-type": "privateseller"}) != None
                    car_dict["ort"] = car.find("div", attrs={"class": "sc-grid-col-12",
                                                             "data-item-name": "vendor-contact-city"}).text

                    car_dict["price"] = "".join(re.findall(r'[0-9]+', car.find("div", attrs={"class": "cldt-price"}).text))

                    ausstattung = []
                    for i in car.find_all("div", attrs={
                        "class": "cldt-equipment-block sc-grid-col-3 sc-grid-col-m-4 sc-grid-col-s-12 sc-pull-left"}):
                        for span in i.find_all("span"):
                            ausstattung.append(i.text)
                    ausstattung2 = []
                    for element in list(set(ausstattung)):
                        austattung_liste = element.split("\n")
                        ausstattung2.extend(austattung_liste)
                    #car_dict["ausstattung_liste"] = sorted(list(set(ausstattung2)))
                    multiple_cars_dict[URL] = car_dict
                    visited_urls.append(URL)
                    print(car_dict)
                except Exception as e:
                    print("Detailseite: " + str(e) + " " * 50)
                    pass
            print("")

        else:
            print("\U0001F634")
            sleep(60)

    if len(multiple_cars_dict) > 0:
        df = pd.DataFrame(multiple_cars_dict).T
        df.to_csv("out/data/autos/" + re.sub("[.,:,-, ]", "_", str(datetime.now())) + ".csv", sep=";",
                  index_label="url")
        try:
            engine = sqlalchemy.create_engine('sqlite:///scraped.sqlite')
            df.to_sql('autos',engine, None, 'replace', True, None, None, None, None)
            print("DataFrame saved in DB")
        except Exception as error:
            print("Error while connecting to sqlite: ", error)

    else:
        print("Keine Daten")
    with open("out/data/visited/visited_urls.json", "w") as file:
        json.dump(visited_urls, file)
