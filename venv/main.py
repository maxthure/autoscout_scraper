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

car_folder = Path("out/data/autos/")
#car_folder = Path("/media/thure/INTENSO/Scraper/data/autos")
filterset = {"url", "country", "date", "Fahrzeughalter", "HU/AU neu", "Garantie", "Scheckheftgepflegt",
             "Nichtraucherfahrzeug", "Marke", "Modell", "Angebotsnummer", "Erstzulassung", "Außenfarbe", "Lackierung",
             "Innenausstattung", "Karosserieform", "Anzahl Türen", "Sitzplätze", "Getriebeart", "Hubraum", "Kraftstoff",
             "Schadstoffklasse", "Feinstaubplakette", "Leistung", "Kilometerstand", "Getriebe", "Kraftstoffverbr.*",
             "Ausstattung", "haendler", "ort", "price", "Winterreifen", "Einparkhilfe selbstlenkendes System", "TV",
             "Navigationssystem", "Sitzheizung", "Einparkhilfe Sensoren hinten", "Fahrerairbag", "teilb. Rücksitzbank",
             "Notrufsystem", "Start/Stop-Automatik", "Verkehrszeichenerkennung", "Lichtsensor", "Beheizbares Lenkrad",
             "Dachreling", "Get√∂nte;Zentralverriegelung mit Funkfernbedienung", "ESP", "Head-up display",
             "Wegfahrsperre", "Totwinkel-Assistent", "LED-Scheinwerfer", "Schlüssellose Zentralverriegelung",
             "Klimaanlage", "Luftfederung", "Sportsitze", "Tagfahrlicht", "Massagesitze", "ABS", "Skisack", "Armlehne",
             "Kurvenlicht", "Alarmanlage", "Einparkhilfe Kamera", "Beifahrerairbag", "Klimaautomatik",
             "Behindertengerecht", "Schaltwippen", "Tempomat", "Isofix", "Nebelscheinwerfer", "Traktionskontrolle",
             "Tuning", "Notbremsassistent", "Rechtslenker", "Windschott(für Cabrio)", "Beheizbare Frontscheibe",
             "Regensensor", "Radio", "LED-Tagfahrlicht", "MP3", "Lordosenstütze", "Standheizung", "Schiebedach",
             "Bordcomputer", "Scheiben", "Sprachsteuerung", "Lederlenkrad", "Schiebetür;Einparkhilfe Sensoren vorne",
             "Sportfahrwerk", "Reifendruckkontrollsystem", "Berganfahrassistent", "Elektrische Seitenspiegel",
             "Seitenairbag", "USB", "Katalysator", "Kopfairbag", "Elektr. Fensterheber", "Zentralverriegelung",
             "Müdigkeitswarnsystem", "Nachtsicht-Assistent", "Xenonscheinwerfer", "Servolenkung", "Elektrische Sitze",
             "Elektrische Heckklappe", "Sportpaket", "Multifunktionslenkrad", "Bluetooth", "DAB-Radio", "Panoramadach",
             "Anhängerkupplung", "Freisprecheinrichtung", "Abstandstempomat", "Spurhalteassistent", "Sitzbelüftung",
             "Touchscreen", "Alufelgen", "Airbag hinten", "CD", "Soundsystem", "HU Prüfung", "Schlüsselnummer",
             "Farbe laut Hersteller"}
ausstattungsset = {"ABS",
                   "Airbag hinten",
                   "Alarmanlage",
                   "Beifahrerairbag",
                   "ESP",
                   "Fahrerairbag",
                   "Isofix",
                   "Kopfairbag",
                   "Kurvenlicht",
                   "LED-Scheinwerfer",
                   "LED-Tagfahrlicht",
                   "Müdigkeitswarnsystem",
                   "Nachtsicht-Assistent",
                   "Nebelscheinwerfer",
                   "Notbremsassistent",
                   "Notrufsystem",
                   "Reifendruckkontrollsystem",
                   "Schlüssellose Zentralverriegelung",
                   "Seitenairbag",
                   "Spurhalteassistent",
                   "Tagfahrlicht",
                   "Totwinkel-Assistent",
                   "Verkehrszeichenerkennung",
                   "Wegfahrsperre",
                   "Xenonscheinwerfer",
                   "Zentralverriegelung",
                   "Zentralverriegelung mit Funkfernbedienung",
                   "Abstandstempomat",
                   "Armlehne",
                   "Beheizbare Frontscheibe",
                   "Beheizbares Lenkrad",
                   "Berganfahrassistent",
                   "Einparkhilfe Kamera",
                   "Einparkhilfe selbstlenkendes System",
                   "Einparkhilfe Sensoren hinten",
                   "Einparkhilfe Sensoren vorne",
                   "Elektr. Fensterheber",
                   "Elektrische Heckklappe",
                   "Elektrische Seitenspiegel",
                   "Elektrische Sitze",
                   "Head-up display",
                   "Klimaanlage",
                   "Klimaautomatik",
                   "Lederlenkrad",
                   "Lichtsensor",
                   "Lordosenstütze",
                   "Luftfederung",
                   "Massagesitze",
                   "Panoramadach",
                   "Regensensor",
                   "Schaltwippen",
                   "Schiebedach",
                   "Schiebetür",
                   "Servolenkung",
                   "Sitzbelüftung",
                   "Sitzheizung",
                   "Standheizung",
                   "Start/Stop-Automatik",
                   "teilb. Rücksitzbank",
                   "Tempomat",
                   "Bluetooth",
                   "Bordcomputer",
                   "CD",
                   "DAB-Radio",
                   "Freisprecheinrichtung",
                   "MP3",
                   "Multifunktionslenkrad",
                   "Navigationssystem",
                   "Radio",
                   "Soundsystem",
                   "Sprachsteuerung",
                   "Touchscreen",
                   "TV",
                   "USB",
                   "Alufelgen",
                   "Anhängerkupplung",
                   "Behindertengerecht",
                   "Dachreling",
                   "Garantie",
                   "Getönte",
                   "Scheiben",
                   "Katalysator",
                   "Rechtslenker",
                   "Skisack",
                   "Sportfahrwerk",
                   "Sportpaket",
                   "Sportsitze",
                   "Traktionskontrolle",
                   "Tuning",
                   "Windschott(für Cabrio)",
                   "Winterreifen"
                   }


def main():
    create_folder()
    engine = create_engine_to_db()
    visited_urls = get_visited_urls(engine)
    multiple_cars_dict = check_for_deleted_cars(engine, visited_urls)
    multiple_cars_dict = scrape_autoscout(visited_urls, multiple_cars_dict)
    save_cars(multiple_cars_dict, engine)


def create_folder():
    if not os.path.isdir(car_folder):
        os.makedirs(car_folder)
        print(car_folder, "erstellt.")
    else:
        print(car_folder, "existiert bereits")


def create_engine_to_db():
    try:
        #engine = sqlalchemy.create_engine('sqlite:////media/thure/INTENSO/Scraper/data/scraped.sqlite')
        engine = sqlalchemy.create_engine('sqlite:///out/data/scraped.sqlite')
        return engine
    except Exception as error:
        print("Error while connecting to sqlite: ", error)


def get_visited_urls(engine):
    if not engine.dialect.has_table(engine, "autos"):
        print("DB existiert noch nicht.")
        return []
    try:
        connection = engine.connect()
        metadata = sqlalchemy.MetaData()
        autos = sqlalchemy.Table('autos', metadata, autoload=True, autoload_with=engine)
        query = sqlalchemy.select([autos.columns.url.distinct()]).where(autos.columns.deleted == False)
        result_proxy = connection.execute(query)
        result_set = result_proxy.fetchall()
        visited_urls = [r[0] for r in result_set]
        print(f'Länge der Liste besuchter URLs: {len(visited_urls)}')
        return visited_urls
    except Exception as error:
        print("Error while connecting to sqlite: ", error)
    finally:
        connection.close()


def check_for_deleted_cars(engine, visited_urls):
    car_counter = 1
    deleted_urls = []
    multiple_cars_dict = {}

    for URL in visited_urls:
        print(f'Gelöschte Checken | Auto {car_counter}' + ' ' * 50)
        try:
            car_counter += 1
            car_dict = {}
            car_dict["country"] = "Deutschland"
            car_dict["date"] = str(datetime.now())
            car = BeautifulSoup(urllib.request.urlopen('https://www.autoscout24.de' + URL).read(), 'lxml')

            for key, value in zip(car.find_all("dt"), car.find_all("dd")):
                # print(key)
                if key.text.replace("\n", "") in filterset:
                    car_dict[key.text.replace("\n", "")] = value.text.replace("\n", "")
            car_dict["haendler"] = car.find("div", attrs={"class": "cldt-vendor-contact-box",
                                                          "data-vendor-type": "dealer"}) != None
            car_dict["ort"] = car.find("div", attrs={"class": "sc-grid-col-12",
                                                     "data-item-name": "vendor-contact-city"}).text

            car_dict["price"] = \
                int("".join(re.findall(r'[0-9]+', car.find("div", attrs={"class": "cldt-price"}).text)))

            ausstattung = []
            for i in car.find_all("div", attrs={
                "class": "cldt-equipment-block sc-grid-col-3 sc-grid-col-m-4 sc-grid-col-s-12 sc-pull-left"}):
                for span in i.find_all("span"):
                    ausstattung.append(i.text)
            ausstattung2 = []
            for element in list(set(ausstattung)):
                austattung_liste = element.split("\n")
                ausstattung2.extend(austattung_liste)
            for ausstattung_element in ausstattungsset:
                if ausstattung_element in ausstattung2:
                    car_dict[ausstattung_element] = True
                else:
                    car_dict[ausstattung_element] = False

            car_dict["deleted"] = False

            multiple_cars_dict[URL] = car_dict
        except Exception as e:
            if (str(e) == "HTTP Error 404: Not Found"):
                deleted_urls.append(URL)
            print("Detailseite: " + str(e) + " " * 50)
            pass

    try:
        connection = engine.connect()
        metadata = sqlalchemy.MetaData()
        autos = sqlalchemy.Table('autos', metadata, autoload=True, autoload_with=engine)
        for url in deleted_urls:
            query = sqlalchemy.update(autos).values(deleted=True).where(autos.columns.url == url)
            connection.execute(query)
    except Exception as error:
        print("Error while connecting to sqlite: ", error)
    finally:
        connection.close()

    print(deleted_urls)
    return multiple_cars_dict


def scrape_autoscout(visited_urls, multiple_cars_dict):
    car_counter = 1

    marken_model_dic = {"ac": ["cobra"],
                        "alfa-romeo": ["145", "146", "147", "155", "156", "159", "164", "4c", "75", "8c", "90", "6",
                                       "alfasud", "alfetta", "brera", "giulia", "gt", "gtv", "mito", "montreal",
                                       "quadrifoglio", "rz", "spider", "sprint", "sz"],
                        "alpina": ["b3", "b4", "b5", "b6", "b7", "b8", "b9", "b10", "b11", "b12", "c1", "c2", "d10",
                                   "d3", "d4", "d5", "roadster-s"],
                        "aston-martin": ["cygnet", "db", "db7", "db9", "db11", "dbs", "dbx", "lagonda", "rapide", "v8",
                                         "valkyrie", "vantage", "virage", "volante"],
                        "bentley": ["arnage", "azure", "bentayga", "brooklands", "continental", "eight", "flying-spur",
                                    "mulsanne", "s1", "s2", "s3", "turbo-r", "turbo-rt", "turbo-s"],
                        "bmw": ["1er-(alle)", "2002", "2er-(alle)", "3er-(alle)", "4er-(alle)", "5er-(alle)",
                                "6er-(alle)", "7er-(alle)", "8er-(alle)", "m-reihe-(alle)", "m1", "z-reihe-(alle)"],
                        "bugatti": ["centodieci", "chiron", "divo", "eb-110", "eb112", "veyron"],
                        "de-tomaso": [],
                        "ferrari": ["195", "206", "208", "246", "250", "275", "288", "308", "328", "330", "348", "360",
                                    "365", "400", "412", "430-scuderia", "456", "458", "488", "512", "550", "575",
                                    "599", "612", "750", "812", "california", "daytona", "dino-gt4", "enzo-ferrari",
                                    "f12", "f355", "f40", "f430", "", "f50", "f512", "f8-spider", "f8-tributo", "ff",
                                    "fxx", "gtc4-lusso", "laferrari", "mondial", "monza", "portofino", "roma",
                                    "scuderia-spider-16m", "sf90-stradale", "superamerica", "superamerica",
                                    "testarossa"],
                        "fiat": ["124-spider", "500", "500-abarth", "panda"],
                        "honda": ["nsx"],
                        "jaguar": ["420", "d-type", "daimler", "e-pace", "e-type", "f-pace", "f-type", "i-pace",
                                   "mk-ii", "s-type", "souvereign", "x-type", "x300", "xe", "xf", "xj", "xj12", "xj40",
                                   "xj6", "xj8", "xjr", "xjs", "xjsc", "xk", "xk8", "xkr"],
                        "lamborghini": ["asterion", "aventador", "centenario", "countach", "diablo", "espada",
                                        "estoque", "gallardo", "huracan", "jalpa", "lm", "miura", "murciélago",
                                        "reventon", "sian-fkp-37", "terzo-millennio", "urraco-p250", "urus", "veneno"],
                        "lancia": [],
                        "land-rover": ["defender", "discovery", "discovery-sport", "range-rover", "range-rover-evoque",
                                       "range-rover-sport", "range-rover-velar"],
                        "lotus": [],
                        "maserati": [],
                        "mazda": ["rx7", "rx8"],
                        "mclaren": [],
                        "mercedes-benz": ["190", "c-klasse-(alle)", "cl-(alle)", "clk-(alle)", "cls-(alle)",
                                          "e-klasse-(alle)", "g-klasse-(alle)", "s-klasse-(alle)", "sl-(alle)",
                                          "slc-(alle)", "slk-(alle)", "slr", "sls"],
                        "mg": [],
                        "mini": ["1000", "1300", "cooper", "cooper-s", "john-cooper-works", "cooper-cabrio",
                                 "cooper-s-cabrio", "john-cooper-works-cabrio", "cooper-clubman", "cooper-s-clubman",
                                 "john-cooper-works-clubman"],
                        "nissan": ["gt-r", "skyline"],
                        "porsche": ["365", "550", "718-spider", "911", "930", "964", "991", "992", "993", "996", "997",
                                    "912", "918", "924", "928", "944", "959", "968", "boxter", "boxter", "carrera-gt",
                                    "panamera", "taycan"],
                        "renault": ["alpine-a110", "alpine-a310", "r5"],
                        "rolls-royce": [],
                        "ruf": [],
                        "tvr": [],
                        "wiessmann": []}
    # für Testzwecke
    marken_model_dic_test = {"BMW": ["120"]}

    for marke in marken_model_dic:
        if len(marken_model_dic[marke]) == 0:
            car_URLs = []
            car_URLs_unique = []
            print(marke)
            for page in range(1, 21):
                try:
                    url = 'https://www.autoscout24.de/lst/' + marke + '?sort=age&desc=1&ustate=N%2CU&size=20&page=' \
                          + str(page) + '&cy=D&atype=C&'
                    only_a_tags = SoupStrainer("a")
                    soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml', parse_only=only_a_tags)
                    for link in soup.find_all("a"):
                        if r"/angebote/" in str(link.get("href")):
                            car_URLs.append(link.get("href"))

                    car_URLs_unique = [car for car in list(set(car_URLs)) if car not in visited_urls]
                    print(f'Lauf {marke} {model} | Seite {page} | {len(car_URLs_unique)} neue URLs')

                except Exception as e:
                    print("Übersicht: " + str(e) + " " * 50)
                    pass

            if len(car_URLs_unique) > 0:
                for URL in car_URLs_unique:
                    print(f'Lauf {marke} {model} | Auto {car_counter}' + ' ' * 50)
                    try:
                        car_counter += 1
                        car_dict = {}
                        car_dict["country"] = "Deutschland"
                        car_dict["date"] = str(datetime.now())
                        car = BeautifulSoup(urllib.request.urlopen('https://www.autoscout24.de' + URL).read(), 'lxml')

                        for key, value in zip(car.find_all("dt"), car.find_all("dd")):
                            # print(key)
                            if key.text.replace("\n", "") in filterset:
                                car_dict[key.text.replace("\n", "")] = value.text.replace("\n", "")
                        car_dict["haendler"] = car.find("div", attrs={"class": "cldt-vendor-contact-box",
                                                                      "data-vendor-type": "dealer"}) != None
                        car_dict["ort"] = car.find("div", attrs={"class": "sc-grid-col-12",
                                                                 "data-item-name": "vendor-contact-city"}).text

                        car_dict["price"] = \
                            int("".join(re.findall(r'[0-9]+', car.find("div", attrs={"class": "cldt-price"}).text)))

                        ausstattung = []
                        for i in car.find_all("div", attrs={
                            "class": "cldt-equipment-block sc-grid-col-3 sc-grid-col-m-4 sc-grid-col-s-12 sc-pull-left"}):
                            for span in i.find_all("span"):
                                ausstattung.append(i.text)
                        ausstattung2 = []
                        for element in list(set(ausstattung)):
                            austattung_liste = element.split("\n")
                            ausstattung2.extend(austattung_liste)
                        for ausstattung_element in ausstattungsset:
                            if ausstattung_element in ausstattung2:
                                car_dict[ausstattung_element] = True
                            else:
                                car_dict[ausstattung_element] = False

                        car_dict["deleted"] = False

                        multiple_cars_dict[URL] = car_dict
                    except Exception as e:
                        print("Detailseite: " + str(e) + " " * 50)
                        pass

        for model in marken_model_dic[marke]:
            car_URLs = []
            car_URLs_unique = []
            print(marke + ' ' + model)
            for page in range(1, 21):
                try:
                    url = 'https://www.autoscout24.de/lst/' + marke + '/' + model + '?sort=age&desc=1&ustate=N%2CU&size=20&page=' \
                          + str(page) + '&cy=D&atype=C&'
                    only_a_tags = SoupStrainer("a")
                    soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml', parse_only=only_a_tags)
                    for link in soup.find_all("a"):
                        if r"/angebote/" in str(link.get("href")):
                            car_URLs.append(link.get("href"))

                    car_URLs_unique = [car for car in list(set(car_URLs)) if car not in visited_urls]
                    print(f'Lauf {marke} {model} | Seite {page} | {len(car_URLs_unique)} neue URLs')

                except Exception as e:
                    print("Übersicht: " + str(e) + " " * 50)
                    pass

            if len(car_URLs_unique) > 0:
                for URL in car_URLs_unique:
                    print(f'Lauf {marke} {model} | Auto {car_counter}' + ' ' * 50)
                    try:
                        car_counter += 1
                        car_dict = {}
                        car_dict["country"] = "Deutschland"
                        #car_dict["date"] = str(datetime.now())
                        car = BeautifulSoup(urllib.request.urlopen('https://www.autoscout24.de' + URL).read(), 'lxml')

                        for key, value in zip(car.find_all("dt"), car.find_all("dd")):
                            # print(key)
                            if key.text.replace("\n", "") in filterset:
                                car_dict[key.text.replace("\n", "")] = value.text.replace("\n", "")
                        car_dict["haendler"] = car.find("div", attrs={"class": "cldt-vendor-contact-box",
                                                                      "data-vendor-type": "dealer"}) != None
                        car_dict["ort"] = car.find("div", attrs={"class": "sc-grid-col-12",
                                                                 "data-item-name": "vendor-contact-city"}).text

                        car_dict["price"] = \
                            int("".join(re.findall(r'[0-9]+', car.find("div", attrs={"class": "cldt-price"}).text)))

                        ausstattung = []
                        for i in car.find_all("div", attrs={
                            "class": "cldt-equipment-block sc-grid-col-3 sc-grid-col-m-4 sc-grid-col-s-12 sc-pull-left"}):
                            for span in i.find_all("span"):
                                ausstattung.append(i.text)
                        ausstattung2 = []
                        for element in list(set(ausstattung)):
                            austattung_liste = element.split("\n")
                            ausstattung2.extend(austattung_liste)
                        for ausstattung_element in ausstattungsset:
                            if ausstattung_element in ausstattung2:
                                car_dict[ausstattung_element] = True
                            else:
                                car_dict[ausstattung_element] = False

                        car_dict["deleted"] = False

                        multiple_cars_dict[URL] = car_dict
                    except Exception as e:
                        print("Detailseite: " + str(e) + " " * 50)
                        pass

    return multiple_cars_dict


def save_cars(multiple_cars_dict, engine):
    if len(multiple_cars_dict) > 0:
        df = pd.DataFrame(multiple_cars_dict).T
        df.to_csv(str(car_folder) + "/" + re.sub("[.,:,-, ]", "_", str(datetime.now())) + ".csv", sep=";",
                  index_label="url")
        try:
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')',
                                                                                                                   '')
            df.to_sql('autos', engine, None, 'append', True, "url", None, None, None)
            print("DataFrame saved in DB")
        except Exception as error:
            print("Error while connecting to sqlite: ", error)

    else:
        print("Keine Daten")


if __name__ == "__main__":
    main()
