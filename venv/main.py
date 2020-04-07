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
path_to_visited_urls = "out/data/visited/visited_urls.json"


def main():
    create_folder()
    engine = create_engine_to_db()
    visited_urls = get_visited_urls(engine)
    multiple_cars_dict = scrape_autoscout(visited_urls)
    save_cars(multiple_cars_dict, engine)


def create_folder():
    if not os.path.isdir(car_folder):
        os.makedirs(car_folder)
        print(car_folder, "erstellt.")
    else:
        print(car_folder, "existiert bereits")


def create_engine_to_db():
    try:
        engine = sqlalchemy.create_engine('sqlite:///scraped.sqlite')
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
        query = sqlalchemy.select([autos.columns.url])
        result_proxy = connection.execute(query)
        result_set = result_proxy.fetchall()
        visited_urls = [r[0] for r in result_set]
        print(f'Länge der Liste besuchter URLs: {len(visited_urls)}')
        return visited_urls
    except Exception as error:
        print("Error while connecting to sqlite: ", error)
    finally:
        connection.close()


def scrape_autoscout(visited_urls):
    countries = {"Deutschland": "D"}

    car_counter = 1
    filterset = {"url", "country", "date", "Fahrzeughalter", "HU/AU neu", "Garantie", "Scheckheftgepflegt",
                 "Nichtraucherfahrzeug", "Marke", "Modell", "Angebotsnummer", "Erstzulassung",
                 "Außenfarbe", "Lackierung", "Farbe laut Hersteller", "Innenausstattung", "Karosserieform",
                 "Anzahl Türen", "Sitzplätze",
                 "Schlüsselnummer", "Getriebeart", "Hubraum", "Anstriebsart", "Kraftstoff", "Schadstoffklasse",
                 "Feinstaubplakette", "Leistung",
                 "Kilometerstand", "Getriebe", "Kraftstoffverbr.*", "Ausstattung", "ort", "price", "HU Prüfung"}
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

    multiple_cars_dict = {}

    for country in countries:

        car_URLs = []
        car_URLs_unique = []
        # Suche nach Hersteller + Modell
        make = 'BMW'
        model = '120'
        for page in range(1, 2):
            try:
                url = 'https://www.autoscout24.de/lst/' + make + '/' + model + '?sort=age&desc=1&ustate=N%2CU&size=20&page=' \
                      + str(page) + '&cy=' + countries[country] + '&atype=C&'
                only_a_tags = SoupStrainer("a")
                soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml', parse_only=only_a_tags)
                for link in soup.find_all("a"):
                    if r"/angebote/" in str(link.get("href")):
                        car_URLs.append(link.get("href"))

                car_URLs_unique = [car for car in list(set(car_URLs)) if car not in visited_urls]
                print(f'Lauf {make} {model} | {country} | Seite {page} | {len(car_URLs_unique)} neue URLs')

            except Exception as e:
                print("Übersicht: " + str(e) + " " * 50)
                pass

        if len(car_URLs_unique) > 0:
            for URL in car_URLs_unique:
                print(f'Lauf {make} {model} | {country} | Auto {car_counter}' + ' ' * 50)
                try:
                    car_counter += 1
                    car_dict = {}
                    car_dict["country"] = country
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
                        "".join(re.findall(r'[0-9]+', car.find("div", attrs={"class": "cldt-price"}).text))

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
                    multiple_cars_dict[URL] = car_dict
                    print(car_dict)
                except Exception as e:
                    print("Detailseite: " + str(e) + " " * 50)
                    pass
            print("")

    return multiple_cars_dict


def save_cars(multiple_cars_dict, engine):
    if len(multiple_cars_dict) > 0:
        df = pd.DataFrame(multiple_cars_dict).T
        df.to_csv("out/data/autos/" + re.sub("[.,:,-, ]", "_", str(datetime.now())) + ".csv", sep=";",
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
