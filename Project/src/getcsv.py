import requests
import shutil

url_csv = "https://www.datosabiertos.gob.pe/sites/default/files/Programas%20de%20Universidades.csv"

path_folder_csv = "C:/Users/BRAYAN LIPE/Documents/UNSA/2020/SEMESTRE B/Proyecto Final de Carrera/Project/files/dataset.csv"


def download_file(url):
    with requests.get(url, stream=True) as r:
        r.raw.decode_content = True
        with open(path_folder_csv, "wb") as file:
            shutil.copyfileobj(r.raw, file)


download_file(url_csv)
