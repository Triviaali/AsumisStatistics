import json
from os import path
from typing import List
import sqlite3
import pandas as pd
from pandas import DataFrame


def normalize_path(filepath: str) -> path:
    return path.normpath(filepath)


def json_to_dict(filename: str) -> dict:
    filepath = normalize_path(filename)
    with open(filepath, 'r', encoding='utf-8') as file:
        data: dict = json.loads(file.read())
        return data


def write_csv(columns: List[str], rows: List[list], filename: str):
    # Use ; as a separator since some input might have commas.
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(f"{';'.join(columns)}\n")
        for row in rows:
            file.write(f"{';'.join(row)}\n")


def join_keys_values(datapoint: dict):
    keys = datapoint['keys']
    values = datapoint['values']

    # keys.extend(values)
    # .extend is in-place operation so use +
    extended = keys + values

    res = [str(val) for val in extended]

    return res


def json_to_row_form_csv(input_file: str, output_file: str):
    data = json_to_dict(input_file)

    columns = data['columns']
    data = data['data']

    row_data = [join_keys_values(datapoint) for datapoint in data]

    write_csv(columns=columns, rows=row_data, filename=output_file)


def ansiotulo_indeksi():
    json_to_row_form_csv(normalize_path('data\\ansiotasoindeksi.json'), "tehtava1_output.csv")


def osake_yhtio_indeksi():
    json_to_row_form_csv(normalize_path('data\\uusien_osakeasuntojen_hintaindeksi.json'), "tehtava2_output.csv")


def vuokraindeksi():
    json_to_row_form_csv(normalize_path('data\\vuokraindeksi.json'), "tehtava3_output.csv")


def tehtava4():
    db_connection = sqlite3.connect(normalize_path('data\\database.db'))
    sql_query = """select vuosineljannes, SUM("kauppojen lukumäärä") as "Kauppojen Lukumäärä" from uusien_osakeasuntojen_hintaindeksi
                where alue LIKE 'Helsinki' or alue like 'Espoo-Kauniainen' and talotyyppi like 'Yksiö'
                group BY vuosineljannes;"""
    res = db_connection.execute(sql_query)
    return res.fetchall()


def tehtava5():
    db_connection = sqlite3.connect(normalize_path('data\\database.db'))

    # Was not sure about the assigment so one for quarters and number of rooms.
    sql_query = """select uoh.vuosineljannes, uoh.alue, uoh.huoneluku, round((ai.Ansiotasoindeksi / uoh.Indeksi),2) as "Hinta-indeksi", round((ai.ansiotasoindeksi / vi.Indeksi), 2) as "Vuokra-Indeksi" from uusien_osakeasuntojen_hintaindeksi uoh
                    LEFT join vuokraindeksi vi on vi.Vuosineljannes = uoh.Vuosineljannes
                    left join ansiotasoindeksi ai on uoh.Vuosineljannes = ai.Vuosineljannes
                    where uoh.alue like 'Vantaa'
                    group by uoh.Vuosineljannes, uoh.huoneluku
                    order by uoh.vuosineljannes, uoh.Talotyyppi DESC;"""

    # One by quarters
    sql_queery_quarters = """select uoh.vuosineljannes, uoh.alue, round((ai.Ansiotasoindeksi / uoh.Indeksi),2) as "Hinta-indeksi", round((ai.ansiotasoindeksi / vi.Indeksi), 2) as "Vuokra-Indeksi" from uusien_osakeasuntojen_hintaindeksi uoh
                    LEFT join vuokraindeksi vi on vi.Vuosineljannes = uoh.Vuosineljannes
                    left join ansiotasoindeksi ai on uoh.Vuosineljannes = ai.Vuosineljannes
                    where uoh.alue like 'Vantaa'
                    group by uoh.Vuosineljannes
                    order by uoh.vuosineljannes, uoh.Talotyyppi DESC;"""

    res = db_connection.execute(sql_query)
    return res.fetchall()


def tehtava6():
    # Read omistus dataframe
    omistus_dataframe: DataFrame = pd.read_csv(normalize_path('tehtava2_output.csv'), header=0, sep=';')
    # Read vuokra dataframe
    vuokra_dataframe: DataFrame = pd.read_csv(normalize_path('tehtava3_output.csv'), header=0, sep=";")
    # Join dataframes on columns
    merged_dataframe = pd.merge(omistus_dataframe, vuokra_dataframe, on=['Vuosineljannes', 'Alue', 'Huoneluku'])

    # Group by necessary fields and create new aggregate columns.
    grouped_and_aggregated = merged_dataframe.groupby(["Alue", "Huoneluku"]).agg(
        kmp=pd.NamedAgg(column="Kauppojen lukumäärä", aggfunc="max"),
        kmy=pd.NamedAgg(column="Kauppojen lukumäärä", aggfunc="sum"),
        vsk=pd.NamedAgg(column="Indeksi_y", aggfunc="mean"),
        vnmax=pd.NamedAgg(column="Vuokra_per_nelio", aggfunc="max")).reset_index()

    # Rename columns
    res = grouped_and_aggregated.set_axis(
        ["Alue", "Huoneluku", "Kauppojen määrän maksimi", "Kauppojen määrä yhteensä", "Vuokraindeksin keskiarvo",
         "Vuokra_per_nelio maksimi"], axis=1, inplace=False)

    # Round floats
    res = res.round(2)

    # Save in the same format as previous files.
    res.to_csv('tehtava6_output.csv', sep=";", encoding='utf-8')


if __name__ == "__main__":
    # Exercises 1 to 3

    ansiotulo_indeksi()

    osake_yhtio_indeksi()

    vuokraindeksi()

    # Exercise 4

    [print(row) for row in tehtava4()]

    # Exercise 5

    [print(row) for row in tehtava5()]

    # Exercise 6

    tehtava6()
