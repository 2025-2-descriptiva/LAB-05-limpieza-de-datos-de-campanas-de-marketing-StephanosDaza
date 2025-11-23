"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import os
    import pandas as pd
    import zipfile
    import io
    import glob

    input_path = "files/input/"
    output_path = "files/output/"
    os.makedirs(output_path, exist_ok=True)

    dfs = []
    for zip_path in glob.glob(rf"{input_path}*.zip"):  
        with zipfile.ZipFile(zip_path) as z:
            for nombre in z.namelist():
                if nombre.lower().endswith(".csv"):
                    with z.open(nombre) as f:
                        dfs.append(pd.read_csv(f))
    df = pd.concat(dfs, ignore_index=True)

    cliente=df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    cliente["job"] = cliente["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    cliente["education"] = cliente["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
    cliente["credit_default"] = (cliente["credit_default"].astype(str).str.strip().str.lower().eq("yes").astype("int8"))
    cliente["mortgage"] = (cliente["mortgage"].astype(str).str.strip().str.lower().eq("yes").astype("int8")) 

    cliente.to_csv(os.path.join(output_path, "client.csv"), index=False)


    month_map = {
        # Inglés
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12}
    
    df["month"] = df["month"].astype(str).str.strip().str.lower().map(month_map)

    campaña=df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]].copy()
    campaña["previous_outcome"] = (campaña["previous_outcome"].astype(str).str.strip().str.lower().eq("success").astype("int8"))
    campaña["campaign_outcome"] = (campaña["campaign_outcome"].astype(str).str.strip().str.lower().eq("yes").astype("int8"))
    campaña["last_contact_date"] = pd.to_datetime(campaña.assign(year=2022).loc[:, ["year", "month", "day"]])
    campaña.drop(columns=["day", "month"], inplace=True)  

    campaña.to_csv(os.path.join(output_path, "campaign.csv"), index=False)
    
    economia=df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
    
    economia.to_csv(os.path.join(output_path, "economics.csv"), index=False)



    return None


if __name__ == "__main__":
    clean_campaign_data()