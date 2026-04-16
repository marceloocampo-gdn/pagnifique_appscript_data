import json
from decimal import Decimal
from datetime import date, datetime, time  
from utils import (
    load_config,
    make_client,
    write_gsheet,
    snowflake_login,
    descargar_query_cond
)

def normalize_values(df):
    """
    Normaliza tipos no compatibles con Google Sheets:
    - Decimal → float
    - date → 'YYYY-MM-DD'
    - datetime → 'YYYY-MM-DD HH:MM:SS'
    - time → 'HH:MM:SS'
    """

    def convert_value(x):
        # Decimal → float
        if isinstance(x, Decimal):
            return float(x)

        # datetime → 'YYYY-MM-DD HH:MM:SS'
        if isinstance(x, datetime):
            return x.strftime("%Y-%m-%d %H:%M:%S")

        # date → 'YYYY-MM-DD'
        if isinstance(x, date):
            return x.strftime("%Y-%m-%d")

        # time → 'HH:MM:SS'
        if isinstance(x, time):
            return x.strftime("%H:%M:%S")

        return x

    return df.applymap(convert_value)


def main():

    print("Iniciando ejecución...")

    # 1. CONFIG
    cfg = load_config()

    # Google
    gc = make_client(cfg["google"]["service_account_json"])
    sheet_url = cfg["google"]["sheet_url"]

    # Snowflake
    with open(cfg["snowflake"]["service_account_json"], "r", encoding="utf-8") as file:
        snow_keys = json.load(file)

    print("Conectando a Snowflake...")
    user, cursor, conn = snowflake_login(
        user=snow_keys["user"],
        password=snow_keys["password"],
        account=snow_keys["account"],
        database=snow_keys["database"],
        schema=snow_keys["schema"],
        require_passcode=False
    )

    # 2. EJECUTAR TODAS LAS CONSULTAS
    for q in cfg["queries"]:

        sql_file = q["file"]
        worksheet_name = q["worksheet"]
        write_range = q.get("range", "A1")

        print(f"\nEjecutando SQL: {sql_file}")

        df = descargar_query_cond(
            cursor=cursor,
            query=sql_file
        )

        print(f"Filas obtenidas: {len(df)}")

        # Convertir valores no compatibles con Google Sheets
        df = normalize_values(df)

        print(f"Cargando resultados en hoja '{worksheet_name}'...")
        write_gsheet(
            df=df,
            spreadsheet_url=sheet_url,
            worksheet_name=worksheet_name,
            gc=gc,
            cell_range=write_range,
            clean=False
        )

    print("\nTODAS las consultas fueron procesadas con éxito.")


if __name__ == "__main__":
    main()
