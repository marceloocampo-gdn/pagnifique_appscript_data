from utils import make_client, read_sheet_to_df

gc = make_client("secrets/google_sa.json")

df = read_sheet_to_df(
    gc=gc,
    sheet_url="https://docs.google.com/spreadsheets/d/1uJsS-OdxPnRG-aiQpXosWfrKvWpb6VTKc_uDA6PryT4/edit",
    worksheet="Prueba"
)

print(df.head())
print("Conexión correcta ✔")
