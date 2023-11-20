import pandas as pd
from pysus.online_data.SIH import download

ano = 2019
estado = 'DF'
meses = [1, 2, 3]  # Exemplo: Baixar dados para janeiro, fevereiro e março

dados_completos = pd.DataFrame()

for mes in meses:
    caminho_arquivo = download(estado, ano, mes)

    # Verifique se o arquivo foi baixado
    if caminho_arquivo:
        df = pd.read_parquet(caminho_arquivo)
        dados_completos = pd.concat([dados_completos, df], ignore_index=True)
    else:
        print(f"Erro ao baixar dados para {estado} no mês {mes} do ano {ano}")

# Prossiga para salvar em CSV somente se houver dados
if not dados_completos.empty:
    caminho_arquivo_csv = '/home/daniel.moraes/amb_testes_daniel/pysus/arquivos_csv/SIH_DF.csv'
    dados_completos.to_csv(caminho_arquivo_csv, index=False)
else:
    print("Nenhum dado foi baixado.")

