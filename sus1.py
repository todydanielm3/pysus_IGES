import pandas as pd
from pysus.online_data.SIH import download

ano = 2023
estado = 'DF'
meses = [1, 2, 3]  # Exemplo: Baixar dados para janeiro, fevereiro e março

dados_completos = pd.DataFrame()

for mes in meses:
    caminho_arquivo = ''


    # Verifique se o arquivo foi baixado
    if caminho_arquivo:
        df = pd.read_parquet(caminho_arquivo)
        dados_completos = pd.concat([dados_completos, df], ignore_index=True)
    else:
        print(f"Erro ao baixar dados para {estado} no mês {mes} do ano {ano}")

# Prossiga para salvar em CSV somente se houver dados
if not dados_completos.empty:
    caminho_arquivo = 'arquivos_csv/SIH_DF.txt'
    #dados_completos.to_csv(caminho_arquivo_csv, index=False)
    #arquivo txt
    dados_completos.to_csv(caminho_arquivo, sep='\t', index=False)


else:
    print("Nenhum dado foi baixado.")

