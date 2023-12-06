import pandas as pd
from pysus.ftp.databases.sih import SIH
from sqlalchemy import create_engine

def download_sih_data(uf, year, months, local_dir):
    sih = SIH().load()  # Carrega os arquivos do DATASUS
    files = sih.get_files('RD', uf=uf, year=year, month=months)  # Obtém os arquivos desejados
    sih.download(files, local_dir=local_dir)  # Baixa os arquivos

    data_frames = []
    for file in files:
        file_path = local_dir + file.name + '.parquet'  # Caminho do arquivo baixado
        df = pd.read_parquet(file_path)  # Lê cada arquivo Parquet como DataFrame
        data_frames.append(df)
    return pd.concat(data_frames, ignore_index=True)

def save_to_txt(data_frame, file_name):
    data_frame.to_csv(file_name, sep='\t', index=False)

def insert_into_db(data_frame, db_name, table_name, user, password, host, port):
    # Cria uma string de conexão e um engine do SQLAlchemy
    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(conn_string)

    # Insere dados no banco de dados
    data_frame.to_sql(table_name, engine, if_exists='append', index=False)

    print("Dados inseridos com sucesso no banco de dados.")

# Defina os parâmetros para os quais você deseja extrair os dados
uf = 'DF'  # Estado de São Paulo
year = 2023
months = [10]
#months = [1, 2]  # Janeiro e Fevereiro
local_dir = './'  # Diretório local para salvar os arquivos baixados

# Baixe os dados e carregue-os em um DataFrame
sih_data = download_sih_data(uf, year, months, local_dir)

# Salve os dados em um arquivo .txt
file_name = 'sih_data_2023_01_02_SP.txt'
save_to_txt(sih_data, file_name)

# Insira os dados no banco de dados PostgreSQL
db_name = 'sih_db'  # Nome do banco de dados
table_name = 'sih_table'  # Nome da tabela
user = 'daniel_iges'  # Seu usuário do banco de dados
password = 'iges'  # Sua senha do banco de dados
host = 'localhost'
port = '5432'

insert_into_db(sih_data, db_name, table_name, user, password, host, port)
