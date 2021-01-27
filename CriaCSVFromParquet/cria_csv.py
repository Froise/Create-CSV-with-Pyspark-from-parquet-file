# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, time, date
import boto3
import json

from typing import List
from typing import Tuple

from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import LongType, StructType, StructField, ArrayType, StringType, DecimalType
from pyspark.sql.dataframe import DataFrame as DFrame

valor_ambiente = "{AMBIENTE}"
ambiente = 'dev'
if valor_ambiente.upper() == 'HOMOLOGACAO':
    ambiente = 'hml'
elif valor_ambiente.upper() == 'PRODUCAO':
    ambiente = 'prd'
else:
    ambiente = 'dev'

secret = f'{ambiente}/senha/redshiftDW'
region = 'us-east-1'
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region
)

resp = client.get_secret_value(SecretId=secret).get('SecretString')
resp = json.loads(resp)

NOME_JOB = "GerarArquivoCSV"


def transform(self, f):
    return f(self)

DFrame.transform = transform



def mes_processamento(mes, ano, operacao = 0):
    vigente = datetime.combine(date(ano,mes, 1),time(23, 59, 59, 999999))
    if operacao == 0:
        if mes == 1:
            mes = 12
            ano = ano - 1
            vigente = datetime.combine(date(ano, mes, 1),time(00, 00, 00))
            print(vigente,'inicio do mes')
        else:
            mes = mes - 1
            vigente = datetime.combine(date(ano, mes, 1),time(00, 00, 00))
            print(vigente,'inicio do mes')
    elif operacao == 1:
        vigente = datetime.combine(date(ano, mes, 1),time(23, 59, 59, 999999))
        vigente = vigente - timedelta(days=1)
        print(vigente,'fim do mes vigente')
    else:
        vigente = datetime.combine(date(ano, mes, 1),time(23, 59, 59, 999999))
        print(vigente,'mes processamento')
    return vigente


def criar_tabelas(sqlContext: SQLContext, tabelas: List[Tuple[str, str]]):
    for tabela in tabelas:
        caminho, nome_tabela = tabela
        sqlContext.registerDataFrameAsTable(
            spark.read.parquet(caminho),
            nome_tabela
        )




# TODO um tratamento para tratar os dados como o CPF para não vir como numerico e ficar estranho na tabela
# NOTE cria lista com as tabelas com sqlcontext
def criar_tabelas_datalake(sqlContext: SQLContext) -> None:
    tabelas = [
        (AB_ASSISTENCIA_TB, "ab_assistencia_tb")
        ]

    criar_tabelas(sqlContext, tabelas)



def listar_assistencias(sqlContext: SQLContext) -> DataFrame:
    query = (
    """
    SELECT 
        concat('ID#',s.cliente_id) as pk
        ,concat(s.apolice_id,'#',s.proposta_id,'#',s.nome_fornecedor) as sk
        ,s.apolice_id as APOLICE
        ,s.proposta_id as PROPOSTA
        ,s.situacao
        ,s.nome_fornecedor
        ,s.produto_id as PRODUTO
        ,s.nome_produto as NOME_PRODUTO
        ,s.assistencia_id as ID_PACOTE_ASSISTENCIA
        ,s.nome_assistencia as NOME_PACOTE_ASSISTENCIA
        ,s.cidade as CIDADE
        ,s.uf as ESTADO
        ,s.cnpj_subgrupos as SUB_GRUPO 
        ,s.dt_inicio_vigencia as DT_INIC_VIG
        ,s.dt_fim_vigencia as DT_FIM_VIG
        ,s.cliente_id as CLIENTE_ID
        ,s.valor_mensal as VLR_PACOTE_MENSAL
        ,s.op
        ,s.data_exportacao
    FROM {}_assistencia_tb as s
    WHERE s.segurado_cliente_id is not null
    GROUP BY
        s.apolice_id
        ,s.proposta_id
        ,s.situacao
        ,s.nome_fornecedor
        ,s.produto_id
        ,s.nome_produto
        ,s.assistencia_id
        ,s.nome_assistencia
        ,s.cidade
        ,s.uf
        ,s.cnpj_subgrupos
        ,s.dt_inicio_vigencia
        ,s.dt_fim_vigencia
        ,s.cliente_id
        ,s.valor_mensal
        ,s.op
        ,s.data_exportacao
    ORDER BY s.op desc    
    """
    )
    df_assistencias_ab = sqlContext.sql(query.format('AB'))
    fim = df_assistencias_ab.distinct()
    sqlContext.registerDataFrameAsTable(fim, "final")
    return fim


def caminho_csv(prestadora,mes_ano, lista):
        for obj in s3.objects.filter(Prefix=f'{prestadora}/Temp/{mes_ano}/'):
            print(obj.key)
            lista.append(obj.key)


def remove_outros_arquivos(lista):
    print(lista)
    for item in lista:
        if item.endswith('.csv') == False:
            lista.remove(item)


def move_renomeia_arquivo(lista,caminho_prestadora):
    for item in lista:
        copy_source = {
        'Bucket': bucket,
        'Key': item
        }
        s3.copy(copy_source, f'{caminho_prestadora}.csv')

def apaga_folder(prestador,mes_ano):
    s3.objects.filter(Prefix=f"{prestador}/Temp/{mes_ano}").delete()







if __name__ == '__main__':


    print("inicio do processo",datetime.utcnow(),'\n')

    PREFIXO = "{BUCKET_DATA_LAKE}"
    PREFIXO_AB = f"{PREFIXO}/assistencias_db"
    AB_ASSISTENCIA_TB = f"{PREFIXO_AB}/assistencia_tb"

    configuracao = (SparkConf()
        .set("spark.driver.maxResultSize", "2g"))

    spark = (SparkSession
        .builder
        .config(conf=configuracao)
        .appName(NOME_JOB)
        .enableHiveSupport()
        .getOrCreate())

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)


    criar_tabelas_datalake(sqlContext)

# NOTE CAMPO DE TESTES ABAIXO 

    lista_Fornecedor_1 = []
    lista_Fornecedor_2 = []
    bucket = f'seguradora-{ambiente}-assistencia'
    bucket_csv = f's3://seguradora-{ambiente}-assistencia/'

    df = listar_assistencias(sqlContext)

    #TODO tirar os prints
    print("apolices a processar: ", df.count())

    from datetime import datetime, timedelta, date, time
    mes = int(datetime.today().month)
    ano = int(datetime.today().year)

    dt_ini = mes_processamento(mes,ano,0)
    dt_fim = mes_processamento(mes,ano,1)

    print('dt inicio do processo:',dt_ini, 'dt fim do processo:',dt_fim,'\n')
    print('qtd sem filtragem' ,df.count())

    print("antes do filtro de delete",df.count())
    df = df.filter(df.op != "D")
    print("depois do filtro de delete",df.count())

    df = df.sort(col("data_exportacao").desc())
    filtro_assistencias = df.filter( (df.DT_FIM_VIG.isNull()) | (df.DT_FIM_VIG >= dt_ini))
    print('dt_fim null or >= ',dt_ini ,' Quantidade:' ,filtro_assistencias.count())

    filtro_assistencias = filtro_assistencias.filter( (df.DT_INIC_VIG.isNotNull()) & (df.DT_INIC_VIG <= dt_fim))
    print('dt_inicio not null e <= ',dt_fim,' Quantidade:' ,filtro_assistencias.count(),'\n')

    filtro_a = filtro_assistencias.drop_duplicates(subset=['pk','sk'])
    print("filtragem pela pk e sk, removendo duplicados:", filtro_a.count())

    sqlContext.registerDataFrameAsTable(filtro_a,"assistencias_tb")
    df_assistencias = sqlContext.sql(
        """
        SELECT 
            tb.APOLICE
            ,tb.PROPOSTA
            ,tb.PRODUTO
            ,concat('"',trim(tb.NOME_PRODUTO),'"') as NOME_PRODUTO
            ,tb.ID_PACOTE_ASSISTENCIA
            ,concat('"',trim(NOME_PACOTE_ASSISTENCIA),'"') as NOME_PACOTE_ASSISTENCIA
            ,tb.SUB_GRUPO
            ,tb.CLIENTE_ID
            ,to_timestamp(tb.DT_INIC_VIG) as DT_INIC_VIG
            ,to_timestamp(tb.DT_FIM_VIG) as DT_FIM_VIG
            ,tb.VLR_PACOTE_MENSAL
            ,concat('"',trim(tb.CIDADE),'"') as CIDADE
            ,concat('"',trim(tb.ESTADO),'"') as ESTADO
            ,tb.nome_fornecedor
        FROM assistencias_tb as tb
        """)
    df_assistencias = df_assistencias.transform(lambda df: df.withColumn("DT_FIM_VIG", col("DT_FIM_VIG").cast(StringType())))


    assistencias = (df_assistencias
        .transform(lambda df: df.withColumn("APOLICE", col('APOLICE').cast(LongType())))
        .transform(lambda df: df.withColumn("PROPOSTA", col('PROPOSTA').cast(LongType())))
        .transform(lambda df: df.withColumn("PRODUTO", col('PRODUTO').cast(LongType())))
        .transform(lambda df: df.withColumn("NOME-PRODUTO", col('NOME_PRODUTO').cast(StringType())))
        .transform(lambda df: df.withColumn("ID-PACOTE-ASSISTENCIA", col('ID_PACOTE_ASSISTENCIA').cast(StringType())))
        .transform(lambda df: df.withColumn("NOME-PACOTE-ASSISTENCIA", col('NOME_PACOTE_ASSISTENCIA').cast(StringType())))
        .transform(lambda df: df.withColumn("SUB-GRUPO", col('SUB_GRUPO').cast(LongType())))
        .transform(lambda df: df.withColumn("CLIENTE-ID", col('CLIENTE_ID').cast(LongType())))
        .transform(lambda df: df.withColumn("DT-INIC-VIG", date_format(col("DT_INIC_VIG"),"dd/MM/yyyy").cast(StringType())))
        .transform(lambda df: df.withColumn("VLR-PACOTE-ASSISTENCIA", col('VLR_PACOTE_MENSAL').cast(DecimalType(10,2))))
        .transform(lambda df: df.withColumn("DT-FIM-VIG", date_format(col("DT_FIM_VIG"),"dd/MM/yyyy").cast(StringType())))
        .transform(lambda df: df.withColumn("CIDADE", col('CIDADE').cast(StringType())))
        .transform(lambda df: df.withColumn("ESTADO", col('ESTADO').cast(StringType())))
        )
    assistencias = assistencias.fillna({'CLIENTE-ID':'0','SUB-GRUPO':'0','PRODUTO':'0','VLR-PACOTE-ASSISTENCIA':'0'})

    df_M = assistencias.select(
    'APOLICE'
    ,'PROPOSTA'
    ,'PRODUTO'
    ,'NOME-PRODUTO'
    ,'ID-PACOTE-ASSISTENCIA'
    ,'NOME-PACOTE-ASSISTENCIA'
    ,'SUB-GRUPO'
    ,'CLIENTE-ID'
    ,'DT-INIC-VIG'
    ,'DT-FIM-VIG'
    ,'VLR-PACOTE-MENSAL'
    ,'CIDADE'
    ,'ESTADO'
    ).where('nome_fornecedor = "Fornecedor_1"')
    print('processados Fornecedor_1:',df_M.count())

    df_T = assistencias.select(
    'APOLICE'
    ,'PROPOSTA'
    ,'PRODUTO'
    ,'NOME-PRODUTO'
    ,'ID-PACOTE-ASSISTENCIA'
    ,'NOME-PACOTE-ASSISTENCIA'
    ,'SUB-GRUPO'
    ,'CLIENTE-ID'
    ,'DT-INIC-VIG'
    ,'DT-FIM-VIG'
    ,'VLR-PACOTE-MENSAL'
    ,'CIDADE'
    ,'ESTADO'
    ).where('nome_fornecedor = "Fornecedor_2"')
    print('processados Fornecedor_1:',df_T.count(),'\n')


        

    mes_processo = dt_ini.month
    ano_processo = dt_ini.year
    if mes_processo < 10:
        mes_processo = '0'+str(mes_processo)
    mes_ano_csv = str(mes_processo)+ str(ano_processo)


  
    print("Criando csv da Fornecedor_1", datetime.utcnow())
    df_T.coalesce(1).write.csv(bucket_csv+'Fornecedor_1/Temp/'+mes_ano_csv, header=True,sep=';',quote='\\',encoding='UTF-8',nullValue='', emptyValue='')
    print("fim csv da Fornecedor_1", datetime.utcnow(),'\n')

    print("Criando csv da Fornecedor_2", datetime.utcnow())
    df_M.coalesce(1).write.csv(bucket_csv+'Fornecedor_2/Temp/'+mes_ano_csv, header=True,sep=';',quote='\\',encoding='UTF-8',nullValue='', emptyValue='')
    print("fim csv da Fornecedor_2", datetime.utcnow(),'\n')

    print('fim do processamento',datetime.utcnow())

    print("conecta no s3\n")
    client = boto3.resource('s3')

    s3 = client.Bucket(bucket)

    print("pega os arquivos a serem processados")    
    caminho_csv('Fornecedor_1',mes_ano_csv,lista_Fornecedor_1)
    caminho_csv('Fornecedor_2',mes_ano_csv,lista_Fornecedor_2)


    print("apaga os arquivos que nao sao .csv do folder temporario")    
    remove_outros_arquivos(lista_Fornecedor_1)
    remove_outros_arquivos(lista_Fornecedor_2)


    print("renomeia e move os arquivos para pasta de faturamento")

    print(f'Fornecedor_1/faturamento/Fornecedor_1-{mes_ano_csv}')    
    move_renomeia_arquivo(lista_Fornecedor_1,f'Fornecedor_1/faturamento/FORNECEDOR_1-{mes_ano_csv}')    
    print(f'Fornecedor_2/faturamento/Fornecedor_2-{mes_ano_csv}')
    move_renomeia_arquivo(lista_Fornecedor_2,f'Fornecedor_2/faturamento/FORNECEDOR_2-{mes_ano_csv}')

    print("move arquivos para pasta de enviados")    
    move_renomeia_arquivo(lista_Fornecedor_1,f'Fornecedor_1/enviados/FORNECEDOR_1-{mes_ano_csv}')
    move_renomeia_arquivo(lista_Fornecedor_2,f'Fornecedor_2/enviados/FORNECEDOR_2-{mes_ano_csv}')

    print("apaga folder temporario")
    apaga_folder('Fornecedor_1',mes_ano_csv)
    apaga_folder('Fornecedor_2',mes_ano_csv)
    