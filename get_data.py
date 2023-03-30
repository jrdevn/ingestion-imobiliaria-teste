from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import boto3
from dotenv import load_dotenv
import os

def get_data_brokers():
    load_dotenv()
    html = requests.get("https://www.brokersecia.com/imovel/?finalidade=venda").content
    soup = BeautifulSoup(html, 'html.parser')
    retorno = make_dictionary_from_html(soup)
    df = pd.read_json(retorno)
    df.index.name = "id"
    name_file = 'dados_imobiliaria.csv'
    df.to_csv(name_file)
    s3 = connect_s3()
    s3.Bucket('ingestion-imobiliaria').upload_file(Filename=name_file, Key=name_file)
    

def make_dictionary_from_html(conteudo):
    #teste = conteudo.find_all("h2", class_="imovelcard__info__tag")
    #teste2 = conteudo.find_all("h2", class_="imovelcard__info__local")
    dados_imobiliaria = []
    teste3 = conteudo.find_all("div", class_="imovelcard")
    for row in teste3:
        dict_imobiliaria = {}
        infos_array = []
        
        ## venda ou aluguel - opcao
        opcao = row.find("h2", class_="imovelcard__info__tag")
        if opcao != None:
            dict_imobiliaria["opcao"] = opcao.text
            
        ## local - logradouro
        local = row.find("h2", class_="imovelcard__info__local")
        if local != None: 
            dict_imobiliaria["local"] = local.text    
            
        ## valor     
        valor = row.find("p", class_="imovelcard__valor__valor")
        if valor != None: 
            dict_imobiliaria["valor"] = valor.text    
            
        ## descricao do imovel   
        descricao = row.find("div", class_="imovelcard__info__tab__content__descricao")
        if descricao != None:
            descricao = descricao.h3.text
            dict_imobiliaria["descricao"] = remove_breaklines_string(descricao)
            
        ## tipo de imovel, apartamento, casa e etc..    
        tipo_imovel = row.find("p", class_="imovelcard__info__ref")
        if tipo_imovel != None:
            tipo_imovel = tipo_imovel.text.split('-')
            dict_imobiliaria["tipo_imovel"] = (tipo_imovel[1].lstrip())
            
        ## as infos do imovel
        infos = row.find_all("div", class_="imovelcard__info__feature")
        if infos != None:
            for info in infos:
                conteudo = info.find("p").text
                infos_array.append(f"{conteudo}")
            if len(infos_array) > 0:
                dict_imobiliaria["infos"] = ', '.join(infos_array)

        ## verifica se é um dicionario valido
        if dict_imobiliaria != {} and dict_imobiliaria != None:
            dados_imobiliaria.append(dict_imobiliaria)
    if len(dados_imobiliaria) > 0:
        dados_imobiliaria = json.dumps(dados_imobiliaria, ensure_ascii=False)
        
    return dados_imobiliaria    



def remove_breaklines_string(value):
    return ''.join(value.splitlines())

def connect_s3():
   s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
    )
   return s3

get_data_brokers()