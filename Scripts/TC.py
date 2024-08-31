
import google.auth
import gspread
import openai
import os
import pandas as pd
import re
import itertools
import json
import streamlit as st

#import gradio as gr

from google.auth import default
from google.colab import auth
from gspread_dataframe import set_with_dataframe
from tqdm import tqdm
from openai.embeddings_utils import get_embedding
from openai.embeddings_utils import cosine_similarity
from langchain.callbacks import get_openai_callback
from langchain.chat_models import ChatOpenAI
from langchain.prompts.chat import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    SystemMessagePromptTemplate,
)
from langchain.schema import HumanMessage, SystemMessage
from datetime import datetime

from google.oauth2.service_account import Credentials



OPENAI_API_KEY= st.secrets["OPENAI_API_KEY"]
openai.api_key = OPENAI_API_KEY


def tc(spreadsheetUrl,sheetName,columnName_Title,spreadsheetUrl_DB):


  def retrieve_spreadsheet(spreadsheet_url, sheet_name):
    """
    Retrieves data from a specified Google Sheets worksheet and converts it into a pandas DataFrame.

    Args:
        spreadsheet_url (str): The URL of the Google Sheets.
        sheet_name (str): The name of the sheet within the Google Sheets.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the worksheet,
                      or None if an error occurs.
    """
    json_key = '''
    {
    "type": "service_account",
    "project_id": "invertible-now-393117",
    "private_key_id": "7c8140d98b3d44bd4b51d752bc8b6a685a6977c5",
    "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCnfif/GUDnQBPa\\ndZyL7ZHRQqFP0vFKsj+egkH5tp8osXLEQmZSi5/2w6ffDQnLvL94hKMvmgoDi+a+\\noZK8He6g43C7W8vTjbq8kdeYbJCrDjSYVTltgBHog6oME1l5yvVbCEhIEwEK/KGz\\n5Skn9Kgw4fnejI1aWKbjZ45p3AdKPPU7prvG5Yl43jav1mBj4mSyHloFepsaVBcF\\nCwq975BvCxGf0SjIP4xWzQy8V4jUmP3WZzNeLwMXnLv9Wuv0ITLRJi+gJN5Nxm2+\\n9r4drri7WOPICjPJ1Rv7N3fjmdagVvAXVTJTGJUJiUJu4jErJs59ptZebw6aPyJS\\nuxK9qEaNAgMBAAECggEAC76laFZpfi24lquDmC5G+ND+xb2pbM719hP1M2DyZSSY\\nQxnS2fvvchrDJTlhU/d+x6EpXjejdx8yxXBH/UfuCTsZlxG3R7TbAMkLQKVwOYZr\\n+riTJ9IAr3i4DlO3BPrN3J3Gj8NBYfdYEWjCy4n010SpREk/yjOINE75JgQnQLXL\\ncv2KzMPqTMiy5jgkP2H/CXXXRMktNsySqSc50vS98JW+w+bjZIc7tiC/mbjQtQQR\\n9RS5pTM480LJiOLXPiwGmr/LESYySqKBnZo23G+ixabf9Vaq92t3pXdf3XhNwvyq\\nTKHiqGSIr7vVJhHPJO4oLH/u2c7szn+n9Jsr/fi/AwKBgQDdm+M6FLplwK/kde8q\\njng1LHj/d20Pi2zb0M2ORe6XBPGiLabV9R8R2iCX7ByONYzJ0GXY2fli/vb2JhOh\\nPNK8/cMDkiXECCl2NUO/yKVlvZvXHRHn8Ihd+gMSBntn2iAACc6+AAP5do8blNT2\\nLFKU64VSmpBVHPRUbGB5RJxz7wKBgQDBfFWxTtwBK5PO/F90eXUUrIIUInVh5Bcy\\nauwu/542T71jbtecTnRU3WkYHoDBa4DpAYngxg/nUXSC+7ezY16SanEUxcIUSvjl\\nmCL+aEiDBasiNuXlZj3IAfvA7Mp4SK8GfG1JYxXicio0j6FCTrkzi7f8m3eiCond\\n5qAWjd4BQwKBgCeE0B2gaqkQlo1QNqlJJMieuKkd+/XksDH252EyuVx3Bjwclf7b\\nqoG9e0h8U49Mn2Gx5yenn2B3BUVZ/vAm75HCUw+E9XUi23n3/6/osQ4WpP7UcUgC\\nTd8sYXXKcCFR9ZjsJtEdIZhP+y84+E06FDP4WBsl8w0qj6uqc/3MLXZDAoGAbd7L\\nzm6ocaWsPmqDTeG2gXHgP8y9eUQLhB7BVYLj9ZVcRz1nBCRs3NAJ4J9Zn/wK7MVp\\n5RCzcTiI/+QukZhI2L3GzvPpXJqiMcYtgOf43SX34urnq1del9fAfPI5mwozEWzQ\\npk6026zWmJhDCyMm+cVKShCCY6q2VSKkH4qZ2X8CgYBcxwtbeACxAKNWntG1kGub\\nJqLci+nalkQJw2FPEvrv4ygq/5Z/dY5MfyEB80euU06pyr1nKuxrs3+Nc6YhWHDc\\n/iCMzxrL1I2Ops7pleeS4yvJPk5xgVC8DAguE2aoGr0CfVvoWa/IkDT9L3WiGKhQ\\nNdKjgHIks4SzAHJL/ReT0g==\\n-----END PRIVATE KEY-----\\n",
    "client_email": "kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com",
    "client_id": "115925417558125099001",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/kalungi-google-colab%40invertible-now-393117.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
    }
    '''
    key_dict = json.loads(json_key)
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(key_dict, scopes=scope)
    client = gspread.authorize(credentials)
    try:
        spreadsheet = client.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_values()
        dataframe = pd.DataFrame(data[1:], columns=data[0])
        return dataframe
    except PermissionError:
        print("Incorrect spreadsheet URL or missing share with kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com. Please correct it and try again.")
        return None
    except gspread.exceptions.WorksheetNotFound:
        print(f"Incorrect or non-existent sheet name. Please correct it and try again.")
        return None
    except gspread.exceptions.SpreadsheetNotFound:
        print("Incorrect or non-existent spreadsheet URL. Please correct it and try again.")
        return None


  def write_into_spreadsheet(spreadsheetUrl, sheet_name, dataframe):
      """
      Writes data from a DataFrame into a specified Google Sheets worksheet.

      Args:
          spreadsheet_url (str): The URL of the Google Sheets.
          sheet_name (str): The name of the sheet within the Google Sheets.
          dataframe (pd.DataFrame): The DataFrame containing data to be written.

      Returns:
          None
      """
      json_key = '''
      {
      "type": "service_account",
      "project_id": "invertible-now-393117",
      "private_key_id": "7c8140d98b3d44bd4b51d752bc8b6a685a6977c5",
      "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCnfif/GUDnQBPa\\ndZyL7ZHRQqFP0vFKsj+egkH5tp8osXLEQmZSi5/2w6ffDQnLvL94hKMvmgoDi+a+\\noZK8He6g43C7W8vTjbq8kdeYbJCrDjSYVTltgBHog6oME1l5yvVbCEhIEwEK/KGz\\n5Skn9Kgw4fnejI1aWKbjZ45p3AdKPPU7prvG5Yl43jav1mBj4mSyHloFepsaVBcF\\nCwq975BvCxGf0SjIP4xWzQy8V4jUmP3WZzNeLwMXnLv9Wuv0ITLRJi+gJN5Nxm2+\\n9r4drri7WOPICjPJ1Rv7N3fjmdagVvAXVTJTGJUJiUJu4jErJs59ptZebw6aPyJS\\nuxK9qEaNAgMBAAECggEAC76laFZpfi24lquDmC5G+ND+xb2pbM719hP1M2DyZSSY\\nQxnS2fvvchrDJTlhU/d+x6EpXjejdx8yxXBH/UfuCTsZlxG3R7TbAMkLQKVwOYZr\\n+riTJ9IAr3i4DlO3BPrN3J3Gj8NBYfdYEWjCy4n010SpREk/yjOINE75JgQnQLXL\\ncv2KzMPqTMiy5jgkP2H/CXXXRMktNsySqSc50vS98JW+w+bjZIc7tiC/mbjQtQQR\\n9RS5pTM480LJiOLXPiwGmr/LESYySqKBnZo23G+ixabf9Vaq92t3pXdf3XhNwvyq\\nTKHiqGSIr7vVJhHPJO4oLH/u2c7szn+n9Jsr/fi/AwKBgQDdm+M6FLplwK/kde8q\\njng1LHj/d20Pi2zb0M2ORe6XBPGiLabV9R8R2iCX7ByONYzJ0GXY2fli/vb2JhOh\\nPNK8/cMDkiXECCl2NUO/yKVlvZvXHRHn8Ihd+gMSBntn2iAACc6+AAP5do8blNT2\\nLFKU64VSmpBVHPRUbGB5RJxz7wKBgQDBfFWxTtwBK5PO/F90eXUUrIIUInVh5Bcy\\nauwu/542T71jbtecTnRU3WkYHoDBa4DpAYngxg/nUXSC+7ezY16SanEUxcIUSvjl\\nmCL+aEiDBasiNuXlZj3IAfvA7Mp4SK8GfG1JYxXicio0j6FCTrkzi7f8m3eiCond\\n5qAWjd4BQwKBgCeE0B2gaqkQlo1QNqlJJMieuKkd+/XksDH252EyuVx3Bjwclf7b\\nqoG9e0h8U49Mn2Gx5yenn2B3BUVZ/vAm75HCUw+E9XUi23n3/6/osQ4WpP7UcUgC\\nTd8sYXXKcCFR9ZjsJtEdIZhP+y84+E06FDP4WBsl8w0qj6uqc/3MLXZDAoGAbd7L\\nzm6ocaWsPmqDTeG2gXHgP8y9eUQLhB7BVYLj9ZVcRz1nBCRs3NAJ4J9Zn/wK7MVp\\n5RCzcTiI/+QukZhI2L3GzvPpXJqiMcYtgOf43SX34urnq1del9fAfPI5mwozEWzQ\\npk6026zWmJhDCyMm+cVKShCCY6q2VSKkH4qZ2X8CgYBcxwtbeACxAKNWntG1kGub\\nJqLci+nalkQJw2FPEvrv4ygq/5Z/dY5MfyEB80euU06pyr1nKuxrs3+Nc6YhWHDc\\n/iCMzxrL1I2Ops7pleeS4yvJPk5xgVC8DAguE2aoGr0CfVvoWa/IkDT9L3WiGKhQ\\nNdKjgHIks4SzAHJL/ReT0g==\\n-----END PRIVATE KEY-----\\n",
      "client_email": "kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com",
      "client_id": "115925417558125099001",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/kalungi-google-colab%40invertible-now-393117.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
      }
      '''
      key_dict = json.loads(json_key)
      scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
      credentials = Credentials.from_service_account_info(key_dict, scopes=scope)
      client = gspread.authorize(credentials)
      spreadsheet = client.open_by_url(spreadsheetUrl)
      worksheet = spreadsheet.worksheet(sheet_name)
      set_with_dataframe(worksheet, dataframe, include_index=False, resize=True, allow_formulas=True)



  sheetName_Seniority = "Seniority"
  sheetName_Standard = "DB"
  sheetName_Chief= "Chief"
  sheetName_Function = "Function"
  sheetName_Database= "Database"
  columvar_Variation= "Variation"
  columnvar_Seniority = "Seniority"
  columnvar_Function = "Function"
  columnstand_Seniority = "Seniority"
  columnstand_Function = "Function"
  columnstand_DB = "Standard Title"
  columconcat_DB = "contact_title"
  columnName_Repo= "Title"
  columnName_RepoStandard= "Standard Title"


  #OpenAI: ABM Request
  spreadsheetUrl_request = "https://docs.google.com/spreadsheets/d/1t0dtznfuZqlChc-baRA5zg2_hjWN_LMFPU3fvxW9f2w/edit#gid=0"
  sheetName_request = "Control"
  df_request = retrieve_spreadsheet(spreadsheetUrl_request, sheetName_request)


  #Spreadsheet donde estan los Titulos no limpios
  df= retrieve_spreadsheet(spreadsheetUrl, sheetName)
  df_titulos = retrieve_spreadsheet(spreadsheetUrl_DB, sheetName_Database)

  df_Standard= retrieve_spreadsheet(spreadsheetUrl_DB, sheetName_Standard)
  df_Chief= retrieve_spreadsheet(spreadsheetUrl_DB, sheetName_Chief)
  df_Seniority = retrieve_spreadsheet(spreadsheetUrl_DB, sheetName_Seniority)
  df_Function = retrieve_spreadsheet(spreadsheetUrl_DB, sheetName_Function)


  conocimiento_df = df_Standard
  conocimiento_df['Embedding'] = conocimiento_df[columnstand_DB].apply(lambda x: get_embedding(x, engine='text-embedding-ada-002'))


  def buscar(busqueda, datos, n_resultados=1):
      busqueda_embed = get_embedding(busqueda, engine="text-embedding-ada-002")
      datos["Similitud"] = datos['Embedding'].apply(lambda x: cosine_similarity(x, busqueda_embed))
      datos = datos.sort_values("Similitud", ascending=False)
      return datos.iloc[:n_resultados][[columnstand_DB, "Similitud"]]

  def Chief(title, df_Chief):
      for index, row in df_Chief.iterrows():
          variation = row[columvar_Variation]
          if re.search(r'\b' + re.escape(variation) + r'\b', title, re.IGNORECASE):
              return row['concat_title']  # Asume que esta es la columna que contiene el resultado deseado
      return None

  def Seniority(title, df_Seniority):
      matches = []
      for index, row in df_Seniority.iterrows():
          variation = row[columvar_Variation]
          if re.search(r'\b' + re.escape(variation) + r'\b', title, re.IGNORECASE):
              matches.append(row[columnvar_Seniority])
      return matches

  def Function(title, df_Function):
      matches = []
      for index, row in df_Function.iterrows():
          variation = row[columvar_Variation]
          if re.search(r'\b' + re.escape(variation) + r'\b', title, re.IGNORECASE):
              matches.append(row[columnvar_Function])
      return matches

  def generate_combinations(seniorities, functions):
      return [f"{seniority}{function}" for seniority, function in itertools.product(seniorities, functions)]

  def Keywords(text):
      keywords = [r'\bretired',r'\bassistant',r'\bconsultant',r'\bAdvisor to',r'\bProject Owner']
      regex_pattern = '|'.join(keywords)
      if pd.notna(text) and re.search(regex_pattern, text, re.IGNORECASE):
          return True
      else:
          return False

  #Buscando los titles en una DB con titulos limpios y suicios y haciendo merge
  df[columnName_Title] = df[columnName_Title]
  df_titulos = df_titulos.drop_duplicates(subset=[columnName_Repo]).copy()
  df_titulos[columnName_Repo] = df_titulos[columnName_Repo].apply(lambda x: str(x).strip())
  
  df = pd.merge(df, df_titulos[[columnName_Repo,'Standard']],
                left_on=columnName_Title,
                right_on=columnName_Repo,
                how='left')

  df['Standard_AI'] = df['Standard'].fillna('')
  df.drop(columns=[columnName_Repo,'Standard'], inplace=True)

  df['IA_QA'] = 'TRUE'
  filas_relevantes = df[df['Standard_AI'] == ''].shape[0]

  df['IA_QA'] = df.apply(lambda row: 'FALSE' if row['Standard_AI'] != '' else row['IA_QA'], axis=1)

  df = df.copy()

  # Inicializar listas para guardar los resultados
  indices_to_update = []
  standards = []
  similitudes = []


  num_rows = df.shape[0]
  df= df.drop_duplicates()


  st.write("Generating embbedings")
  index_progress = 0
  progress_bar = st.progress(0)

  print("Generating embbedings")
  for index, row in tqdm(df.iterrows(), total=filas_relevantes):
    if row['IA_QA'] == 'TRUE':
        title = row[columnName_Title]
        query = f"Tell me what Standard Title you think the following non-standard title should have: {title}"
        response = buscar(query, conocimiento_df)
        
        # Guardar los resultados y los índices
        indices_to_update.append(index)
        standards.append(response.iloc[0][columnstand_DB])
        similitudes.append(response.iloc[0]['Similitud'])
    
    # Increment index_progress
    index_progress += 1
    
    # Update progress bar
    progress_value = min(index_progress / filas_relevantes, 1.0)
    progress_bar.progress(progress_value)
  
  # Actualizar solo las filas correspondientes en el DataFrame
  for i, idx in enumerate(indices_to_update):
      df.at[idx, 'Standard_AI'] = standards[i]
      df.at[idx, 'Similitud'] = similitudes[i]

  filas_relevantes_2 = df[df['IA_QA'] == 'TRUE'].shape[0]
  index_progress = 0
  st.write("Processing rows")
  progress_bar = st.progress(0)

  for index, row in tqdm(df[df['IA_QA'] == 'TRUE'].iterrows(), total=filas_relevantes_2):
      title = row[columnName_Title]
      Chief_result = Chief(title, df_Chief)
      # Reset de variables para cada fila
      keywords= Keywords(title)
      df.at[index, 'Keywords'] = keywords
      seniority_result, function_result, title_concat = None, None, None
      multiple_seniority, multiple_function = False, False

      if Chief_result is None:
          seniority_matches = Seniority(title, df_Seniority)
          function_matches = Function(title, df_Function)

          # Revisar si hay múltiples coincidencias
          multiple_seniority = len(seniority_matches) > 1
          multiple_function = len(function_matches) > 1
          df.at[index, 'Multiple Seniority'] = multiple_seniority
          df.at[index, 'Multiple Function'] = multiple_function

          # Generar todas las combinaciones posibles
          title_combinations = generate_combinations(seniority_matches, function_matches)

          # Buscar la primera coincidencia válida
          for title_concat in title_combinations:
              standard_title_row = df_Standard[df_Standard['concat_title'] == title_concat]
              if not standard_title_row.empty:
                  standard_title = standard_title_row['Standard Title'].iloc[0]
                  break  # Encontrar la primera coincidencia válida
          else:
              standard_title = "X"  # Usar "X" si no se encuentra una coincidencia
      else:
          title_concat = Chief_result
          df.at[index, 'Chief'] = True
          standard_title_row = df_Standard[df_Standard['concat_title'] == title_concat]
          standard_title = standard_title_row['Standard Title'].iloc[0]

      index_progress += 1
    
      # Update progress bar
      progress_value = min(index_progress / filas_relevantes_2, 1.0)
      progress_bar.progress(progress_value)

      # Actualizar el DataFrame con los resultados encontrados
      df.at[index, 'Standard title'] = standard_title


  # Limpieza y preparación de los datos
  df['Standard_AI'] = df['Standard_AI'].apply(lambda x: str(x).rstrip(' .'))
  df_Standard[columnstand_DB] = df_Standard[columnstand_DB].apply(lambda x: str(x).strip())


  #Merge
  df = pd.merge(df, df_Standard[[columnstand_DB, 'Seniority', 'Function', 'Persona']],
                left_on='Standard title',
                right_on=columnstand_DB,
                how='left')
  # Manejo de valores nulos para las nuevas columnas
  df['Seniority'] = df['Seniority'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df['Function'] = df['Function'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df['Persona'] = df['Persona'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df.drop(columns=[columnstand_DB], inplace=True)

  df_test=df

  df['fit'] = ''
  df_test.loc[df_test['IA_QA'] == 'FALSE', 'fit'] = True
  #df_test.loc[df_test['Chief'] == True, 'fit'] = True
  df_test.loc[df_test['Standard title'] == df_test['Standard_AI'], 'fit'] = True
  df_test.loc[df_test['Keywords'] == True, 'fit'] = False
  df_test.loc[df_test['Standard title'] == 'X', 'fit'] = False
  df_test.loc[df_test['IA_QA'] == 'FALSE', 'Standard title'] = df_test['Standard_AI']
  df_test.loc[df_test['fit'] == False, 'Standard title'] = 'X'

  df_test = df_test[['title', 'Standard title', 'fit','IA_QA']]


  write_into_spreadsheet(spreadsheetUrl, sheetName, df_test)

  #OpenAI: ABM Request

  nuevo_registro = {
      'User': "Streamlit",
      'Request Date': datetime.now(),
      'Script Name': "Title Cleanin QA Script 4.0",
      'Script Url': "https://colab.research.google.com/drive/1PQOBmxcdBEvqLsOxCanD59P7Uxjli8zv#scrollTo=Da5oMfExbFoC",
      'Nº request': filas_relevantes_2,
      'API Key': "EdGuj"

  }

  nuevo_registro_df = pd.DataFrame([nuevo_registro])
  df_request = pd.concat([df_request, nuevo_registro_df], ignore_index=True)


  # Selecciona la hoja de cálculo por nombre
  sheetName_request = 'Control'
  write_into_spreadsheet(spreadsheetUrl_request, sheetName_request, df_request)
  return df_test

st.title("Title Cleaning")

option = st.selectbox(
    "Select a Client ICP",
    ("Select Scraper Type","Onfleet (DSP)","Onfleet (Resellers)")
)

# Conditional inputs based on the selected scraper type
if option == "Onfleet (DSP)":
  st.write("Use this tool when you need to make Title Cleaning standarization for a list of contact. to use this tool you should have created a title repository and a database with your standardized titles, below is a video on how to do it. It is important to note that you will also have to share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com.")
  st.write("[Tutorial >](https://www.loom.com/looms/videos)")
  spreadsheet_url = st.text_input("Select a spreadsheet Url", "https://docs.google.com/spreadsheets/d/19hsZxx29AuBJ4zGBh8iB7ImqbT_lWZHKeXFd3mExkb0/edit?gid=0#gid=0")
  sheet_name = st.text_input("Select Sheet Name", "TC")
  column_name = st.text_input("Select Column Name", "title")
  spreadsheetUrl_DB = st.text_input("Select Title DB Url", "https://docs.google.com/spreadsheets/d/173_FgevHCEA9jTOlHyp16hYsTXxNzwclZiLjUbcMl4Q/edit#gid=87246784")

elif option == "Onfleet (Resellers)":
  st.write("Use This tool when you need to make Title Cleaning standarization for a list of contact. to use this tool you should have created a title repository and a database with your standardized titles, below is a video on how to do it. It is important to note that you will also have to share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com.")
  st.write("[Tutorial >](https://www.loom.com/looms/videos)")
  spreadsheet_url = st.text_input("Select a spreadsheet Url", "https://docs.google.com/spreadsheets/d/19hsZxx29AuBJ4zGBh8iB7ImqbT_lWZHKeXFd3mExkb0/edit?gid=0#gid=0")
  sheet_name = st.text_input("Select Sheet Name", "TC")
  column_name = st.text_input("Select Column Name", "title")
  spreadsheetUrl_DB = st.text_input("Select Title DB Url", "https://docs.google.com/spreadsheets/d/1tbNbX1y-FEbE4hZXB6B2jfHEN3Xa2c3BA7PLGNKu0-s/edit#gid=0")


if option != "Select Client ICP":
  if st.button("Iniciar procesamiento"):
      if not spreadsheet_url or not spreadsheetUrl_DB:
          st.error("Please enter both the Spreadsheet URL and a Title DB Url")
      else:
          with st.spinner("Running the TC Tool. This could take a few minutes depending on the list size..."):
              try:
                  result = tc(spreadsheet_url,sheet_name,column_name,spreadsheetUrl_DB)
                  st.success("TC completed!")
                  st.dataframe(result)
              except Exception as e:
                  st.error(f"An error occurred: {str(e)}")
                  st.exception(e)
