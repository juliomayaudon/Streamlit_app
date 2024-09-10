
import http.client
import os
from dotenv import load_dotenv
import openai

load_dotenv()
OPENAI_API_KEY= st.secrets["OPENAI_API_KEY"]
openai.api_key = OPENAI_API_KEY
zenrowsApiKey = '56157ea4b37129fd19ea02fa07291069a0378c19'
serper_api= '81ead61f8203d7445b4c38d383d58422eb6963ae'


def buscar_enlaces_organicos(keywords, row):
    conn = http.client.HTTPSConnection("google.serper.dev")
    payload = json.dumps({
      'q': f' {keywords} site:{row}',
      "num": 8
    })
    headers = {
      'X-API-KEY': serper_api,
      'Content-Type': 'application/json'
    }
    conn.request("POST", "/search", payload, headers)
    res = conn.getresponse()
    data = res.read()

    # Convertir el texto de la respuesta a un diccionario de Python
    data_dict = json.loads(data.decode("utf-8"))

    # Acceder a los elementos en la sección 'organic'
    organic_results = data_dict.get('organic', [])

    # Extraer los enlaces de cada resultado orgánico y convertirlos a HTTP
    links = [result['link'].replace("https://", "http://") for result in organic_results if 'link' in result and not result['link'].lower().endswith('.pdf')]

    # Añadir el dominio con HTTP
    links.append(f'http://{row}')

    return links[:3]

def get_text_from_url(url):
    client = ZenRowsClient(zenrowsApiKey)
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
    try:
        response = client.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            for script_or_style in soup(['script', 'style']):
                script_or_style.extract()
            text = soup.get_text()
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            return '\n'.join(chunk for chunk in chunks if chunk)
        else:
            return f"Error {response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"Error de solicitud: {e}"


def process_url_data(urls):
    combined_text = ''
    for url in urls:
        text = get_text_from_url(url)
        if not text.startswith("Error"):
            combined_text += text + " "
    return combined_text


def get_text_chunks(text):
    text_splitter = CharacterTextSplitter(separator="\n", chunk_size=1000, chunk_overlap=200, length_function=len)
    return text_splitter.split_text(text)

def get_vectors(text_chunks):
    openai_api_key = os.getenv('OPENAI_API_KEY')
    embeddings = OpenAIEmbeddings(openai_api_key=openai_api_key)
    return FAISS.from_texts(texts=text_chunks, embedding=embeddings)

def get_response_from_chain(vectorstore, search_question, llm_question):
    docs = vectorstore.similarity_search(search_question)
    llm = ChatOpenAI(temperature=0.9, max_tokens= 4000, model="gpt-4o-mini")
    chain = load_qa_chain(llm, chain_type="stuff")
    return chain.run(input_documents=docs, question=llm_question)


def split_text(text):
    global error_message
    if text == error_message:
        # Si text es igual a error_message, retorna error_message sin cambios
        return error_message, None
    if pd.isna(text) or not text:
        return None, None
    # Dividir el texto en la primera aparición de un punto o una coma
    parts = re.split(r'\. |, ', text, 1)
    # Asignar la primera palabra (Yes, No o Maybe) a la columna 'QA'
    QA = parts[0].split()[0] if len(parts) > 0 else None
    # Asignar el texto restante a la columna 'Reason'
    reason = parts[1] if len(parts) > 1 else None
    return QA, reason


def format_keywords(input_string):
    # Divide el input_string en una lista separada por comas y quita espacios adicionales
    keywords_list = [keyword.strip() for keyword in input_string.split(",")]
    # Formatea cada palabra clave con comillas
    keywords_final = ['"' + keyword + '"' for keyword in keywords_list]
    # Une las palabras clave con " | " entre ellas
    return " | ".join(keywords_final)



#KEYWORDS FUNCTIONS

def check_for_error(response):
    error_keywords = [r'\berror\b',r'\btimeout\b',r'\b403\b']
    regex_pattern = '|'.join(error_keywords)
    if pd.notna(response) and re.search(regex_pattern, response, re.IGNORECASE):
        return True
    else:
        return False


#AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA



import json
import requests
import os
import google.auth
import gspread
import pandas as pd
import tiktoken
import tqdm
import openlimit
import re
import warnings


from google.auth import default
from gspread_dataframe import set_with_dataframe
from bs4 import BeautifulSoup

from bs4 import BeautifulSoup
from langchain.text_splitter import CharacterTextSplitter
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.chains import ConversationalRetrievalChain
from langchain.chains.question_answering import load_qa_chain
from langchain.llms import OpenAI
from langchain.callbacks import get_openai_callback
from openlimit.utilities import num_tokens_consumed_by_embedding_request
from langchain.chat_models import ChatOpenAI
from zenrows import ZenRowsClient





import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials


error_message = "Error 422"

def process_data(spreadsheet_url, sheet_name, column_name, formatted_keywords, prompt, serper_API, progress_bar):
    cost_per_prompt_token = 0.000015 / 1000
    cost_per_completion_token = 0.0006 / 1000
    totalcost = 0

    # Autenticación y lectura de Google Sheets
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
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_values()
    dataframe = pd.DataFrame(data[1:], columns=data[0])

    num_rows = dataframe.shape[0]

    for index, row in dataframe.iterrows():
        try:
            domain = row[column_name]
            links_obtenidos = buscar_enlaces_organicos(formatted_keywords, domain)
            text = process_url_data(links_obtenidos)
            if text != error_message:
                text_chunks = get_text_chunks(text)

                if text_chunks:
                    vectorstore = get_vectors(text_chunks)
                    num_tokens = num_tokens_consumed_by_embedding_request(text_chunks)
                    search_question = "Chemical, Shipping, delivery"
                    llm_question = prompt
                    with get_openai_callback() as cb:
                        response = get_response_from_chain(vectorstore, search_question, llm_question)
                        error = check_for_error(response)
                        dataframe.at[index, 'Error'] = error
                        prompt_cost = cb.prompt_tokens * cost_per_prompt_token
                        output_cost = cb.completion_tokens * cost_per_completion_token
                        total_cost = prompt_cost + output_cost
                        totalcost += total_cost
                        dataframe.at[index, 'result'] = response
                else:
                    dataframe.at[index, 'result'] = ""
            else:
                dataframe.at[index, 'result'] = error_message
        except Exception as e:
            error_message = str(e)
            dataframe.at[index, 'QA'] = error_message
            print(f"Error processing row {index}: {error_message}")

        # Actualizar la barra de progreso
        progress_bar.progress((index + 1) / num_rows)

    df_final = dataframe
    df_final['QA'], df_final['Reason'] = zip(*df_final['result'].apply(split_text))
    df_final = df_final[[column_name, 'QA', 'Reason', 'result', 'Error']]

    worksheet.clear()
    set_with_dataframe(worksheet, df_final, include_index=False, resize=True, allow_formulas=True)

    return df_final, totalcost

st.title("QA with Searching Keyword")

spreadsheet_url = st.text_input("URL de Google Sheets", "https://docs.google.com/spreadsheets/d/1WdRriLXggLZlz1dIoyiGMEdu13YVWibJLp7u5-Z6Gjo/edit?gid=352666901#gid=352666901")
sheet_name = st.text_input("Nombre de la hoja", "Test")
column_name = st.text_input("Nombre de la columna", "domain")
serper_API = st.text_input("Seleccione un API de Serper", "81ead61f8203d7445b4c38d383d58422eb6963ae")

keywords = st.text_area("Introduce las keywords separadas por comas", "Delivery, Shipping, last mile, White Glove, final mile")
keywords_list = [keyword.strip() for keyword in keywords.split(',')]
keywords_final = ['"' + keyword + '"' for keyword in keywords_list]
formatted_keywords = " | ".join(keywords_final)
st.write("Keywords formateadas:", formatted_keywords)

prompt = st.text_area("Introduce el prompt", "Assess if the company is a manufacturer or provides any delivery or shipping of Chemical products or derivatives by searching for terms or phrases indicating this kind of services  including but not limited to 'Chemical Distributors', 'Chemical Manuufacturers', 'Shipping', 'Delivery'. Respond in the following manner: Yes. Provide a brief explanation (no more than 300 characters) on why it qualifies. No. Provide a brief explanation (no more than 300 characters) on why it does not qualify. Maybe. If the information is ambiguous or insufficient, briefly explain (no more than 300 characters) why it's not possible to determine.")

if st.button("Iniciar procesamiento"):
    if not spreadsheet_url or not serper_API:
        st.error("Please enter both the Spreadsheet URL and the Serper API key")
    else:
        with st.spinner("Running the scraper. This could take a few minutes depending on the list size..."):
            try:
                progress_bar = st.progress(0)
                result, totalcost = process_data(spreadsheet_url, sheet_name, column_name, formatted_keywords, prompt, serper_API, progress_bar)
                st.success("Scraping completed!")
                st.dataframe(result)
                st.write(f"El costo total fue: ${totalcost:.6f}")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.exception(e)
