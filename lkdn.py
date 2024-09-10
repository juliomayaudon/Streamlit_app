
import asyncio
import base64
import calendar
import gspread
import json
import nest_asyncio
import numpy as np
import pandas as pd
import random
import re
import requests
import sys
import time
import tldextract
import urllib.parse
import uuid
import streamlit as st
import traceback
import platform

from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from io import StringIO
from operator import itemgetter
from playwright.async_api import async_playwright
from playwright.sync_api import sync_playwright
from tqdm import tqdm
from urllib.parse import quote, unquote, urlencode,urlparse


def safe_extract(data, *keys):
    """
    Safely extracts a value from a nested dictionary using a sequence of keys.

    Args:
        data (dict): The dictionary to extract the value from.
        *keys (str): A variable number of keys to traverse the dictionary.

    Returns:
        The value corresponding to the sequence of keys if it exists,
        otherwise returns None.
    """
    try:
        for key in keys:
            data = data[key]
        return data
    except (KeyError, IndexError, TypeError):
        return None
# Apply the nest_asyncio patch to allow nested event loops
# Check if the platform is Windows and set the appropriate event loop policy

if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Ensure there is an event loop in the current thread
try:
    loop = asyncio.get_event_loop()
except RuntimeError:  # If there's no event loop, create a new one
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

async def retrieve_tokens(li_at):
    """
    Retrieve LinkedIn tokens and cookies using a given authentication cookie.

    Args:
        li_at (str): The LinkedIn authentication cookie.

    Returns:
        tuple: Contains JSESSIONID, li_a cookie, csrf_token, and a dictionary of all cookies.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await context.add_cookies([{
            'name': 'li_at',
            'value': li_at,
            'domain': '.linkedin.com',
            'path': '/',
            'secure': True,
            'httpOnly': True,
            'sameSite': 'None'
        }])

        page = await context.new_page()
        csrf_token = None

        # Function to capture the CSRF token from request headers
        def log_request(request):
            nonlocal csrf_token
            if request.url.startswith('https://www.linkedin.com/sales-api/salesApiAccess'):
                csrf_token = request.headers.get('csrf-token')

        page.on('request', log_request)

        try:
            # First navigation attempt
            await page.goto('https://www.linkedin.com/sales/home')
        except Exception:
            await browser.close()
            print('The li_at cookie was misspelled or has expired. Please correct it and try again.')
            return None, None, None, None

        await page.wait_for_timeout(5000)

        try:
            # Second navigation attempt
            await page.goto('https://www.linkedin.com/sales/search/people?query=(filters%3AList((type%3ACURRENT_COMPANY%2Cvalues%3AList((id%3Aurn%253Ali%253Aorganization%253A18875652%2CselectionType%3AINCLUDED)))))')
        except Exception:
            await browser.close()
            print('The li_at cookie was misspelled or has expired. Please correct it and try again.')
            return None, None, None, None

        await page.wait_for_timeout(10000)

        cookies = await context.cookies()
        cookies_dict = {}
        JSESSIONID = None
        li_a = None

        for cookie in cookies:
            cookies_dict[cookie['name']] = cookie['value']
            if cookie['name'] == 'JSESSIONID':
                JSESSIONID = cookie['value']
            elif cookie['name'] == 'li_a':
                li_a = cookie['value']

        await browser.close()
        return JSESSIONID, li_a, csrf_token, cookies_dict

#kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com
def write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe):
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
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet(sheet_name)
    set_with_dataframe(worksheet, dataframe, include_index=False, resize=True, allow_formulas=True)
def write_into_csv(dataframe, file_name):
    """
    Writes data from a DataFrame into a CSV file with the specified file name.

    Args:
        dataframe (pd.DataFrame): The DataFrame containing data to be written.
        file_name (str): The name of the CSV file to be created (without the .csv extension).

    Returns:
        None
    """
    dataframe.to_csv(f'{file_name}.csv',index=False,escapechar='\\')
#kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com
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
        st.error("Incorrect spreadsheet URL or missing share with kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com. Please correct it and try again.")
        return None
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Incorrect or non-existent sheet name. Please correct it and try again.")
        return None
    except gspread.exceptions.SpreadsheetNotFound:
        st.error("Incorrect or non-existent spreadsheet URL. Please correct it and try again.")
        return None
#Scrape
def sales_navigator_lead_export(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name, progress_bar, max_pages=26):
    """
    Exports leads from LinkedIn Sales Navigator based on a DataFrame of search queries.

    Args:
        li_at (str): LinkedIn authentication cookie.
        JSESSIONID (str): Session ID cookie.
        li_a (str): LinkedIn cookie.
        csrf_token (str): CSRF token for authentication.
        dataframe (pd.DataFrame): DataFrame containing search queries.
        column_name (str): Column name in the DataFrame with search query URLs.
        max_pages (int, optional): Maximum number of pages to fetch for each query (default is 26).

    Returns:
        pd.DataFrame: A DataFrame containing the retrieved lead information.
    """
    cookies = {
        'li_at': li_at,
        'JSESSIONID': JSESSIONID,
        'li_a': li_a,
    }
    headers = {
        'csrf-token': csrf_token,
        'referer': 'https://www.linkedin.com/sales/search/people',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
    }
    def format_duration(years, months):
        year_suffix = " year" if years == 1 else " years"
        month_suffix = " month" if months == 1 else " months"
        formatted_values = [f"{years}{year_suffix}", f"{months}{month_suffix}"]
        return ' '.join(formatted_values) + ' in role' if formatted_values else None
    def build_logo_url(contact, index):
        root_url = safe_extract(contact, 'currentPositions', 0, 'companyUrnResolutionResult', 'companyPictureDisplayImage', 'rootUrl')
        segment = safe_extract(contact, 'currentPositions', 0, 'companyUrnResolutionResult', 'companyPictureDisplayImage', 'artifacts', index, 'fileIdentifyingUrlPathSegment')
        return root_url + segment if root_url and segment else None
    def extract_company_urn(contact):
        urn = safe_extract(contact, 'currentPositions', 0, 'companyUrn')
        return re.search(r'\d+', urn).group() if urn and re.search(r'\d+', urn) else None
    def format_start_date(contact):
        month = safe_extract(contact, 'currentPositions', 0, 'startedOn', 'month')
        year = safe_extract(contact, 'currentPositions', 0, 'startedOn', 'year')
        if month and year:
            month_name = calendar.month_abbr[month]
            return f"{month_name} {year}"
        return str(year) if year else None
    def extract_entity_urn(contact):
        urn = safe_extract(contact, 'entityUrn')
        return re.search(r'\(([^,]+)', urn).group(1) if urn and re.search(r'\(([^,]+)', urn) else None
    def map_degree(degree):
        mapping = {3: "3rd", 2: "2nd", 1: "1st", -1: "Out of network"}
        return mapping.get(degree, degree)
    def build_linkedin_profile_url(contact):
        urn = extract_entity_urn(contact)
        return f'https://www.linkedin.com/in/{urn}/' if urn else None
    def build_profile_url(contact):
        urn = extract_entity_urn(contact)
        return f'https://www.linkedin.com/sales/lead/{urn},' if urn else None
    def build_company_url(contact):
        company_urn = extract_company_urn(contact)
        return f'https://www.linkedin.com/sales/company/{company_urn}/' if company_urn else None
    def build_regular_company_url(contact):
        company_urn = extract_company_urn(contact)
        return f'https://www.linkedin.com/company/{company_urn}/' if company_urn else None
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    columnName_values = dataframe[column_name].tolist()
    print("Sales Navigator lead export")
    num_rows = len(columnName_values)
    df_final = pd.DataFrame()
    too_many_requests = False
    for index, value in enumerate(columnName_values):
        if index != 0 and index % 300 == 0:
            print("Waiting for 90 seconds...")
            time.sleep(90)
        if too_many_requests:
            break
        query_pattern = re.compile(r'\#(?=query=)')
        corrected_value = query_pattern.sub('?', value)
        query_params = unquote(urlparse(corrected_value).query)
        request_url = f'https://www.linkedin.com/sales-api/salesApiLeadSearch?q=searchQuery&{query_params}&start=0&count=100&decorationId=com.linkedin.sales.deco.desktop.searchv2.LeadSearchResult-14'
        loop_counter = 0
        page_counter = 0
        while True:
            response = requests.get(url=request_url, cookies=cookies, headers=headers)
            status_code = response.status_code
            if status_code == 400:
                df_final = pd.concat([df_final, pd.DataFrame({'query': [value], 'error': ['Wrong query']})])
                break
            if status_code == 429:
                print("Too many requests!")
                too_many_requests = True
                break
            if status_code == 200:
                data = response.json()
                totalDisplayCount = data.get('metadata', {}).get('totalDisplayCount', None)
                try:
                    totalDisplayCount = int(totalDisplayCount)
                except ValueError:
                    totalDisplayCount = 2500
                if not totalDisplayCount:
                    df_final = pd.concat([df_final, pd.DataFrame({'query': [value], 'error': ['No result found']})])
                    break
                contact_details = safe_extract(data['elements'])
                for contact in contact_details:
                    contact_info = {
                        'query': value,
                        'error': None,
                        'contact_lastName': safe_extract(contact, 'lastName'),
                        'contact_geoRegion': safe_extract(contact, 'geoRegion'),
                        'contact_openLink': safe_extract(contact, 'openLink'),
                        'contact_premium': safe_extract(contact, 'premium'),
                        'durationInRole': format_duration(
                            safe_extract(contact, 'currentPositions', 0, 'tenureAtPosition', 'numYears'),
                            safe_extract(contact, 'currentPositions', 0, 'tenureAtPosition', 'numMonths')
                        ),
                        'currentPositions_companyName': safe_extract(contact, 'currentPositions', 0, 'companyName'),
                        'currentPositions_title': safe_extract(contact, 'currentPositions', 0, 'title'),
                        'currentPositions_companyUrnResolutionResult_name': safe_extract(contact, 'currentPositions', 0, 'companyUrnResolutionResult', 'name'),
                        'logo200x200': build_logo_url(contact, 0),
                        'logo100x100': build_logo_url(contact, 1),
                        'logo400x400': build_logo_url(contact, 2),
                        'currentPositions_companyUrnResolutionResult_location': safe_extract(contact, 'currentPositions', 0, 'companyUrnResolutionResult', 'location'),
                        'currentPositions_companyUrn': extract_company_urn(contact),
                        'currentPositions_current': safe_extract(contact, 'currentPositions', 0, 'current'),
                        'durationInCompany': format_duration(
                            safe_extract(contact, 'currentPositions', 0, 'tenureAtCompany', 'numYears'),
                            safe_extract(contact, 'currentPositions', 0, 'tenureAtCompany', 'numMonths')
                        ),
                        'startDate': format_start_date(contact),
                        'contact_entityUrn': extract_entity_urn(contact),
                        'contact_degree': map_degree(safe_extract(contact, 'degree')),
                        'contact_fullName': safe_extract(contact, 'fullName'),
                        'contact_firstName': safe_extract(contact, 'firstName'),
                        'linkedInProfileUrl': build_linkedin_profile_url(contact),
                        'profileUrl': build_profile_url(contact),
                        'companyUrl': build_company_url(contact),
                        'regularCompanyUrl': build_regular_company_url(contact),
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    df_final = pd.concat([df_final, pd.DataFrame(contact_info, index=[0])])
                page_counter += 1
                if page_counter >= max_pages:
                    break
                if totalDisplayCount == 2500 and loop_counter >= 25:
                    break
                if int(urlparse(request_url).query.split('&start=')[1].split('&')[0]) + 100 < data['paging']['total']:
                    start_val = int(urlparse(request_url).query.split('&start=')[1].split('&')[0]) + 100
                    request_url = request_url.replace(f'start={start_val - 100}', f'start={start_val}')
                    loop_counter += 1
                else:
                    break
        index += 1
        progress_bar.progress((index) / num_rows)

    columns_rename = {
        'contact_lastName': 'lastName',
        'contact_geoRegion': 'location',
        'contact_openLink': 'isOpenLink',
        'contact_premium': 'isPremium',
        'currentPositions_companyName': 'companyName',
        'currentPositions_title': 'title',
        'currentPositions_companyUrnResolutionResult_name': 'linkedinCompanyName',
        'currentPositions_companyUrnResolutionResult_location': 'companyLocation',
        'currentPositions_companyUrn': 'companyId',
        'currentPositions_current': 'isCurrent',
        'startDate': 'startDateInRole',
        'contact_entityUrn': 'vmid',
        'contact_degree': 'connectionDegree',
        'contact_fullName': 'fullName',
        'contact_firstName': 'firstName'
    }
    df_final.rename(columns=columns_rename, inplace=True)
    columns_desired = ['query', 'error', 'profileUrl', 'linkedInProfileUrl', 'vmid', 'firstName', 'lastName', 'fullName', 'location', 'title', 'companyId', 'companyUrl', 'regularCompanyUrl', 'companyName', 'linkedinCompanyName', 'startDateInRole', 'durationInRole', 'durationInCompany', 'companyLocation', 'logo100x100', 'logo200x200', 'logo400x400', 'connectionDegree', 'isCurrent', 'isOpenLink', 'isPremium', 'timestamp']
    df_final = df_final.reindex(columns=columns_desired)
    df_final = df_final[columns_desired]
    return df_final
def sales_navigator_account_export(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name, progress_bar, max_pages=17):
    """
    Exports account data from LinkedIn Sales Navigator based on a DataFrame of search queries.

    Args:
        li_at (str): LinkedIn authentication cookie.
        JSESSIONID (str): Session ID cookie.
        li_a (str): LinkedIn cookie.
        csrf_token (str): CSRF token for authentication.
        dataframe (pd.DataFrame): DataFrame containing search queries.
        column_name (str): Column name in the DataFrame with search query URLs.
        max_pages (int, optional): Maximum number of pages to fetch for each query (default is 17).

    Returns:
        pd.DataFrame: A DataFrame containing the retrieved account information.
    """
    cookies = {
        'li_at': li_at,
        'JSESSIONID': JSESSIONID,
        'li_a': li_a,
    }
    headers = {
        'csrf-token': csrf_token,
        'referer': 'https://www.linkedin.com/sales/search/people',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
    }
    def build_logo_url(contact, index):
        root_url = safe_extract(contact, 'companyPictureDisplayImage', 'rootUrl')
        segment = safe_extract(contact, 'companyPictureDisplayImage', 'artifacts', index, 'fileIdentifyingUrlPathSegment')
        return root_url + segment if root_url and segment else None
    def extract_company_urn(contact):
        urn = safe_extract(contact, 'entityUrn')
        return re.search(r'\d+', urn).group() if urn and re.search(r'\d+', urn) else None
    def build_company_url(contact):
        company_urn = extract_company_urn(contact)
        return f'https://www.linkedin.com/sales/company/{company_urn}/' if company_urn else None
    def build_regular_company_url(contact):
        company_urn = extract_company_urn(contact)
        return f'https://www.linkedin.com/company/{company_urn}/' if company_urn else None
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    columnName_values = dataframe[column_name].tolist()
    print("Sales Navigator account export")
    num_rows = len(columnName_values)
    df_final = pd.DataFrame()
    too_many_requests = False
    for index, value in enumerate(columnName_values):
        if index != 0 and (index % 300 == 0 or index % 299 == 0):
            print("Waiting for 90 seconds...")
            time.sleep(90)
        if too_many_requests:
            break
        query_pattern = re.compile(r'\#(?=query=)')
        corrected_value = query_pattern.sub('?', value)
        query_params = unquote(urlparse(corrected_value).query)
        request_url = f'https://www.linkedin.com/sales-api/salesApiAccountSearch?q=searchQuery&{query_params}&start=0&count=100&decorationId=com.linkedin.sales.deco.desktop.searchv2.AccountSearchResult-4'
        loop_counter = 0
        page_counter = 0
        while True:
            response = requests.get(url=request_url, cookies=cookies, headers=headers)
            status_code = response.status_code
            if status_code == 400:
                df_final = pd.concat([df_final, pd.DataFrame({'query': [value], 'error': ['Wrong query']})])
                break
            if status_code == 429:
                st.write("Too many requests!")
                too_many_requests = True
                break
            if status_code == 200:
                data = response.json()
                total_display_count = data.get('metadata', {}).get('totalDisplayCount', None)
                try:
                    total_display_count = int(total_display_count)
                except ValueError:
                    total_display_count = 1600
                if not total_display_count:
                    df_final = pd.concat([df_final, pd.DataFrame({'query': [value], 'error': ['No result found']})])
                    break
                account_details = safe_extract(data, 'elements')
                if not account_details:
                    account_details = []
                for account in account_details:
                    account_info = {
                        'query': value,
                        'error': None,
                        'companyName': safe_extract(account, 'companyName'),
                        'description': safe_extract(account, 'description'),
                        'industry': safe_extract(account, 'industry'),
                        'employeeCountRange': safe_extract(account, 'employeeCountRange'),
                        'employeesCount': safe_extract(account, 'employeeDisplayCount'),
                        'companyId': extract_company_urn(account),
                        'companyUrl': build_company_url(account),
                        'regularCompanyUrl': build_regular_company_url(account),
                        'logo200x200': build_logo_url(account, 0),
                        'logo100x100': build_logo_url(account, 1),
                        'logo400x400': build_logo_url(account, 2),
                        'isHiring': False,
                        'had2SeniorLeadershipChanges': False,
                        'has1Connection': False,
                        'hadFundingEvent': False,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    spotlight_badges = safe_extract(account, 'spotlightBadges')
                    if not spotlight_badges:
                        spotlight_badges = []
                    for badge in spotlight_badges:
                        badge_id = safe_extract(badge, 'id')
                        if badge_id == 'FIRST_DEGREE_CONNECTION':
                            account_info['has1Connection'] = True
                        if badge_id == 'HIRING_ON_LINKEDIN':
                            account_info['isHiring'] = True
                        if badge_id == 'SENIOR_LEADERSHIP_CHANGE':
                            account_info['had2SeniorLeadershipChanges'] = True
                        if badge_id == 'RECENT_FUNDING_EVENT':
                            account_info['hadFundingEvent'] = True
                    df_final = pd.concat([df_final, pd.DataFrame(account_info, index=[0])])
                page_counter += 1
                if page_counter >= max_pages:
                    break
                if total_display_count == 1600 and loop_counter >= 16:
                    break
                start_val = int(urlparse(request_url).query.split('&start=')[1].split('&')[0]) + 100
                if start_val < data['paging']['total']:
                    request_url = request_url.replace(f'start={start_val - 100}', f'start={start_val}')
                    loop_counter += 1
                else:
                    break
        index += 1
        progress_bar.progress((index) / num_rows)

    columns_desired = ['query', 'error', 'companyUrl', 'companyName', 'description', 'companyId', 'regularCompanyUrl', 'industry', 'employeesCount', 'employeeCountRange', 'logo100x100', 'logo200x200', 'logo400x400', 'isHiring', 'had2SeniorLeadershipChanges', 'has1Connection', 'hadFundingEvent', 'timestamp']
    df_final = df_final.reindex(columns=columns_desired)
    df_final = df_final[columns_desired]
    return df_final
def linkedin_account(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name, cookies_dict, progress_bar,location_count=100):
    """
    Retrieves LinkedIn account information based on a DataFrame of search queries.

    Args:
        li_at (str): LinkedIn authentication cookie.
        JSESSIONID (str): Session ID cookie.
        li_a (str): LinkedIn cookie.
        csrf_token (str): CSRF token for authentication.
        dataframe (pd.DataFrame): DataFrame containing search queries.
        column_name (str): Column name in the DataFrame with search query URLs.
        cookies_dict (dict): Dictionary of cookies for the session.
        location_count (int, optional): Maximum number of locations to fetch for each query (default is 100).

    Returns:
        pd.DataFrame: A DataFrame containing the retrieved account information.
    """
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def get_company(data):
        if not data or "elements" not in data or not data["elements"]:
            return {}
        company = data["elements"][0]
        return company
    def extract_from_pattern(value, pattern):
        match = pattern.search(value)
        return match.group(1) if match else None
    def create_salesNavigatorLink(mainCompanyID):
        return f"https://www.linkedin.com/sales/company/{mainCompanyID}/" if mainCompanyID else None
    def extract_domain_from_website(website):
        try:
            return tldextract.extract(website).registered_domain
        except (ValueError, AttributeError):
            return None
    def safe_str(value):
        return str(value) if value is not None else None
    def get_full_name(mapping, key):
        return mapping.get(key, key)
    def extract_hashtag(value):
        parts = value.split(":")
        return parts[-1] if len(parts) > 0 else None
    dict_geographicArea = {'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'}

    dict_country = {'AD': 'Andorra', 'AE': 'United Arab Emirates', 'AF': 'Afghanistan', 'AG': 'Antigua and Barbuda', 'AI': 'Anguilla', 'AL': 'Albania', 'AM': 'Armenia', 'AN': 'Netherlands Antilles', 'AO': 'Angola', 'AQ': 'Antarctica', 'AR': 'Argentina', 'AS': 'American Samoa', 'AT': 'Austria', 'AU': 'Australia', 'AW': 'Aruba', 'AX': 'Aland Islands', 'AZ': 'Azerbaijan', 'BA': 'Bosnia and Herzegovina', 'BB': 'Barbados', 'BD': 'Bangladesh', 'BE': 'Belgium', 'BF': 'Burkina Faso', 'BG': 'Bulgaria', 'BH': 'Bahrain', 'BI': 'Burundi', 'BJ': 'Benin', 'BM': 'Bermuda', 'BN': 'Brunei Darussalam', 'BO': 'Bolivia', 'BR': 'Brazil', 'BS': 'Bahamas', 'BT': 'Bhutan', 'BV': 'Bouvet Island', 'BW': 'Botswana', 'BY': 'Belarus', 'BZ': 'Belize', 'CA': 'Canada', 'CB': 'Caribbean Nations', 'CC': 'Cocos (Keeling) Islands', 'CD': 'Democratic Republic of the Congo', 'CF': 'Central African Republic', 'CG': 'Congo', 'CH': 'Switzerland', 'CI': "Cote D'Ivoire (Ivory Coast)", 'CK': 'Cook Islands', 'CL': 'Chile', 'CM': 'Cameroon', 'CN': 'China', 'CO': 'Colombia', 'CR': 'Costa Rica', 'CS': 'Serbia and Montenegro', 'CU': 'Cuba', 'CV': 'Cape Verde', 'CX': 'Christmas Island', 'CY': 'Cyprus', 'CZ': 'Czech Republic', 'DE': 'Germany', 'DJ': 'Djibouti', 'DK': 'Denmark', 'DM': 'Dominica', 'DO': 'Dominican Republic', 'DZ': 'Algeria', 'EC': 'Ecuador', 'EE': 'Estonia', 'EG': 'Egypt', 'EH': 'Western Sahara', 'ER': 'Eritrea', 'ES': 'Spain', 'ET': 'Ethiopia', 'FI': 'Finland', 'FJ': 'Fiji', 'FK': 'Falkland Islands (Malvinas)', 'FM': 'Federated States of Micronesia', 'FO': 'Faroe Islands', 'FR': 'France', 'FX': 'France, Metropolitan', 'GA': 'Gabon', 'GB': 'United Kingdom', 'GD': 'Grenada', 'GE': 'Georgia', 'GF': 'French Guiana', 'GH': 'Ghana', 'GI': 'Gibraltar', 'GL': 'Greenland', 'GM': 'Gambia', 'GN': 'Guinea', 'GP': 'Guadeloupe', 'GQ': 'Equatorial Guinea', 'GR': 'Greece', 'GS': 'S. Georgia and S. Sandwich Islands', 'GT': 'Guatemala', 'GU': 'Guam', 'GW': 'Guinea-Bissau', 'GY': 'Guyana', 'HK': 'Hong Kong', 'HM': 'Heard Island and McDonald Islands', 'HN': 'Honduras', 'HR': 'Croatia', 'HT': 'Haiti', 'HU': 'Hungary', 'ID': 'Indonesia', 'IE': 'Ireland', 'IL': 'Israel', 'IN': 'India', 'IO': 'British Indian Ocean Territory', 'IQ': 'Iraq', 'IR': 'Iran', 'IS': 'Iceland', 'IT': 'Italy', 'JM': 'Jamaica', 'JO': 'Jordan', 'JP': 'Japan', 'KE': 'Kenya', 'KG': 'Kyrgyzstan', 'KH': 'Cambodia', 'KI': 'Kiribati', 'KM': 'Comoros', 'KN': 'Saint Kitts and Nevis', 'KP': 'Korea (North)', 'KR': 'Korea', 'KW': 'Kuwait', 'KY': 'Cayman Islands', 'KZ': 'Kazakhstan', 'LA': 'Laos', 'LB': 'Lebanon', 'LC': 'Saint Lucia', 'LI': 'Liechtenstein', 'LK': 'Sri Lanka', 'LR': 'Liberia', 'LS': 'Lesotho', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'LV': 'Latvia', 'LY': 'Libya', 'MA': 'Morocco', 'MC': 'Monaco', 'MD': 'Moldova', 'MG': 'Madagascar', 'MH': 'Marshall Islands', 'MK': 'Macedonia', 'ML': 'Mali', 'MM': 'Myanmar', 'MN': 'Mongolia', 'MO': 'Macao', 'MP': 'Northern Mariana Islands', 'MQ': 'Martinique', 'MR': 'Mauritania', 'MS': 'Montserrat', 'MT': 'Malta', 'MU': 'Mauritius', 'MV': 'Maldives', 'MW': 'Malawi', 'MX': 'Mexico', 'MY': 'Malaysia', 'MZ': 'Mozambique', 'NA': 'Namibia', 'NC': 'New Caledonia', 'NE': 'Niger', 'NF': 'Norfolk Island', 'NG': 'Nigeria', 'NI': 'Nicaragua', 'NL': 'Netherlands', 'NO': 'Norway', 'NP': 'Nepal', 'NR': 'Nauru', 'NU': 'Niue', 'NZ': 'New Zealand', 'OM': 'Sultanate of Oman', 'OO': 'Other', 'PA': 'Panama', 'PE': 'Peru', 'PF': 'French Polynesia', 'PG': 'Papua New Guinea', 'PH': 'Philippines', 'PK': 'Pakistan', 'PL': 'Poland', 'PM': 'Saint Pierre and Miquelon', 'PN': 'Pitcairn', 'PR': 'Puerto Rico', 'PS': 'Palestinian Territory', 'PT': 'Portugal', 'PW': 'Palau', 'PY': 'Paraguay', 'QA': 'Qatar', 'RE': 'Reunion', 'RO': 'Romania', 'RU': 'Russian Federation', 'RW': 'Rwanda', 'SA': 'Saudi Arabia', 'SB': 'Solomon Islands', 'SC': 'Seychelles', 'SD': 'Sudan', 'SE': 'Sweden', 'SG': 'Singapore', 'SH': 'Saint Helena', 'SI': 'Slovenia', 'SJ': 'Svalbard and Jan Mayen', 'SK': 'Slovak Republic', 'SL': 'Sierra Leone', 'SM': 'San Marino', 'SN': 'Senegal', 'SO': 'Somalia', 'SR': 'Suriname', 'ST': 'Sao Tome and Principe', 'SV': 'El Salvador', 'SY': 'Syria', 'SZ': 'Swaziland', 'TC': 'Turks and Caicos Islands', 'TD': 'Chad', 'TF': 'French Southern Territories', 'TG': 'Togo', 'TH': 'Thailand', 'TJ': 'Tajikistan', 'TK': 'Tokelau', 'TL': 'Timor-Leste', 'TM': 'Turkmenistan', 'TN': 'Tunisia', 'TO': 'Tonga', 'TP': 'East Timor', 'TR': 'Turkey', 'TT': 'Trinidad and Tobago', 'TV': 'Tuvalu', 'TW': 'Taiwan', 'TZ': 'Tanzania', 'UA': 'Ukraine', 'UG': 'Uganda', 'US': 'United States', 'UY': 'Uruguay', 'UZ': 'Uzbekistan', 'VA': 'Vatican City State (Holy See)', 'VC': 'Saint Vincent and the Grenadines', 'VE': 'Venezuela', 'VG': 'Virgin Islands (British)', 'VI': 'Virgin Islands (U.S.)', 'VN': 'Vietnam', 'VU': 'Vanuatu', 'WF': 'Wallis and Futuna', 'WS': 'Samoa', 'YE': 'Yemen', 'YT': 'Mayotte', 'YU': 'Yugoslavia', 'ZA': 'South Africa', 'ZM': 'Zambia', 'ZW': 'Zimbabwe'}
    def fetch_linkedin_insights(mainCompanyID, max_retries=3, retry_delay=5):
        if mainCompanyID is None:
            return {}
        for attempt in range(max_retries):
            with requests.session() as session:
                session.cookies['li_at'] = li_at
                session.cookies["JSESSIONID"] = JSESSIONID
                session.headers.update(headers)
                session.headers["csrf-token"] = JSESSIONID.strip('"')
                params = {'q': 'company', 'company': 'urn:li:fsd_company:' + mainCompanyID, 'decorationId': 'com.linkedin.voyager.dash.premium.companyinsights.CompanyInsightsCardCollection-12'}
                try:
                    response = session.get(insights_url, params=params)
                    response.raise_for_status()
                    return response.json()
                except (requests.ConnectionError, requests.Timeout) as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        return {}
                except Exception as e:
                    return {}
        return {}
    def format_growth(percentage):
        if percentage is None:
            return None
        if percentage > 0:
            return f"{percentage}% increase"
        elif percentage < 0:
            return f"{percentage}% decrease"
        else:
            return "No change"
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(1)
        except AttributeError:
            return None
    def generate_columns(count, patterns):
        return [pattern.format(i+1) for i in range(count) for pattern in patterns]
    pattern_universalName = re.compile(r"^(?:https?:\/\/)?(?:[\w]+\.)?linkedin\.com\/(?:company|company-beta|school)\/([A-Za-z0-9\._\%&'-]+?)(?:\/|\?|#|$)", re.IGNORECASE)
    df_final = pd.DataFrame()
    max_confirmedLocations = 0
    insights_url = 'https://www.linkedin.com/voyager/api/voyagerPremiumDashCompanyInsightsCard'
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    #-->Loop
    print("LinkedIn account scrape")
    num_rows = len(columnName_values)

    for index, company in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        df_loop_base = pd.DataFrame()
        df_loop_confirmedLocations = pd.DataFrame()
        df_loop_premium_not_final = pd.DataFrame()
        df_loop_headcountInsights = pd.DataFrame()
        df_loop_latestHeadcountByFunction = pd.DataFrame()
        df_loop_headcountGrowthByFunction = pd.DataFrame()
        df_loop_jobOpeningsByFunction = pd.DataFrame()
        df_loop_jobOpeningsGrowthByFunction = pd.DataFrame()
        df_loop_hireCounts = pd.DataFrame()
        df_loop_seniorHires = pd.DataFrame()
        df_loop_hiresInsights = pd.DataFrame()
        df_loop_alumniInsights = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        error = None
        if wordToSearch is None:
            error = "Invalid LinkedIn URL"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.progress((index) / num_rows)
            continue
        #-->WAIT FOR EMPLOYEESONLINKEDIN TO BE MORE THAN 0 START
        max_employeesOnLinkedIn_tries = 3
        employeesOnLinkedIn_tries = 0
        params = {
            "decorationId": "com.linkedin.voyager.deco.organization.web.WebFullCompanyMain-12",
            "q": "universalName",
            "universalName": urllib.parse.unquote(wordToSearch),
        }
        request_url = 'https://www.linkedin.com/voyager/api/organization/companies'
        while employeesOnLinkedIn_tries < max_employeesOnLinkedIn_tries:
            try:
                response = requests.get(url=request_url, cookies=cookies_dict, headers=headers, params=params)
                response.raise_for_status()
                response_json = response.json()
                company = get_company(response_json)
                employeesOnLinkedIn = int(company.get('staffCount', 0))
                if employeesOnLinkedIn > 0:
                    break
            except (requests.RequestException, KeyError, ValueError):
                employeesOnLinkedIn = 0
            employeesOnLinkedIn_tries += 1
            time.sleep(5)
        #-->WAIT FOR EMPLOYEESONLINKEDIN TO BE MORE THAN 0 END
        try:
            params = {
                "decorationId": "com.linkedin.voyager.deco.organization.web.WebFullCompanyMain-12",
                "q": "universalName",
                "universalName": urllib.parse.unquote(wordToSearch),
            }
            request_url = f'https://www.linkedin.com/voyager/api/organization/companies'
            response = requests.get(url=request_url, cookies=cookies_dict, headers=headers, params=params)
            response_json = response.json()
            company = get_company(response_json)
        except KeyError as e:
            company = {}
            error = "LinkedIn down"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.progress((index) / num_rows)
            continue
        except Exception as e:
            company = {}
            error = f'{e}'
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.progress((index) / num_rows)
            time.sleep(2.5)
            continue
        #-->DATA MANIPULATION START<--
        #-->BASE FIELDS<--
        #-->companyUrl
        companyUrl = company.get('url', None)
        if companyUrl and not companyUrl.endswith('/'):
            companyUrl += '/'
        #-->mainCompanyID
        pattern_mainCompanyID = re.compile(r"urn:li:fs_normalized_company:(.+)")
        mainCompanyID = extract_from_pattern(company.get('entityUrn', ''), pattern_mainCompanyID)
        #-->salesNavigatorLink
        salesNavigatorLink = create_salesNavigatorLink(mainCompanyID)
        #-->universalName
        universalName = company.get('universalName', None)
        #-->companyName
        companyName = company.get('name', None)
        #-->followerCount
        followerCount = company.get('followingInfo', {}).get('followerCount', '')
        #-->employeesOnLinkedIn
        employeesOnLinkedIn = company.get('staffCount', None)
        #-->tagLine
        tagLine = company.get('tagline', None)
        #-->description
        description = company.get('description', None)
        #-->website
        website = company.get('companyPageUrl', None)
        #-->domain
        domain = extract_domain_from_website(website)
        domain = domain.lower() if domain is not None else None
        #-->industryCode
        pattern_industryCode = re.compile(r"urn:li:fs_industry:(.+)")
        industryUrn = company.get('companyIndustries', [{}])[0].get('entityUrn', '')
        industryCode = extract_from_pattern(industryUrn, pattern_industryCode)
        #-->industry
        industry = company.get('companyIndustries', [{}])[0].get('localizedName', '')
        #-->companySize
        start_companySize = safe_str(company.get('staffCountRange', {}).get('start'))
        end_companySize = safe_str(company.get('staffCountRange', {}).get('end'))
        companySize = (start_companySize or "") + ("-" if start_companySize and end_companySize else "") + (end_companySize or "")
        companySize = companySize if companySize else None
        #-->headquarter
        city_headquarter = safe_str(company.get('headquarter', {}).get('city'))
        geographicArea_headquarter = safe_str(company.get('headquarter', {}).get('geographicArea'))
        geographicArea_headquarter = get_full_name(dict_geographicArea, geographicArea_headquarter)
        headquarter = (city_headquarter or "") + (", " if city_headquarter and geographicArea_headquarter else "") + (geographicArea_headquarter or "")
        headquarter = headquarter if headquarter else None
        #-->founded
        founded = company.get('foundedOn', {}).get('year', '')
        #-->specialities
        list_specialities = company.get('specialities', [])
        specialities = ', '.join(list_specialities) if list_specialities else None
        #-->companyType
        companyType = company.get('companyType', {}).get('localizedName', '')
        #-->phone
        phone = company.get('phone', {}).get('number', '')
        #-->headquarter_line1, headquarter_line2, headquarter_city, headquarter_geographicArea, headquarter_postalCode, headquarter_country, headquarter_companyAddress
        headquarter_line1 = safe_str(company.get('headquarter', {}).get('line1'))
        headquarter_line2 = safe_str(company.get('headquarter', {}).get('line2'))
        headquarter_city = safe_str(company.get('headquarter', {}).get('city'))
        headquarter_geographicArea = safe_str(company.get('headquarter', {}).get('geographicArea'))
        headquarter_geographicArea = get_full_name(dict_geographicArea, headquarter_geographicArea)
        headquarter_postalCode = safe_str(company.get('headquarter', {}).get('postalCode'))
        headquarter_country = safe_str(company.get('headquarter', {}).get('country'))
        headquarter_country = get_full_name(dict_country, headquarter_country)
        components_headquarter_companyAddress = [headquarter_line1,headquarter_line2,headquarter_city,headquarter_geographicArea,headquarter_postalCode,headquarter_country]
        headquarter_companyAddress = ', '.join(filter(None, components_headquarter_companyAddress))
        #-->locationN_description, locationN_line1, locationN_line2, locationN_city, locationN_geographicArea, locationN_postalCode, locationN_country, locationN_companyAddress
        confirmedLocations = company.get('confirmedLocations', [])
        flattened_confirmedLocations = {}
        for idx, location in enumerate(confirmedLocations, start=1):
            prefix = f"location{idx}_"
            geographicArea = get_full_name(dict_geographicArea, location.get("geographicArea", ""))
            country = get_full_name(dict_country, location.get("country", ""))
            keys = ['description', 'line1', 'line2', 'city', 'postalCode']
            for key in keys:
                flattened_confirmedLocations[prefix + key] = location.get(key, "")
            flattened_confirmedLocations[prefix + "geographicArea"] = geographicArea
            flattened_confirmedLocations[prefix + "country"] = country
            address_components = [location.get('line1'), location.get('line2'), location.get('city'), geographicArea, location.get('postalCode'), country]
            address = ', '.join(filter(None, address_components))
            flattened_confirmedLocations[prefix + "companyAddress"] = address
        if 'idx' in locals() and idx > max_confirmedLocations:
                max_confirmedLocations = idx
        df_loop_confirmedLocations = pd.DataFrame([flattened_confirmedLocations])
        #-->banner10000x10000, banner400x400, banner200x200
        background_image = company.get('backgroundCoverImage', {})
        image = background_image.get('image', {})
        vector_image = image.get('com.linkedin.common.VectorImage', {})
        root_url = vector_image.get('rootUrl')
        banner_names = ['banner10000x10000', 'banner400x400', 'banner200x200']
        banners = dict.fromkeys(banner_names, None)
        if root_url:
            artifacts = vector_image.get('artifacts', [])
            sorted_artifacts = sorted(artifacts, key=lambda x: x.get('width', 0), reverse=True)
            for idx, artifact in enumerate(sorted_artifacts):
                if idx >= len(banner_names):
                    break
                banner_name = banner_names[idx]
                banners[banner_name] = root_url + artifact.get('fileIdentifyingUrlPathSegment')
        banner10000x10000 = banners['banner10000x10000']
        banner400x400 = banners['banner400x400']
        banner200x200 = banners['banner200x200']
        #-->logo400x400, logo200x200, logo100x100
        logo_image = company.get('logo', {})
        image = logo_image.get('image', {})
        vector_image = image.get('com.linkedin.common.VectorImage', {})
        root_url = vector_image.get('rootUrl')
        logo_names = ['logo400x400', 'logo200x200', 'logo100x100']
        logos = dict.fromkeys(logo_names, None)
        if root_url:
            artifacts = vector_image.get('artifacts', [])
            sorted_artifacts = sorted(artifacts, key=lambda x: x.get('width', 0), reverse=True)
            for idx, artifact in enumerate(sorted_artifacts):
                if idx >= len(logo_names):
                    break
                logo_name = logo_names[idx]
                logos[logo_name] = root_url + artifact.get('fileIdentifyingUrlPathSegment')
        logo400x400 = logos['logo400x400']
        logo200x200 = logos['logo200x200']
        logo100x100 = logos['logo100x100']
        #-->showcase
        showcase = company.get('showcase', None)
        #-->autoGenerated
        autoGenerated = company.get('autoGenerated', None)
        #-->isClaimable
        isClaimable = company.get('claimable', None)
        #-->jobSearchPageUrl
        jobSearchPageUrl = company.get('jobSearchPageUrl', None)
        #-->associatedHashtags
        associatedHashtags = ', '.join(filter(None, (extract_hashtag(item) for item in company.get('associatedHashtags', []))))
        #-->callToActionUrl
        callToActionUrl = safe_str(company.get('callToAction', {}).get('url'))
        #-->timestamp
        current_timestamp = datetime.now()
        timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        #-->Create dataframe with base fields
        all_variables = locals()
        selected_vars = {var: [all_variables[var]] for var in ["companyUrl", "mainCompanyID", "salesNavigatorLink", "universalName", "companyName", "followerCount","employeesOnLinkedIn", "tagLine", "description", "website", "domain", "industryCode", "industry","companySize", "headquarter", "founded", "specialities", "companyType", "phone", "headquarter_line1","headquarter_line2", "headquarter_city", "headquarter_geographicArea", "headquarter_postalCode","headquarter_country", "headquarter_companyAddress", "banner10000x10000", "banner400x400","banner200x200", "logo400x400", "logo200x200", "logo100x100", "showcase", "autoGenerated","isClaimable", "jobSearchPageUrl", "associatedHashtags", "callToActionUrl", "timestamp"]}
        df_loop_base = pd.DataFrame(selected_vars)
        #-->PREMIUM FIELDS<--
        try:
            company_insights = fetch_linkedin_insights(mainCompanyID)
        except KeyError as e:
            company_insights = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.progress((index) / num_rows)
            continue
        if len(company_insights.get('elements', [])) > 0:
            #-->headcountInsights, functionHeadcountInsights, hiresInsights, alumniInsights, jobOpeningsInsights
            headcountInsights = [company_union.get('companyInsightsUnion', {}).get('headcountInsights') for company_union in company_insights.get('elements', [])]
            functionHeadcountInsights = [{key: insights.get(key)  for key in ['latestHeadcountByFunction', 'headcountGrowthByFunction'] if insights.get(key) is not None} for company_union in company_insights.get('elements', [])  for insights in [company_union.get('companyInsightsUnion', {}).get('functionHeadcountInsights', {})] if insights.get('latestHeadcountByFunction') or insights.get('headcountGrowthByFunction')]
            hiresInsights = [insight.get('hiresInsights') for company_union in company_insights.get('elements', []) for insight in [company_union.get('companyInsightsUnion', {})] if insight.get('hiresInsights') is not None]
            alumniInsights = [insight.get('alumniInsights') for company_union in company_insights.get('elements', []) for insight in [company_union.get('companyInsightsUnion', {})] if insight.get('alumniInsights') is not None]
            jobOpeningsInsights = [{key: insights.get(key)  for key in ['jobOpeningsByFunction', 'jobOpeningsGrowthByFunction'] if insights.get(key) is not None} for company_union in company_insights.get('elements', [])  for insights in [company_union.get('companyInsightsUnion', {}).get('jobOpeningsInsights', {})] if insights.get('jobOpeningsByFunction') or insights.get('jobOpeningsGrowthByFunction')]
            #-->totalEmployeeCount
            totalEmployeeCount = headcountInsights[0].get('totalEmployees') if headcountInsights and headcountInsights[0] else None
            #-->growth6Mth, growth1Yr, growth2Yr
            #growth_data = headcountInsights[0]['growthPeriods'] if headcountInsights and headcountInsights[0] else []
            growth_data = safe_extract(headcountInsights, 0, 'growthPeriods')
            if growth_data is not None:
                growth_dict = {item['monthDifference']: item['changePercentage'] for item in growth_data}
                growth6Mth = format_growth(growth_dict.get(6, None))
                growth1Yr = format_growth(growth_dict.get(12, None))
                growth2Yr = format_growth(growth_dict.get(24, None))
            else:
                growth_dict = {}
                growth6Mth = None
                growth1Yr = None
                growth2Yr = None
            #-->averageTenure
            tenure_text = headcountInsights[0]['headcounts']['medianTenureYears']['text'] if headcountInsights and headcountInsights[0] and 'headcounts' in headcountInsights[0] and 'medianTenureYears' in headcountInsights[0]['headcounts'] else ''
            averageTenure = tenure_text.split("Median employee tenure ‧ ", 1)[-1] if "Median employee tenure ‧ " in tenure_text else None
            #Create dataframe with some premium fields
            all_variables = locals()
            selected_vars_premium = {var: [all_variables.get(var, None)] for var in ["totalEmployeeCount", "growth6Mth", "growth1Yr", "growth2Yr", "averageTenure"]}
            df_loop_premium_not_final = pd.DataFrame(selected_vars_premium)
            #-->headcountGrowthMonthNDate, headcountGrowthMonthNCount
            headcount_data = headcountInsights[0].get('headcounts', {}) if headcountInsights and headcountInsights[0] else {}
            headcount_growth_data = headcount_data.get('headcountGrowth', [])
            flattened_headcount = {f"headcountGrowthMonth{i}Date": None for i in range(25)}
            flattened_headcount.update({f"headcountGrowthMonth{i}Count": None for i in range(25)})
            if isinstance(headcount_growth_data, list):
                for i, entry in enumerate(headcount_growth_data[:25]):
                    date = entry.get('startedOn')
                    if date:
                        flattened_headcount[f"headcountGrowthMonth{i}Date"] = f"{date.get('month')}/{date.get('day')}/{date.get('year')}"
                    flattened_headcount[f"headcountGrowthMonth{i}Count"] = entry.get('employeeCount')
            df_loop_headcountInsights = pd.DataFrame([flattened_headcount])
            df_loop_headcountInsights = df_loop_headcountInsights[['headcountGrowthMonth0Date', 'headcountGrowthMonth0Count', 'headcountGrowthMonth1Date', 'headcountGrowthMonth1Count', 'headcountGrowthMonth2Date', 'headcountGrowthMonth2Count', 'headcountGrowthMonth3Date', 'headcountGrowthMonth3Count', 'headcountGrowthMonth4Date', 'headcountGrowthMonth4Count', 'headcountGrowthMonth5Date', 'headcountGrowthMonth5Count', 'headcountGrowthMonth6Date', 'headcountGrowthMonth6Count', 'headcountGrowthMonth7Date', 'headcountGrowthMonth7Count', 'headcountGrowthMonth8Date', 'headcountGrowthMonth8Count', 'headcountGrowthMonth9Date', 'headcountGrowthMonth9Count', 'headcountGrowthMonth10Date', 'headcountGrowthMonth10Count', 'headcountGrowthMonth11Date', 'headcountGrowthMonth11Count', 'headcountGrowthMonth12Date', 'headcountGrowthMonth12Count', 'headcountGrowthMonth13Date', 'headcountGrowthMonth13Count', 'headcountGrowthMonth14Date', 'headcountGrowthMonth14Count', 'headcountGrowthMonth15Date', 'headcountGrowthMonth15Count', 'headcountGrowthMonth16Date', 'headcountGrowthMonth16Count', 'headcountGrowthMonth17Date', 'headcountGrowthMonth17Count', 'headcountGrowthMonth18Date', 'headcountGrowthMonth18Count', 'headcountGrowthMonth19Date', 'headcountGrowthMonth19Count', 'headcountGrowthMonth20Date', 'headcountGrowthMonth20Count', 'headcountGrowthMonth21Date', 'headcountGrowthMonth21Count', 'headcountGrowthMonth22Date', 'headcountGrowthMonth22Count', 'headcountGrowthMonth23Date', 'headcountGrowthMonth23Count', 'headcountGrowthMonth24Date', 'headcountGrowthMonth24Count']]
            #-->distributionFunction and distributionFunctionPercentage
            mapping_latestHeadcountByFunction = {
                "urn:li:fsd_function:1": ('distributionAccounting', 'distributionAccountingPercentage'),
                "urn:li:fsd_function:2": ('distributionAdministrative', 'distributionAdministrativePercentage'),
                "urn:li:fsd_function:3": ('distributionArtsAndDesign', 'distributionArtsAndDesignPercentage'),
                "urn:li:fsd_function:4": ('distributionBusinessDevelopment', 'distributionBusinessDevelopmentPercentage'),
                "urn:li:fsd_function:5": ('distributionCommunityAndSocialServices', 'distributionCommunityAndSocialServicesPercentage'),
                "urn:li:fsd_function:6": ('distributionConsulting', 'distributionConsultingPercentage'),
                "urn:li:fsd_function:7": ('distributionEducation', 'distributionEducationPercentage'),
                "urn:li:fsd_function:8": ('distributionEngineering', 'distributionEngineeringPercentage'),
                "urn:li:fsd_function:9": ('distributionEntrepreneurship', 'distributionEntrepreneurshipPercentage'),
                "urn:li:fsd_function:10": ('distributionFinance', 'distributionFinancePercentage'),
                "urn:li:fsd_function:11": ('distributionHealthcareServices', 'distributionHealthcareServicesPercentage'),
                "urn:li:fsd_function:12": ('distributionHumanResources', 'distributionHumanResourcesPercentage'),
                "urn:li:fsd_function:13": ('distributionInformationTechnology', 'distributionInformationTechnologyPercentage'),
                "urn:li:fsd_function:14": ('distributionLegal', 'distributionLegalPercentage'),
                "urn:li:fsd_function:15": ('distributionMarketing', 'distributionMarketingPercentage'),
                "urn:li:fsd_function:16": ('distributionMediaAndCommunication', 'distributionMediaAndCommunicationPercentage'),
                "urn:li:fsd_function:17": ('distributionMilitaryAndProtectiveServices', 'distributionMilitaryAndProtectiveServicesPercentage'),
                "urn:li:fsd_function:18": ('distributionOperations', 'distributionOperationsPercentage'),
                "urn:li:fsd_function:19": ('distributionProductManagement', 'distributionProductManagementPercentage'),
                "urn:li:fsd_function:20": ('distributionProgramAndProjectManagement', 'distributionProgramAndProjectManagementPercentage'),
                "urn:li:fsd_function:21": ('distributionPurchasing', 'distributionPurchasingPercentage'),
                "urn:li:fsd_function:22": ('distributionQualityAssurance', 'distributionQualityAssurancePercentage'),
                "urn:li:fsd_function:23": ('distributionRealEstate', 'distributionRealEstatePercentage'),
                "urn:li:fsd_function:24": ('distributionResearch', 'distributionResearchPercentage'),
                "urn:li:fsd_function:25": ('distributionSales', 'distributionSalesPercentage'),
                "urn:li:fsd_function:26": ('distributionSupport', 'distributionSupportPercentage')
            }
            try:
                data_latestHeadcountByFunction = functionHeadcountInsights[0]['latestHeadcountByFunction']['countByFunction']
            except (IndexError, KeyError):
                data_latestHeadcountByFunction = []
            flattened_latestHeadcountByFunction = {k: None for key_tuple in mapping_latestHeadcountByFunction.values() for k in key_tuple}
            for item in data_latestHeadcountByFunction:
                function_urn = item.get('functionUrn')
                if function_urn in mapping_latestHeadcountByFunction:
                    var, var_per = mapping_latestHeadcountByFunction[function_urn]
                    flattened_latestHeadcountByFunction[var] = item.get('functionCount')
                    flattened_latestHeadcountByFunction[var_per] = item.get('functionPercentage')
            df_loop_latestHeadcountByFunction = pd.DataFrame([flattened_latestHeadcountByFunction])
            #-->growth6MthFunction and growth1YrFunction
            try:
                data_headcountGrowthByFunction = functionHeadcountInsights[0]['headcountGrowthByFunction']
            except (IndexError, KeyError):
                data_headcountGrowthByFunction = []
            mapping_headcountGrowthByFunction = {
                "urn:li:fsd_function:1": ('growth6MthAccounting', 'growth1YrAccounting'),
                "urn:li:fsd_function:2": ('growth6MthAdministrative', 'growth1YrAdministrative'),
                "urn:li:fsd_function:3": ('growth6MthArtsAndDesign', 'growth1YrArtsAndDesign'),
                "urn:li:fsd_function:4": ('growth6MthBusinessDevelopment', 'growth1YrBusinessDevelopment'),
                "urn:li:fsd_function:5": ('growth6MthCommunityAndSocialServices', 'growth1YrCommunityAndSocialServices'),
                "urn:li:fsd_function:6": ('growth6MthConsulting', 'growth1YrConsulting'),
                "urn:li:fsd_function:7": ('growth6MthEducation', 'growth1YrEducation'),
                "urn:li:fsd_function:8": ('growth6MthEngineering', 'growth1YrEngineering'),
                "urn:li:fsd_function:9": ('growth6MthEntrepreneurship', 'growth1YrEntrepreneurship'),
                "urn:li:fsd_function:10": ('growth6MthFinance', 'growth1YrFinance'),
                "urn:li:fsd_function:11": ('growth6MthHealthcareServices', 'growth1YrHealthcareServices'),
                "urn:li:fsd_function:12": ('growth6MthHumanResources', 'growth1YrHumanResources'),
                "urn:li:fsd_function:13": ('growth6MthInformationTechnology', 'growth1YrInformationTechnology'),
                "urn:li:fsd_function:14": ('growth6MthLegal', 'growth1YrLegal'),
                "urn:li:fsd_function:15": ('growth6MthMarketing', 'growth1YrMarketing'),
                "urn:li:fsd_function:16": ('growth6MthMediaAndCommunication', 'growth1YrMediaAndCommunication'),
                "urn:li:fsd_function:17": ('growth6MthMilitaryAndProtectiveServices', 'growth1YrMilitaryAndProtectiveServices'),
                "urn:li:fsd_function:18": ('growth6MthOperations', 'growth1YrOperations'),
                "urn:li:fsd_function:19": ('growth6MthProductManagement', 'growth1YrProductManagement'),
                "urn:li:fsd_function:20": ('growth6MthProgramAndProjectManagement', 'growth1YrProgramAndProjectManagement'),
                "urn:li:fsd_function:21": ('growth6MthPurchasing', 'growth1YrPurchasing'),
                "urn:li:fsd_function:22": ('growth6MthQualityAssurance', 'growth1YrQualityAssurance'),
                "urn:li:fsd_function:23": ('growth6MthRealEstate', 'growth1YrRealEstate'),
                "urn:li:fsd_function:24": ('growth6MthResearch', 'growth1YrResearch'),
                "urn:li:fsd_function:25": ('growth6MthSales', 'growth1YrSales'),
                "urn:li:fsd_function:26": ('growth6MthSupport', 'growth1YrSupport')
            }
            flattened_headcountGrowthByFunction = {column_name: None for urn_values in mapping_headcountGrowthByFunction.values() for column_name in urn_values}
            for entry in data_headcountGrowthByFunction:
                if 'function' in entry and 'entityUrn' in entry['function']:
                    function_urn = entry['function']['entityUrn']
                elif 'functionUrn' in entry:
                    function_urn = entry['functionUrn']
                else:
                    function_urn = None
                column_info = mapping_headcountGrowthByFunction.get(function_urn)
                if column_info:
                    for period in entry['growthPeriods']:
                        month_diff = period['monthDifference']
                        change_percentage = period['changePercentage']
                        if month_diff == 6:
                            column_name = column_info[0]
                        elif month_diff == 12:
                            column_name = column_info[1]
                        else:
                            continue
                        flattened_headcountGrowthByFunction[column_name] = change_percentage
            df_loop_headcountGrowthByFunction = pd.DataFrame([flattened_headcountGrowthByFunction])
            #-->distributionFunctionJobs and distributionFunctionPercentageJobs
            mapping_jobOpeningsByFunction = {
                "urn:li:fsd_function:1": ('distributionAccountingJobs', 'distributionAccountingPercentageJobs'),
                "urn:li:fsd_function:2": ('distributionAdministrativeJobs', 'distributionAdministrativePercentageJobs'),
                "urn:li:fsd_function:3": ('distributionArtsAndDesignJobs', 'distributionArtsAndDesignPercentageJobs'),
                "urn:li:fsd_function:4": ('distributionBusinessDevelopmentJobs', 'distributionBusinessDevelopmentPercentageJobs'),
                "urn:li:fsd_function:5": ('distributionCommunityAndSocialServicesJobs', 'distributionCommunityAndSocialServicesPercentageJobs'),
                "urn:li:fsd_function:6": ('distributionConsultingJobs', 'distributionConsultingPercentageJobs'),
                "urn:li:fsd_function:7": ('distributionEducationJobs', 'distributionEducationPercentageJobs'),
                "urn:li:fsd_function:8": ('distributionEngineeringJobs', 'distributionEngineeringPercentageJobs'),
                "urn:li:fsd_function:9": ('distributionEntrepreneurshipJobs', 'distributionEntrepreneurshipPercentageJobs'),
                "urn:li:fsd_function:10": ('distributionFinanceJobs', 'distributionFinancePercentageJobs'),
                "urn:li:fsd_function:11": ('distributionHealthcareServicesJobs', 'distributionHealthcareServicesPercentageJobs'),
                "urn:li:fsd_function:12": ('distributionHumanResourcesJobs', 'distributionHumanResourcesPercentageJobs'),
                "urn:li:fsd_function:13": ('distributionInformationTechnologyJobs', 'distributionInformationTechnologyPercentageJobs'),
                "urn:li:fsd_function:14": ('distributionLegalJobs', 'distributionLegalPercentageJobs'),
                "urn:li:fsd_function:15": ('distributionMarketingJobs', 'distributionMarketingPercentageJobs'),
                "urn:li:fsd_function:16": ('distributionMediaAndCommunicationJobs', 'distributionMediaAndCommunicationPercentageJobs'),
                "urn:li:fsd_function:17": ('distributionMilitaryAndProtectiveServicesJobs', 'distributionMilitaryAndProtectiveServicesPercentageJobs'),
                "urn:li:fsd_function:18": ('distributionOperationsJobs', 'distributionOperationsPercentageJobs'),
                "urn:li:fsd_function:19": ('distributionProductManagementJobs', 'distributionProductManagementPercentageJobs'),
                "urn:li:fsd_function:20": ('distributionProgramAndProjectManagementJobs', 'distributionProgramAndProjectManagementPercentageJobs'),
                "urn:li:fsd_function:21": ('distributionPurchasingJobs', 'distributionPurchasingPercentageJobs'),
                "urn:li:fsd_function:22": ('distributionQualityAssuranceJobs', 'distributionQualityAssurancePercentageJobs'),
                "urn:li:fsd_function:23": ('distributionRealEstateJobs', 'distributionRealEstatePercentageJobs'),
                "urn:li:fsd_function:24": ('distributionResearchJobs', 'distributionResearchPercentageJobs'),
                "urn:li:fsd_function:25": ('distributionSalesJobs', 'distributionSalesPercentageJobs'),
                "urn:li:fsd_function:26": ('distributionSupportJobs', 'distributionSupportPercentageJobs')
            }
            try:
                data_jobOpeningsByFunction = jobOpeningsInsights[0]['jobOpeningsByFunction'][0]['countByFunction']
            except (IndexError, KeyError):
                data_jobOpeningsByFunction = []
            flattened_jobOpeningsByFunction = {k: None for key_tuple in mapping_jobOpeningsByFunction.values() for k in key_tuple}
            for item in data_jobOpeningsByFunction:
                function_urn = item.get('functionUrn')
                if function_urn in mapping_jobOpeningsByFunction:
                    var, var_per = mapping_jobOpeningsByFunction[function_urn]
                    flattened_jobOpeningsByFunction[var] = item.get('functionCount')
                    flattened_jobOpeningsByFunction[var_per] = item.get('functionPercentage')
            df_loop_jobOpeningsByFunction = pd.DataFrame([flattened_jobOpeningsByFunction])
            #-->growth3MthFunctionJobs, growth6MthFunctionJobs, growth1YrFunctionJobs
            mapping_jobOpeningsGrowthByFunction = {
                "urn:li:fsd_function:1": ('growth3MthAccountingJobs', 'growth6MthAccountingJobs', 'growth1YrAccountingJobs'),
                "urn:li:fsd_function:2": ('growth3MthAdministrativeJobs', 'growth6MthAdministrativeJobs', 'growth1YrAdministrativeJobs'),
                "urn:li:fsd_function:3": ('growth3MthArtsAndDesignJobs', 'growth6MthArtsAndDesignJobs', 'growth1YrArtsAndDesignJobs'),
                "urn:li:fsd_function:4": ('growth3MthBusinessDevelopmentJobs', 'growth6MthBusinessDevelopmentJobs', 'growth1YrBusinessDevelopmentJobs'),
                "urn:li:fsd_function:5": ('growth3MthCommunityAndSocialServicesJobs', 'growth6MthCommunityAndSocialServicesJobs', 'growth1YrCommunityAndSocialServicesJobs'),
                "urn:li:fsd_function:6": ('growth3MthConsultingJobs', 'growth6MthConsultingJobs', 'growth1YrConsultingJobs'),
                "urn:li:fsd_function:7": ('growth3MthEducationJobs', 'growth6MthEducationJobs', 'growth1YrEducationJobs'),
                "urn:li:fsd_function:8": ('growth3MthEngineeringJobs', 'growth6MthEngineeringJobs', 'growth1YrEngineeringJobs'),
                "urn:li:fsd_function:9": ('growth3MthEntrepreneurshipJobs', 'growth6MthEntrepreneurshipJobs', 'growth1YrEntrepreneurshipJobs'),
                "urn:li:fsd_function:10": ('growth3MthFinanceJobs', 'growth6MthFinanceJobs', 'growth1YrFinanceJobs'),
                "urn:li:fsd_function:11": ('growth3MthHealthcareServicesJobs', 'growth6MthHealthcareServicesJobs', 'growth1YrHealthcareServicesJobs'),
                "urn:li:fsd_function:12": ('growth3MthHumanResourcesJobs', 'growth6MthHumanResourcesJobs', 'growth1YrHumanResourcesJobs'),
                "urn:li:fsd_function:13": ('growth3MthInformationTechnologyJobs', 'growth6MthInformationTechnologyJobs', 'growth1YrInformationTechnologyJobs'),
                "urn:li:fsd_function:14": ('growth3MthLegalJobs', 'growth6MthLegalJobs', 'growth1YrLegalJobs'),
                "urn:li:fsd_function:15": ('growth3MthMarketingJobs', 'growth6MthMarketingJobs', 'growth1YrMarketingJobs'),
                "urn:li:fsd_function:16": ('growth3MthMediaAndCommunicationJobs', 'growth6MthMediaAndCommunicationJobs', 'growth1YrMediaAndCommunicationJobs'),
                "urn:li:fsd_function:17": ('growth3MthMilitaryAndProtectiveServicesJobs', 'growth6MthMilitaryAndProtectiveServicesJobs', 'growth1YrMilitaryAndProtectiveServicesJobs'),
                "urn:li:fsd_function:18": ('growth3MthOperationsJobs', 'growth6MthOperationsJobs', 'growth1YrOperationsJobs'),
                "urn:li:fsd_function:19": ('growth3MthProductManagementJobs', 'growth6MthProductManagementJobs', 'growth1YrProductManagementJobs'),
                "urn:li:fsd_function:20": ('growth3MthProgramAndProjectManagementJobs', 'growth6MthProgramAndProjectManagementJobs', 'growth1YrProgramAndProjectManagementJobs'),
                "urn:li:fsd_function:21": ('growth3MthPurchasingJobs', 'growth6MthPurchasingJobs', 'growth1YrPurchasingJobs'),
                "urn:li:fsd_function:22": ('growth3MthQualityAssuranceJobs', 'growth6MthQualityAssuranceJobs', 'growth1YrQualityAssuranceJobs'),
                "urn:li:fsd_function:23": ('growth3MthRealEstateJobs', 'growth6MthRealEstateJobs', 'growth1YrRealEstateJobs'),
                "urn:li:fsd_function:24": ('growth3MthResearchJobs', 'growth6MthResearchJobs', 'growth1YrResearchJobs'),
                "urn:li:fsd_function:25": ('growth3MthSalesJobs', 'growth6MthSalesJobs', 'growth1YrSalesJobs'),
                "urn:li:fsd_function:26": ('growth3MthSupportJobs', 'growth6MthSupportJobs', 'growth1YrSupportJobs')
            }
            try:
                data_jobOpeningsGrowthByFunction = jobOpeningsInsights[0]['jobOpeningsGrowthByFunction']
            except (IndexError, KeyError):
                data_jobOpeningsGrowthByFunction = []
            flattened_jobOpeningsGrowthByFunction = {column_name: None for urn_values in mapping_jobOpeningsGrowthByFunction.values() for column_name in urn_values}
            for entry in data_jobOpeningsGrowthByFunction:
                function_data = entry.get('function')
                if function_data:
                    function_urn = function_data.get('entityUrn')
                column_info = mapping_jobOpeningsGrowthByFunction.get(function_urn)
                if column_info:
                    for period in entry['growthPeriods']:
                        month_diff = period['monthDifference']
                        change_percentage = period['changePercentage']
                        if month_diff == 3:
                            column_name = column_info[0]
                        elif month_diff == 6:
                            column_name = column_info[1]
                        elif month_diff == 12:
                            column_name = column_info[2]
                        else:
                            continue
                        flattened_jobOpeningsGrowthByFunction[column_name] = change_percentage
            df_loop_jobOpeningsGrowthByFunction = pd.DataFrame([flattened_jobOpeningsGrowthByFunction])
            #-->totalNumberOfSeniorHires
            totalNumberOfSeniorHires = hiresInsights[0].get('totalNumberOfSeniorHires', {}) if hiresInsights and hiresInsights[0] else None
            #-->hireAllCountMonthNDate, hireAllCountMonthN, hireSeniorCountMonthNDate, hireSeniorCountMonthN
            hireCounts = hiresInsights[0].get('hireCounts', {}) if hiresInsights and hiresInsights[0] else []
            flattened_hire = {f"hireAllCountMonth{i}Date": None for i in range(25)}
            flattened_hire.update({f"hireAllCountMonth{i}": None for i in range(25)})
            flattened_hire.update({f"hireSeniorCountMonth{i}Date": None for i in range(25)})
            flattened_hire.update({f"hireSeniorCountMonth{i}": None for i in range(25)})
            if isinstance(hireCounts, list):
                for i, entry in enumerate(hireCounts[:25]):
                    date = entry.get('yearMonthOn')
                    if date:
                        formatted_date = f"{date.get('month')}/{date.get('day')}/{date.get('year')}"
                        flattened_hire[f"hireAllCountMonth{i}Date"] = formatted_date
                        flattened_hire[f"hireSeniorCountMonth{i}Date"] = formatted_date
                    flattened_hire[f"hireAllCountMonth{i}"] = entry.get('allEmployeeHireCount')
                    flattened_hire[f"hireSeniorCountMonth{i}"] = entry.get('seniorHireCount')
            columns_order = [item for sublist in [[f"hireAllCountMonth{i}Date", f"hireAllCountMonth{i}", f"hireSeniorCountMonth{i}Date", f"hireSeniorCountMonth{i}"] for i in range(25)] for item in sublist]
            df_loop_hireCounts = pd.DataFrame([flattened_hire])
            df_loop_hireCounts = df_loop_hireCounts[columns_order]
            #-->seniorHiresN_title, seniorHiresN_linkedInUrl, seniorHiresN_fullName, seniorHiresN_date
            seniorHires = hiresInsights[0].get('seniorHires', {}) if hiresInsights and hiresInsights[0] else {}
            flattened_seniorHire = {}
            for i, entry in enumerate(seniorHires, 1):
                title = entry.get('hiredPosition', {}).get('text')
                linkedInUrl = entry.get('entityLockup', {}).get('navigationUrl')
                fullName = entry.get('entityLockup', {}).get('title', {}).get('text')
                hire_date = entry.get('hireYearMonthOn', {})
                month = hire_date.get('month')
                day = hire_date.get('day')
                year = hire_date.get('year')
                date = f"{month}/{day}/{year}" if month and day and year else None
                flattened_seniorHire[f"seniorHires{i}_title"] = title
                flattened_seniorHire[f"seniorHires{i}_linkedInUrl"] = linkedInUrl
                flattened_seniorHire[f"seniorHires{i}_fullName"] = fullName
                flattened_seniorHire[f"seniorHires{i}_date"] = date
            df_loop_seniorHires = pd.DataFrame([flattened_seniorHire])
            #-->Concatenate df_loop_hiresInsights
            df_loop_hiresInsights = pd.concat([df_loop_hireCounts, df_loop_seniorHires], axis=1)
            df_loop_hiresInsights.insert(0, 'totalNumberOfSeniorHires', totalNumberOfSeniorHires)
            #-->alumniN_currentTitle, alumniN_linkedInUrl, alumniN_fullName, alumniN_exitDate, alumniN_exitTitle
            alumni_elements = alumniInsights[0].get('alumni', []) if alumniInsights else []
            flattened_alumni = {}
            for j, alumni in enumerate(alumni_elements, 1):
                entity_lockup = alumni.get('entityLockup', {})
                subtitle = entity_lockup.get('subtitle', {})
                title = entity_lockup.get('title', {})
                exit_year_month_on = alumni.get('exitYearMonthOn', {})
                exit_date = "{}/{}/{}".format(exit_year_month_on.get('month', ''), exit_year_month_on.get('day', ''), exit_year_month_on.get('year', '')) if exit_year_month_on else None
                alumni_prefix = f"alumni{j}_"
                flattened_alumni[alumni_prefix + 'currentTitle'] = subtitle.get('text')
                flattened_alumni[alumni_prefix + 'linkedInUrl'] = entity_lockup.get('navigationUrl')
                flattened_alumni[alumni_prefix + 'fullName'] = title.get('text')
                flattened_alumni[alumni_prefix + 'exitDate'] = exit_date
                flattened_alumni[alumni_prefix + 'exitTitle'] = alumni.get('exitedPosition', {}).get('text')
            df_loop_alumniInsights = pd.DataFrame([flattened_alumni])
            #-->DATA MANIPULATION END<--
            error = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_loop_final = df_loop_final.dropna(axis=1, how='all')
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.progress((index) / num_rows)


        else:
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.progress((index) / num_rows)
            continue
    COLUMN_PATTERNS = {
        'location': [
            'location{}_description', 'location{}_line1', 'location{}_line2',
            'location{}_city', 'location{}_postalCode', 'location{}_geographicArea',
            'location{}_country', 'location{}_companyAddress'
        ]
    }
    MAX_COUNTS = {'location': min(max_confirmedLocations, 100)}
    try:
        location_int_count = int(location_count)
    except ValueError:
        location_int_count = max_confirmedLocations
    counts = {'location': location_int_count}
    columns_lists = {}
    for key, max_count in MAX_COUNTS.items():
        count = min(counts[key], max_count)
        columns_lists[key] = generate_columns(count, COLUMN_PATTERNS[key])
    final_list = ["query", "error", "companyUrl", "mainCompanyID", "salesNavigatorLink", "universalName", "companyName", "followerCount","employeesOnLinkedIn", "tagLine", "description", "website", "domain", "industryCode", "industry","companySize", "headquarter", "founded", "specialities", "companyType", "phone", "headquarter_line1","headquarter_line2", "headquarter_city", "headquarter_geographicArea", "headquarter_postalCode","headquarter_country", "headquarter_companyAddress", "banner10000x10000", "banner400x400","banner200x200", "logo400x400", "logo200x200", "logo100x100", "showcase", "autoGenerated","isClaimable", "jobSearchPageUrl", "associatedHashtags", "callToActionUrl", "timestamp"]
    locations_list = columns_lists['location']
    final_list.extend(locations_list)
    fields_check = include_headcountInsights = include_headcountByFunction = include_headcountGrowthByFunction = include_jobOpeningsByFunction = include_jobOpeningsGrowthByFunction = include_hiresInsights = include_alumniInsights = bool(li_a)
    if fields_check == True:
        temporal_list_1 = ["totalEmployeeCount", "growth6Mth", "growth1Yr", "growth2Yr", "averageTenure"]
        final_list.extend(temporal_list_1)
    if include_headcountInsights == True:
        headcountInsights_column_list = ['headcountGrowthMonth0Date', 'headcountGrowthMonth0Count', 'headcountGrowthMonth1Date', 'headcountGrowthMonth1Count', 'headcountGrowthMonth2Date', 'headcountGrowthMonth2Count', 'headcountGrowthMonth3Date', 'headcountGrowthMonth3Count', 'headcountGrowthMonth4Date', 'headcountGrowthMonth4Count', 'headcountGrowthMonth5Date', 'headcountGrowthMonth5Count', 'headcountGrowthMonth6Date', 'headcountGrowthMonth6Count', 'headcountGrowthMonth7Date', 'headcountGrowthMonth7Count', 'headcountGrowthMonth8Date', 'headcountGrowthMonth8Count', 'headcountGrowthMonth9Date', 'headcountGrowthMonth9Count', 'headcountGrowthMonth10Date', 'headcountGrowthMonth10Count', 'headcountGrowthMonth11Date', 'headcountGrowthMonth11Count', 'headcountGrowthMonth12Date', 'headcountGrowthMonth12Count', 'headcountGrowthMonth13Date', 'headcountGrowthMonth13Count', 'headcountGrowthMonth14Date', 'headcountGrowthMonth14Count', 'headcountGrowthMonth15Date', 'headcountGrowthMonth15Count', 'headcountGrowthMonth16Date', 'headcountGrowthMonth16Count', 'headcountGrowthMonth17Date', 'headcountGrowthMonth17Count', 'headcountGrowthMonth18Date', 'headcountGrowthMonth18Count', 'headcountGrowthMonth19Date', 'headcountGrowthMonth19Count', 'headcountGrowthMonth20Date', 'headcountGrowthMonth20Count', 'headcountGrowthMonth21Date', 'headcountGrowthMonth21Count', 'headcountGrowthMonth22Date', 'headcountGrowthMonth22Count', 'headcountGrowthMonth23Date', 'headcountGrowthMonth23Count', 'headcountGrowthMonth24Date', 'headcountGrowthMonth24Count']
        final_list.extend(headcountInsights_column_list)
    if include_headcountByFunction == True:
        headcountByFunction_column_list = ['distributionAccounting', 'distributionAccountingPercentage', 'distributionAdministrative', 'distributionAdministrativePercentage', 'distributionArtsAndDesign', 'distributionArtsAndDesignPercentage', 'distributionBusinessDevelopment', 'distributionBusinessDevelopmentPercentage', 'distributionCommunityAndSocialServices', 'distributionCommunityAndSocialServicesPercentage', 'distributionConsulting', 'distributionConsultingPercentage', 'distributionEducation', 'distributionEducationPercentage', 'distributionEngineering', 'distributionEngineeringPercentage', 'distributionEntrepreneurship', 'distributionEntrepreneurshipPercentage', 'distributionFinance', 'distributionFinancePercentage', 'distributionHealthcareServices', 'distributionHealthcareServicesPercentage', 'distributionHumanResources', 'distributionHumanResourcesPercentage', 'distributionInformationTechnology', 'distributionInformationTechnologyPercentage', 'distributionLegal', 'distributionLegalPercentage', 'distributionMarketing', 'distributionMarketingPercentage', 'distributionMediaAndCommunication', 'distributionMediaAndCommunicationPercentage', 'distributionMilitaryAndProtectiveServices', 'distributionMilitaryAndProtectiveServicesPercentage', 'distributionOperations', 'distributionOperationsPercentage', 'distributionProductManagement', 'distributionProductManagementPercentage', 'distributionProgramAndProjectManagement', 'distributionProgramAndProjectManagementPercentage', 'distributionPurchasing', 'distributionPurchasingPercentage', 'distributionQualityAssurance', 'distributionQualityAssurancePercentage', 'distributionRealEstate', 'distributionRealEstatePercentage', 'distributionResearch', 'distributionResearchPercentage', 'distributionSales', 'distributionSalesPercentage', 'distributionSupport', 'distributionSupportPercentage']
        final_list.extend(headcountByFunction_column_list)
    if include_headcountGrowthByFunction == True:
        headcountGrowthByFunction_column_list = ['growth6MthAccounting', 'growth1YrAccounting', 'growth6MthAdministrative', 'growth1YrAdministrative', 'growth6MthArtsAndDesign', 'growth1YrArtsAndDesign', 'growth6MthBusinessDevelopment', 'growth1YrBusinessDevelopment', 'growth6MthCommunityAndSocialServices', 'growth1YrCommunityAndSocialServices', 'growth6MthConsulting', 'growth1YrConsulting', 'growth6MthEducation', 'growth1YrEducation', 'growth6MthEngineering', 'growth1YrEngineering', 'growth6MthEntrepreneurship', 'growth1YrEntrepreneurship', 'growth6MthFinance', 'growth1YrFinance', 'growth6MthHealthcareServices', 'growth1YrHealthcareServices', 'growth6MthHumanResources', 'growth1YrHumanResources', 'growth6MthInformationTechnology', 'growth1YrInformationTechnology', 'growth6MthLegal', 'growth1YrLegal', 'growth6MthMarketing', 'growth1YrMarketing', 'growth6MthMediaAndCommunication', 'growth1YrMediaAndCommunication', 'growth6MthMilitaryAndProtectiveServices', 'growth1YrMilitaryAndProtectiveServices', 'growth6MthOperations', 'growth1YrOperations', 'growth6MthProductManagement', 'growth1YrProductManagement', 'growth6MthProgramAndProjectManagement', 'growth1YrProgramAndProjectManagement', 'growth6MthPurchasing', 'growth1YrPurchasing', 'growth6MthQualityAssurance', 'growth1YrQualityAssurance', 'growth6MthRealEstate', 'growth1YrRealEstate', 'growth6MthResearch', 'growth1YrResearch', 'growth6MthSales', 'growth1YrSales', 'growth6MthSupport', 'growth1YrSupport']
        final_list.extend(headcountGrowthByFunction_column_list)
    if include_jobOpeningsByFunction == True:
        jobOpeningsByFunction_column_list = ['distributionAccountingJobs', 'distributionAccountingPercentageJobs', 'distributionAdministrativeJobs', 'distributionAdministrativePercentageJobs', 'distributionArtsAndDesignJobs', 'distributionArtsAndDesignPercentageJobs', 'distributionBusinessDevelopmentJobs', 'distributionBusinessDevelopmentPercentageJobs', 'distributionCommunityAndSocialServicesJobs', 'distributionCommunityAndSocialServicesPercentageJobs', 'distributionConsultingJobs', 'distributionConsultingPercentageJobs', 'distributionEducationJobs', 'distributionEducationPercentageJobs', 'distributionEngineeringJobs', 'distributionEngineeringPercentageJobs', 'distributionEntrepreneurshipJobs', 'distributionEntrepreneurshipPercentageJobs', 'distributionFinanceJobs', 'distributionFinancePercentageJobs', 'distributionHealthcareServicesJobs', 'distributionHealthcareServicesPercentageJobs', 'distributionHumanResourcesJobs', 'distributionHumanResourcesPercentageJobs', 'distributionInformationTechnologyJobs', 'distributionInformationTechnologyPercentageJobs', 'distributionLegalJobs', 'distributionLegalPercentageJobs', 'distributionMarketingJobs', 'distributionMarketingPercentageJobs', 'distributionMediaAndCommunicationJobs', 'distributionMediaAndCommunicationPercentageJobs', 'distributionMilitaryAndProtectiveServicesJobs', 'distributionMilitaryAndProtectiveServicesPercentageJobs', 'distributionOperationsJobs', 'distributionOperationsPercentageJobs', 'distributionProductManagementJobs', 'distributionProductManagementPercentageJobs', 'distributionProgramAndProjectManagementJobs', 'distributionProgramAndProjectManagementPercentageJobs', 'distributionPurchasingJobs', 'distributionPurchasingPercentageJobs', 'distributionQualityAssuranceJobs', 'distributionQualityAssurancePercentageJobs', 'distributionRealEstateJobs', 'distributionRealEstatePercentageJobs', 'distributionResearchJobs', 'distributionResearchPercentageJobs', 'distributionSalesJobs', 'distributionSalesPercentageJobs', 'distributionSupportJobs', 'distributionSupportPercentageJobs']
        final_list.extend(jobOpeningsByFunction_column_list)
    if include_jobOpeningsGrowthByFunction == True:
        jobOpeningsGrowthByFunction_column_list = ['growth3MthAccountingJobs', 'growth6MthAccountingJobs', 'growth1YrAccountingJobs', 'growth3MthAdministrativeJobs', 'growth6MthAdministrativeJobs', 'growth1YrAdministrativeJobs', 'growth3MthArtsAndDesignJobs', 'growth6MthArtsAndDesignJobs', 'growth1YrArtsAndDesignJobs', 'growth3MthBusinessDevelopmentJobs', 'growth6MthBusinessDevelopmentJobs', 'growth1YrBusinessDevelopmentJobs', 'growth3MthCommunityAndSocialServicesJobs', 'growth6MthCommunityAndSocialServicesJobs', 'growth1YrCommunityAndSocialServicesJobs', 'growth3MthConsultingJobs', 'growth6MthConsultingJobs', 'growth1YrConsultingJobs', 'growth3MthEducationJobs', 'growth6MthEducationJobs', 'growth1YrEducationJobs', 'growth3MthEngineeringJobs', 'growth6MthEngineeringJobs', 'growth1YrEngineeringJobs', 'growth3MthEntrepreneurshipJobs', 'growth6MthEntrepreneurshipJobs', 'growth1YrEntrepreneurshipJobs', 'growth3MthFinanceJobs', 'growth6MthFinanceJobs', 'growth1YrFinanceJobs', 'growth3MthHealthcareServicesJobs', 'growth6MthHealthcareServicesJobs', 'growth1YrHealthcareServicesJobs', 'growth3MthHumanResourcesJobs', 'growth6MthHumanResourcesJobs', 'growth1YrHumanResourcesJobs', 'growth3MthInformationTechnologyJobs', 'growth6MthInformationTechnologyJobs', 'growth1YrInformationTechnologyJobs', 'growth3MthLegalJobs', 'growth6MthLegalJobs', 'growth1YrLegalJobs', 'growth3MthMarketingJobs', 'growth6MthMarketingJobs', 'growth1YrMarketingJobs', 'growth3MthMediaAndCommunicationJobs', 'growth6MthMediaAndCommunicationJobs', 'growth1YrMediaAndCommunicationJobs', 'growth3MthMilitaryAndProtectiveServicesJobs', 'growth6MthMilitaryAndProtectiveServicesJobs', 'growth1YrMilitaryAndProtectiveServicesJobs', 'growth3MthOperationsJobs', 'growth6MthOperationsJobs', 'growth1YrOperationsJobs', 'growth3MthProductManagementJobs', 'growth6MthProductManagementJobs', 'growth1YrProductManagementJobs', 'growth3MthProgramAndProjectManagementJobs', 'growth6MthProgramAndProjectManagementJobs', 'growth1YrProgramAndProjectManagementJobs', 'growth3MthPurchasingJobs', 'growth6MthPurchasingJobs', 'growth1YrPurchasingJobs', 'growth3MthQualityAssuranceJobs', 'growth6MthQualityAssuranceJobs', 'growth1YrQualityAssuranceJobs', 'growth3MthRealEstateJobs', 'growth6MthRealEstateJobs', 'growth1YrRealEstateJobs', 'growth3MthResearchJobs', 'growth6MthResearchJobs', 'growth1YrResearchJobs', 'growth3MthSalesJobs', 'growth6MthSalesJobs', 'growth1YrSalesJobs', 'growth3MthSupportJobs', 'growth6MthSupportJobs', 'growth1YrSupportJobs']
        final_list.extend(jobOpeningsGrowthByFunction_column_list)
    if include_hiresInsights == True:
        hiresInsights_column_list = ['totalNumberOfSeniorHires', 'hireAllCountMonth0Date', 'hireAllCountMonth0', 'hireSeniorCountMonth0Date', 'hireSeniorCountMonth0', 'hireAllCountMonth1Date', 'hireAllCountMonth1', 'hireSeniorCountMonth1Date', 'hireSeniorCountMonth1', 'hireAllCountMonth2Date', 'hireAllCountMonth2', 'hireSeniorCountMonth2Date', 'hireSeniorCountMonth2', 'hireAllCountMonth3Date', 'hireAllCountMonth3', 'hireSeniorCountMonth3Date', 'hireSeniorCountMonth3', 'hireAllCountMonth4Date', 'hireAllCountMonth4', 'hireSeniorCountMonth4Date', 'hireSeniorCountMonth4', 'hireAllCountMonth5Date', 'hireAllCountMonth5', 'hireSeniorCountMonth5Date', 'hireSeniorCountMonth5', 'hireAllCountMonth6Date', 'hireAllCountMonth6', 'hireSeniorCountMonth6Date', 'hireSeniorCountMonth6', 'hireAllCountMonth7Date', 'hireAllCountMonth7', 'hireSeniorCountMonth7Date', 'hireSeniorCountMonth7', 'hireAllCountMonth8Date', 'hireAllCountMonth8', 'hireSeniorCountMonth8Date', 'hireSeniorCountMonth8', 'hireAllCountMonth9Date', 'hireAllCountMonth9', 'hireSeniorCountMonth9Date', 'hireSeniorCountMonth9', 'hireAllCountMonth10Date', 'hireAllCountMonth10', 'hireSeniorCountMonth10Date', 'hireSeniorCountMonth10', 'hireAllCountMonth11Date', 'hireAllCountMonth11', 'hireSeniorCountMonth11Date', 'hireSeniorCountMonth11', 'hireAllCountMonth12Date', 'hireAllCountMonth12', 'hireSeniorCountMonth12Date', 'hireSeniorCountMonth12', 'hireAllCountMonth13Date', 'hireAllCountMonth13', 'hireSeniorCountMonth13Date', 'hireSeniorCountMonth13', 'hireAllCountMonth14Date', 'hireAllCountMonth14', 'hireSeniorCountMonth14Date', 'hireSeniorCountMonth14', 'hireAllCountMonth15Date', 'hireAllCountMonth15', 'hireSeniorCountMonth15Date', 'hireSeniorCountMonth15', 'hireAllCountMonth16Date', 'hireAllCountMonth16', 'hireSeniorCountMonth16Date', 'hireSeniorCountMonth16', 'hireAllCountMonth17Date', 'hireAllCountMonth17', 'hireSeniorCountMonth17Date', 'hireSeniorCountMonth17', 'hireAllCountMonth18Date', 'hireAllCountMonth18', 'hireSeniorCountMonth18Date', 'hireSeniorCountMonth18', 'hireAllCountMonth19Date', 'hireAllCountMonth19', 'hireSeniorCountMonth19Date', 'hireSeniorCountMonth19', 'hireAllCountMonth20Date', 'hireAllCountMonth20', 'hireSeniorCountMonth20Date', 'hireSeniorCountMonth20', 'hireAllCountMonth21Date', 'hireAllCountMonth21', 'hireSeniorCountMonth21Date', 'hireSeniorCountMonth21', 'hireAllCountMonth22Date', 'hireAllCountMonth22', 'hireSeniorCountMonth22Date', 'hireSeniorCountMonth22', 'hireAllCountMonth23Date', 'hireAllCountMonth23', 'hireSeniorCountMonth23Date', 'hireSeniorCountMonth23', 'hireAllCountMonth24Date', 'hireAllCountMonth24', 'hireSeniorCountMonth24Date', 'hireSeniorCountMonth24', 'seniorHires1_title', 'seniorHires1_linkedInUrl', 'seniorHires1_fullName', 'seniorHires1_date', 'seniorHires2_title', 'seniorHires2_linkedInUrl', 'seniorHires2_fullName', 'seniorHires2_date', 'seniorHires3_title', 'seniorHires3_linkedInUrl', 'seniorHires3_fullName', 'seniorHires3_date']
        final_list.extend(hiresInsights_column_list)
    if include_alumniInsights == True:
        alumniInsights_column_list = ['alumni1_currentTitle', 'alumni1_linkedInUrl', 'alumni1_fullName', 'alumni1_exitDate', 'alumni1_exitTitle', 'alumni2_currentTitle', 'alumni2_linkedInUrl', 'alumni2_fullName', 'alumni2_exitDate', 'alumni2_exitTitle', 'alumni3_currentTitle', 'alumni3_linkedInUrl', 'alumni3_fullName', 'alumni3_exitDate', 'alumni3_exitTitle']
        final_list.extend(alumniInsights_column_list)
    missing_columns = set(final_list) - set(df_final.columns)
    if missing_columns:
        df_final = pd.concat([df_final, pd.DataFrame(columns=list(missing_columns))], axis=1)
    df_final = df_final[final_list]
    return df_final
def linkedin_lead(csrf_token, dataframe, column_name, progress_bar, cookies_dict):
    """
    Retrieves LinkedIn lead information based on a DataFrame of search queries.

    Args:
        csrf_token (str): CSRF token for authentication.
        dataframe (pd.DataFrame): DataFrame containing search queries.
        column_name (str): Column name in the DataFrame with search query URLs.
        cookies_dict (dict): Dictionary of cookies for the session.

    Returns:
        pd.DataFrame: A DataFrame containing the retrieved lead information.
    """
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def get_profile(data):
        if not data or "status" in data and data["status"] != 200:
            return {}
        profile = data["profile"]
        if "miniProfile" in profile:
            if "picture" in profile["miniProfile"]:
                profile["displayPictureUrl"] = profile["miniProfile"]["picture"][
                    "com.linkedin.common.VectorImage"
                ]["rootUrl"]
                images_data = profile["miniProfile"]["picture"][
                    "com.linkedin.common.VectorImage"
                ]["artifacts"]
                for img in images_data:
                    w, h, url_segment = itemgetter(
                        "width", "height", "fileIdentifyingUrlPathSegment"
                    )(img)
                    profile[f"img_{w}_{h}"] = url_segment
            profile["profile_id"] = profile["miniProfile"]["entityUrn"].split(":")[3]
            profile["profile_urn"] = profile["miniProfile"]["entityUrn"]
            profile["member_urn"] = profile["miniProfile"]["objectUrn"]
            profile["public_id"] = profile["miniProfile"]["publicIdentifier"]
            del profile["miniProfile"]
        del profile["defaultLocale"]
        del profile["supportedLocales"]
        del profile["versionTag"]
        del profile["showEducationOnProfileTopCard"]
        experience = data["positionView"]["elements"]
        for item in experience:
            if "company" in item and "miniCompany" in item["company"]:
                if "logo" in item["company"]["miniCompany"]:
                    logo = item["company"]["miniCompany"]["logo"].get(
                        "com.linkedin.common.VectorImage"
                    )
                    if logo:
                        item["companyLogoUrl"] = logo["rootUrl"]
                del item["company"]["miniCompany"]
        profile["experience"] = experience
        education = data["educationView"]["elements"]
        for item in education:
            if "school" in item:
                if "logo" in item["school"]:
                    item["school"]["logoUrl"] = item["school"]["logo"][
                        "com.linkedin.common.VectorImage"
                    ]["rootUrl"]
                    del item["school"]["logo"]
        profile["education"] = education
        languages = data["languageView"]["elements"]
        for item in languages:
            del item["entityUrn"]
        profile["languages"] = languages
        publications = data["publicationView"]["elements"]
        for item in publications:
            del item["entityUrn"]
            for author in item.get("authors", []):
                del author["entityUrn"]
        profile["publications"] = publications
        certifications = data["certificationView"]["elements"]
        for item in certifications:
            del item["entityUrn"]
        profile["certifications"] = certifications
        volunteer = data["volunteerExperienceView"]["elements"]
        for item in volunteer:
            del item["entityUrn"]
        profile["volunteer"] = volunteer
        honors = data["honorView"]["elements"]
        for item in honors:
            del item["entityUrn"]
        profile["honors"] = honors
        projects = data["projectView"]["elements"]
        for item in projects:
            del item["entityUrn"]
        profile["projects"] = projects
        return profile
    def get_profile_contact_info(data):
        contact_info = {
            "email_address": data.get("emailAddress"),
            "websites": [],
            "twitter": data.get("twitterHandles"),
            "birthdate": data.get("birthDateOn"),
            "ims": data.get("ims"),
            "phone_numbers": data.get("phoneNumbers", []),
        }
        websites = data.get("websites", [])
        for item in websites:
            if "com.linkedin.voyager.identity.profile.StandardWebsite" in item["type"]:
                item["label"] = item["type"]["com.linkedin.voyager.identity.profile.StandardWebsite"]["category"]
            elif "" in item["type"]:
                item["label"] = item["type"]["com.linkedin.voyager.identity.profile.CustomWebsite"]["label"]
            del item["type"]
        contact_info["websites"] = websites
        return contact_info
    def get_profile_skills(data):
        skills = data.get("elements", [])
        for item in skills:
            del item["entityUrn"]
        return skills
    def get_profile_network_info(data):
        return data.get("data", data)
    def generate_columns(count, patterns):
        return [pattern.format(i+1) for i in range(count) for pattern in patterns]
    MAX_COUNTS = {'experience': 5, 'education': 3, 'language': 10, 'certification': 10, 'volunteer': 3, 'honor': 10}
    COLUMN_PATTERNS = {
        'experience': [
            'experience{}_company', 'experience{}_companyId', 'experience{}_companyUrl',
            'experience{}_jobTitle', 'experience{}_jobLocation', 'experience{}_jobDescription',
            'experience{}_jobDateRange', 'experience{}_jobDuration', 'experience{}_companyIndustry',
            'experience{}_companySize'
        ],
        'education': [
            'education{}_school', 'education{}_schoolId', 'education{}_schoolUrl',
            'education{}_schoolDegree', 'education{}_schoolFieldOfStudy', 'education{}_schoolDescription',
            'education{}_schoolDateRange', 'education{}_schoolDuration', 'education{}_schoolActive'
        ],
        'language': [
            'language{}_name', 'language{}_proficiency'
        ],
        'certification': [
            'certification{}_name', 'certification{}_dateRange', 'certification{}_duration',
            'certification{}_url', 'certification{}_companyName', 'certification{}_companyId',
            'certification{}_companyUrl', 'certification{}_universalName', 'certification{}_logo100x100',
            'certification{}_logo200x200', 'certification{}_logo400x400', 'certification{}_showcase',
            'certification{}_companyActive'
        ],
        'volunteer': [
            'volunteer{}_role', 'volunteer{}_companyName', 'volunteer{}_dateRange',
            'volunteer{}_duration', 'volunteer{}_description', 'volunteer{}_cause'
        ],
        'honor': [
            'honor{}_title', 'honor{}_issuer', 'honor{}_date'
        ]
    }
    counts = {'experience': 5, 'education': 3, 'language': 10, 'certification': 10, 'volunteer': 3, 'honor': 10}
    columns_lists = {}
    for key, max_count in MAX_COUNTS.items():
        count = counts[key]
        if count > max_count:
            raise ValueError(f"{key.capitalize()} count exceeds {max_count}. Change the number and execute the script again.")
        columns_lists[key] = generate_columns(count, COLUMN_PATTERNS[key])
    def create_linkedinProfileUrl(universalName):
        return f"https://www.linkedin.com/in/{universalName}/" if universalName else None
    def create_linkedinProfile(vmid):
        return f"https://www.linkedin.com/in/{vmid}/" if vmid else None
    def extract_userId(member_urn, pattern):
        if member_urn:
            match = pattern.search(member_urn)
            return match.group(1) if match else None
        return None
    def create_linkedinSalesNavigatorUrl(vmid):
        return f"https://www.linkedin.com/sales/people/{vmid},name" if vmid else None
    def create_picture_urls(base_url, img_suffix):
        return f"{base_url}{img_suffix}" if base_url and img_suffix else None
    def create_birthdate(birthdateDay, birthdateMonth):
        return f"{birthdateMonth} {birthdateDay}" if birthdateDay and birthdateMonth else None
    def create_twitter(twitterUsername):
        return f"https://twitter.com/{twitterUsername}/" if twitterUsername else None
    def extract_industryCode(industryUrn, pattern):
        if industryUrn:
            match = pattern.search(industryUrn)
            return match.group(1) if match else None
        return None
    def extract_company_id(company_urn):
        match = re.search(r"urn:li:(fs_miniCompany|company):(.+)", company_urn or '')
        return match.group(2) if match else None
    def create_company_url(company_id):
        return f"https://www.linkedin.com/company/{company_id}/" if company_id else None
    def date_range_str(start_date, end_date):
        if not start_date.get('month') or not start_date.get('year'):
            return None
        start_str = f"{months[start_date.get('month')]} {start_date.get('year')}"
        end_str = f"{months[end_date.get('month')]} {end_date.get('year')}" if end_date and end_date.get('month') and end_date.get('year') else "Present"
        return f"{start_str} - {end_str}"
    def date_diff_in_years_format(start_date, end_date=None):
        if not start_date.get('month') or not start_date.get('year'):
            return None
        start = datetime(start_date['year'], start_date['month'], 1)
        end = datetime.now() if not end_date or not end_date.get('year') or not end_date.get('month') else datetime(end_date['year'], end_date['month'], 1)
        delta = end - start
        years, remainder = divmod(delta.days, 365.25)
        months = round(remainder / 30.44)
        if months == 12:
            years += 1
            months = 0
        year_str = f"{int(years)} yr" if years == 1 else f"{int(years)} yrs" if years > 1 else ""
        month_str = f"{months} mo" if months == 1 else f"{months} mos" if months > 1 else ""
        return f"{year_str} {month_str}".strip()
    def flattened_experience(profile):
        experiences = profile.get('experience', [])
        flattened_data = {
            f"experience{idx+1}_{key}": value
            for idx, experience in enumerate(experiences)
            for key, value in {
                "company": experience.get('companyName'),
                "companyId": extract_company_id(experience.get('companyUrn')),
                "companyUrl": create_company_url(extract_company_id(experience.get('companyUrn'))),
                "jobTitle": experience.get('title'),
                "jobLocation": experience.get('locationName'),
                "jobDescription": experience.get('description'),
                "jobDateRange": date_range_str(experience.get('timePeriod', {}).get('startDate', {}), experience.get('timePeriod', {}).get('endDate')),
                "jobDuration": date_diff_in_years_format(experience.get('timePeriod', {}).get('startDate', {}), experience.get('timePeriod', {}).get('endDate')),
                "companyIndustry": ', '.join(experience.get('company', {}).get('industries', [])),
                "companySize": "-".join([str(val) for val in experience.get('company', {}).get('employeeCountRange', {}).values() if val])
            }.items()
        }
        return flattened_data
    def extract_school_id(school_urn):
        match = re.search(r"urn:li:school:(.+)", school_urn or '')
        return match.group(1) if match else None
    def create_school_url(school_id):
        return f"https://www.linkedin.com/school/{school_id}/" if school_id else None
    def flattened_education(profile):
        educations = profile.get('education', [])
        flattened_data = {
            f"education{idx+1}_{key}": value
            for idx, education in enumerate(educations)
            for key, value in {
                "school": education.get('schoolName'),
                "schoolId": extract_school_id(education.get('schoolUrn')),
                "schoolUrl": create_school_url(extract_school_id(education.get('schoolUrn'))),
                "schoolDegree": education.get('degreeName'),
                "schoolFieldOfStudy": education.get('fieldOfStudy'),
                "schoolDescription": education.get('description'),
                "schoolDateRange": date_range_str(education.get('timePeriod', {}).get('startDate', {}), education.get('timePeriod', {}).get('endDate')),
                "schoolDuration": date_diff_in_years_format(education.get('timePeriod', {}).get('startDate', {}), education.get('timePeriod', {}).get('endDate')),
                "schoolActive": education.get('active', False)
            }.items()
        }
        return flattened_data
    def flattened_project(profile):
        projects = profile.get('projects', [])
        flattened_data = {
            f"project{idx+1}_{key}": value
            for idx, project in enumerate(projects)
            for key, value in {
                "title": project.get('title'),
                "description": project.get('description'),
                "dateRange": date_range_str(project.get('timePeriod', {}).get('startDate', {}), project.get('timePeriod', {}).get('endDate', {})),
                "duration": date_diff_in_years_format(project.get('timePeriod', {}).get('startDate', {}), project.get('timePeriod', {}).get('endDate', {})),
            }.items()
        }
        return flattened_data
    def flattened_languages(profile):
        languages = profile.get('languages', [])
        flattened_data = {
            f"language{idx+1}_{key}": value
            for idx, language in enumerate(languages)
            for key, value in {
                "name": language.get('name'),
                "proficiency": language.get('proficiency'),
            }.items()
        }
        return flattened_data
    def get_logo_url(artifacts, root_url, desired_position=None):
        if not artifacts or not root_url:
            return None
        valid_artifacts = [artifact for artifact in artifacts if artifact.get('width') is not None]
        if not valid_artifacts:
            return None
        sorted_artifacts = sorted(valid_artifacts, key=lambda x: x.get('width'))
        position_map = {
            'lowest': 0,
            'middle': len(sorted_artifacts) // 2,
            'highest': -1
        }
        desired_position_index = position_map.get(desired_position)
        if desired_position_index is None:
            return None
        desired_artifact = sorted_artifacts[desired_position_index]
        return root_url + desired_artifact.get('fileIdentifyingUrlPathSegment')
    def flattened_certification(profile):
        certifications = profile.get('certifications', [])
        flattened_data = {
            f"certification{idx+1}_{key}": value
            for idx, certification in enumerate(certifications)
            for key, value in {
                "name": certification.get('name'),
                "dateRange": date_range_str(certification.get('timePeriod', {}).get('startDate', {}), certification.get('timePeriod', {}).get('endDate')),
                "duration": date_diff_in_years_format(certification.get('timePeriod', {}).get('startDate', {}), certification.get('timePeriod', {}).get('endDate')),
                "url": certification.get('url'),
                "companyName": certification.get('company', {}).get('name'),
                "companyId": extract_company_id(certification.get('company', {}).get('objectUrn')),
                "companyUrl": create_company_url(extract_company_id(certification.get('company', {}).get('objectUrn'))),
                "universalName": certification.get('company', {}).get('universalName'),
                "logo100x100": get_logo_url(certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('artifacts'),
                                            certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('rootUrl'), 'lowest'),
                "logo200x200": get_logo_url(certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('artifacts'),
                                            certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('rootUrl'), 'middle'),
                "logo400x400": get_logo_url(certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('artifacts'),
                                            certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('rootUrl'), 'highest'),
                "showcase": certification.get('company', {}).get('showcase'),
                "companyActive": certification.get('company', {}).get('active')
            }.items()
        }
        return flattened_data
    def flattened_volunteer(profile):
        volunteers = profile.get('volunteer', [])
        flattened_data = {
            f"volunteer{idx+1}_{key}": value
            for idx, volunteer in enumerate(volunteers)
            for key, value in {
                "role": volunteer.get('role'),
                "companyName": volunteer.get('companyName'),
                "dateRange": date_range_str(volunteer.get('timePeriod', {}).get('startDate', {}), volunteer.get('timePeriod', {}).get('endDate')),
                "duration": date_diff_in_years_format(volunteer.get('timePeriod', {}).get('startDate', {}), volunteer.get('timePeriod', {}).get('endDate')),
                "description": volunteer.get('description'),
                "cause": volunteer.get('cause')
            }.items()
        }
        return flattened_data
    def flattened_honor(profile):
        honors = profile.get('honors', [])
        flattened_data = {
            f"honor{idx+1}_{key}": value
            for idx, honor in enumerate(honors)
            for key, value in {
                "title": honor.get('title'),
                "issuer": honor.get('issuer'),
                "date": f"{months[honor.get('issueDate', {}).get('month', 0)]} {honor.get('issueDate', {}).get('year', '')}" if honor.get('issueDate') else None
            }.items() if value is not None
        }
        return flattened_data
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(3)
        except AttributeError:
            return None
    pattern_universalName = re.compile(r"https?://([\w]+\.)?linkedin\.com/(in|sales/lead|sales/people)/([A-z0-9\._\%&'-]+)/?")
    df_final = pd.DataFrame()
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    print("LinkedIn lead scrape")
    num_rows = len(columnName_values)


    for index, profile in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        df_loop_base = pd.DataFrame()
        df_loop_websites = pd.DataFrame()
        df_loop_experience = pd.DataFrame()
        df_loop_education = pd.DataFrame()
        df_loop_project = pd.DataFrame()
        df_loop_languages = pd.DataFrame()
        df_loop_certifications = pd.DataFrame()
        df_loop_volunteers = pd.DataFrame()
        df_loop_honors = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        error = None
        if wordToSearch is None:
            error = "Invalid LinkedIn URL"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_websites, df_loop_experience, df_loop_education, df_loop_project, df_loop_languages, df_loop_certifications, df_loop_volunteers, df_loop_honors], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.progress((index) / num_rows)
            continue
        try:
            request_url_profileView = f'https://www.linkedin.com/voyager/api/identity/profiles/{urllib.parse.unquote(wordToSearch)}/profileView'
            response_profileView = requests.get(url=request_url_profileView, cookies=cookies_dict, headers=headers)
            response_profileView_json = response_profileView.json()
            profile = get_profile(response_profileView_json)
        except KeyError as e:
            profile = {}
            error = "LinkedIn down"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_websites, df_loop_experience, df_loop_education, df_loop_project, df_loop_languages, df_loop_certifications, df_loop_volunteers, df_loop_honors], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.progress((index) / num_rows)
            continue
        except Exception as e:
            profile = {}
            error = str(e)
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_websites, df_loop_experience, df_loop_education, df_loop_project, df_loop_languages, df_loop_certifications, df_loop_volunteers, df_loop_honors], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.progress((index) / num_rows)
            continue
        #-->network_info
        try:
            request_url_networkinfo = f'https://www.linkedin.com/voyager/api/identity/profiles/{urllib.parse.unquote(wordToSearch)}/networkinfo'
            response_networkinfo = requests.get(url=request_url_networkinfo, cookies=cookies_dict, headers=headers)
            response_networkinfo_json = response_networkinfo.json()
            profile["network_info"] = get_profile_network_info(response_networkinfo_json) or {}
        except Exception as e:
            profile["network_info"] = {}
        #-->contact_info
        try:
            request_url_profileContactInfo = f'https://www.linkedin.com/voyager/api/identity/profiles/{urllib.parse.unquote(wordToSearch)}/profileContactInfo'
            response_profileContactInfo = requests.get(url=request_url_profileContactInfo, cookies=cookies_dict, headers=headers)
            response_profileContactInfo_json = response_profileContactInfo.json()
            profile["contact_info"] = get_profile_contact_info(response_profileContactInfo_json) or {}
        except Exception as e:
            profile["contact_info"] = {}
        #-->skills_info
        try:
            request_url_skills = 'https://www.linkedin.com/voyager/api/identity/profiles/zainkahn/skills'
            response_skills = requests.get(url=request_url_skills, cookies=cookies_dict, headers=headers)
            response_skills_json = response_skills.json()
            profile["skills_info"] = get_profile_skills(response_skills_json) or {}
        except Exception as e:
            profile["skills_info"] = {}
        #-->universalName
        universalName = profile.get('public_id', None)
        #-->linkedinProfileUrl
        linkedinProfileUrl = create_linkedinProfileUrl(universalName)
        #-->description
        description = profile.get('summary', None)
        #-->headline
        headline = profile.get('headline', None)
        #-->location
        location = profile.get('geoLocationName', None)
        #-->country
        country = profile.get('geoCountryName', None)
        #-->firstName
        firstName = profile.get('firstName', None)
        #-->lastName
        lastName = profile.get('lastName', None)
        #-->fullName
        fullName = ' '.join([name for name in (firstName, lastName) if name is not None]).strip()
        #-->subscribers
        subscribers = profile.get("network_info", {}).get('followersCount', None)
        #-->connectionDegree
        connectionDegree = profile.get("network_info", {}).get("distance", {}).get("value", None)
        connectionDegree_mapping = {"DISTANCE_3": "3rd", "DISTANCE_2": "2nd", "DISTANCE_1": "1st", "OUT_OF_NETWORK": "Out of network"}
        connectionDegree = connectionDegree_mapping.get(connectionDegree, connectionDegree)
        #-->vmid
        vmid = profile.get('profile_id', None)
        #-->linkedinProfile
        linkedinProfile = create_linkedinProfile(vmid)
        #-->userId
        pattern_userId = re.compile(r"urn:li:member:(.+)")
        userId = extract_userId(profile.get('member_urn', None), pattern_userId)
        #-->linkedinSalesNavigatorUrl
        linkedinSalesNavigatorUrl = create_linkedinSalesNavigatorUrl(vmid)
        #-->connectionsCount
        connectionsCount = profile.get("network_info", {}).get('connectionsCount', None)
        #-->picture100x100, picture200x200, picture400x400, picture800x800
        img_sizes = ['100_100', '200_200', '400_400', '800_800']
        displayPictureUrl = profile.get('displayPictureUrl', None)
        picture_urls = {}
        for size in img_sizes:
            img_suffix = profile.get(f'img_{size}', None)
            picture_urls[size] = create_picture_urls(displayPictureUrl, img_suffix)
        picture100x100 = picture_urls['100_100']
        picture200x200 = picture_urls['200_200']
        picture400x400 = picture_urls['400_400']
        picture800x800 = picture_urls['800_800']
        #-->birthdate
        months = [None, 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        birthdateDay = profile.get("contact_info", {}).get("birthdate", {}).get('day', None) if isinstance(profile.get("contact_info", {}).get("birthdate"), dict) else None
        birthdateMonthNumber = profile.get("contact_info", {}).get("birthdate", {}).get('month', None) if isinstance(profile.get("contact_info", {}).get("birthdate"), dict) else None
        birthdateMonth = months[birthdateMonthNumber] if birthdateMonthNumber else None
        birthdate = create_birthdate(birthdateDay, birthdateMonth)
        #-->emailAddress
        emailAddress = profile.get("contact_info", {}).get("email_address", None)
        #-->twitter
        twitterUsername = next((t.get('name') for t in (profile.get("contact_info", {}).get("twitter") or []) if 'name' in t), None)
        twitter = create_twitter(twitterUsername)
        #-->phoneNumberType and phoneNumberValue
        phone_data = next((pn for pn in profile.get("contact_info", {}).get("phone_numbers", []) if 'number' in pn), {})
        phoneNumberType = phone_data.get("type", None)
        phoneNumberValue = phone_data.get("number", None)
        #-->industryCode
        industryCode_pattern = re.compile(r"urn:li:fs_industry:(.+)")
        industryCode = extract_industryCode(profile.get('industryUrn', None), industryCode_pattern)
        #-->industry
        industry = profile.get('industryName', None)
        #-->isStudent
        isStudent = profile.get('student', None)
        #-->df_loop_experience
        df_loop_experience = pd.DataFrame([flattened_experience(profile)])
        #-->df_loop_education
        df_loop_education = pd.DataFrame([flattened_education(profile)])
        #-->df_loop_project
        df_loop_project = pd.DataFrame([flattened_project(profile)])
        #-->df_loop_languages
        df_loop_languages = pd.DataFrame([flattened_languages(profile)])
        #-->df_loop_certifications
        df_loop_certifications = pd.DataFrame([flattened_certification(profile)])
        #-->df_loop_volunteers
        df_loop_volunteers = pd.DataFrame([flattened_volunteer(profile)])
        #-->df_loop_honors
        df_loop_honors = pd.DataFrame([flattened_honor(profile)])
        #-->allSkills
        skills = profile["skills_info"]
        allSkills = ", ".join([skill["name"] for skill in skills])
        #-->timestamp
        current_timestamp = datetime.now()
        timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        all_variables = locals()
        selected_vars = {var: [all_variables[var]] for var in ["universalName", "linkedinProfileUrl", "firstName", "lastName", "fullName", "description", "headline", "location", "country", "subscribers", "connectionDegree", "vmid", "linkedinProfile", "userId", "linkedinSalesNavigatorUrl", "connectionsCount", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "birthdate", "emailAddress", "twitter", "phoneNumberType", "phoneNumberValue", "industryCode", "industry", "isStudent", "allSkills", "timestamp"]}
        df_loop_base = pd.DataFrame(selected_vars)
        error = None
        df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
        df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_websites, df_loop_experience, df_loop_education, df_loop_project, df_loop_languages, df_loop_certifications, df_loop_volunteers, df_loop_honors], axis=1)
        df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.progress((index) / num_rows)

    selected_columns = []
    for columns in columns_lists.values():
        selected_columns.extend(columns)
    final_columns = ["query","error","universalName", "linkedinProfileUrl", "firstName", "lastName", "fullName", "description", "headline", "location", "country", "subscribers", "connectionDegree", "vmid", "linkedinProfile", "userId", "linkedinSalesNavigatorUrl", "connectionsCount", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "birthdate", "emailAddress", "twitter", "phoneNumberType", "phoneNumberValue", "industryCode", "industry", "isStudent", "allSkills", "timestamp"]
    final_columns.extend(selected_columns)
    df_final = df_final.reindex(columns=final_columns)
    df_final = df_final[final_columns]
    return df_final
def company_activity_extractor(csrf_token, dataframe, column_name, cookies_dict):
    """
    Extract and process LinkedIn company activity data from a provided DataFrame containing LinkedIn URLs.

    Args:
        csrf_token (str): The CSRF token for authenticating LinkedIn API requests.
        dataframe (pd.DataFrame): A pandas DataFrame containing LinkedIn company URLs.
        column_name (str): The name of the column in the DataFrame containing the LinkedIn URLs.
        cookies_dict (dict): A dictionary containing the cookies required for LinkedIn API requests.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted company activity data, including any errors encountered during the extraction process.
    """
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def create_sharedPostUrl(sharedPostUrl):
        if sharedPostUrl:
            return f"https://www.linkedin.com/feed/update/{sharedPostUrl}/"
        return None
    def extract_sharedJobUrl(sharedJobUrl):
        if sharedJobUrl:
            match = re.search(r'(https://www\.linkedin\.com/jobs/view/\d+)/', sharedJobUrl)
            if match:
                return match.group(1) + "/"
        return None
    def extract_postDate(postDate):
        if postDate:
            match = re.search(r'^(.*?)\s*•', postDate)
        if match:
            return match.group(1).strip()
        return None
    def create_profileUrl(profileUrl):
        if profileUrl:
            return f"https://www.linkedin.com/in/{profileUrl}/"
        return None
    class ForbiddenAccessException(Exception):
        pass
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(1)
        except AttributeError:
            return None
    def fetch_company_updates(company, start=0, pagination_token=None, accumulated_elements=None):
        if accumulated_elements is None:
            accumulated_elements = []
        params = {
            "companyUniversalName": company,
            "q": "companyFeedByUniversalName",
            "moduleKey": "member-share",
            "count": 100,
            "start": start,
        }
        if pagination_token:
            params["paginationToken"] = pagination_token
        request_url = 'https://www.linkedin.com/voyager/api/feed/updates'
        try:
            response = requests.get(url=request_url, params=params, cookies=cookies_dict, headers=headers)
            response.raise_for_status()
            response_json = response.json()
        except requests.HTTPError as http_err:
            if http_err.response.status_code == 403:
                raise ForbiddenAccessException('Access denied. Please check your permissions or authentication tokens.')
            else:
                print(f'HTTP error occurred: {http_err}')
                return accumulated_elements
        except Exception as err:
            print(f'An error occurred: {err}')
            return accumulated_elements
        if 'elements' in response_json and 'paging' in response_json:
            new_elements = response_json.get('elements', [])
            accumulated_elements.extend(new_elements)
            total = response_json['paging'].get('total', 0)
            next_start = start + params["count"]
            if next_start < total:
                pagination_token = response_json.get('metadata', {}).get('paginationToken')
                return fetch_company_updates(company, start=next_start, pagination_token=pagination_token, accumulated_elements=accumulated_elements)
        else:
            print('Unexpected response structure:', response_json)
        return accumulated_elements
    pattern_universalName = re.compile(r"^(?:https?:\/\/)?(?:[\w]+\.)?linkedin\.com\/(?:company|company-beta|school)\/([A-Za-z0-9\._\%&'-]+?)(?:\/|\?|#|$)", re.IGNORECASE)
    df_final = pd.DataFrame()
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    print("LinkedIn company activity extractor")
    progress_bar = tqdm(total = len(columnName_values))
    for index, company in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        df_loop_base = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        error = None
        if wordToSearch is None:
            error = "Invalid LinkedIn URL"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        try:
            company_posts = fetch_company_updates(wordToSearch)
            if not company_posts:
                error = "No activities"
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_final = pd.concat([df_final, df_loop_final])
                index += 1
                progress_bar.update(1)
                continue
        except ForbiddenAccessException as e:
            company_posts = {}
            error = "LinkedIn down"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        except Exception as e:
            company_posts = {}
            error = str(e)
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        #-->DATA MANIPULATION START<--
        for post in company_posts:
            #-->postUrl
            postUrl = safe_extract(post, 'permalink')
            #-->imgUrl
            rootUrl = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.ImageComponent', 'images', 0, 'attributes', 0, 'vectorImage', 'rootUrl')
            fileIdentifyingUrlPathSegment = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.ImageComponent', 'images', 0, 'attributes', 0, 'vectorImage', 'artifacts', 5, 'fileIdentifyingUrlPathSegment')
            imgUrl = None
            if rootUrl and fileIdentifyingUrlPathSegment:
                imgUrl = rootUrl + fileIdentifyingUrlPathSegment
            #-->postContent
            postContent = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'commentary', 'text', 'text')
            #-->postType
            postType = None
            if postContent:
                postType = "Text"
            if imgUrl:
                postType = "Image"
            #-->likeCount
            likeCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numLikes')
            #-->commentCount
            commentCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numComments')
            #-->repostCount
            repostCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numShares')
            #-->postDate
            postDate = extract_postDate(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'actor', 'subDescription', 'text'))
            #-->action
            action = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'header', 'text', 'text')
            if not action:
                action = "Post"
            #-->profileUrl
            profileUrl = create_profileUrl(wordToSearch)
            #-->sharedPostUrl
            sharedPostUrl = create_sharedPostUrl(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'resharedUpdate', 'updateMetadata', 'urn'))
            #-->sharedJobUrl
            sharedJobUrl = extract_sharedJobUrl(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.EntityComponent', 'ctaButton', 'navigationContext', 'actionTarget'))
            #-->isSponsored
            isSponsored = safe_extract(post, 'isSponsored')
            #-->DATA MANIPULATION END<--
            current_timestamp = datetime.now()
            timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["postUrl","imgUrl","postContent","postType","likeCount","commentCount","repostCount","postDate","action","profileUrl","timestamp","sharedPostUrl","sharedJobUrl","isSponsored"]}
            df_loop_base = pd.DataFrame(selected_vars)
            error = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
    progress_bar.close()
    #-->Columns manipulation
    final_columns = ["query","error","postUrl","imgUrl","postContent","postType","likeCount","commentCount","repostCount","postDate","action","profileUrl","timestamp","sharedPostUrl","sharedJobUrl","isSponsored"]
    df_final = df_final.reindex(columns=final_columns, fill_value=None)
    return df_final
def job_offers_extractor(csrf_token, dataframe, column_name, cookies_dict):
    """
    Extracts and processes job offer data from LinkedIn based on a provided DataFrame containing LinkedIn URLs.

    Args:
        csrf_token (str): The CSRF token for authenticating LinkedIn API requests.
        dataframe (pd.DataFrame): A pandas DataFrame containing LinkedIn company URLs.
        column_name (str): The name of the column in the DataFrame containing the LinkedIn URLs.
        cookies_dict (dict): A dictionary containing the cookies required for LinkedIn API requests.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted job offer data, including any errors encountered during the extraction process.
    """
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(1)
        except AttributeError:
            return None
    def extract_main_company_id(company):
        pattern = re.compile(r"urn:li:fs_normalized_company:(.+)")
        entity_urn = company.get('entityUrn', '')
        match = pattern.search(entity_urn)
        return match.group(1) if match else None
    def get_all_job_postings(companyId):
        #-->Get the last 1000 active job postings
        job_postings = []
        start = 0
        count = 100
        has_more = True
        headers['accept'] = 'application/vnd.linkedin.normalized+json+2.1'
        while has_more:
            request_url = f'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollectionLite-61&count={count}&q=jobSearch&query=(origin:COMPANY_PAGE_JOBS_CLUSTER_EXPANSION,locationUnion:(geoId:92000000),selectedFilters:(company:List({companyId}),originToLandingJobPostings:List({companyId})),spellCorrectionEnabled:true)&servedEventEnabled=false&start={start}'
            response = requests.get(url = request_url, cookies = cookies_dict, headers = headers)
            if response.status_code != 200:
                print(f"Failed to fetch job postings: {response.status_code}")
                break
            response_payload = response.json()
            elements = response_payload.get("included", [])
            current_job_postings = [i for i in elements if i["$type"] == 'com.linkedin.voyager.dash.jobs.JobPosting']
            if not current_job_postings:
                has_more = False
            else:
                job_postings.extend(current_job_postings)
                start += count
        if 'accept' in headers:
            del headers['accept']
        return job_postings
    def get_company(data):
        #-->Get company information
        if not data or "elements" not in data or not data["elements"]:
            return {}
        company = data["elements"][0]
        return company
    class ForbiddenAccessException(Exception):
        pass
    pattern_universalName = re.compile(r"^(?:https?:\/\/)?(?:[\w]+\.)?linkedin\.com\/(?:company|company-beta|school)\/([A-Za-z0-9\._\%&'-]+?)(?:\/|\?|#|$)", re.IGNORECASE)
    df_final = pd.DataFrame()
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    print('LinkedIn job offers extractor')
    progress_bar = tqdm(total = len(columnName_values))
    for index, company in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        if wordToSearch:
            wordToSearch = str(wordToSearch)
        error = None
        if wordToSearch is None:
            error = 'Invalid LinkedIn company URL'
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        try:
            int(wordToSearch)
        except ValueError:
            params = {
                "decorationId": "com.linkedin.voyager.deco.organization.web.WebFullCompanyMain-12",
                "q": "universalName",
                "universalName": urllib.parse.unquote(wordToSearch),
            }
            request_url = f'https://www.linkedin.com/voyager/api/organization/companies'
            response = requests.get(url=request_url, cookies=cookies_dict, headers=headers, params=params)
            response_json = response.json()
            company_result = get_company(response_json)
            wordToSearch = extract_main_company_id(company_result)
            if wordToSearch is None:
                error = 'Invalid LinkedIn company URL'
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_final = pd.concat([df_final, df_loop_final])
                index += 1
                progress_bar.update(1)
                continue
        try:
            job_postings = get_all_job_postings(wordToSearch)
            if not job_postings:
                error = 'No job offers'
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_final = pd.concat([df_final, df_loop_final])
                index += 1
                progress_bar.update(1)
                continue
        except ForbiddenAccessException as e:
            job_postings = []
            error = 'Invalid LinkedIn company URL'
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        except Exception as e:
            job_postings = []
            error = str(e)
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        for job_posting in job_postings:
             #-->DATA MANIPULATION START<--
            repostedJob = safe_extract(job_posting, 'repostedJob')
            title = safe_extract(job_posting, 'title')
            posterId = safe_extract(job_posting, 'posterId')
            contentSource = safe_extract(job_posting, 'contentSource')
            entityUrn = safe_extract(job_posting, 'entityUrn')
            try:
                entityUrn = entityUrn.split(':')[-1]
            except:
                pass
            jobUrl = None
            if entityUrn:
                jobUrl = f'https://www.linkedin.com/jobs/search/?currentJobId={entityUrn}'
            #-->DATA MANIPULATION END<--
            current_timestamp = datetime.now()
            timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ['entityUrn', 'jobUrl', 'title', 'posterId', 'contentSource', 'repostedJob', 'timestamp']}
            df_loop_base = pd.DataFrame(selected_vars)
            error = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
    progress_bar.close()
    job_postings_rename_dict = {
        "entityUrn": "jobOfferId",
        "jobUrl": "jobOfferUrl",
        "title": "jobOfferTitle",
    }
    df_final.rename(columns = job_postings_rename_dict, inplace = True)
    #-->Columns manipulation
    final_columns = ['query', 'error', 'jobOfferId', 'jobOfferUrl', 'jobOfferTitle', 'posterId', 'contentSource', 'repostedJob', 'timestamp']
    df_final = df_final.reindex(columns=final_columns, fill_value=None)
    return df_final
def job_offers_details_extractor(csrf_token, dataframe, column_name, cookies_dict):
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def convertToTimestamp(milliseconds):
        if milliseconds:
            return datetime.utcfromtimestamp(milliseconds/1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            None
    def get_job(job_id):
        # -->Get job offer details
        params = {
            "decorationId": "com.linkedin.voyager.deco.jobs.web.shared.WebLightJobPosting-23", }
        request_url = f'https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}'
        response = requests.get(url=request_url, params=params,
                                headers=headers, cookies=cookies_dict)
        if response.status_code != 200:
            print(f"Failed to fetch job offer: {response.status_code}")
            return {}
        data = response.json()
        return data
    class ForbiddenAccessException(Exception):
        pass
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    columnName_values = dataframe[column_name].tolist()
    df_final = pd.DataFrame()
    print('LinkedIn job offers scraping')
    progress_bar = tqdm(total=len(columnName_values))
    for item in columnName_values:
        job_json = get_job(item)
        # Company
        name = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "companyResolutionResult", "name")
        picture_artifacts = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "companyResolutionResult", "logo", "image", "com.linkedin.common.VectorImage", "artifacts")
        picture_rootUrl = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "companyResolutionResult", "logo", "image", "com.linkedin.common.VectorImage", "rootUrl")
        picture100x100 = picture200x200 = picture400x400 = None
        if picture_artifacts and picture_rootUrl:
            for artifact in picture_artifacts:
                file_segment = artifact['fileIdentifyingUrlPathSegment']
                if '100_100' in file_segment:
                    picture100x100 = f"{picture_rootUrl}{file_segment}"
                elif '200_200' in file_segment:
                    picture200x200 = f"{picture_rootUrl}{file_segment}"
                elif '400_400' in file_segment:
                    picture400x400 = f"{picture_rootUrl}{file_segment}"
                if picture100x100 and picture200x200 and picture400x400:
                    break
        universalName = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "companyResolutionResult", "universalName")
        url = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "companyResolutionResult", "url")
        company = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "company")
        try:
            company = company.split(':')[-1]
        except:
            pass
        # Job
        jobState = safe_extract(job_json, "jobState")
        text = safe_extract(job_json, "description", "text")
        title = safe_extract(job_json, "title")
        workRemoteAllowed = safe_extract(job_json, "workRemoteAllowed")
        companyApplyUrl = safe_extract(job_json, "applyMethod", "com.linkedin.voyager.jobs.OffsiteApply", "companyApplyUrl")
        talentHubJob = safe_extract(job_json, "talentHubJob")
        formattedLocation = safe_extract(job_json, "formattedLocation")
        try:
            listedAt = convertToTimestamp(safe_extract(job_json, "listedAt"))
        except:
            listedAt = safe_extract(job_json, "listedAt")
        jobPostingId = safe_extract(job_json, "jobPostingId")
        onsite = False
        onsite_localizedName = safe_extract(job_json, "workplaceTypesResolutionResults", "urn:li:fs_workplaceType:1", "localizedName")
        if onsite_localizedName:
            onsite = True
        remote = False
        remote_localizedName = safe_extract(job_json, "workplaceTypesResolutionResults", "urn:li:fs_workplaceType:2", "localizedName")
        if remote_localizedName:
            remote = True
        hybrid = False
        hybrid_localizedName = safe_extract(job_json, "workplaceTypesResolutionResults", "urn:li:fs_workplaceType:3", "localizedName")
        if hybrid_localizedName:
            hybrid = True
        all_variables = locals()
        selected_vars = {var: [all_variables[var]] for var in ["jobPostingId", "text", "title", "formattedLocation", "companyApplyUrl", "listedAt", "workRemoteAllowed", "talentHubJob", "onsite", "remote", "hybrid", "jobState", "name", "company", "universalName", "url", "picture100x100", "picture200x200", "picture400x400"]}
        df_loop = pd.DataFrame(selected_vars)
        df_final = pd.concat([df_final, df_loop])
        time.sleep(1.5)
        progress_bar.update(1)
    progress_bar.close()
    job_rename_dict = {
        "name": "companyName",
        "picture100x100": "companyPicture100x100",
        "picture200x200": "companyPicture200x200",
        "picture400x400": "companyPicture400x400",
        "universalName": "companyUniversalName",
        "url": "companyUrl",
        "company": "companyId",
        "text": "jobOfferDescription",
        "title": "jobOfferTitle",
        "workRemoteAllowed": "jobOfferWorkRemoteAllowed",
        "companyApplyUrl": "jobOfferCompanyApplyUrl",
        "talentHubJob": "jobOfferTalentHubJob",
        "formattedLocation": "jobOfferFormattedLocation",
        "listedAt": "jobOfferListedAt",
        "jobPostingId": "jobOfferId",
    }
    df_final.rename(columns=job_rename_dict, inplace=True)
    return df_final
def post_commenters_extractor(csrf_token, dataframe, column_name, cookies_dict):
    """
    Extracts and processes LinkedIn post commenters' data from a provided DataFrame containing LinkedIn post URLs.

    Args:
        csrf_token (str): The CSRF token for authenticating LinkedIn API requests.
        dataframe (pd.DataFrame): A pandas DataFrame containing LinkedIn post URLs.
        column_name (str): The name of the column in the DataFrame containing the LinkedIn post URLs.
        cookies_dict (dict): A dictionary containing the cookies required for LinkedIn API requests.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted commenters' data, including any errors encountered during the extraction process.
    """
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def create_profileLink(profileLink):
        if profileLink:
            return f"https://www.linkedin.com/in/{profileLink}/"
        return None
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(1)
        except AttributeError:
            return None
    def extract_author(author):
        if author:
            match = re.search(r"^(.*?)-activity", author)
            if match:
                return match.group(1)
        else:
            return None
    def create_postUrl(postUrl):
        if postUrl:
            return f"https://www.linkedin.com/feed/update/urn:li:activity:{postUrl}/"
        return None
    class ForbiddenAccessException(Exception):
        pass
    def fetch_activity_comments(activity, start=0, pagination_token=None, accumulated_elements=None):
        if accumulated_elements is None:
            accumulated_elements = []
        params = {
                "count": 100,
                "start": start,
                "q": "comments",
                "sortOrder": "RELEVANCE",
                "updateId": f"activity:{activity}"
        }
        if pagination_token:
            params["paginationToken"] = pagination_token
        request_url = f"https://www.linkedin.com/voyager/api/feed/comments"
        try:
            response = requests.get(url=request_url, params=params, cookies=cookies_dict, headers=headers)
            response.raise_for_status()
            response_json = response.json()
        except requests.HTTPError as http_err:
            if http_err.response.status_code == 403:
                raise ForbiddenAccessException('Access denied. Please check your permissions or authentication tokens.')
            else:
                print(f'HTTP error occurred: {http_err}')
                return accumulated_elements
        except Exception as err:
            print(f'An error occurred: {err}')
            return accumulated_elements
        if 'elements' in response_json and 'paging' in response_json:
            new_elements = response_json.get('elements', [])
            accumulated_elements.extend(new_elements)
            total = response_json['paging'].get('total', 0)
            next_start = start + params["count"]
            if next_start < total:
                pagination_token = response_json.get('metadata', {}).get('paginationToken')
                return fetch_activity_comments(activity, start=next_start, pagination_token=pagination_token, accumulated_elements=accumulated_elements)
        else:
            print('Unexpected response structure:', response_json)
        return accumulated_elements
    pattern_universalName = re.compile(r"https?://(?:[\w]+\.)?linkedin\.com/feed/update/urn:li:activity:(\d+)/?")
    df_final = pd.DataFrame()
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    print("LinkedIn post commenters extractor")
    progress_bar = tqdm(total = len(columnName_values))
    for index, profile in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        df_loop_base = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        if wordToSearch:
            wordToSearch = str(wordToSearch)
        error = None
        if wordToSearch is None:
            error = "Invalid LinkedIn activity URL"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        try:
            activity_comments = fetch_activity_comments(wordToSearch)
            if not activity_comments:
                error = "No comments"
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_final = pd.concat([df_final, df_loop_final])
                index += 1
                progress_bar.update(1)
                continue
        except ForbiddenAccessException as e:
            activity_comments = {}
            error = "Invalid activity"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        except Exception as e:
            activity_comments = {}
            error = str(e)
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        #-->DATA MANIPULATION START<--
        for comment in activity_comments:
            check_company = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.CompanyActor')
            #-->check_company
            if check_company:
                vmid = safe_extract(comment, 'commenterProfileId')
                profileLink = create_profileLink(vmid)
                publicIdentifier = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.CompanyActor', 'miniCompany', 'universalName')
                publicProfileLink = create_profileLink(publicIdentifier)
                firstName = lastName = occupation = degree = None
                fullName = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.CompanyActor', 'miniCompany', 'name')
                commentText = safe_extract(comment, 'commentV2', 'text')
                commentUrl = safe_extract(comment, 'permalink')
                isFromPostAuthor = safe_extract(comment, 'commenterForDashConversion', 'author')
                commentDate = None
                commentDate_ms = safe_extract(comment, 'createdTime')
                likesCount = safe_extract(comment, 'socialDetail', 'totalSocialActivityCounts', 'numLikes')
                commentsCount = safe_extract(comment, 'socialDetail', 'totalSocialActivityCounts', 'numComments')
                if commentDate_ms:
                    commentDate_datetime = datetime.fromtimestamp(commentDate_ms / 1000.0, tz=timezone.utc)
                    commentDate = commentDate_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                postUrl = create_postUrl(wordToSearch)
                current_timestamp = datetime.now()
                timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                banner200x800 = banner350x1400 = None
                picture_artifacts = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.CompanyActor', 'miniCompany', 'logo', 'com.linkedin.common.VectorImage', 'artifacts')
                picture_rootUrl = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.CompanyActor', 'miniCompany', 'logo', 'com.linkedin.common.VectorImage', 'rootUrl')
                picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
                if picture_artifacts and picture_rootUrl:
                    for artifact in picture_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '100_100' in file_segment:
                            picture100x100 = f"{picture_rootUrl}{file_segment}"
                        elif '200_200' in file_segment:
                            picture200x200 = f"{picture_rootUrl}{file_segment}"
                        elif '400_400' in file_segment:
                            picture400x400 = f"{picture_rootUrl}{file_segment}"
                        elif '800_800' in file_segment:
                            picture800x800 = f"{picture_rootUrl}{file_segment}"
                        if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                            break
                all_variables = locals()
                selected_vars = {var: [all_variables[var]] for var in ["profileLink","vmid","publicProfileLink","publicIdentifier","firstName","lastName","fullName","occupation","degree","commentText","commentUrl","isFromPostAuthor","commentDate","likesCount","commentsCount","postUrl","timestamp","banner200x800","banner350x1400","picture100x100","picture200x200","picture400x400","picture800x800"]}
                df_loop_base = pd.DataFrame(selected_vars)
                error = "Company profile"
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
                df_final = pd.concat([df_final, df_loop_final])
                continue
            check_InfluencerActor = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor')
            #-->vmid
            vmid = safe_extract(comment, 'commenterProfileId')
            #-->profileLink
            profileLink = create_profileLink(vmid)
            #-->publicIdentifier
            publicIdentifier = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'publicIdentifier')
            #-->publicProfileLink
            publicProfileLink = create_profileLink(publicIdentifier)
            #-->firstName
            firstName = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'firstName')
            #-->lastName
            lastName = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'lastName')
            #-->fullName
            fullName = None
            if firstName and lastName:
                fullName = firstName + " " + lastName
                fullName = fullName.strip() if fullName is not None else None
            elif firstName:
                fullName = firstName
                fullName = fullName.strip() if fullName is not None else None
            else:
                fullName = lastName
                fullName = fullName.strip() if fullName is not None else None
            #-->occupation
            occupation = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'occupation')
            #-->degree
            degree = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'distance', 'value')
            degree_mapping = {"DISTANCE_2": "2nd", "DISTANCE_1": "1st", "OUT_OF_NETWORK": "3rd"}
            degree = degree_mapping.get(degree, degree)
            #-->comment
            commentText = safe_extract(comment, 'commentV2', 'text')
            #-->commentUrl
            commentUrl = safe_extract(comment, 'permalink')
            #-->isFromPostAuthor
            isFromPostAuthor = safe_extract(comment, 'commenterForDashConversion', 'author')
            #-->commentDate
            commentDate = None
            commentDate_ms = safe_extract(comment, 'createdTime')
            if commentDate_ms:
                commentDate_datetime = datetime.utcfromtimestamp(commentDate_ms / 1000.0)
                commentDate = commentDate_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            #-->likesCount
            likesCount = safe_extract(comment, 'socialDetail', 'totalSocialActivityCounts', 'numLikes')
            #-->commentsCount
            commentsCount = safe_extract(comment, 'socialDetail', 'totalSocialActivityCounts', 'numComments')
            #-->banner200x800 and banner350x1400
            background_artifacts = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'backgroundImage', 'com.linkedin.common.VectorImage', 'artifacts')
            background_rootUrl = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'backgroundImage', 'com.linkedin.common.VectorImage', 'rootUrl')
            banner200x800 = banner350x1400 = None
            if background_artifacts and background_rootUrl:
                for artifact in background_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '200_800' in file_segment:
                        banner200x800 = f"{background_rootUrl}{file_segment}"
                    elif '350_1400' in file_segment:
                        banner350x1400 = f"{background_rootUrl}{file_segment}"
                    if banner200x800 and banner350x1400:
                        break
            #-->picture100x100, picture200x200, picture400x400 and picture800x800
            picture_artifacts = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'picture', 'com.linkedin.common.VectorImage', 'artifacts')
            picture_rootUrl = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'picture', 'com.linkedin.common.VectorImage', 'rootUrl')
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            postUrl = create_postUrl(wordToSearch)
            #-->check_InfluencerActor
            if check_InfluencerActor:
                publicIdentifier = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'publicIdentifier')
                publicProfileLink = create_profileLink(publicIdentifier)
                firstName = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'firstName')
                lastName = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'lastName')
                fullName = None
                if firstName and lastName:
                    fullName = firstName + " " + lastName
                    fullName = fullName.strip() if fullName is not None else None
                elif firstName:
                    fullName = firstName
                    fullName = fullName.strip() if fullName is not None else None
                else:
                    fullName = lastName
                    fullName = fullName.strip() if fullName is not None else None
                occupation = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'occupation')
                degree = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'distance', 'value')
                degree_mapping = {"DISTANCE_2": "2nd", "DISTANCE_1": "1st", "OUT_OF_NETWORK": "3rd"}
                degree = degree_mapping.get(degree, degree)
                background_artifacts = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'backgroundImage', 'com.linkedin.common.VectorImage', 'artifacts')
                background_rootUrl = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'backgroundImage', 'com.linkedin.common.VectorImage', 'rootUrl')
                banner200x800 = banner350x1400 = None
                if background_artifacts and background_rootUrl:
                    for artifact in background_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '200_800' in file_segment:
                            banner200x800 = f"{background_rootUrl}{file_segment}"
                        elif '350_1400' in file_segment:
                            banner350x1400 = f"{background_rootUrl}{file_segment}"
                        if banner200x800 and banner350x1400:
                            break
                picture_artifacts = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'picture', 'com.linkedin.common.VectorImage', 'artifacts')
                picture_rootUrl = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'picture', 'com.linkedin.common.VectorImage', 'rootUrl')
                picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
                if picture_artifacts and picture_rootUrl:
                    for artifact in picture_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '100_100' in file_segment:
                            picture100x100 = f"{picture_rootUrl}{file_segment}"
                        elif '200_200' in file_segment:
                            picture200x200 = f"{picture_rootUrl}{file_segment}"
                        elif '400_400' in file_segment:
                            picture400x400 = f"{picture_rootUrl}{file_segment}"
                        elif '800_800' in file_segment:
                            picture800x800 = f"{picture_rootUrl}{file_segment}"
                        if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                            break
            #-->DATA MANIPULATION END<--
            current_timestamp = datetime.now()
            timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["profileLink","vmid","publicProfileLink","publicIdentifier","firstName","lastName","fullName","occupation","degree","commentText","commentUrl","isFromPostAuthor","commentDate","likesCount","commentsCount","postUrl","timestamp","banner200x800","banner350x1400","picture100x100","picture200x200","picture400x400","picture800x800"]}
            df_loop_base = pd.DataFrame(selected_vars)
            error = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
    progress_bar.close()
    #-->Columns manipulation
    final_columns = ["query","error","profileLink","vmid","publicProfileLink","publicIdentifier","firstName","lastName","fullName","occupation","degree","commentText","commentUrl","isFromPostAuthor","commentDate","likesCount","commentsCount","postUrl","timestamp","banner200x800","banner350x1400","picture100x100","picture200x200","picture400x400","picture800x800"]
    df_final = df_final.reindex(columns=final_columns, fill_value=None)
    return df_final
def profile_activity_extractor(csrf_token, dataframe, column_name, cookies_dict):
    """
    Extracts and processes LinkedIn profile activity data from a provided DataFrame containing LinkedIn profile URLs.

    Args:
        csrf_token (str): The CSRF token for authenticating LinkedIn API requests.
        dataframe (pd.DataFrame): A pandas DataFrame containing LinkedIn profile URLs.
        column_name (str): The name of the column in the DataFrame containing the LinkedIn profile URLs.
        cookies_dict (dict): A dictionary containing the cookies required for LinkedIn API requests.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted profile activity data, including any errors encountered during the extraction process.
    """
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def create_sharedPostUrl(sharedPostUrl):
        if sharedPostUrl:
            return f"https://www.linkedin.com/feed/update/{sharedPostUrl}/"
        return None
    def extract_sharedJobUrl(sharedJobUrl):
        if sharedJobUrl:
            match = re.search(r'(https://www\.linkedin\.com/jobs/view/\d+)/', sharedJobUrl)
            if match:
                return match.group(1) + "/"
        return None
    def extract_postDate(postDate):
        if postDate:
            match = re.search(r'^(.*?)\s*•', postDate)
            if match:
                return match.group(1).strip()
        return None
    def create_profileUrl(profileUrl):
        if profileUrl:
            return f"https://www.linkedin.com/in/{profileUrl}/"
        return None
    class ForbiddenAccessException(Exception):
        pass
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(3)
        except AttributeError:
            return None
    def fetch_person_updates(profile, start=0, pagination_token=None, accumulated_elements=None):
        if accumulated_elements is None:
            accumulated_elements = []
        params = {
            "profileId": profile,
            "q": "memberShareFeed",
            "moduleKey": "member-share",
            "count": 100,
            "start": start,
        }
        if pagination_token:
            params["paginationToken"] = pagination_token
        request_url = 'https://www.linkedin.com/voyager/api/feed/updates'
        try:
            response = requests.get(url=request_url, params=params, cookies=cookies_dict, headers=headers)
            response.raise_for_status()
            response_json = response.json()
        except requests.HTTPError as http_err:
            if http_err.response.status_code == 403:
                raise ForbiddenAccessException('Access denied. Please check your permissions or authentication tokens.')
            else:
                print(f'HTTP error occurred: {http_err}')
                return accumulated_elements
        except Exception as err:
            print(f'An error occurred: {err}')
            return accumulated_elements
        if 'elements' in response_json:
            new_elements = response_json.get('elements', [])
            if not new_elements:
                return accumulated_elements
            accumulated_elements.extend(new_elements)
            next_start = start + len(new_elements)
            pagination_token = response_json.get('metadata', {}).get('paginationToken')
            return fetch_person_updates(profile, start=next_start, pagination_token=pagination_token, accumulated_elements=accumulated_elements)
        else:
            print('Unexpected response structure:', response_json)
        return accumulated_elements
    pattern_universalName = re.compile(r"https?://([\w]+\.)?linkedin\.com/(in|sales/lead|sales/people)/([A-z0-9\._\%&'-]+)/?")
    df_final = pd.DataFrame()
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    print("LinkedIn profile activity extractor")
    progress_bar = tqdm(total = len(columnName_values))
    for index, profile in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        df_loop_base = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        error = None
        if wordToSearch is None:
            error = "Invalid LinkedIn URL"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        try:
            profile_posts = fetch_person_updates(wordToSearch)
            if not profile_posts:
                error = "No activities"
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_final = pd.concat([df_final, df_loop_final])
                index += 1
                progress_bar.update(1)
                continue
        except ForbiddenAccessException as e:
            profile_posts = {}
            error = "LinkedIn down"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        except Exception as e:
            profile_posts = {}
            error = str(e)
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            continue
        #-->DATA MANIPULATION START<--
        for post in profile_posts:
            #-->postUrl
            postUrl = safe_extract(post, 'permalink')
            #-->imgUrl
            rootUrl = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.ImageComponent', 'images', 0, 'attributes', 0, 'vectorImage', 'rootUrl')
            fileIdentifyingUrlPathSegment = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.ImageComponent', 'images', 0, 'attributes', 0, 'vectorImage', 'artifacts', 5, 'fileIdentifyingUrlPathSegment')
            imgUrl = None
            if rootUrl and fileIdentifyingUrlPathSegment:
                imgUrl = rootUrl + fileIdentifyingUrlPathSegment
            #-->postContent
            postContent = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'commentary', 'text', 'text')
            #-->postType
            postType = None
            if postContent:
                postType = "Text"
            if imgUrl:
                postType = "Image"
            #-->likeCount
            likeCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numLikes')
            #-->commentCount
            commentCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numComments')
            #-->repostCount
            repostCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numShares')
            #-->postDate
            postDate = extract_postDate(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'actor', 'subDescription', 'text'))
            #-->action
            action = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'header', 'text', 'text')
            if not action:
                action = "Post"
            #-->profileUrl
            profileUrl = create_profileUrl(wordToSearch)
            #-->sharedPostUrl
            sharedPostUrl = create_sharedPostUrl(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'resharedUpdate', 'updateMetadata', 'urn'))
            #-->sharedJobUrl
            sharedJobUrl = extract_sharedJobUrl(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.EntityComponent', 'ctaButton', 'navigationContext', 'actionTarget'))
            #-->isSponsored
            isSponsored = safe_extract(post, 'isSponsored')
            #-->DATA MANIPULATION END<--
            current_timestamp = datetime.now()
            timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["postUrl","imgUrl","postContent","postType","likeCount","commentCount","repostCount","postDate","action","profileUrl","timestamp","sharedPostUrl","sharedJobUrl","isSponsored"]}
            df_loop_base = pd.DataFrame(selected_vars)
            error = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
    progress_bar.close()
    #-->Columns manipulation
    final_columns = ["query","error","postUrl","imgUrl","postContent","postType","likeCount","commentCount","repostCount","postDate","action","profileUrl","timestamp","sharedPostUrl","sharedJobUrl","isSponsored"]
    df_final = df_final.reindex(columns=final_columns, fill_value=None)
    return df_final
#Search
def linkedin_search_scripts(
    csrf_token: str = None,
    dataframe: pd.DataFrame = None,
    script_type: str = None,
    first_name_column_name: str = None,
    last_name_column_name: int = None,
    company_name_column_name: int = None,
    query_column_name: str = None,
    company_column_name: str = None,
    cookies_dict: dict = None,
) -> None:
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    MAX_SEARCH_COUNT = 49
    MAX_REPEATED_REQUESTS = 3
    def get_id_from_urn(urn):
        return urn.split(":")[3]
    def extract_vmid(urn_id):
        if urn_id is not None:
            return urn_id[:39]
        return None
    def extract_profile_url(url):
        if url is not None:
            return url.split('?')[0] + '/'
        return url
    def create_vmid_url(vmid):
        return f"https://www.linkedin.com/in/{vmid}/" if vmid else None
    def create_maincompanyid_url(maincompanyid):
        return f"https://www.linkedin.com/company/{maincompanyid}/" if maincompanyid else None
    def fetch(url):
        response = requests.get(
            "https://www.linkedin.com/voyager/api"+url, headers=headers, cookies=cookies_dict)
        response.raise_for_status()
        return response
    def search(params, limit=-1, offset=0):
        """
        Perform a LinkedIn search.

        :param params: Search parameters (see code)
        :type params: dict
        :param limit: Maximum length of the returned list, defaults to -1 (no limit)
        :type limit: int, optional
        :param offset: Index to start searching from
        :type offset: int, optional

        :return: List of search results
        :rtype: list
        """
        count = MAX_SEARCH_COUNT
        if limit is None:
            limit = -1
        results = []
        while True:
            if limit > -1 and limit - len(results) < count:
                count = limit - len(results)
            default_params = {
                "count": str(count),
                "filters": "List()",
                "origin": "GLOBAL_SEARCH_HEADER",
                "q": "all",
                "start": len(results) + offset,
                "queryContext": "List(spellCorrectionEnabled->true,relatedSearchesEnabled->true,kcardTypes->PROFILE|COMPANY)",
                "includeWebMetadata": "true",
            }
            default_params.update(params)
            keywords = f"keywords:{urllib.parse.quote(default_params['keywords'])}," if "keywords" in default_params else ""
            res = fetch(
                f"/graphql?variables=(start:{default_params['start']},origin:{default_params['origin']},"
                f"query:("
                f"{keywords}"
                f"flagshipSearchIntent:SEARCH_SRP,"
                f"queryParameters:{default_params['filters']},"
                f"includeFiltersInResponse:false))&queryId=voyagerSearchDashClusters"
                f".b0928897b71bd00a5a7291755dcd64f0"
            )
            data = res.json()
            data_clusters = data.get("data", {}).get("searchDashClustersByAll", {})
            if not data_clusters:
                return []
            if data_clusters.get("_type") != "com.linkedin.restli.common.CollectionResponse":
                return []
            new_elements = []
            for it in data_clusters.get("elements", []):
                if it.get("_type") != "com.linkedin.voyager.dash.search.SearchClusterViewModel":
                    continue
                for el in it.get("items", []):
                    if el.get("_type") != "com.linkedin.voyager.dash.search.SearchItem":
                        continue
                    e = el.get("item", {}).get("entityResult", {})
                    if not e:
                        continue
                    if e.get("_type") != "com.linkedin.voyager.dash.search.EntityResultViewModel":
                        continue
                    new_elements.append(e)
            results.extend(new_elements)
            if (-1 < limit <= len(results)) or len(results) / count >= MAX_REPEATED_REQUESTS or not new_elements:
                break
        return results
    def search_people(keywords=None, connection_of=None, network_depths=None, current_company=None,
                    past_companies=None, nonprofit_interests=None, profile_languages=None, regions=None,
                    industries=None, schools=None, contact_interests=None, service_categories=None,
                    include_private_profiles=False, keyword_first_name=None, keyword_last_name=None,
                    keyword_title=None, keyword_company=None, keyword_school=None, network_depth=None, title=None,
                    **kwargs):
        """
        Perform a LinkedIn search for people.

        :param keywords: Keywords to search on
        :type keywords: str, optional
        :param current_company: A list of company URN IDs (str)
        :type current_company: list, optional
        :param past_companies: A list of company URN IDs (str)
        :type past_companies: list, optional
        :param regions: A list of geo URN IDs (str)
        :type regions: list, optional
        :param industries: A list of industry URN IDs (str)
        :type industries: list, optional
        :param schools: A list of school URN IDs (str)
        :type schools: list, optional
        :param profile_languages: A list of 2-letter language codes (str)
        :type profile_languages: list, optional
        :param contact_interests: A list containing one or both of "proBono" and "boardMember"
        :type contact_interests: list, optional
        :param service_categories: A list of service category URN IDs (str)
        :type service_categories: list, optional
        :param network_depth: Deprecated, use `network_depths`. One of "F", "S" and "O" (first, second and third+ respectively)
        :type network_depth: str, optional
        :param network_depths: A list containing one or many of "F", "S" and "O" (first, second and third+ respectively)
        :type network_depths: list, optional
        :param include_private_profiles: Include private profiles in search results. If False, only public profiles are included. Defaults to False
        :type include_private_profiles: boolean, optional
        :param keyword_first_name: First name
        :type keyword_first_name: str, optional
        :param keyword_last_name: Last name
        :type keyword_last_name: str, optional
        :param keyword_title: Job title
        :type keyword_title: str, optional
        :param keyword_company: Company name
        :type keyword_company: str, optional
        :param keyword_school: School name
        :type keyword_school: str, optional
        :param connection_of: Connection of LinkedIn user, given by profile URN ID
        :type connection_of: str, optional
        :param limit: Maximum length of the returned list, defaults to -1 (no limit)
        :type limit: int, optional

        :return: List of profiles (minimal data only)
        :rtype: list
        """
        filters = ["(key:resultType,value:List(PEOPLE))"]
        if connection_of:
            filters.append(f"(key:connectionOf,value:List({quote(connection_of)})")
        if network_depths:
            stringify = " | ".join(map(quote, network_depths))
            filters.append(f"(key:network,value:List({stringify}))")
        elif network_depth:
            filters.append(f"(key:network,value:List({quote(network_depth)}))")
        if regions:
            stringify = " | ".join(map(quote, regions))
            filters.append(f"(key:geoUrn,value:List({stringify}))")
        if industries:
            stringify = " | ".join(map(quote, industries))
            filters.append(f"(key:industry,value:List({stringify}))")
        if current_company:
            stringify = " | ".join(map(quote, current_company))
            filters.append(f"(key:currentCompany,value:List({stringify}))")
        if past_companies:
            stringify = " | ".join(map(quote, past_companies))
            filters.append(f"(key:pastCompany,value:List({stringify}))")
        if profile_languages:
            stringify = " | ".join(map(quote, profile_languages))
            filters.append(f"(key:profileLanguage,value:List({stringify}))")
        if nonprofit_interests:
            stringify = " | ".join(map(quote, nonprofit_interests))
            filters.append(f"(key:nonprofitInterest,value:List({stringify}))")
        if schools:
            stringify = " | ".join(map(quote, schools))
            filters.append(f"(key:schools,value:List({stringify}))")
        if service_categories:
            stringify = " | ".join(map(quote, service_categories))
            filters.append(f"(key:serviceCategory,value:List({stringify}))")
        keyword_title = keyword_title if keyword_title else title
        if keyword_first_name:
            filters.append(
                f"(key:firstName,value:List({quote(keyword_first_name)}))")
        if keyword_last_name:
            filters.append(
                f"(key:lastName,value:List({quote(keyword_last_name)}))")
        if keyword_title:
            filters.append(f"(key:title,value:List({quote(keyword_title)}))")
        if keyword_company:
            filters.append(f"(key:company,value:List({quote(keyword_company)}))")
        if keyword_school:
            filters.append(f"(key:school,value:List({quote(keyword_school)}))")
        params = {"filters": "List({})".format(",".join(filters))}
        if keywords:
            params["keywords"] = keywords
        data = search(params, **kwargs)
        results = []
        for item in data:
            if not include_private_profiles and (item.get("entityCustomTrackingInfo") or {}).get("memberDistance") == "OUT_OF_NETWORK":
                continue
            results.append(
                {
                    "urn_id": item.get("entityUrn").split(':')[-1] if item.get("entityUrn") else None,
                    "distance": (item.get("entityCustomTrackingInfo") or {}).get("memberDistance"),
                    "jobtitle": (item.get("primarySubtitle") or {}).get("text"),
                    "location": (item.get("secondarySubtitle") or {}).get("text"),
                    "name": (item.get("title") or {}).get("text"),
                    "profile_url": item.get("navigationUrl"),
                }
            )
        return results
    def search_companies(keywords=None, **kwargs):
        """
        Perform a LinkedIn search for companies.

        :param keywords: A list of search keywords (str)
        :type keywords: list, optional

        :return: List of companies
        :rtype: list
        """
        filters = ["(key:resultType,value:List(COMPANIES))"]
        params = {
            "filters": "List({})".format(",".join(filters)),
            "queryContext": "List(spellCorrectionEnabled->true)",
        }
        if keywords:
            params["keywords"] = keywords
        data = search(params, **kwargs)
        results = []
        for item in data:
            if "company" not in item.get("trackingUrn"):
                continue
            results.append(
                {
                    "urn_id": get_id_from_urn(item.get("trackingUrn", None)),
                    "name": (item.get("title") or {}).get("text", None),
                    "headline": (item.get("primarySubtitle") or {}).get("text", None),
                    "subline": (item.get("secondarySubtitle") or {}).get("text", None),
                }
            )
        return results
    def search_jobs(keywords=None, companies=None, experience=None, job_type=None, job_title=None,
                    industries=None, location_name=None, remote=None, listed_at=24 * 60 * 60, distance=None, limit=-1,
                    offset=0, **kwargs,):
        """
        Perform a LinkedIn search for jobs.

        :param keywords: Search keywords (str)
        :type keywords: str, optional
        :param companies: A list of company URN IDs (str)
        :type companies: list, optional
        :param experience: A list of experience levels, one or many of "1", "2", "3", "4", "5" and "6" (internship, entry level, associate, mid-senior level, director and executive, respectively)
        :type experience: list, optional
        :param job_type:  A list of job types , one or many of "F", "C", "P", "T", "I", "V", "O" (full-time, contract, part-time, temporary, internship, volunteer and "other", respectively)
        :type job_type: list, optional
        :param job_title: A list of title URN IDs (str)
        :type job_title: list, optional
        :param industries: A list of industry URN IDs (str)
        :type industries: list, optional
        :param location_name: Name of the location to search within. Example: "Kyiv City, Ukraine"
        :type location_name: str, optional
        :param remote: Filter for remote jobs, onsite or hybrid. onsite:"1", remote:"2", hybrid:"3"
        :type remote: list, optional
        :param listed_at: maximum number of seconds passed since job posting. 86400 will filter job postings posted in last 24 hours.
        :type listed_at: int/str, optional. Default value is equal to 24 hours.
        :param distance: maximum distance from location in miles
        :type distance: int/str, optional. If not specified, None or 0, the default value of 25 miles applied.
        :param limit: maximum number of results obtained from API queries. -1 means maximum which is defined by constants and is equal to 1000 now.
        :type limit: int, optional, default -1
        :param offset: indicates how many search results shall be skipped
        :type offset: int, optional
        :return: List of jobs
        :rtype: list
        """
        count = MAX_SEARCH_COUNT
        if limit is None:
            limit = -1
        query = {"origin": "JOB_SEARCH_PAGE_QUERY_EXPANSION"}
        if keywords:
            query["keywords"] = quote(keywords)
        if location_name:
            query["locationFallback"] = quote(location_name)
        query["selectedFilters"] = {}
        if companies:
            query["selectedFilters"]["company"] = f"List({','.join(map(quote, companies))})"
        if experience:
            query["selectedFilters"]["experience"] = f"List({','.join(map(quote, experience))})"
        if job_type:
            query["selectedFilters"]["jobType"] = f"List({','.join(map(quote, job_type))})"
        if job_title:
            query["selectedFilters"]["title"] = f"List({','.join(map(quote, job_title))})"
        if industries:
            query["selectedFilters"]["industry"] = f"List({','.join(map(quote, industries))})"
        if distance:
            query["selectedFilters"]["distance"] = f"List({quote(distance)})"
        if remote:
            query["selectedFilters"]["workplaceType"] = f"List({','.join(map(quote, remote))})"
        query["selectedFilters"]["timePostedRange"] = f"List(r{listed_at})"
        query["spellCorrectionEnabled"] = "true"
        query = (
            str(query)
            .replace(" ", "")
            .replace("'", "")
            .replace("KEYWORD_PLACEHOLDER", keywords or "")
            .replace("LOCATION_PLACEHOLDER", location_name or "")
            .replace("{", "(")
            .replace("}", ")")
        )
        results = []
        while True:
            if limit > -1 and limit - len(results) < count:
                count = limit - len(results)
            default_params = {
                "decorationId": "com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-174",
                "count": count,
                "q": "jobSearch",
                "query": query,
                "start": len(results) + offset,
            }
            res = fetch(
                f"/voyagerJobsDashJobCards?{urlencode(default_params, safe='(),:')}",
            )
            data = res.json()
            elements = data.get("included", [])
            new_data = [
                i
                for i in elements
                if i["$type"] == "com.linkedin.voyager.dash.jobs.JobPosting"
            ]
            if not new_data:
                break
            results.extend(new_data)
            if (
                (-1 < limit <= len(results))
                or len(results) / count >= MAX_REPEATED_REQUESTS
            ) or len(elements) == 0:
                break
        return results
    def people_search_first_name_last_name_company_name(dataframe, first_name_column_name, last_name_column_name, company_name_column_name):
        dataframe_final = pd.DataFrame()
        for index, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0], desc='Processing rows'):
            dataframe_loop = pd.DataFrame()
            keyword_first_name = row[first_name_column_name]
            keyword_last_name = row[last_name_column_name]
            keyword_company = row[company_name_column_name]
            error = None
            person_result = search_people(keyword_first_name=keyword_first_name,
                                        keyword_last_name=keyword_last_name, keyword_company=keyword_company, limit=5)
            first_person_result = safe_extract(person_result, 0)
            if first_person_result:
                urn_id = safe_extract(first_person_result, 'urn_id')
                vmid = extract_vmid(urn_id)
                linkedin_url = create_vmid_url(vmid)
                distance = safe_extract(first_person_result, 'distance')
                jobtitle = safe_extract(first_person_result, 'jobtitle')
                location = safe_extract(first_person_result, 'location')
                name = safe_extract(first_person_result, 'name')
                profile_url = safe_extract(first_person_result, 'profile_url')
                public_linkedin_url = extract_profile_url(profile_url)
            if not first_person_result:
                error = "No results found"
                vmid = linkedin_url = distance = jobtitle = location = name = public_linkedin_url = None
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["keyword_first_name", "keyword_last_name", "keyword_company",
                                                                "error", "vmid", "linkedin_url", "distance", "jobtitle", "location", "name", "public_linkedin_url"]}
            dataframe_loop = pd.DataFrame(selected_vars)
            dataframe_final = pd.concat([dataframe_final, dataframe_loop])
            time.sleep(random.uniform(10, 17))
        all_conversations_rename_dict = {
            "keyword_first_name": first_name_column_name,
            "keyword_last_name": last_name_column_name,
            "keyword_company": company_name_column_name,
            "error": "Error",
            "vmid": "vmid",
            "linkedin_url": "LinkedIn URL",
            "distance": "Distance",
            "jobtitle": "Job title",
            "location": "Location",
            "name": "Full name",
            "public_linkedin_url": "Public LinkedIn URL",
        }
        dataframe_final.rename(columns=all_conversations_rename_dict, inplace=True)
        return dataframe_final
    def people_search_any_query(dataframe, query_column_name):
        dataframe_final = pd.DataFrame()
        for index, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0], desc='Processing rows'):
            dataframe_loop = pd.DataFrame()
            keywords = row[query_column_name]
            error = None
            person_result = search_people(keywords=keywords, limit=5)
            first_person_result = safe_extract(person_result, 0)
            if first_person_result:
                urn_id = safe_extract(first_person_result, 'urn_id')
                vmid = extract_vmid(urn_id)
                linkedin_url = create_vmid_url(vmid)
                distance = safe_extract(first_person_result, 'distance')
                jobtitle = safe_extract(first_person_result, 'jobtitle')
                location = safe_extract(first_person_result, 'location')
                name = safe_extract(first_person_result, 'name')
                profile_url = safe_extract(first_person_result, 'profile_url')
                public_linkedin_url = extract_profile_url(profile_url)
            if not first_person_result:
                error = "No results found"
                vmid = linkedin_url = distance = jobtitle = location = name = public_linkedin_url = None
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["keywords", "error", "vmid",
                                                                "linkedin_url", "distance", "jobtitle", "location", "name", "public_linkedin_url"]}
            dataframe_loop = pd.DataFrame(selected_vars)
            dataframe_final = pd.concat([dataframe_final, dataframe_loop])
            time.sleep(random.uniform(10, 17))
        all_conversations_rename_dict = {
            "keywords": query_column_name,
            "error": "Error",
            "vmid": "vmid",
            "linkedin_url": "LinkedIn URL",
            "distance": "Distance",
            "jobtitle": "Job title",
            "location": "Location",
            "name": "Full name",
            "public_linkedin_url": "Public LinkedIn URL",
        }
        dataframe_final.rename(columns=all_conversations_rename_dict, inplace=True)
        return dataframe_final
    def company_search_company_name(dataframe, company_column_name):
        dataframe_final = pd.DataFrame()
        for index, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0], desc='Processing rows'):
            dataframe_loop = pd.DataFrame()
            keywords = row[company_column_name]
            error = None
            company_result = search_companies(keywords=keywords)
            first_company_result = safe_extract(company_result, 0)
            if first_company_result:
                urn_id = safe_extract(first_company_result, 'urn_id')
                linkedin_url = create_maincompanyid_url(urn_id)
                name = safe_extract(first_company_result, 'name')
                headline = safe_extract(first_company_result, 'headline')
                subline = safe_extract(first_company_result, 'subline')
            if not first_company_result:
                error = "No results found"
                urn_id = linkedin_url = name = headline = subline = None
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in [
                "keywords", "error", "urn_id", "linkedin_url", "name", "headline", "subline"]}
            dataframe_loop = pd.DataFrame(selected_vars)
            dataframe_final = pd.concat([dataframe_final, dataframe_loop])
            time.sleep(random.uniform(10, 17))
        all_conversations_rename_dict = {
            "keywords": company_column_name,
            "error": "Error",
            "urn_id": "Company ID",
            "linkedin_url": "LinkedIn URL",
            "name": "Company name",
            "headline": "Headline",
            "subline": "Subline",
        }
        dataframe_final.rename(columns=all_conversations_rename_dict, inplace=True)
        return dataframe_final
    if script_type == "people_search_first_name_last_name_company_name":
        dataframe_final = people_search_first_name_last_name_company_name(dataframe, first_name_column_name, last_name_column_name, company_name_column_name)
        return dataframe_final
    if script_type == "people_search_any_query":
        dataframe_final = people_search_any_query(dataframe, query_column_name)
        return dataframe_final
    if script_type == "company_search_company_name":
        dataframe_final = company_search_company_name(dataframe, company_column_name)
        return dataframe_final
#Outreach
def linkedin_outreach_scripts(
    result_column_name: str = "Done?",
    csrf_token: str = None,
    dataframe: pd.DataFrame = None,
    script_type: str = None,
    conversation_id_column_name: str = None,
    waiting_time_min: int = None,
    waiting_time_max: int = None,
    message_column_name: str = None,
    vmid_column_name: str = None,
    action: str = None,
    cookies_dict: dict = None,
    invitation_id_column_name: str = None,
    invitation_shared_secret_column_name: str = None,
    unique_identifier_column_name: str = None
) -> None:
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    #Functional functions
    def convertToTimestamp(milliseconds):
        if milliseconds:
            return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return None
    def get_conversations():
        params = {"keyVersion": "LEGACY_INBOX"}
        request_url = "https://www.linkedin.com/voyager/api/messaging/conversations"
        response = requests.get(url=request_url, params=params,
                                cookies=cookies_dict, headers=headers)
        return response.json()
    def get_conversation(conversation_id):
        # -->Get a conversation passing conversation id
        request_url = f'https://www.linkedin.com/voyager/api/messaging/conversations/{conversation_id}/events'
        response = requests.get(
            url=request_url, cookies=cookies_dict, headers=headers)
        return response.json()
    def get_conversation_id_using_vmid(vmid):
        # -->Helper function
        # -->Get id of a conversation passing person vmid
        request_url = f"https://www.linkedin.com/voyager/api/messaging/conversations?\
        keyVersion=LEGACY_INBOX&q=participants&recipients=List({vmid})"
        response = requests.get(
            url=request_url, cookies=cookies_dict, headers=headers)
        data = response.json()
        if safe_extract(data, "elements") == []:
            return None
        dashEntityUrn = safe_extract(data, "elements", 0, "dashEntityUrn")
        try:
            dashEntityUrn = dashEntityUrn.split(':')[-1]
        except:
            pass
        return dashEntityUrn
    def get_user_profile():
        # -->Get data from current user
        request_url = "https://www.linkedin.com/voyager/api/me"
        response = requests.get(
            url=request_url, cookies=cookies_dict, headers=headers)
        data = response.json()
        return data
    def generate_trackingId_as_charString():
        # -->Helper function
        random_int_array = [random.randrange(256) for _ in range(16)]
        rand_byte_array = bytearray(random_int_array)
        return "".join([chr(i) for i in rand_byte_array])
    def send_message(message_body, conversation_id, is_premium):
        # -->Send message passing a conversation id
        params = {"action": "create"}
        if not conversation_id:
            return print("Must provide conversation id!")
        if is_premium == True and len(message_body) > 300:
            return print("The message must be less than 300 characters!")
        if is_premium == False and len(message_body) > 200:
            return print("The message must be less than 200 characters!")
        message_event = {
            "eventCreate": {
                "originToken": str(uuid.uuid4()),
                "value": {
                    "com.linkedin.voyager.messaging.create.MessageCreate": {
                        "attributedBody": {
                            "text": message_body,
                            "attributes": [],
                        },
                        "attachments": [],
                    }
                },
                "trackingId": generate_trackingId_as_charString(),
            },
            "dedupeByClientGeneratedToken": False,
        }
        if conversation_id:
            request_url = f"https://www.linkedin.com/voyager/api/messaging/conversations/{conversation_id}/events"
            response = requests.post(url=request_url, params=params, data=json.dumps(
                message_event), headers=headers, cookies=cookies_dict)
            if response.status_code == 201:
                return print("Message sent!")
            else:
                return print("Message not sent!")
    def mark_conversation_as_seen(conversation_id):
        # -->Mark conversation as seen using a conversation id
        payload = json.dumps({"patch": {"$set": {"read": True}}})
        request_url = f"https://www.linkedin.com/voyager/api/messaging/conversations/{conversation_id}"
        response = requests.post(
            url=request_url, data=payload, headers=headers, cookies=cookies_dict)
        if response.status_code == 200:
            return print("Conversation marked as seen!")
        else:
            return print("Conversation not marked as seen!")
    def get_all_invitations():
        # -->Get all current connection requests
        invitations = []
        start = 0
        count = 100
        has_more = True
        params = {"start": start, "count": count,
                "includeInsights": True, "q": "receivedInvitation"}
        while has_more:
            request_url = "https://www.linkedin.com/voyager/api/relationships/invitationViews"
            response = requests.get(
                url=request_url, params=params, cookies=cookies_dict, headers=headers)
            if response.status_code != 200:
                print(f"Failed to fetch invitations: {response.status_code}")
                break
            response_payload = response.json()
            current_invitations = [
                element for element in response_payload.get("elements", [])]
            if not current_invitations:
                has_more = False
            else:
                invitations.extend(current_invitations)
                params['start'] += count
        return invitations
    def reply_invitation(invitation_id, invitation_shared_secret, action, isGenericInvitation=True):
        # -->Accept a invitation using invitation id and invitation shared secret
        # action: accept or ignore
        params = {"action": action}
        payload = json.dumps({"invitationId": invitation_id, "invitationSharedSecret":
                            invitation_shared_secret, "isGenericInvitation": isGenericInvitation})
        request_url = f"https://www.linkedin.com/voyager/api/relationships/invitations/{invitation_id}"
        response = requests.post(url=request_url, params=params,
                                data=payload, cookies=cookies_dict, headers=headers)
        if action == "accept":
            if response.status_code == 200:
                return print(f"Invitation accepted!")
            else:
                return print(f"Invitation not accepted!")
        elif action == "ignore":
            if response.status_code == 200:
                return print(f"Invitation ignored!")
            else:
                return print(f"Invitation not ignored!")
    def generate_trackingId():
        # -->Helper function
        random_int_array = [random.randrange(256) for _ in range(16)]
        rand_byte_array = bytearray(random_int_array)
        return base64.b64encode(rand_byte_array).decode('utf-8')
    def add_connection(uniqueIdentifier, message=""):
        # -->Send connection request passing its vmid or universal name
        if len(message) > 300:
            return print("Message too long. Max size is 300 characters")
        trackingId = generate_trackingId()
        payload = {"trackingId": trackingId, "message": message, "invitations": [], "excludeInvitations": [
        ], "invitee": {"com.linkedin.voyager.growth.invitation.InviteeProfile": {"profileId": uniqueIdentifier}}}
        headers['accept'] = 'application/vnd.linkedin.normalized+json+2.1'
        request_url = "https://www.linkedin.com/voyager/api/growth/normInvitations"
        response = requests.post(url=request_url, data=json.dumps(
            payload), headers=headers, cookies=cookies_dict)
        if 'accept' in headers:
            del headers['accept']
        if response.status_code == 201:
            return print("Connection request sent!")
        else:
            return print("Connection request not sent!")
    def remove_connection(uniqueIdentifier):
        # -->Remove connection using vmid or universal name
        headers['accept'] = 'application/vnd.linkedin.normalized+json+2.1'
        request_url = f"https://www.linkedin.com/voyager/api/identity/profiles/{uniqueIdentifier}/profileActions?action=disconnect"
        response = requests.post(
            url=request_url, headers=headers, cookies=cookies_dict)
        if 'accept' in headers:
            del headers['accept']
        if response.status_code == 200:
            return print("Connection removed!")
        else:
            return print("Connection not removed!")
    def follow_unfollow_profile(uniqueIdentifier, action):
        # -->Follow or unfollow using vmid
        if action == "unfollow":
            action_variable = "unfollowByEntityUrn"
        elif action == "follow":
            action_variable = "followByEntityUrn"
        headers['accept'] = 'application/vnd.linkedin.normalized+json+2.1'
        payload = {"urn": f"urn:li:fs_followingInfo:{uniqueIdentifier}"}
        request_url = f"https://www.linkedin.com/voyager/api/feed/follows?action={action_variable}"
        response = requests.post(url=request_url, headers=headers, data=json.dumps(
            payload), cookies=cookies_dict)
        if 'accept' in headers:
            del headers['accept']
        if action == "unfollow":
            if response.status_code == 200:
                return print("Unfollowed!")
            else:
                return print(f"Not unfollowed!")
        elif action == "follow":
            if response.status_code == 200:
                return print("Followed!")
            else:
                return print("Not followed!")
    def get_all_connections():
        # -->Get all current connections
        connections = []
        start = 0
        count = 100
        has_more = True
        params = {"decorationId": "com.linkedin.voyager.dash.deco.web.mynetwork.ConnectionListWithProfile-16",
                "start": start, "count": count, "q": "search", "sortType": "RECENTLY_ADDED"}
        while has_more:
            request_url = 'https://www.linkedin.com/voyager/api/relationships/dash/connections'
            response = requests.get(
                url=request_url, params=params, cookies=cookies_dict, headers=headers)
            if response.status_code != 200:
                print(f"Failed to fetch connections: {response.status_code}")
                break
            response_payload = response.json()
            current_connections = [
                element for element in response_payload.get("elements", [])]
            if not current_connections:
                has_more = False
            else:
                connections.extend(current_connections)
                params['start'] += count
        return connections
    def get_all_sent_invitations():
        # -->Get all current sent connection requests
        invitations = []
        start = 0
        count = 100
        has_more = True
        while has_more:
            request_url = f"https://www.linkedin.com/voyager/api/graphql?variables=(start:{start},count:{count},invitationType:CONNECTION)&queryId=voyagerRelationshipsDashSentInvitationViews.ae305855593fe45f647a54949e3b3c96"
            response = requests.get(
                url=request_url, cookies=cookies_dict, headers=headers)
            if response.status_code != 200:
                print(f"Failed to fetch sent invitations: {response.status_code}")
                break
            response_payload = response.json()
            current_invitations = [element for element in safe_extract(
                response_payload, "data", "relationshipsDashSentInvitationViewsByInvitationType", "elements")]
            if not current_invitations:
                has_more = False
            else:
                invitations.extend(current_invitations)
                start += count
        return invitations
    def withdraw_invitation(invitation_id):
        # -->Withdraw a invitation using invitation id
        params = {"action": "withdraw"}
        payload = json.dumps({"invitationId": invitation_id})
        request_url = f"https://www.linkedin.com/voyager/api/relationships/invitations/{invitation_id}"
        response = requests.post(url=request_url, params=params,
                                data=payload, cookies=cookies_dict, headers=headers)
        if response.status_code == 200:
            return print(f"Invitation withdrawed!")
        else:
            return print(f"Invitation not withdrawed!")
    #Using functions
    def get_last_20_conversations():
        # -->get_conversations
        all_conversations_json = get_conversations()
        all_conversations = safe_extract(all_conversations_json, "elements")
        df_all_conversations_final = pd.DataFrame()
        for conversation in all_conversations:
            df_all_conversations_loop = pd.DataFrame()
            conversation_dashEntityUrn = safe_extract(conversation, "dashEntityUrn")
            try:
                conversation_dashEntityUrn = conversation_dashEntityUrn.split(':')[-1]
            except:
                pass
            # Conversation
            conversation_inboxType = safe_extract(conversation, "inboxType")
            conversation_unreadCount = safe_extract(conversation, "unreadCount")
            conversation_lastActivityAt = convertToTimestamp(safe_extract(conversation, "lastActivityAt"))
            conversation_lastReadAt = convertToTimestamp(safe_extract(conversation, "lastReadAt"))
            conversation_archived = safe_extract(conversation, "archived")
            conversation_blocked = safe_extract(conversation, "blocked")
            conversation_starred = safe_extract(conversation, "starred")
            conversation_withNonConnection = safe_extract(conversation, "withNonConnection")
            conversation_muted = safe_extract(conversation, "muted")
            # Latest message
            latest_createdAt = convertToTimestamp(safe_extract(conversation, "events", 0, "createdAt"))
            latest_eventContent = safe_extract(conversation, "events", 0, "eventContent", "com.linkedin.voyager.messaging.event.MessageEvent", "attributedBody", "text")
            latest_firstName = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "firstName")
            latest_lastName = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "lastName")
            latest_dashEntityUrn = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "dashEntityUrn")
            try:
                latest_dashEntityUrn = latest_dashEntityUrn.split(':')[-1]
            except:
                pass
            latest_standardizedPronoun = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "standardizedPronoun")
            latest_occupation = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "occupation")
            latest_objectUrn = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "objectUrn")
            try:
                latest_objectUrn = latest_objectUrn.split(':')[-1]
            except:
                pass
            latest_background_artifacts = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "artifacts")
            latest_background_rootUrl = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "rootUrl")
            latest_banner200x800 = latest_banner350x1400 = None
            if latest_background_artifacts and latest_background_rootUrl:
                for latest_artifact in latest_background_artifacts:
                    latest_file_segment = latest_artifact['fileIdentifyingUrlPathSegment']
                    if '200_800' in latest_file_segment:
                        latest_banner200x800 = f"{latest_background_rootUrl}{latest_file_segment}"
                    elif '350_1400' in latest_file_segment:
                        latest_banner350x1400 = f"{latest_background_rootUrl}{latest_file_segment}"
                    if latest_banner200x800 and latest_banner350x1400:
                        break
            latest_publicIdentifier = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "publicIdentifier")
            latest_picture_artifacts = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "artifacts")
            latest_picture_rootUrl = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "rootUrl")
            latest_picture100x100 = latest_picture200x200 = latest_picture400x400 = latest_picture800x800 = None
            if latest_picture_artifacts and latest_picture_rootUrl:
                for latest_artifact in latest_picture_artifacts:
                    latest_file_segment = latest_artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in latest_file_segment:
                        latest_picture100x100 = f"{latest_picture_rootUrl}{latest_file_segment}"
                    elif '200_200' in latest_file_segment:
                        latest_picture200x200 = f"{latest_picture_rootUrl}{latest_file_segment}"
                    elif '400_400' in latest_file_segment:
                        latest_picture400x400 = f"{latest_picture_rootUrl}{latest_file_segment}"
                    elif '800_800' in latest_file_segment:
                        latest_picture800x800 = f"{latest_picture_rootUrl}{latest_file_segment}"
                    if latest_picture100x100 and latest_picture200x200 and latest_picture400x400 and latest_picture800x800:
                        break
            latest_nameInitials = safe_extract(conversation, "events", 0, "from", "com.linkedin.voyager.messaging.MessagingMember", "nameInitials")
            # Person
            firstName = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "firstName")
            lastName = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "lastName")
            dashEntityUrn = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "dashEntityUrn")
            try:
                dashEntityUrn = dashEntityUrn.split(':')[-1]
            except:
                pass
            standardizedPronoun = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "standardizedPronoun")
            occupation = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "occupation")
            objectUrn = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "objectUrn")
            try:
                objectUrn = objectUrn.split(':')[-1]
            except:
                pass
            background_artifacts = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "artifacts")
            background_rootUrl = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "rootUrl")
            banner200x800 = banner350x1400 = None
            if background_artifacts and background_rootUrl:
                for artifact in background_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '200_800' in file_segment:
                        banner200x800 = f"{background_rootUrl}{file_segment}"
                    elif '350_1400' in file_segment:
                        banner350x1400 = f"{background_rootUrl}{file_segment}"
                    if banner200x800 and banner350x1400:
                        break
            publicIdentifier = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "publicIdentifier")
            picture_artifacts = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "artifacts")
            picture_rootUrl = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "rootUrl")
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            nameInitials = safe_extract(conversation, "participants", 0, "com.linkedin.voyager.messaging.MessagingMember", "nameInitials")
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["conversation_dashEntityUrn", "conversation_inboxType", "conversation_unreadCount", "conversation_lastActivityAt", "conversation_lastReadAt", "conversation_archived", "conversation_blocked", "conversation_starred", "conversation_withNonConnection", "conversation_muted", "latest_createdAt", "latest_eventContent", "latest_firstName", "latest_lastName", "latest_dashEntityUrn", "latest_standardizedPronoun", "latest_occupation", "latest_objectUrn", "latest_banner200x800", "latest_banner350x1400", "latest_publicIdentifier", "latest_picture100x100", "latest_picture200x200", "latest_picture400x400", "latest_picture800x800", "latest_nameInitials", "firstName", "lastName", "dashEntityUrn", "standardizedPronoun", "occupation", "objectUrn", "banner200x800", "banner350x1400", "publicIdentifier", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "nameInitials"]}
            df_all_conversations_loop = pd.DataFrame(selected_vars)
            df_all_conversations_final = pd.concat(
                [df_all_conversations_final, df_all_conversations_loop])
        all_conversations_rename_dict = {
            "conversation_dashEntityUrn": "Conversation - ID",
            "conversation_inboxType": "Conversation - Inbox type",
            "conversation_unreadCount": "Conversation - Unread messages count",
            "conversation_lastActivityAt": "Conversation - Last activity",
            "conversation_lastReadAt": "Conversation - Last read",
            "conversation_archived": "Conversation - Archived?",
            "conversation_blocked": "Conversation - Blocked?",
            "conversation_starred": "Conversation - Starred?",
            "conversation_withNonConnection": "Conversation - With non connection?",
            "conversation_muted": "Conversation - Muted?",
            "latest_createdAt": "Latest message - Creation time",
            "latest_eventContent": "Latest message - Text",
            "latest_firstName": "Latest message - Person first name",
            "latest_lastName": "Latest message - Person last name",
            "latest_dashEntityUrn": "Latest message - Person vmid",
            "latest_standardizedPronoun": "Latest message - Person pronoun",
            "latest_occupation": "Latest message - Occupation",
            "latest_objectUrn": "Latest message - Person user ID",
            "latest_banner200x800": "Latest message - Person banner 200x800",
            "latest_banner350x1400": "Latest message - Person banner 350x1400",
            "latest_publicIdentifier": "Latest message - Person universal name",
            "latest_picture100x100": "Latest message - Person picture 100x100",
            "latest_picture200x200": "Latest message - Person banner 200x200",
            "latest_picture400x400": "Latest message - Person picture 400x400",
            "latest_picture800x800": "Latest message - Person picture 800x800",
            "latest_nameInitials": "Latest message - Person name initials",
            "firstName": "Person - First name",
            "lastName": "Person - Last name",
            "dashEntityUrn": "Person - vmid",
            "standardizedPronoun": "Person - Pronoun",
            "occupation": "Person - Occupation",
            "objectUrn": "Person - User ID",
            "banner200x800": "Person - Banner 200x800",
            "banner350x1400": "Person - Banner 350x1400",
            "publicIdentifier": "Person - Universal name",
            "picture100x100": "Person - Picture 100x100",
            "picture200x200": "Person - Picture 200x200",
            "picture400x400": "Person - Picture 400x400",
            "picture800x800": "Person - Picture 800x800",
            "nameInitials": "Person - Name initials"
        }
        df_all_conversations_final.rename(columns=all_conversations_rename_dict, inplace=True)
        return df_all_conversations_final
    def get_all_messages_from_conversation(dataframe, conversation_id_column_name):
        dataframe.drop_duplicates(subset=[conversation_id_column_name], inplace=True)
        columnName_values = dataframe[conversation_id_column_name].tolist()
        df_all_messages_final = pd.DataFrame()
        for item in columnName_values:
            all_messages_json = get_conversation(item)
            all_messages = safe_extract(all_messages_json, "elements")
            for message in all_messages:
                df_all_messages_loop = pd.DataFrame()
                conversation_dashEntityUrn = item
                createdAt = convertToTimestamp(safe_extract(message, "createdAt"))
                eventContent = safe_extract(message, "eventContent", "com.linkedin.voyager.messaging.event.MessageEvent", "attributedBody", "text")
                firstName = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "firstName")
                lastName = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "lastName")
                dashEntityUrn = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "dashEntityUrn")
                try:
                    dashEntityUrn = dashEntityUrn.split(':')[-1]
                except:
                    pass
                occupation = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "occupation")
                objectUrn = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "objectUrn")
                try:
                    objectUrn = objectUrn.split(':')[-1]
                except:
                    pass
                background_artifacts = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "artifacts")
                background_rootUrl = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "rootUrl")
                banner200x800 = banner350x1400 = None
                if background_artifacts and background_rootUrl:
                    for artifact in background_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '200_800' in file_segment:
                            banner200x800 = f"{background_rootUrl}{file_segment}"
                        elif '350_1400' in file_segment:
                            banner350x1400 = f"{background_rootUrl}{file_segment}"
                        if banner200x800 and banner350x1400:
                            break
                publicIdentifier = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "publicIdentifier")
                picture_artifacts = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "artifacts")
                picture_rootUrl = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "picture", "com.linkedin.common.VectorImage", "rootUrl")
                picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
                if picture_artifacts and picture_rootUrl:
                    for artifact in picture_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '100_100' in file_segment:
                            picture100x100 = f"{picture_rootUrl}{file_segment}"
                        elif '200_200' in file_segment:
                            picture200x200 = f"{picture_rootUrl}{file_segment}"
                        elif '400_400' in file_segment:
                            picture400x400 = f"{picture_rootUrl}{file_segment}"
                        elif '800_800' in file_segment:
                            picture800x800 = f"{picture_rootUrl}{file_segment}"
                        if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                            break
                publicIdentifier = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "miniProfile", "publicIdentifier")
                nameInitials = safe_extract(message, "from", "com.linkedin.voyager.messaging.MessagingMember", "nameInitials")
                all_variables = locals()
                selected_vars = {var: [all_variables[var]] for var in ["conversation_dashEntityUrn", "createdAt", "eventContent", "firstName", "lastName", "dashEntityUrn", "occupation", "objectUrn", "banner200x800", "banner350x1400", "publicIdentifier", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "publicIdentifier", "nameInitials"]}
                df_all_messages_loop = pd.DataFrame(selected_vars)
                df_all_messages_final = pd.concat(
                    [df_all_messages_final, df_all_messages_loop])
        df_all_messages_final['createdAt'] = pd.to_datetime(df_all_messages_final['createdAt'])
        df_all_messages_final.sort_values(by=['conversation_dashEntityUrn', 'createdAt'], inplace=True)
        all_messages_rename_dict = {
            "conversation_dashEntityUrn": "Conversation - ID",
            "createdAt": "Message - Creation time",
            "eventContent": "Message - Text",
            "firstName": "Person - First name",
            "lastName": "Person - Last name",
            "dashEntityUrn": "Person - vmid",
            "occupation": "Person - Occupation",
            "objectUrn": "Person - User ID",
            "banner200x800": "Person - Banner 200x800",
            "banner350x1400": "Person - Banner 350x1400",
            "publicIdentifier": "Person - Universal name",
            "picture100x100": "Person - Picture 100x100",
            "picture200x200": "Person - Picture 200x200",
            "picture400x400": "Person - Picture 400x400",
            "picture800x800": "Person - Picture 800x800",
            "nameInitials": "Person - Name initials"
        }
        df_all_messages_final.rename(columns=all_messages_rename_dict, inplace=True)
        return df_all_messages_final
    def obtain_current_user_profile():
        #-->get_user_profile
        profile_data = get_user_profile()
        plainId = safe_extract(profile_data, "plainId")
        firstName = safe_extract(profile_data, "miniProfile", "firstName")
        lastName = safe_extract(profile_data, "miniProfile", "lastName")
        dashEntityUrn = safe_extract(profile_data, "miniProfile", "dashEntityUrn")
        try:
            dashEntityUrn = dashEntityUrn.split(':')[-1]
        except:
            pass
        occupation = safe_extract(profile_data, "miniProfile", "dashEntityUrn")
        background_artifacts = safe_extract(profile_data, "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "artifacts")
        background_rootUrl = safe_extract(profile_data, "miniProfile", "backgroundImage", "com.linkedin.common.VectorImage", "rootUrl")
        banner200x800 = banner350x1400 = None
        if background_artifacts and background_rootUrl:
            for artifact in background_artifacts:
                file_segment = artifact['fileIdentifyingUrlPathSegment']
                if '200_800' in file_segment:
                    banner200x800 = f"{background_rootUrl}{file_segment}"
                elif '350_1400' in file_segment:
                    banner350x1400 = f"{background_rootUrl}{file_segment}"
                if banner200x800 and banner350x1400:
                    break
        publicIdentifier = safe_extract(profile_data, "miniProfile", "publicIdentifier")
        picture_artifacts = safe_extract(profile_data, "miniProfile", "picture", "com.linkedin.common.VectorImage", "artifacts")
        picture_rootUrl = safe_extract(profile_data, "miniProfile", "picture", "com.linkedin.common.VectorImage", "rootUrl")
        picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
        if picture_artifacts and picture_rootUrl:
            for artifact in picture_artifacts:
                file_segment = artifact['fileIdentifyingUrlPathSegment']
                if '100_100' in file_segment:
                    picture100x100 = f"{picture_rootUrl}{file_segment}"
                elif '200_200' in file_segment:
                    picture200x200 = f"{picture_rootUrl}{file_segment}"
                elif '400_400' in file_segment:
                    picture400x400 = f"{picture_rootUrl}{file_segment}"
                elif '800_800' in file_segment:
                    picture800x800 = f"{picture_rootUrl}{file_segment}"
                if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                    break
        premiumSubscriber = safe_extract(profile_data, "premiumSubscriber")
        all_variables = locals()
        selected_vars = {var: [all_variables[var]] for var in ["plainId", "firstName", "lastName", "dashEntityUrn", "occupation", "banner200x800", "banner350x1400", "publicIdentifier", "picture100x100", "picture200x200", "picture400x400","picture800x800", "premiumSubscriber"]}
        df_profile_data = pd.DataFrame(selected_vars)
        profile_data_rename_dict = {
            "plainId": "User ID",
            "firstName": "First name",
            "lastName": "Last name",
            "dashEntityUrn": "vmid",
            "occupation": "Occupation",
            "banner200x800": "Person - Banner 200x800",
            "banner350x1400": "Person - Banner 350x1400",
            "publicIdentifier": "Universal name",
            "picture100x100": "Person - Picture 100x100",
            "picture200x200": "Person - Picture 200x200",
            "picture400x400": "Person - Picture 400x400",
            "picture800x800": "Person - Picture 800x800",
            "premiumSubscriber": "Premium subscriber?"
        }
        df_profile_data.rename(columns = profile_data_rename_dict, inplace = True)
        return df_profile_data
    def send_message_using_vmid(dataframe, waiting_time_min, waiting_time_max, message_column_name, vmid_column_name, result_column_name):
        profile_data = get_user_profile()
        premiumSubscriber = safe_extract(profile_data, "premiumSubscriber")
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        def send_message_and_store_result_with_waiting(row):
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            send_message(row[message_column_name], get_conversation_id_using_vmid(row[vmid_column_name]), premiumSubscriber)
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            return result.strip()
        tqdm.pandas(desc="Sending messages")
        dataframe[result_column_name] = dataframe.progress_apply(send_message_and_store_result_with_waiting, axis=1)
        return dataframe
    def mark_conversation_as_seen_using_conversation_id(dataframe, waiting_time_min, waiting_time_max, conversation_id_column_name, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        def mark_conversation_as_seen_and_store_result_with_waiting(row):
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            mark_conversation_as_seen(row[conversation_id_column_name])
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            return result.strip()
        tqdm.pandas(desc="Marking as seen conversations")
        dataframe[result_column_name] = dataframe.progress_apply(mark_conversation_as_seen_and_store_result_with_waiting, axis=1)
        return(dataframe)
    def get_all_connection_requests():
        #get_all_invitations
        all_invitations = get_all_invitations()
        df_all_invitations_final = pd.DataFrame()
        for invitation in all_invitations:
            df_all_invitations_loop = pd.DataFrame()
            #Invitation
            entityUrn = safe_extract(invitation, "entityUrn")
            try:
                entityUrn = entityUrn.split(':')[-1]
            except:
                pass
            totalCount = safe_extract(invitation, "insights", 0, "sharedInsight", "com.linkedin.voyager.relationships.shared.SharedConnectionsInsight", "totalCount")
            invitationType = safe_extract(invitation, "invitation", "invitationType")
            try:
                sentTime = convertToTimestamp(safe_extract(invitation, "invitation", "sentTime"))
            except:
                sentTime = safe_extract(invitation, "invitation", "sentTime")
            subtitle = None
            typeLabel = None
            title = None
            #Person
            firstName = safe_extract(invitation, "invitation", "fromMember", "firstName")
            lastName = safe_extract(invitation, "invitation", "fromMember", "lastName")
            fullName = None
            if firstName and lastName:
                fullName = firstName + " " + lastName
                fullName = fullName.strip() if fullName is not None else None
            elif firstName:
                fullName = firstName
                fullName = fullName.strip() if fullName is not None else None
            else:
                fullName = lastName
                fullName = fullName.strip() if fullName is not None else None
            dashEntityUrn = safe_extract(invitation, "invitation", "fromMember", "dashEntityUrn")
            try:
                dashEntityUrn = dashEntityUrn.split(':')[-1]
            except:
                pass
            occupation = safe_extract(invitation, "invitation", "fromMember", "occupation")
            objectUrn = safe_extract(invitation, "invitation", "fromMember", "objectUrn")
            try:
                objectUrn = objectUrn.split(':')[-1]
            except:
                pass
            background_artifacts = safe_extract(invitation, "invitation", "fromMember", "backgroundImage", "com.linkedin.common.VectorImage", "artifacts")
            background_rootUrl = safe_extract(invitation, "invitation", "fromMember", "backgroundImage", "com.linkedin.common.VectorImage", "rootUrl")
            banner200x800 = banner350x1400 = None
            if background_artifacts and background_rootUrl:
                for artifact in background_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '200_800' in file_segment:
                        banner200x800 = f"{background_rootUrl}{file_segment}"
                    elif '350_1400' in file_segment:
                        banner350x1400 = f"{background_rootUrl}{file_segment}"
                    if banner200x800 and banner350x1400:
                        break
            publicIdentifier = safe_extract(invitation, "invitation", "fromMember", "publicIdentifier")
            picture_artifacts = safe_extract(invitation, "invitation", "fromMember", "picture", "com.linkedin.common.VectorImage", "artifacts")
            picture_rootUrl = safe_extract(invitation, "invitation", "fromMember", "picture", "com.linkedin.common.VectorImage", "rootUrl")
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            #Invitation
            customMessage = safe_extract(invitation, "invitation", "customMessage")
            sharedSecret = safe_extract(invitation, "invitation", "sharedSecret")
            unseen = safe_extract(invitation, "invitation", "unseen")
            #CONTENT_SERIES
            check_invitationType = safe_extract(invitation, "genericInvitationView", "invitationType")
            if check_invitationType:
                #Invitation
                entityUrn = safe_extract(invitation, "invitation", "entityUrn")
                try:
                    entityUrn = entityUrn.split(':')[-1]
                except:
                    pass
                invitationType = safe_extract(invitation, "genericInvitationView", "invitationType")
                try:
                    sentTime = convertToTimestamp(safe_extract(invitation, "genericInvitationView", "sentTime"))
                except:
                    sentTime = safe_extract(invitation, "genericInvitationView", "sentTime")
                #Company
                dashEntityUrn = safe_extract(invitation, "genericInvitationView", "primaryImage", "attributes", 0, "miniCompany", "objectUrn")
                try:
                    dashEntityUrn = dashEntityUrn.split(':')[-1]
                except:
                    pass
                fullName = safe_extract(invitation, "genericInvitationView", "primaryImage", "attributes", 0, "miniCompany", "name")
                picture_artifacts = safe_extract(invitation, "genericInvitationView", "primaryImage", "attributes", 0, "miniCompany", "logo", "com.linkedin.common.VectorImage", "artifacts")
                picture_rootUrl = safe_extract(invitation, "genericInvitationView", "primaryImage", "attributes", 0, "miniCompany", "logo", "com.linkedin.common.VectorImage", "rootUrl")
                picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
                if picture_artifacts and picture_rootUrl:
                    for artifact in picture_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '100_100' in file_segment:
                            picture100x100 = f"{picture_rootUrl}{file_segment}"
                        elif '200_200' in file_segment:
                            picture200x200 = f"{picture_rootUrl}{file_segment}"
                        elif '400_400' in file_segment:
                            picture400x400 = f"{picture_rootUrl}{file_segment}"
                        elif '800_800' in file_segment:
                            picture800x800 = f"{picture_rootUrl}{file_segment}"
                        if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                            break
                publicIdentifier = safe_extract(invitation, "genericInvitationView", "primaryImage", "attributes", 0, "miniCompany", "universalName")
                #Invitation
                subtitle = safe_extract(invitation, "genericInvitationView", "subtitle", "text")
                typeLabel = safe_extract(invitation, "genericInvitationView", "typeLabel")
                title = safe_extract(invitation, "genericInvitationView", "title", "text")
                sharedSecret = safe_extract(invitation, "genericInvitationView", "sharedSecret")
                unseen = safe_extract(invitation, "genericInvitationView", "unseen")
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["entityUrn", "totalCount", "invitationType", "sentTime", "subtitle", "typeLabel", "title", "firstName", "lastName", "fullName", "dashEntityUrn", "occupation", "objectUrn", "banner200x800", "banner350x1400", "publicIdentifier", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "customMessage", "sharedSecret", "unseen"]}
            df_all_invitations_loop = pd.DataFrame(selected_vars)
            df_all_invitations_final = pd.concat([df_all_invitations_final, df_all_invitations_loop])
        all_invitations_rename_dict = {
            "entityUrn": "Invitation - ID",
            "totalCount": "Invitation - Shared connections count",
            "invitationType": "Invitation - Type",
            "sentTime": "Invitation - Creation time",
            "subtitle": "Invitation - Subtitle",
            "typeLabel": "Invitation - Type label",
            "title": "Invitation - Title",
            "firstName": "First name",
            "lastName": "Last name",
            "fullName": "Full name",
            "dashEntityUrn": "vmid",
            "occupation": "Occupation",
            "objectUrn": "User ID",
            "banner200x800": "Banner 200x800",
            "banner350x1400": "Banner 350x1400",
            "publicIdentifier": "Universal name",
            "picture100x100": "Picture 100x100",
            "picture200x200": "Picture 200x200",
            "picture400x400": "Picture 400x400",
            "picture800x800": "Picture 800x800",
            "customMessage": "Invitation - Custom message?",
            "sharedSecret": "Invitation - Shared secret",
            "unseen": "Invitation - Unseen?"
        }
        df_all_invitations_final.rename(columns = all_invitations_rename_dict, inplace = True)
        return df_all_invitations_final
    def accept_or_remove_connection_requests(dataframe, waiting_time_min, waiting_time_max, action, invitation_id_column_name, invitation_shared_secret_column_name, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        def accept_or_ignore_and_store_result_with_waiting(row):
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            reply_invitation(row[invitation_id_column_name], row[invitation_shared_secret_column_name], action, True)
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            return result.strip()
        tqdm.pandas(desc="Accepting/ignoring connection requests")
        dataframe[result_column_name] = dataframe.progress_apply(accept_or_ignore_and_store_result_with_waiting, axis=1)
        return dataframe
    def send_connection_requests(dataframe, waiting_time_min, waiting_time_max, vmid_column_name, message_column_name, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        def send_connection_and_store_result_with_waiting(row):
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            add_connection(row[vmid_column_name], row[message_column_name])
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            return result.strip()
        tqdm.pandas(desc="Sending connection requests")
        dataframe[result_column_name] = dataframe.progress_apply(send_connection_and_store_result_with_waiting, axis=1)
        return dataframe
    def remove_connections(dataframe, waiting_time_min, waiting_time_max, unique_identifier_column_name, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        def send_connection_and_store_result_with_waiting(row):
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            remove_connection(row[unique_identifier_column_name])
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            return result.strip()
        tqdm.pandas(desc="Removing connections")
        dataframe[result_column_name] = dataframe.progress_apply(send_connection_and_store_result_with_waiting, axis=1)
        return dataframe
    def follow_or_unfollow_profiles(dataframe, waiting_time_min, waiting_time_max, vmid_column_name, action, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        def follow_or_unfollow_and_store_result_with_waiting(row):
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            follow_unfollow_profile("urn:li:fsd_profile:"+row[vmid_column_name], action)
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            return result.strip()
        tqdm.pandas(desc="Following/unfollowing profiles")
        dataframe[result_column_name] = dataframe.progress_apply(follow_or_unfollow_and_store_result_with_waiting, axis=1)
        return dataframe
    def get_all_connections_profiles():
        #-->get_all_connections
        all_connections = get_all_connections()
        df_all_connections_final = pd.DataFrame()
        for connection in all_connections:
            df_all_connections_loop = pd.DataFrame()
            lastName = safe_extract(connection, "connectedMemberResolutionResult", "lastName")
            firstName = safe_extract(connection, "connectedMemberResolutionResult", "firstName")
            picture_artifacts = safe_extract(connection, "connectedMemberResolutionResult", "profilePicture", "displayImageReference", "vectorImage", "artifacts")
            picture_rootUrl = safe_extract(connection, "connectedMemberResolutionResult", "profilePicture", "displayImageReference", "vectorImage", "rootUrl")
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            entityUrn = safe_extract(connection, "connectedMemberResolutionResult", "entityUrn")
            try:
                entityUrn = entityUrn.split(':')[-1]
            except:
                pass
            headline = safe_extract(connection, "connectedMemberResolutionResult", "headline")
            publicIdentifier = safe_extract(connection, "connectedMemberResolutionResult", "publicIdentifier")
            createdAt = convertToTimestamp(safe_extract(connection, "createdAt"))
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["firstName", "lastName", "headline", "entityUrn", "publicIdentifier", "createdAt", "picture100x100", "picture200x200", "picture400x400", "picture800x800"]}
            df_all_connections_loop = pd.DataFrame(selected_vars)
            df_all_connections_final = pd.concat([df_all_connections_final, df_all_connections_loop])
        all_conversations_rename_dict = {
            "firstName": "First name",
            "lastName": "Last name",
            "headline": "Headline",
            "entityUrn": "vmid",
            "publicIdentifier": "Universal name",
            "createdAt": "Creation time",
            "picture100x100": "Picture 100x100",
            "picture200x200": "Picture 200x200",
            "picture400x400": "Picture 400x400",
            "picture800x800": "Picture 800x800"
        }
        df_all_connections_final.rename(columns = all_conversations_rename_dict, inplace = True)
        df_all_connections_final = df_all_connections_final.dropna(subset=['vmid'])
        df_all_connections_final = df_all_connections_final[df_all_connections_final['vmid'] != '']
        return df_all_connections_final
    def get_all_conversations_with_connections(waiting_time_min, waiting_time_max):
        #-->get_all_connections
        all_connections = get_all_connections()
        df_all_connections_final = pd.DataFrame()
        for connection in all_connections:
            df_all_connections_loop = pd.DataFrame()
            lastName = safe_extract(connection, "connectedMemberResolutionResult", "lastName")
            firstName = safe_extract(connection, "connectedMemberResolutionResult", "firstName")
            picture_artifacts = safe_extract(connection, "connectedMemberResolutionResult", "profilePicture", "displayImageReference", "vectorImage", "artifacts")
            picture_rootUrl = safe_extract(connection, "connectedMemberResolutionResult", "profilePicture", "displayImageReference", "vectorImage", "rootUrl")
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            entityUrn = safe_extract(connection, "connectedMemberResolutionResult", "entityUrn")
            try:
                entityUrn = entityUrn.split(':')[-1]
            except:
                pass
            headline = safe_extract(connection, "connectedMemberResolutionResult", "headline")
            publicIdentifier = safe_extract(connection, "connectedMemberResolutionResult", "publicIdentifier")
            createdAt = convertToTimestamp(safe_extract(connection, "createdAt"))
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["firstName", "lastName", "headline", "entityUrn", "publicIdentifier", "createdAt", "picture100x100", "picture200x200", "picture400x400", "picture800x800"]}
            df_all_connections_loop = pd.DataFrame(selected_vars)
            df_all_connections_final = pd.concat([df_all_connections_final, df_all_connections_loop])
        all_conversations_rename_dict = {
            "firstName": "First name",
            "lastName": "Last name",
            "headline": "Headline",
            "entityUrn": "vmid",
            "publicIdentifier": "Universal name",
            "createdAt": "Creation time",
            "picture100x100": "Picture 100x100",
            "picture200x200": "Picture 200x200",
            "picture400x400": "Picture 400x400",
            "picture800x800": "Picture 800x800"
        }
        df_all_connections_final.rename(columns = all_conversations_rename_dict, inplace = True)
        df_all_connections_final.replace('', np.nan, inplace = True)
        columns_to_ignore = {'Creation time'}
        df_all_connections_final.dropna(how = 'all', subset = [col for col in df_all_connections_final.columns if col not in columns_to_ignore], inplace = True)
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        #get_all_conversations_with_connections
        def get_all_conversations_with_connections(vmid):
            conversation_id = get_conversation_id_using_vmid(vmid)
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            return conversation_id
        tqdm.pandas(desc="Searching for conversation IDs")
        df_all_connections_final['Conversation ID'] = df_all_connections_final['vmid'].progress_apply(get_all_conversations_with_connections)
        return df_all_connections_final
    def get_all_sent_connection_requests():
        #get_all_sent_invitations
        all_sent_invitations = get_all_sent_invitations()
        df_all_sent_invitations_final = pd.DataFrame()
        for invitations in all_sent_invitations:
            df_all_sent_invitations_loop = pd.DataFrame()
            #Invitee
            invitee_cardActionTarget = safe_extract(invitations, "cardActionTarget")
            #Invitation
            sentTimeLabel = safe_extract(invitations, "sentTimeLabel")
            try:
                sentTimeLabel = convertToTimestamp(sentTimeLabel)
            except:
                sentTimeLabel = safe_extract(invitations, "sentTimeLabel")
            #Invitee
            invitee_firstName = safe_extract(invitations, "invitation", "inviteeMemberResolutionResult", "firstName")
            invitee_lastName = safe_extract(invitations, "invitation", "inviteeMemberResolutionResult", "lastName")
            picture_artifacts = safe_extract(invitations, "invitation", "inviteeMemberResolutionResult", "profilePicture", "displayImageReferenceResolutionResult", "vectorImage", "artifacts")
            picture_rootUrl = safe_extract(invitations, "invitation", "inviteeMemberResolutionResult", "profilePicture", "displayImageReferenceResolutionResult", "vectorImage", "rootUrl")
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            invitee_entityUrn = safe_extract(invitations, "invitation", "inviteeMemberResolutionResult", "entityUrn")
            try:
                invitee_entityUrn = invitee_entityUrn.split(':')[-1]
            except:
                pass
            #Invitation
            inviterFollowingInvitee = safe_extract(invitations, "invitation", "inviterFollowingInvitee")
            genericInvitationType = safe_extract(invitations, "invitation", "genericInvitationType")
            invitationState = safe_extract(invitations, "invitation", "invitationState")
            invitationId = safe_extract(invitations, "invitation", "invitationId")
            if invitationId:
                invitationId = str(invitationId)
            message = safe_extract(invitations, "invitation", "message")
            #Inviter
            inviter_firstName = safe_extract(invitations, "invitation", "genericInviter", "memberProfileUrn", "firstName")
            inviter_lastName = safe_extract(invitations, "invitation", "genericInviter", "memberProfileUrn", "lastName")
            inviter_objectUrn = safe_extract(invitations, "invitation", "genericInviter", "memberProfileUrn", "objectUrn")
            try:
                inviter_objectUrn = inviter_objectUrn.split(':')[-1]
            except:
                pass
            inviter_entityUrn = safe_extract(invitations, "invitation", "genericInviter", "memberProfileUrn", "entityUrn")
            try:
                inviter_entityUrn = inviter_entityUrn.split(':')[-1]
            except:
                pass
            try:
                inviter_entityUrn = inviter_entityUrn.split(':')[-1]
            except:
                pass
            inviter_publicIdentifier = safe_extract(invitations, "invitation", "genericInviter", "memberProfileUrn", "publicIdentifier")
            #Invitation
            invitationType = safe_extract(invitations, "invitation", "invitationType")
            #Invitee
            subtitle = safe_extract(invitations, "subtitle", "text")
            title = safe_extract(invitations, "title", "text")
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["invitationId", "sentTimeLabel", "message", "inviterFollowingInvitee", "genericInvitationType", "invitationType", "invitationState", "invitee_firstName", "invitee_lastName", "title", "subtitle", "invitee_cardActionTarget", "invitee_entityUrn", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "inviter_firstName", "inviter_lastName", "inviter_objectUrn", "inviter_entityUrn", "inviter_publicIdentifier"]}
            df_all_sent_invitations_loop = pd.DataFrame(selected_vars)
            df_all_sent_invitations_final = pd.concat([df_all_sent_invitations_final, df_all_sent_invitations_loop])
        all_sent_invitations_rename_dict = {
            "invitationId": "Invitation - ID",
            "sentTimeLabel": "Invitation - Sent time",
            "message": "Invitation - Message",
            "inviterFollowingInvitee": "Invitation - Inviter following invitee?",
            "genericInvitationType": "Invitation - Generic invitation type",
            "invitationType": "Invitation - Type",
            "invitationState": "Invitation - State",
            "invitee_firstName": "Invitee - First name",
            "invitee_lastName": "Invitee - Last name",
            "title": "Invitee - Full name",
            "subtitle": "Invitee - Occupation",
            "invitee_cardActionTarget": "Invitee - Public LinkedIn URL",
            "invitee_entityUrn": "Invitee - vmid",
            "picture100x100": "Invitee - Picture 100x100",
            "picture200x200": "Invitee - Picture 200x200",
            "picture400x400": "Invitee - Picture 400x400",
            "picture800x800": "Invitee - Picture 800x800",
            "inviter_firstName": "Inviter - First name",
            "inviter_lastName": "Inviter - Last name",
            "inviter_objectUrn": "Inviter - User ID",
            "inviter_entityUrn": "Invitee - vmid",
            "inviter_publicIdentifier": "Invitee - Universal name"
        }
        df_all_sent_invitations_final.rename(columns = all_sent_invitations_rename_dict, inplace = True)
        return df_all_sent_invitations_final
    def withdraw_connection_requests(dataframe, waiting_time_min, waiting_time_max, invitation_id_column_name, result_column_name):
        waiting_time_min_seconds = int(waiting_time_min)
        waiting_time_max_seconds = int(waiting_time_max)
        def withdraw_invitation_with_waiting(row):
            captured_output = StringIO()
            old_stdout = sys.stdout
            sys.stdout = captured_output
            withdraw_invitation(row[invitation_id_column_name])
            sys.stdout = old_stdout
            result = captured_output.getvalue()
            captured_output.close()
            time.sleep(random.randint(waiting_time_min_seconds, waiting_time_max_seconds))
            return result.strip()
        tqdm.pandas(desc="Withdrawing connection requests")
        dataframe[result_column_name] = dataframe.progress_apply(withdraw_invitation_with_waiting, axis=1)
        return dataframe
    #Check script_type
    if script_type == "get_last_20_conversations":
        dataframe_result = get_last_20_conversations()
        return dataframe_result
    if script_type == "get_all_messages_from_conversation":
        dataframe_result = get_all_messages_from_conversation(dataframe, conversation_id_column_name)
        return dataframe_result
    if script_type == "obtain_current_user_profile":
        dataframe_result = obtain_current_user_profile()
        return dataframe_result
    if script_type == "send_message_using_vmid":
        dataframe_result = send_message_using_vmid(dataframe, waiting_time_min, waiting_time_max, message_column_name, vmid_column_name, result_column_name)
        return dataframe_result
    if script_type == "mark_conversation_as_seen_using_conversation_id":
        dataframe_result = mark_conversation_as_seen_using_conversation_id(dataframe, waiting_time_min, waiting_time_max, conversation_id_column_name, result_column_name)
        return dataframe_result
    if script_type == "get_all_connection_requests":
        dataframe_result = get_all_connection_requests()
        return dataframe_result
    if script_type == "accept_or_remove_connection_requests":
        dataframe_result = accept_or_remove_connection_requests(dataframe, waiting_time_min, waiting_time_max, action, invitation_id_column_name, invitation_shared_secret_column_name, result_column_name)
        return dataframe_result
    if script_type == "send_connection_requests":
        dataframe_result = send_connection_requests(dataframe, waiting_time_min, waiting_time_max, vmid_column_name, message_column_name, result_column_name)
        return dataframe_result
    if script_type == "remove_connections":
        dataframe_result = remove_connections(dataframe, waiting_time_min, waiting_time_max, unique_identifier_column_name, result_column_name)
        return dataframe_result
    if script_type == "follow_or_unfollow_profiles":
        dataframe_result = follow_or_unfollow_profiles(dataframe, waiting_time_min, waiting_time_max, vmid_column_name, action, result_column_name)
        return dataframe_result
    if script_type == "get_all_connections_profiles":
        dataframe_result = get_all_connections_profiles()
        return dataframe_result
    if script_type == "get_all_conversations_with_connections":
        dataframe_result = get_all_conversations_with_connections(waiting_time_min, waiting_time_max)
        return dataframe_result
    if script_type == "get_all_sent_connection_requests":
        dataframe_result = get_all_sent_connection_requests()
        return dataframe_result
    if script_type == "withdraw_connection_requests":
        dataframe_result = withdraw_connection_requests(dataframe, waiting_time_min, waiting_time_max, invitation_id_column_name, result_column_name)
        return dataframe_result
#Scrape
def scrape_sales_navigator_lead_export(li_at, spreadsheet_url, sheet_name, column_name, file_name='sales_navigator_lead_export'):
    """
    Orchestrates the extraction of Sales Navigator lead data and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        column_name (str): The name of the column in the spreadsheet containing LinkedIn URLs.
        file_name (str, optional): The name of the output CSV file. Defaults to 'sales_navigator_lead_export'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    if li_a:
        dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
        if dataframe is not None and not dataframe.empty:
            dataframe_result = sales_navigator_lead_export(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name)
            write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
            write_into_csv(dataframe_result, file_name)
    else:
        print("This LinkedIn account doesn't have Sales Navigator suscription.")
def scrape_sales_navigator_account_export(li_at, spreadsheet_url, sheet_name, column_name, file_name='sales_navigator_account_export'):
    """
    Orchestrates the extraction of Sales Navigator account data and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        column_name (str): The name of the column in the spreadsheet containing LinkedIn URLs.
        file_name (str, optional): The name of the output CSV file. Defaults to 'sales_navigator_account_export'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    if li_a:
        dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
        if dataframe is not None and not dataframe.empty:
            dataframe_result = sales_navigator_account_export(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name)
            write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
            write_into_csv(dataframe_result, file_name)
    else:
        print("This LinkedIn account doesn't have Sales Navigator suscription.")
def scrape_linkedin_account(li_at, spreadsheet_url, sheet_name, column_name, location_count, file_name='linkedin_account'):
    """
    Orchestrates the extraction of LinkedIn account data and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        column_name (str): The name of the column in the spreadsheet containing LinkedIn URLs.
        file_name (str, optional): The name of the output CSV file. Defaults to 'linkedin_account'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        dataframe_result = linkedin_account(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name, cookies_dict, location_count)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)



def scrape_linkedin_lead(li_at, spreadsheet_url, sheet_name, column_name, file_name='linkedin_lead'):

    """
    Orchestrates the extraction of LinkedIn lead data and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        column_name (str): The name of the column in the spreadsheet containing LinkedIn URLs.
        file_name (str, optional): The name of the output CSV file. Defaults to 'linkedin_lead'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        dataframe_result = linkedin_lead(csrf_token, dataframe, column_name, cookies_dict)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def scrape_company_activity_extractor(li_at, spreadsheet_url, sheet_name, column_name, file_name='company_activity_extractor'):
    """
    Orchestrates the extraction of LinkedIn company activity data and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        column_name (str): The name of the column in the spreadsheet containing LinkedIn URLs.
        file_name (str, optional): The name of the output CSV file. Defaults to 'company_activity_extractor'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        dataframe_result = company_activity_extractor(csrf_token, dataframe, column_name, cookies_dict)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def scrape_job_offers_extractor(li_at, spreadsheet_url, sheet_name, column_name, file_name='job_offers_extractor'):
    """
    Orchestrates the extraction of LinkedIn job offer data and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        column_name (str): The name of the column in the spreadsheet containing LinkedIn URLs.
        file_name (str, optional): The name of the output CSV file. Defaults to 'job_offers_extractor'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        dataframe_result = job_offers_extractor(csrf_token, dataframe, column_name, cookies_dict)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def scrape_job_offers_details_extractor(li_at, spreadsheet_url, sheet_name, column_name, file_name='job_offers_details_extractor'):
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        dataframe_result = job_offers_details_extractor(csrf_token, dataframe, column_name, cookies_dict)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def scrape_post_commenters_extractor(li_at, spreadsheet_url, sheet_name, column_name, file_name='post_commenters_extractor'):
    """
    Orchestrates the extraction of LinkedIn post commenters' data and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        column_name (str): The name of the column in the spreadsheet containing LinkedIn post URLs.
        file_name (str, optional): The name of the output CSV file. Defaults to 'post_commenters_extractor'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        dataframe_result = post_commenters_extractor(csrf_token, dataframe, column_name, cookies_dict)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def scrape_profile_activity_extractor(li_at, spreadsheet_url, sheet_name, column_name, file_name='profile_activity_extractor'):
    """
    Orchestrates the extraction of LinkedIn profile activity data and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        column_name (str): The name of the column in the spreadsheet containing LinkedIn profile URLs.
        file_name (str, optional): The name of the output CSV file. Defaults to 'profile_activity_extractor'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        dataframe_result = profile_activity_extractor(csrf_token, dataframe, column_name, cookies_dict)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
#Search
def search_people_search_first_name_last_name_company_name(li_at, spreadsheet_url, sheet_name, first_name_column_name, last_name_column_name, company_name_column_name, file_name='people_search_first_name_last_name_company_name'):
    """
    Orchestrates the LinkedIn search for people by first name, last name, and company name, and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        first_name_column_name (str): The name of the column in the spreadsheet containing first names.
        last_name_column_name (str): The name of the column in the spreadsheet containing last names.
        company_name_column_name (str): The name of the column in the spreadsheet containing company names.
        file_name (str, optional): The name of the output CSV file. Defaults to 'people_search_first_name_last_name_company_name'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'people_search_first_name_last_name_company_name'
        query_column_name = None
        company_column_name = None
        dataframe_result = linkedin_search_scripts(csrf_token, dataframe, script_type, first_name_column_name, last_name_column_name, company_name_column_name, query_column_name, company_column_name, cookies_dict)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def search_people_search_any_query(li_at, spreadsheet_url, sheet_name, query_column_name, file_name='people_search_any_query'):
    """
    Orchestrates the LinkedIn search for people using any query and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        query_column_name (str): The name of the column in the spreadsheet containing search queries.
        file_name (str, optional): The name of the output CSV file. Defaults to 'people_search_any_query'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'people_search_any_query'
        first_name_column_name = None
        last_name_column_name = None
        company_name_column_name = None
        company_column_name = None
        dataframe_result = linkedin_search_scripts(csrf_token, dataframe, script_type, first_name_column_name, last_name_column_name, company_name_column_name, query_column_name, company_column_name, cookies_dict)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def search_company_search_company_name(li_at, spreadsheet_url, sheet_name, company_column_name, file_name='company_search_company_name'):
    """
    Orchestrates the LinkedIn search for companies by company name and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        company_column_name (str): The name of the column in the spreadsheet containing company names.
        file_name (str, optional): The name of the output CSV file. Defaults to 'company_search_company_name'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'company_search_company_name'
        first_name_column_name = None
        last_name_column_name = None
        company_name_column_name = None
        query_column_name = None
        dataframe_result = linkedin_search_scripts(csrf_token, dataframe, script_type, first_name_column_name, last_name_column_name, company_name_column_name, query_column_name, company_column_name, cookies_dict)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
#Outreach
def outreach_get_last_20_conversations(li_at, spreadsheet_url, sheet_name, file_name='get_last_20_conversations'):
    """
    Orchestrates the retrieval of the last 20 LinkedIn conversations and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        file_name (str, optional): The name of the output CSV file. Defaults to 'get_last_20_conversations'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    script_type = "get_last_20_conversations"
    dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
    if dataframe_result is not None and not dataframe_result.empty:
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_get_all_messages_from_conversation(li_at, spreadsheet_url, sheet_name, conversation_id_column_name, file_name='get_all_messages_from_conversation'):
    """
    Orchestrates the retrieval of all messages from specified LinkedIn conversations and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        conversation_id_column_name (str): The name of the column in the spreadsheet containing conversation IDs.
        file_name (str, optional): The name of the output CSV file. Defaults to 'get_all_messages_from_conversation'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'get_all_messages_from_conversation'
        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type, dataframe=dataframe, conversation_id_column_name=conversation_id_column_name)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_obtain_current_user_profile(li_at, spreadsheet_url, sheet_name, file_name='obtain_current_user_profile'):
    """
    Orchestrates the retrieval of the current LinkedIn user profile and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        file_name (str, optional): The name of the output CSV file. Defaults to 'obtain_current_user_profile'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    script_type = "obtain_current_user_profile"
    dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
    if dataframe_result is not None and not dataframe_result.empty:
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_send_message_using_vmid(li_at, spreadsheet_url, sheet_name, waiting_time_min, waiting_time_max, message_column_name, vmid_column_name, result_column_name, file_name='send_message_using_vmid'):
    """
    Orchestrates sending messages using LinkedIn VMIDs and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        waiting_time_min (int): The minimum waiting time between sending messages.
        waiting_time_max (int): The maximum waiting time between sending messages.
        message_column_name (str): The name of the column in the spreadsheet containing messages.
        vmid_column_name (str): The name of the column in the spreadsheet containing VMIDs.
        result_column_name (str): The name of the column in the spreadsheet for recording results.
        file_name (str, optional): The name of the output CSV file. Defaults to 'send_message_using_vmid'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'send_message_using_vmid'
        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type, dataframe=dataframe, waiting_time_min=waiting_time_min, waiting_time_max=waiting_time_max, message_column_name=message_column_name, vmid_column_name=vmid_column_name, result_column_name=result_column_name)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_mark_conversation_as_seen_using_conversation_id(li_at, spreadsheet_url, sheet_name, waiting_time_min, waiting_time_max, conversation_id_column_name, result_column_name, file_name='mark_conversation_as_seen_using_conversation_id'):
    """
    Orchestrates marking LinkedIn conversations as seen using conversation IDs and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        waiting_time_min (int): The minimum waiting time between marking conversations as seen.
        waiting_time_max (int): The maximum waiting time between marking conversations as seen.
        conversation_id_column_name (str): The name of the column in the spreadsheet containing conversation IDs.
        result_column_name (str): The name of the column in the spreadsheet for recording results.
        file_name (str, optional): The name of the output CSV file. Defaults to 'mark_conversation_as_seen_using_conversation_id'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'mark_conversation_as_seen_using_conversation_id'
        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type, dataframe=dataframe, waiting_time_min=waiting_time_min, waiting_time_max=waiting_time_max, conversation_id_column_name=conversation_id_column_name, result_column_name=result_column_name)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_get_all_connection_requests(li_at, spreadsheet_url, sheet_name, file_name='get_all_connection_requests'):
    """
    Orchestrates the retrieval of all LinkedIn connection requests and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        file_name (str, optional): The name of the output CSV file. Defaults to 'get_all_connection_requests'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    script_type = "get_all_connection_requests"
    dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
    if dataframe_result is not None and not dataframe_result.empty:
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_accept_or_remove_connection_requests(li_at, spreadsheet_url, sheet_name, waiting_time_min, waiting_time_max, action, invitation_id_column_name, invitation_shared_secret_column_name, result_column_name, file_name='accept_or_remove_connection_requests'):
    """
    Orchestrates accepting or removing LinkedIn connection requests and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        waiting_time_min (int): The minimum waiting time between processing connection requests.
        waiting_time_max (int): The maximum waiting time between processing connection requests.
        action (str): The action to perform on the connection requests ('accept' or 'remove').
        invitation_id_column_name (str): The name of the column in the spreadsheet containing invitation IDs.
        invitation_shared_secret_column_name (str): The name of the column in the spreadsheet containing invitation shared secrets.
        result_column_name (str): The name of the column in the spreadsheet for recording results.
        file_name (str, optional): The name of the output CSV file. Defaults to 'accept_or_remove_connection_requests'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'accept_or_remove_connection_requests'
        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type, dataframe=dataframe, waiting_time_min=waiting_time_min, waiting_time_max=waiting_time_max, action=action, invitation_id_column_name=invitation_id_column_name, invitation_shared_secret_column_name=invitation_shared_secret_column_name, result_column_name=result_column_name)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_send_connection_requests(li_at, spreadsheet_url, sheet_name, waiting_time_min, waiting_time_max, vmid_column_name, message_column_name, result_column_name, file_name='send_connection_requests'):
    """
    Orchestrates sending LinkedIn connection requests and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        waiting_time_min (int): The minimum waiting time between sending connection requests.
        waiting_time_max (int): The maximum waiting time between sending connection requests.
        vmid_column_name (str): The name of the column in the spreadsheet containing VMIDs.
        message_column_name (str): The name of the column in the spreadsheet containing messages.
        result_column_name (str): The name of the column in the spreadsheet for recording results.
        file_name (str, optional): The name of the output CSV file. Defaults to 'send_connection_requests'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'send_connection_requests'
        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type, dataframe=dataframe, waiting_time_min=waiting_time_min, waiting_time_max=waiting_time_max, vmid_column_name=vmid_column_name, message_column_name=message_column_name, result_column_name=result_column_name)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_remove_connections(li_at, spreadsheet_url, sheet_name, waiting_time_min, waiting_time_max, unique_identifier_column_name, result_column_name, file_name='remove_connections'):
    """
    Orchestrates removing LinkedIn connections and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        waiting_time_min (int): The minimum waiting time between removing connections.
        waiting_time_max (int): The maximum waiting time between removing connections.
        unique_identifier_column_name (str): The name of the column in the spreadsheet containing unique identifiers (VMIDs or universal names).
        result_column_name (str): The name of the column in the spreadsheet for recording results.
        file_name (str, optional): The name of the output CSV file. Defaults to 'remove_connections'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'remove_connections'
        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type, waiting_time_min=waiting_time_min, waiting_time_max=waiting_time_max, unique_identifier_column_name=unique_identifier_column_name, result_column_name=result_column_name)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_follow_or_unfollow_profiles(li_at, spreadsheet_url, sheet_name, waiting_time_min, waiting_time_max, vmid_column_name, action, result_column_name, file_name='follow_or_unfollow_profiles'):
    """
    Orchestrates following or unfollowing LinkedIn profiles and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        waiting_time_min (int): The minimum waiting time between following or unfollowing profiles.
        waiting_time_max (int): The maximum waiting time between following or unfollowing profiles.
        vmid_column_name (str): The name of the column in the spreadsheet containing VMIDs.
        action (str): The action to perform on the profiles ('follow' or 'unfollow').
        result_column_name (str): The name of the column in the spreadsheet for recording results.
        file_name (str, optional): The name of the output CSV file. Defaults to 'follow_or_unfollow_profiles'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'follow_or_unfollow_profiles'
        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type, dataframe=dataframe, waiting_time_min=waiting_time_min, waiting_time_max=waiting_time_max, vmid_column_name=vmid_column_name, action=action, result_column_name=result_column_name)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_get_all_connections_profiles(li_at, spreadsheet_url, sheet_name, file_name='get_all_connections_profiles'):
    """
    Orchestrates the retrieval of all LinkedIn connections profiles and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        file_name (str, optional): The name of the output CSV file. Defaults to 'get_all_connections_profiles'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    script_type = "get_all_connections_profiles"
    dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
    if dataframe_result is not None and not dataframe_result.empty:
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_get_all_conversations_with_connections(li_at, spreadsheet_url, sheet_name, waiting_time_min, waiting_time_max, file_name='get_all_conversations_with_connections'):
    """
    Orchestrates the retrieval of all LinkedIn conversations with connections and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        waiting_time_min (int): The minimum waiting time between retrieving conversations.
        waiting_time_max (int): The maximum waiting time between retrieving conversations.
        file_name (str, optional): The name of the output CSV file. Defaults to 'get_all_conversations_with_connections'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    script_type = "get_all_conversations_with_connections"
    dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type, waiting_time_min=waiting_time_min, waiting_time_max=waiting_time_max)
    if dataframe_result is not None and not dataframe_result.empty:
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_get_all_sent_connection_requests(li_at, spreadsheet_url, sheet_name, file_name='get_all_sent_connection_requests'):
    """
    Orchestrates the retrieval of all sent LinkedIn connection requests and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        file_name (str, optional): The name of the output CSV file. Defaults to 'get_all_sent_connection_requests'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    script_type = "get_all_sent_connection_requests"
    dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
    if dataframe_result is not None and not dataframe_result.empty:
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)
def outreach_withdraw_connection_requests(li_at, spreadsheet_url, sheet_name, invitation_id_column_name, waiting_time_min, waiting_time_max, result_column_name, file_name='withdraw_connection_requests'):
    """
    Orchestrates withdrawing LinkedIn connection requests and writes the results to a spreadsheet and a CSV file.

    Args:
        li_at (str): The LinkedIn authentication cookie.
        spreadsheet_url (str): The URL of the Google Spreadsheet.
        sheet_name (str): The name of the sheet within the Google Spreadsheet.
        invitation_id_column_name (str): The name of the column in the spreadsheet containing invitation IDs.
        result_column_name (str): The name of the column in the spreadsheet for recording results.
        file_name (str, optional): The name of the output CSV file. Defaults to 'withdraw_connection_requests'.

    Returns:
        None
    """
    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.get_event_loop().run_until_complete(retrieve_tokens(li_at))
    dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)
    if dataframe is not None and not dataframe.empty:
        script_type = 'withdraw_connection_requests'
        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type, dataframe=dataframe, waiting_time_min=waiting_time_min, waiting_time_max=waiting_time_max, invitation_id_column_name=invitation_id_column_name, result_column_name=result_column_name)
        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
        write_into_csv(dataframe_result, file_name)


st.title("LinkedIn Scripts")

# Dropdown for selecting the scraper function
option = st.selectbox(
    "Select Scraper Type",
    ("Select Scraper Type","LinkedIn Account Scraper", "Profile Scraper","Account Search Export","Lead Search Export")
)

# Conditional inputs based on the selected scraper type
if option == "LinkedIn Account Scraper":
    st.write("The LinkedIn Company Scraper purpose is to extract all the public data available from a LinkedIn company page. Use this when you have a list of company LinkedIn URL and you need to scrape the data of those companies. It is important to note that you will also have to share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com")
    st.write("[Tutorial >](https://www.loom.com/looms/videos)")
    spreadsheet_url = st.text_input("Spreadsheet URL", "https://docs.google.com/spreadsheets/d/1LZI5Sv4XRHeq8xtt2qQhSUJUqDEaOJ_fkt9Lbkuy9Vw/edit?gid=421938140#gid=421938140")
    li_at = st.text_input("Select li_at", "AQEFARABAAAAABEiPKkAAAGRkOzewgAAAZG1ItjPTgAAs3VybjpsaTplbnRlcnByaXNlQXV0aFRva2VuOmVKeGpaQUFDYnRiR1dTQ2FhN1hlR3hETk9lUFpMVVlRdzhYcWh6YVlJZlZ5VlRBREl3QzFKZ2tCXnVybjpsaTplbnRlcnByaXNlUHJvZmlsZToodXJuOmxpOmVudGVycHJpc2VBY2NvdW50OjE4NDkxMDIzNCwxNzg5OTA4MjgpXnVybjpsaTptZW1iZXI6ODc2ODc4OTAwkbePsCyt-91CrCkFtdwIBrIWqKhrC438RrajXN8KQWzrR1zzb0QqlLHiXO1TzQbWrG1IXBH6LgWKu87_nd-VdVXgR26dPo6uVkT4aHR9FK4gRAQMvLSh68qZoZNStN2zge8LPrmvjdghUuWTGyn7sK4N25AEDqPox3goZIuuNFMCZn7qmjyolylX94FOjfjnpZvLMw")
    sheet_name = st.text_input("Select a Sheet Name", "Test")
    column_name = st.text_input("Select a Column name", "url")
    location_count = st.text_input("Select location count", "0")

elif option == "Profile Scraper":
    st.write("The LinkedIn Profile Scraper is a data extraction tool that gathers all available information from a prospect’s LinkedIn profile page. The profile is the most comprehensive data source for an individual on LinkedIn, and this information can be used to locate contact details. It is important to note that you will also have to share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com")
    st.write("[Tutorial >](https://www.loom.com/looms/videos)")
    spreadsheet_url = st.text_input("Spreadsheet URL", "https://docs.google.com/spreadsheets/d/1LZI5Sv4XRHeq8xtt2qQhSUJUqDEaOJ_fkt9Lbkuy9Vw/edit?gid=421938140#gid=421938140")
    li_at = st.text_input("Select li_at", "AQEFARABAAAAABEiPKkAAAGRkOzewgAAAZG1ItjPTgAAs3VybjpsaTplbnRlcnByaXNlQXV0aFRva2VuOmVKeGpaQUFDYnRiR1dTQ2FhN1hlR3hETk9lUFpMVVlRdzhYcWh6YVlJZlZ5VlRBREl3QzFKZ2tCXnVybjpsaTplbnRlcnByaXNlUHJvZmlsZToodXJuOmxpOmVudGVycHJpc2VBY2NvdW50OjE4NDkxMDIzNCwxNzg5OTA4MjgpXnVybjpsaTptZW1iZXI6ODc2ODc4OTAwkbePsCyt-91CrCkFtdwIBrIWqKhrC438RrajXN8KQWzrR1zzb0QqlLHiXO1TzQbWrG1IXBH6LgWKu87_nd-VdVXgR26dPo6uVkT4aHR9FK4gRAQMvLSh68qZoZNStN2zge8LPrmvjdghUuWTGyn7sK4N25AEDqPox3goZIuuNFMCZn7qmjyolylX94FOjfjnpZvLMw")
    sheet_name = st.text_input("Select a Sheet Name", "Test")
    column_name = st.text_input("Select a Column name", "url")

elif option == "Account Search Export":
    st.write("A LinkedIn Sales Navigator account page contains a wealth of information about a company. Use this when scraping companies based on a Sales Navigator query. It is important to note that you will also have to share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com")
    st.write("[Tutorial >](https://www.loom.com/looms/videos)")
    spreadsheet_url = st.text_input("Spreadsheet URL", "https://docs.google.com/spreadsheets/d/1LZI5Sv4XRHeq8xtt2qQhSUJUqDEaOJ_fkt9Lbkuy9Vw/edit?gid=421938140#gid=421938140")
    li_at = st.text_input("Select li_at", "AQEFARABAAAAABEiPKkAAAGRkOzewgAAAZG1ItjPTgAAs3VybjpsaTplbnRlcnByaXNlQXV0aFRva2VuOmVKeGpaQUFDYnRiR1dTQ2FhN1hlR3hETk9lUFpMVVlRdzhYcWh6YVlJZlZ5VlRBREl3QzFKZ2tCXnVybjpsaTplbnRlcnByaXNlUHJvZmlsZToodXJuOmxpOmVudGVycHJpc2VBY2NvdW50OjE4NDkxMDIzNCwxNzg5OTA4MjgpXnVybjpsaTptZW1iZXI6ODc2ODc4OTAwkbePsCyt-91CrCkFtdwIBrIWqKhrC438RrajXN8KQWzrR1zzb0QqlLHiXO1TzQbWrG1IXBH6LgWKu87_nd-VdVXgR26dPo6uVkT4aHR9FK4gRAQMvLSh68qZoZNStN2zge8LPrmvjdghUuWTGyn7sK4N25AEDqPox3goZIuuNFMCZn7qmjyolylX94FOjfjnpZvLMw")
    sheet_name = st.text_input("Select a Sheet Name", "Test")
    column_name = st.text_input("Select a Column name", "url")

elif option == "Lead Search Export":
    st.write("Use LinkedIn Sales Nave as a database and download any search results list from Sales Navigator. Use this when scraping contacts from a LinkedIn Sales Navigator query. It is important to note that you will also have to share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com")
    st.write("[Tutorial >](https://www.loom.com/looms/videos)")
    spreadsheet_url = st.text_input("Spreadsheet URL", "https://docs.google.com/spreadsheets/d/1LZI5Sv4XRHeq8xtt2qQhSUJUqDEaOJ_fkt9Lbkuy9Vw/edit?gid=421938140#gid=421938140")
    li_at = st.text_input("Select li_at", "AQEFARABAAAAABEiPKkAAAGRkOzewgAAAZG1ItjPTgAAs3VybjpsaTplbnRlcnByaXNlQXV0aFRva2VuOmVKeGpaQUFDYnRiR1dTQ2FhN1hlR3hETk9lUFpMVVlRdzhYcWh6YVlJZlZ5VlRBREl3QzFKZ2tCXnVybjpsaTplbnRlcnByaXNlUHJvZmlsZToodXJuOmxpOmVudGVycHJpc2VBY2NvdW50OjE4NDkxMDIzNCwxNzg5OTA4MjgpXnVybjpsaTptZW1iZXI6ODc2ODc4OTAwkbePsCyt-91CrCkFtdwIBrIWqKhrC438RrajXN8KQWzrR1zzb0QqlLHiXO1TzQbWrG1IXBH6LgWKu87_nd-VdVXgR26dPo6uVkT4aHR9FK4gRAQMvLSh68qZoZNStN2zge8LPrmvjdghUuWTGyn7sK4N25AEDqPox3goZIuuNFMCZn7qmjyolylX94FOjfjnpZvLMw")
    sheet_name = st.text_input("Select a Sheet Name", "Test")
    column_name = st.text_input("Select a Column name", "url")

# Timestamp for CSV file naming
current_timestamp = datetime.now()
timestamp = current_timestamp.strftime('%Y%m%d_%H%M%S')
csv_file_name = f"linkedin_{option.lower().replace(' ', '_')}_{timestamp}.csv"


if option != "Select Scraper Type":
  if st.button("Iniciar procesamiento"):
      if not spreadsheet_url or not li_at:
          st.error("Please enter both the Spreadsheet URL and the li_at")
      else:
          with st.spinner("Running the scraper. This could take a few minutes depending on the list size..."):
              try:
                  dataframe = retrieve_spreadsheet(spreadsheet_url, sheet_name)

                  if dataframe is not None and not dataframe.empty:
                      progress_bar = st.progress(0)
                      JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.run(retrieve_tokens(li_at))

                      if option == "LinkedIn Account Scraper":
                          dataframe_result = linkedin_account(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name, cookies_dict, progress_bar, location_count)
                      elif option == "Profile Scraper":
                          dataframe_result = linkedin_lead(csrf_token, dataframe, column_name, progress_bar, cookies_dict)
                      elif option == "Account Search Export":
                          dataframe_result = sales_navigator_account_export(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name, progress_bar, max_pages=17)
                      elif option == "Lead Search Export":
                          dataframe_result = sales_navigator_lead_export(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name, progress_bar, max_pages=26)

                      write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result)
                      st.success("Scraping completed!")
                      st.dataframe(dataframe_result)
              except Exception as e:
                  st.error(f"An error occurred: {e}")
