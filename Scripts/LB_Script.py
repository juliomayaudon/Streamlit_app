
import calendar
import gspread
import json
import math
import numpy as np
import os
import pandas as pd
import re
import requests
import time
import tldextract
import urllib.parse
import warnings
import streamlit as st

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

from tqdm import tqdm
from urllib.parse import urlparse, unquote
from datetime import datetime
from operator import itemgetter
from pyngrok import ngrok

from google.colab import auth
from google.auth import default
from google.colab import files


def linkedin_comp_scrapper(spreadsheetUrl, li_at):
  sheetName = "Companies"
  location_count = "All of them"
  columnName = "url"
  allFields = "False"
  fieldsToKeep = "companyUrl, companyName, description, website, domain, phone, industry, companySize, employeesOnLinkedIn,  founded, mainCompanyID,  salesNavigatorLink, companyType, headquarter_companyAddress, headquarter_city, headquarter_geographicArea, headquarter_country, headquarter_postalCode  "
  fields_check = "True"
  include_headcountInsights = "True"
  include_headcountByFunction = "True"
  include_headcountGrowthByFunction = "True"
  include_jobOpeningsByFunction = "True"
  include_jobOpeningsGrowthByFunction = "True"
  include_hiresInsights = "True"
  include_alumniInsights = "True"

  #-->SELENIUM PART START<--
  from selenium import webdriver
  from selenium.webdriver.chrome.service import Service
  from selenium.webdriver.common.by import By

  service = Service(executable_path=r'/usr/bin/chromedriver')
  options = webdriver.ChromeOptions()
  options.add_argument("--headless")
  options.add_argument("--disable-gpu")
  options.add_argument("--window-size=1920x1080")
  options.add_argument("--lang=en-US")
  options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
  options.add_argument('--no-sandbox')
  options.add_argument('--disable-dev-shm-usage')
  driver = webdriver.Chrome(service=service, options=options)

  driver.get("https://www.linkedin.com")
  driver.add_cookie({
      'name': 'li_at',
      'value': li_at,
      'domain': '.linkedin.com'
  })
  driver.get("https://www.linkedin.com")
  time.sleep(5)

  logs = driver.get_log('performance')
  for entry in logs:
      log = json.loads(entry['message'])['message']
      if log['method'] == 'Network.requestWillBeSent':
          request_url = log['params']['request']['url']
          if request_url.startswith("https://www.linkedin.com/voyager/api/voyagerGlobalAlerts?"):
              data_str = json.dumps(log['params']['request'], indent=4)
              data = json.loads(data_str)
              csrf_token = data['headers']['csrf-token']
              break

  all_cookies = driver.get_cookies()

  cookies_dict = {}
  for cookie in all_cookies:
      cookies_dict[cookie['name']] = cookie['value']

  JSESSIONID = cookies_dict.get("JSESSIONID")

  driver.quit()

  headers = {
      'csrf-token': csrf_token,
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
      'x-restli-protocol-version': '2.0.0',
      "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
      "x-li-lang": "en_US",
  }
  #-->SELENIUM PART END<--

  #-->API FUNCTIONS START<--
  def get_company(data):
      if not data or "elements" not in data or not data["elements"]:
          return {}

      company = data["elements"][0]
      return company
  #-->API FUNCTIONS END<--

  #-->BASE FUNCTIONS START<--
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
  #-->BASE FUNCTIONS END<--
  #-->BASE DICTIONARIES START<--
  dict_geographicArea = {'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'}
  #Last time updated (05/08/2023)
  #https://learn.microsoft.com/en-us/linkedin/shared/references/reference-tables/country-codes
  dict_country = {'AD': 'Andorra', 'AE': 'United Arab Emirates', 'AF': 'Afghanistan', 'AG': 'Antigua and Barbuda', 'AI': 'Anguilla', 'AL': 'Albania', 'AM': 'Armenia', 'AN': 'Netherlands Antilles', 'AO': 'Angola', 'AQ': 'Antarctica', 'AR': 'Argentina', 'AS': 'American Samoa', 'AT': 'Austria', 'AU': 'Australia', 'AW': 'Aruba', 'AX': 'Aland Islands', 'AZ': 'Azerbaijan', 'BA': 'Bosnia and Herzegovina', 'BB': 'Barbados', 'BD': 'Bangladesh', 'BE': 'Belgium', 'BF': 'Burkina Faso', 'BG': 'Bulgaria', 'BH': 'Bahrain', 'BI': 'Burundi', 'BJ': 'Benin', 'BM': 'Bermuda', 'BN': 'Brunei Darussalam', 'BO': 'Bolivia', 'BR': 'Brazil', 'BS': 'Bahamas', 'BT': 'Bhutan', 'BV': 'Bouvet Island', 'BW': 'Botswana', 'BY': 'Belarus', 'BZ': 'Belize', 'CA': 'Canada', 'CB': 'Caribbean Nations', 'CC': 'Cocos (Keeling) Islands', 'CD': 'Democratic Republic of the Congo', 'CF': 'Central African Republic', 'CG': 'Congo', 'CH': 'Switzerland', 'CI': "Cote D'Ivoire (Ivory Coast)", 'CK': 'Cook Islands', 'CL': 'Chile', 'CM': 'Cameroon', 'CN': 'China', 'CO': 'Colombia', 'CR': 'Costa Rica', 'CS': 'Serbia and Montenegro', 'CU': 'Cuba', 'CV': 'Cape Verde', 'CX': 'Christmas Island', 'CY': 'Cyprus', 'CZ': 'Czech Republic', 'DE': 'Germany', 'DJ': 'Djibouti', 'DK': 'Denmark', 'DM': 'Dominica', 'DO': 'Dominican Republic', 'DZ': 'Algeria', 'EC': 'Ecuador', 'EE': 'Estonia', 'EG': 'Egypt', 'EH': 'Western Sahara', 'ER': 'Eritrea', 'ES': 'Spain', 'ET': 'Ethiopia', 'FI': 'Finland', 'FJ': 'Fiji', 'FK': 'Falkland Islands (Malvinas)', 'FM': 'Federated States of Micronesia', 'FO': 'Faroe Islands', 'FR': 'France', 'FX': 'France, Metropolitan', 'GA': 'Gabon', 'GB': 'United Kingdom', 'GD': 'Grenada', 'GE': 'Georgia', 'GF': 'French Guiana', 'GH': 'Ghana', 'GI': 'Gibraltar', 'GL': 'Greenland', 'GM': 'Gambia', 'GN': 'Guinea', 'GP': 'Guadeloupe', 'GQ': 'Equatorial Guinea', 'GR': 'Greece', 'GS': 'S. Georgia and S. Sandwich Islands', 'GT': 'Guatemala', 'GU': 'Guam', 'GW': 'Guinea-Bissau', 'GY': 'Guyana', 'HK': 'Hong Kong', 'HM': 'Heard Island and McDonald Islands', 'HN': 'Honduras', 'HR': 'Croatia', 'HT': 'Haiti', 'HU': 'Hungary', 'ID': 'Indonesia', 'IE': 'Ireland', 'IL': 'Israel', 'IN': 'India', 'IO': 'British Indian Ocean Territory', 'IQ': 'Iraq', 'IR': 'Iran', 'IS': 'Iceland', 'IT': 'Italy', 'JM': 'Jamaica', 'JO': 'Jordan', 'JP': 'Japan', 'KE': 'Kenya', 'KG': 'Kyrgyzstan', 'KH': 'Cambodia', 'KI': 'Kiribati', 'KM': 'Comoros', 'KN': 'Saint Kitts and Nevis', 'KP': 'Korea (North)', 'KR': 'Korea', 'KW': 'Kuwait', 'KY': 'Cayman Islands', 'KZ': 'Kazakhstan', 'LA': 'Laos', 'LB': 'Lebanon', 'LC': 'Saint Lucia', 'LI': 'Liechtenstein', 'LK': 'Sri Lanka', 'LR': 'Liberia', 'LS': 'Lesotho', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'LV': 'Latvia', 'LY': 'Libya', 'MA': 'Morocco', 'MC': 'Monaco', 'MD': 'Moldova', 'MG': 'Madagascar', 'MH': 'Marshall Islands', 'MK': 'Macedonia', 'ML': 'Mali', 'MM': 'Myanmar', 'MN': 'Mongolia', 'MO': 'Macao', 'MP': 'Northern Mariana Islands', 'MQ': 'Martinique', 'MR': 'Mauritania', 'MS': 'Montserrat', 'MT': 'Malta', 'MU': 'Mauritius', 'MV': 'Maldives', 'MW': 'Malawi', 'MX': 'Mexico', 'MY': 'Malaysia', 'MZ': 'Mozambique', 'NA': 'Namibia', 'NC': 'New Caledonia', 'NE': 'Niger', 'NF': 'Norfolk Island', 'NG': 'Nigeria', 'NI': 'Nicaragua', 'NL': 'Netherlands', 'NO': 'Norway', 'NP': 'Nepal', 'NR': 'Nauru', 'NU': 'Niue', 'NZ': 'New Zealand', 'OM': 'Sultanate of Oman', 'OO': 'Other', 'PA': 'Panama', 'PE': 'Peru', 'PF': 'French Polynesia', 'PG': 'Papua New Guinea', 'PH': 'Philippines', 'PK': 'Pakistan', 'PL': 'Poland', 'PM': 'Saint Pierre and Miquelon', 'PN': 'Pitcairn', 'PR': 'Puerto Rico', 'PS': 'Palestinian Territory', 'PT': 'Portugal', 'PW': 'Palau', 'PY': 'Paraguay', 'QA': 'Qatar', 'RE': 'Reunion', 'RO': 'Romania', 'RU': 'Russian Federation', 'RW': 'Rwanda', 'SA': 'Saudi Arabia', 'SB': 'Solomon Islands', 'SC': 'Seychelles', 'SD': 'Sudan', 'SE': 'Sweden', 'SG': 'Singapore', 'SH': 'Saint Helena', 'SI': 'Slovenia', 'SJ': 'Svalbard and Jan Mayen', 'SK': 'Slovak Republic', 'SL': 'Sierra Leone', 'SM': 'San Marino', 'SN': 'Senegal', 'SO': 'Somalia', 'SR': 'Suriname', 'ST': 'Sao Tome and Principe', 'SV': 'El Salvador', 'SY': 'Syria', 'SZ': 'Swaziland', 'TC': 'Turks and Caicos Islands', 'TD': 'Chad', 'TF': 'French Southern Territories', 'TG': 'Togo', 'TH': 'Thailand', 'TJ': 'Tajikistan', 'TK': 'Tokelau', 'TL': 'Timor-Leste', 'TM': 'Turkmenistan', 'TN': 'Tunisia', 'TO': 'Tonga', 'TP': 'East Timor', 'TR': 'Turkey', 'TT': 'Trinidad and Tobago', 'TV': 'Tuvalu', 'TW': 'Taiwan', 'TZ': 'Tanzania', 'UA': 'Ukraine', 'UG': 'Uganda', 'US': 'United States', 'UY': 'Uruguay', 'UZ': 'Uzbekistan', 'VA': 'Vatican City State (Holy See)', 'VC': 'Saint Vincent and the Grenadines', 'VE': 'Venezuela', 'VG': 'Virgin Islands (British)', 'VI': 'Virgin Islands (U.S.)', 'VN': 'Vietnam', 'VU': 'Vanuatu', 'WF': 'Wallis and Futuna', 'WS': 'Samoa', 'YE': 'Yemen', 'YT': 'Mayotte', 'YU': 'Yugoslavia', 'ZA': 'South Africa', 'ZM': 'Zambia', 'ZW': 'Zimbabwe'}
  #-->BASE DICTIONARIES END<--
  #-->PREMIUM FUNCTIONS START<--
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
  #-->PREMIUM FUNCTIONS END<--
  #-->GENERAL FUNCTIONS START<--
  def extract_linkedin_universal_name(linkedin_url, pattern):
      try:
          return pattern.match(str(linkedin_url)).group(1)
      except AttributeError:
          return None
  def get_excel_column_name(n):
      string = ""
      while n > 0:
          n, remainder = divmod(n - 1, 26)
          string = chr(65 + remainder) + string
      return string
  def update_with_backoff(range_name, chunk):
      backoff_time = 1
      max_backoff_time = 64
      while True:
          try:
              worksheet.update(range_name, chunk, value_input_option='RAW')
              break
          except Exception as e:
              if "429" in str(e):
                  time.sleep(backoff_time)
                  backoff_time = min(2 * backoff_time, max_backoff_time)
              else:
                  raise
  def sanitize_data(data):
      if isinstance(data, list):
          return [sanitize_data(item) for item in data]
      elif isinstance(data, dict):
          return {key: sanitize_data(value) for key, value in data.items()}
      elif isinstance(data, float):
        if np.isinf(data) or np.isnan(data):
            return ''
        return data
      else:
          return data
  def generate_columns(count, patterns):
      return [pattern.format(i+1) for i in range(count) for pattern in patterns]
  #-->GENERAL FUNCTIONS END<--

  pattern_universalName = re.compile(r"^(?:https?:\/\/)?(?:[\w]+\.)?linkedin\.com\/(?:company|company-beta)\/([A-Za-z0-9\._\%&'-]+?)(?:\/|\?|#|$)", re.IGNORECASE)

  def safe_extract(data, *keys):
    try:
      for key in keys:
        data = data[key]
      return data
    except (KeyError, IndexError, TypeError):
      return None

  #-->Declare some variables
  df_final = pd.DataFrame()
  max_confirmedLocations = 0
  insights_url = 'https://www.linkedin.com/voyager/api/voyagerPremiumDashCompanyInsightsCard'

  #kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com
  from google.oauth2.service_account import Credentials
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
  worksheet = spreadsheet.worksheet(sheetName)
  data = worksheet.get_all_values()
  dataframe = pd.DataFrame(data[1:], columns=data[0])


  dataframe.drop_duplicates(subset=[columnName], inplace=True)

  dataframe['wordToSearch'] = dataframe[columnName].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))

  original_to_wordToSearch = dict(zip(dataframe[columnName], dataframe['wordToSearch']))

  columnName_values = dataframe[columnName].tolist()

  #-->Loop
  print("LinkedIn company scrape")
  progress_bar = tqdm(total = len(columnName_values))

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
        progress_bar.update(1)
        continue
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
        progress_bar.update(1)
        continue
    except Exception as e:
        company = {}
        error = f'{e}'
        df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
        df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
        df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
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
        progress_bar.update(1)
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
      growth_data = headcountInsights[0]['growthPeriods'] if headcountInsights and headcountInsights[0] else []
      growth_dict = {item['monthDifference']: item['changePercentage'] for item in growth_data}
      growth6Mth = format_growth(growth_dict.get(6, None))
      growth1Yr = format_growth(growth_dict.get(12, None))
      growth2Yr = format_growth(growth_dict.get(24, None))
      #-->averageTenure
      tenure_text = headcountInsights[0]['headcounts']['medianTenureYears']['text'] if headcountInsights and headcountInsights[0] and 'headcounts' in headcountInsights[0] and 'medianTenureYears' in headcountInsights[0]['headcounts'] else ''
      averageTenure = tenure_text.split("Median employee tenure ‧ ", 1)[-1] if "Median employee tenure ‧ " in tenure_text else None
      #Create dataframe with some premium fields
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
          function_urn = entry['function']['entityUrn']
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
      df_final = pd.concat([df_final, df_loop_final])
      index += 1
      progress_bar.update(1)
    else:
        df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
        df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
        df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
        continue
  progress_bar.close()
  #-->LOCATIONS START<--
  COLUMN_PATTERNS = {
      'location': [
          'location{}_description', 'location{}_line1', 'location{}_line2',
          'location{}_city', 'location{}_postalCode', 'location{}_geographicArea',
          'location{}_country', 'location{}_companyAddress'
      ]
  }
  MAX_COUNTS = {'location': max_confirmedLocations}
  try:
      location_int_count = int(location_count)
  except ValueError:
      location_int_count = location_count
  counts = {'location': location_int_count}
  columns_lists = {}
  for key, max_count in MAX_COUNTS.items():
      if counts[key] == "All of them":
          count = max_confirmedLocations
      else:
          count = counts[key]
      count = min(count, max_count)
      columns_lists[key] = generate_columns(count, COLUMN_PATTERNS[key])
  #-->LOCATIONS END<--
  #-->Columns manipulation
  final_list = ["query", "error", "companyUrl", "mainCompanyID", "salesNavigatorLink", "universalName", "companyName", "followerCount","employeesOnLinkedIn", "tagLine", "description", "website", "domain", "industryCode", "industry","companySize", "headquarter", "founded", "specialities", "companyType", "phone", "headquarter_line1","headquarter_line2", "headquarter_city", "headquarter_geographicArea", "headquarter_postalCode","headquarter_country", "headquarter_companyAddress", "banner10000x10000", "banner400x400","banner200x200", "logo400x400", "logo200x200", "logo100x100", "showcase", "autoGenerated","isClaimable", "jobSearchPageUrl", "associatedHashtags", "callToActionUrl", "timestamp"]
  locations_list = columns_lists['location']
  final_list.extend(locations_list)
  if fields_check == 'True':
      temporal_list_1 = ["totalEmployeeCount", "growth6Mth", "growth1Yr", "growth2Yr", "averageTenure"]
      final_list.extend(temporal_list_1)
  if include_headcountInsights == 'True':
      headcountInsights_column_list = ['headcountGrowthMonth0Date', 'headcountGrowthMonth0Count', 'headcountGrowthMonth1Date', 'headcountGrowthMonth1Count', 'headcountGrowthMonth2Date', 'headcountGrowthMonth2Count', 'headcountGrowthMonth3Date', 'headcountGrowthMonth3Count', 'headcountGrowthMonth4Date', 'headcountGrowthMonth4Count', 'headcountGrowthMonth5Date', 'headcountGrowthMonth5Count', 'headcountGrowthMonth6Date', 'headcountGrowthMonth6Count', 'headcountGrowthMonth7Date', 'headcountGrowthMonth7Count', 'headcountGrowthMonth8Date', 'headcountGrowthMonth8Count', 'headcountGrowthMonth9Date', 'headcountGrowthMonth9Count', 'headcountGrowthMonth10Date', 'headcountGrowthMonth10Count', 'headcountGrowthMonth11Date', 'headcountGrowthMonth11Count', 'headcountGrowthMonth12Date', 'headcountGrowthMonth12Count', 'headcountGrowthMonth13Date', 'headcountGrowthMonth13Count', 'headcountGrowthMonth14Date', 'headcountGrowthMonth14Count', 'headcountGrowthMonth15Date', 'headcountGrowthMonth15Count', 'headcountGrowthMonth16Date', 'headcountGrowthMonth16Count', 'headcountGrowthMonth17Date', 'headcountGrowthMonth17Count', 'headcountGrowthMonth18Date', 'headcountGrowthMonth18Count', 'headcountGrowthMonth19Date', 'headcountGrowthMonth19Count', 'headcountGrowthMonth20Date', 'headcountGrowthMonth20Count', 'headcountGrowthMonth21Date', 'headcountGrowthMonth21Count', 'headcountGrowthMonth22Date', 'headcountGrowthMonth22Count', 'headcountGrowthMonth23Date', 'headcountGrowthMonth23Count', 'headcountGrowthMonth24Date', 'headcountGrowthMonth24Count']
      final_list.extend(headcountInsights_column_list)
  if include_headcountByFunction == 'True':
      headcountByFunction_column_list = ['distributionAccounting', 'distributionAccountingPercentage', 'distributionAdministrative', 'distributionAdministrativePercentage', 'distributionArtsAndDesign', 'distributionArtsAndDesignPercentage', 'distributionBusinessDevelopment', 'distributionBusinessDevelopmentPercentage', 'distributionCommunityAndSocialServices', 'distributionCommunityAndSocialServicesPercentage', 'distributionConsulting', 'distributionConsultingPercentage', 'distributionEducation', 'distributionEducationPercentage', 'distributionEngineering', 'distributionEngineeringPercentage', 'distributionEntrepreneurship', 'distributionEntrepreneurshipPercentage', 'distributionFinance', 'distributionFinancePercentage', 'distributionHealthcareServices', 'distributionHealthcareServicesPercentage', 'distributionHumanResources', 'distributionHumanResourcesPercentage', 'distributionInformationTechnology', 'distributionInformationTechnologyPercentage', 'distributionLegal', 'distributionLegalPercentage', 'distributionMarketing', 'distributionMarketingPercentage', 'distributionMediaAndCommunication', 'distributionMediaAndCommunicationPercentage', 'distributionMilitaryAndProtectiveServices', 'distributionMilitaryAndProtectiveServicesPercentage', 'distributionOperations', 'distributionOperationsPercentage', 'distributionProductManagement', 'distributionProductManagementPercentage', 'distributionProgramAndProjectManagement', 'distributionProgramAndProjectManagementPercentage', 'distributionPurchasing', 'distributionPurchasingPercentage', 'distributionQualityAssurance', 'distributionQualityAssurancePercentage', 'distributionRealEstate', 'distributionRealEstatePercentage', 'distributionResearch', 'distributionResearchPercentage', 'distributionSales', 'distributionSalesPercentage', 'distributionSupport', 'distributionSupportPercentage']
      final_list.extend(headcountByFunction_column_list)
  if include_headcountGrowthByFunction == 'True':
      headcountGrowthByFunction_column_list = ['growth6MthAccounting', 'growth1YrAccounting', 'growth6MthAdministrative', 'growth1YrAdministrative', 'growth6MthArtsAndDesign', 'growth1YrArtsAndDesign', 'growth6MthBusinessDevelopment', 'growth1YrBusinessDevelopment', 'growth6MthCommunityAndSocialServices', 'growth1YrCommunityAndSocialServices', 'growth6MthConsulting', 'growth1YrConsulting', 'growth6MthEducation', 'growth1YrEducation', 'growth6MthEngineering', 'growth1YrEngineering', 'growth6MthEntrepreneurship', 'growth1YrEntrepreneurship', 'growth6MthFinance', 'growth1YrFinance', 'growth6MthHealthcareServices', 'growth1YrHealthcareServices', 'growth6MthHumanResources', 'growth1YrHumanResources', 'growth6MthInformationTechnology', 'growth1YrInformationTechnology', 'growth6MthLegal', 'growth1YrLegal', 'growth6MthMarketing', 'growth1YrMarketing', 'growth6MthMediaAndCommunication', 'growth1YrMediaAndCommunication', 'growth6MthMilitaryAndProtectiveServices', 'growth1YrMilitaryAndProtectiveServices', 'growth6MthOperations', 'growth1YrOperations', 'growth6MthProductManagement', 'growth1YrProductManagement', 'growth6MthProgramAndProjectManagement', 'growth1YrProgramAndProjectManagement', 'growth6MthPurchasing', 'growth1YrPurchasing', 'growth6MthQualityAssurance', 'growth1YrQualityAssurance', 'growth6MthRealEstate', 'growth1YrRealEstate', 'growth6MthResearch', 'growth1YrResearch', 'growth6MthSales', 'growth1YrSales', 'growth6MthSupport', 'growth1YrSupport']
      final_list.extend(headcountGrowthByFunction_column_list)
  if include_jobOpeningsByFunction == 'True':
      jobOpeningsByFunction_column_list = ['distributionAccountingJobs', 'distributionAccountingPercentageJobs', 'distributionAdministrativeJobs', 'distributionAdministrativePercentageJobs', 'distributionArtsAndDesignJobs', 'distributionArtsAndDesignPercentageJobs', 'distributionBusinessDevelopmentJobs', 'distributionBusinessDevelopmentPercentageJobs', 'distributionCommunityAndSocialServicesJobs', 'distributionCommunityAndSocialServicesPercentageJobs', 'distributionConsultingJobs', 'distributionConsultingPercentageJobs', 'distributionEducationJobs', 'distributionEducationPercentageJobs', 'distributionEngineeringJobs', 'distributionEngineeringPercentageJobs', 'distributionEntrepreneurshipJobs', 'distributionEntrepreneurshipPercentageJobs', 'distributionFinanceJobs', 'distributionFinancePercentageJobs', 'distributionHealthcareServicesJobs', 'distributionHealthcareServicesPercentageJobs', 'distributionHumanResourcesJobs', 'distributionHumanResourcesPercentageJobs', 'distributionInformationTechnologyJobs', 'distributionInformationTechnologyPercentageJobs', 'distributionLegalJobs', 'distributionLegalPercentageJobs', 'distributionMarketingJobs', 'distributionMarketingPercentageJobs', 'distributionMediaAndCommunicationJobs', 'distributionMediaAndCommunicationPercentageJobs', 'distributionMilitaryAndProtectiveServicesJobs', 'distributionMilitaryAndProtectiveServicesPercentageJobs', 'distributionOperationsJobs', 'distributionOperationsPercentageJobs', 'distributionProductManagementJobs', 'distributionProductManagementPercentageJobs', 'distributionProgramAndProjectManagementJobs', 'distributionProgramAndProjectManagementPercentageJobs', 'distributionPurchasingJobs', 'distributionPurchasingPercentageJobs', 'distributionQualityAssuranceJobs', 'distributionQualityAssurancePercentageJobs', 'distributionRealEstateJobs', 'distributionRealEstatePercentageJobs', 'distributionResearchJobs', 'distributionResearchPercentageJobs', 'distributionSalesJobs', 'distributionSalesPercentageJobs', 'distributionSupportJobs', 'distributionSupportPercentageJobs']
      final_list.extend(jobOpeningsByFunction_column_list)
  if include_jobOpeningsGrowthByFunction == 'True':
      jobOpeningsGrowthByFunction_column_list = ['growth3MthAccountingJobs', 'growth6MthAccountingJobs', 'growth1YrAccountingJobs', 'growth3MthAdministrativeJobs', 'growth6MthAdministrativeJobs', 'growth1YrAdministrativeJobs', 'growth3MthArtsAndDesignJobs', 'growth6MthArtsAndDesignJobs', 'growth1YrArtsAndDesignJobs', 'growth3MthBusinessDevelopmentJobs', 'growth6MthBusinessDevelopmentJobs', 'growth1YrBusinessDevelopmentJobs', 'growth3MthCommunityAndSocialServicesJobs', 'growth6MthCommunityAndSocialServicesJobs', 'growth1YrCommunityAndSocialServicesJobs', 'growth3MthConsultingJobs', 'growth6MthConsultingJobs', 'growth1YrConsultingJobs', 'growth3MthEducationJobs', 'growth6MthEducationJobs', 'growth1YrEducationJobs', 'growth3MthEngineeringJobs', 'growth6MthEngineeringJobs', 'growth1YrEngineeringJobs', 'growth3MthEntrepreneurshipJobs', 'growth6MthEntrepreneurshipJobs', 'growth1YrEntrepreneurshipJobs', 'growth3MthFinanceJobs', 'growth6MthFinanceJobs', 'growth1YrFinanceJobs', 'growth3MthHealthcareServicesJobs', 'growth6MthHealthcareServicesJobs', 'growth1YrHealthcareServicesJobs', 'growth3MthHumanResourcesJobs', 'growth6MthHumanResourcesJobs', 'growth1YrHumanResourcesJobs', 'growth3MthInformationTechnologyJobs', 'growth6MthInformationTechnologyJobs', 'growth1YrInformationTechnologyJobs', 'growth3MthLegalJobs', 'growth6MthLegalJobs', 'growth1YrLegalJobs', 'growth3MthMarketingJobs', 'growth6MthMarketingJobs', 'growth1YrMarketingJobs', 'growth3MthMediaAndCommunicationJobs', 'growth6MthMediaAndCommunicationJobs', 'growth1YrMediaAndCommunicationJobs', 'growth3MthMilitaryAndProtectiveServicesJobs', 'growth6MthMilitaryAndProtectiveServicesJobs', 'growth1YrMilitaryAndProtectiveServicesJobs', 'growth3MthOperationsJobs', 'growth6MthOperationsJobs', 'growth1YrOperationsJobs', 'growth3MthProductManagementJobs', 'growth6MthProductManagementJobs', 'growth1YrProductManagementJobs', 'growth3MthProgramAndProjectManagementJobs', 'growth6MthProgramAndProjectManagementJobs', 'growth1YrProgramAndProjectManagementJobs', 'growth3MthPurchasingJobs', 'growth6MthPurchasingJobs', 'growth1YrPurchasingJobs', 'growth3MthQualityAssuranceJobs', 'growth6MthQualityAssuranceJobs', 'growth1YrQualityAssuranceJobs', 'growth3MthRealEstateJobs', 'growth6MthRealEstateJobs', 'growth1YrRealEstateJobs', 'growth3MthResearchJobs', 'growth6MthResearchJobs', 'growth1YrResearchJobs', 'growth3MthSalesJobs', 'growth6MthSalesJobs', 'growth1YrSalesJobs', 'growth3MthSupportJobs', 'growth6MthSupportJobs', 'growth1YrSupportJobs']
      final_list.extend(jobOpeningsGrowthByFunction_column_list)
  if include_hiresInsights == 'True':
      hiresInsights_column_list = ['totalNumberOfSeniorHires', 'hireAllCountMonth0Date', 'hireAllCountMonth0', 'hireSeniorCountMonth0Date', 'hireSeniorCountMonth0', 'hireAllCountMonth1Date', 'hireAllCountMonth1', 'hireSeniorCountMonth1Date', 'hireSeniorCountMonth1', 'hireAllCountMonth2Date', 'hireAllCountMonth2', 'hireSeniorCountMonth2Date', 'hireSeniorCountMonth2', 'hireAllCountMonth3Date', 'hireAllCountMonth3', 'hireSeniorCountMonth3Date', 'hireSeniorCountMonth3', 'hireAllCountMonth4Date', 'hireAllCountMonth4', 'hireSeniorCountMonth4Date', 'hireSeniorCountMonth4', 'hireAllCountMonth5Date', 'hireAllCountMonth5', 'hireSeniorCountMonth5Date', 'hireSeniorCountMonth5', 'hireAllCountMonth6Date', 'hireAllCountMonth6', 'hireSeniorCountMonth6Date', 'hireSeniorCountMonth6', 'hireAllCountMonth7Date', 'hireAllCountMonth7', 'hireSeniorCountMonth7Date', 'hireSeniorCountMonth7', 'hireAllCountMonth8Date', 'hireAllCountMonth8', 'hireSeniorCountMonth8Date', 'hireSeniorCountMonth8', 'hireAllCountMonth9Date', 'hireAllCountMonth9', 'hireSeniorCountMonth9Date', 'hireSeniorCountMonth9', 'hireAllCountMonth10Date', 'hireAllCountMonth10', 'hireSeniorCountMonth10Date', 'hireSeniorCountMonth10', 'hireAllCountMonth11Date', 'hireAllCountMonth11', 'hireSeniorCountMonth11Date', 'hireSeniorCountMonth11', 'hireAllCountMonth12Date', 'hireAllCountMonth12', 'hireSeniorCountMonth12Date', 'hireSeniorCountMonth12', 'hireAllCountMonth13Date', 'hireAllCountMonth13', 'hireSeniorCountMonth13Date', 'hireSeniorCountMonth13', 'hireAllCountMonth14Date', 'hireAllCountMonth14', 'hireSeniorCountMonth14Date', 'hireSeniorCountMonth14', 'hireAllCountMonth15Date', 'hireAllCountMonth15', 'hireSeniorCountMonth15Date', 'hireSeniorCountMonth15', 'hireAllCountMonth16Date', 'hireAllCountMonth16', 'hireSeniorCountMonth16Date', 'hireSeniorCountMonth16', 'hireAllCountMonth17Date', 'hireAllCountMonth17', 'hireSeniorCountMonth17Date', 'hireSeniorCountMonth17', 'hireAllCountMonth18Date', 'hireAllCountMonth18', 'hireSeniorCountMonth18Date', 'hireSeniorCountMonth18', 'hireAllCountMonth19Date', 'hireAllCountMonth19', 'hireSeniorCountMonth19Date', 'hireSeniorCountMonth19', 'hireAllCountMonth20Date', 'hireAllCountMonth20', 'hireSeniorCountMonth20Date', 'hireSeniorCountMonth20', 'hireAllCountMonth21Date', 'hireAllCountMonth21', 'hireSeniorCountMonth21Date', 'hireSeniorCountMonth21', 'hireAllCountMonth22Date', 'hireAllCountMonth22', 'hireSeniorCountMonth22Date', 'hireSeniorCountMonth22', 'hireAllCountMonth23Date', 'hireAllCountMonth23', 'hireSeniorCountMonth23Date', 'hireSeniorCountMonth23', 'hireAllCountMonth24Date', 'hireAllCountMonth24', 'hireSeniorCountMonth24Date', 'hireSeniorCountMonth24', 'seniorHires1_title', 'seniorHires1_linkedInUrl', 'seniorHires1_fullName', 'seniorHires1_date', 'seniorHires2_title', 'seniorHires2_linkedInUrl', 'seniorHires2_fullName', 'seniorHires2_date', 'seniorHires3_title', 'seniorHires3_linkedInUrl', 'seniorHires3_fullName', 'seniorHires3_date']
      final_list.extend(hiresInsights_column_list)
  if include_alumniInsights == 'True':
      alumniInsights_column_list = ['alumni1_currentTitle', 'alumni1_linkedInUrl', 'alumni1_fullName', 'alumni1_exitDate', 'alumni1_exitTitle', 'alumni2_currentTitle', 'alumni2_linkedInUrl', 'alumni2_fullName', 'alumni2_exitDate', 'alumni2_exitTitle', 'alumni3_currentTitle', 'alumni3_linkedInUrl', 'alumni3_fullName', 'alumni3_exitDate', 'alumni3_exitTitle']
      final_list.extend(alumniInsights_column_list)
  for column in final_list:
      if column not in df_final.columns:
          df_final[column] = None
  df_final = df_final[final_list]
  #-->allFields check
  if allFields == "False":
      cols_to_retain = [col.strip() for col in fieldsToKeep.split(',')]
      cols_to_retain = df_final.columns.intersection(cols_to_retain)
      df_final = df_final[cols_to_retain]

  #Aplicar el Query usando el ID:
  base_P2 ="https://www.linkedin.com/sales/search/people?query=(recentSearchParam%3A(id%3A2744797746%2CdoLogHistory%3Atrue)%2Cfilters%3AList((type%3ACURRENT_COMPANY%2Cvalues%3AList((id%3Aurn%253Ali%253Aorganization%253AKEYWORD%2Ctext%3ARedwood%2520Logistics%2CselectionType%3AINCLUDED%2Cparent%3A(id%3A0))))%2C(type%3ACURRENT_TITLE%2Cvalues%3AList((text%3A%2528%25E2%2580%259Cdirector%25E2%2580%259D%2520AND%2520%2528%25E2%2580%259Ce-commerce%25E2%2580%259D%2520OR%2520%25E2%2580%259Cdigital%2520transformation%25E2%2580%259D%2520OR%2520%25E2%2580%259CIT%25E2%2580%259D%2520OR%2520%25E2%2580%259Cinformation%2520technology%25E2%2580%259D%2520OR%2520%25E2%2580%259Coperations%25E2%2580%259D%2520OR%2520%25E2%2580%259Ctechnology%2520transformation%25E2%2580%259D%2520OR%2520%25E2%2580%259Csupply%2520chain%25E2%2580%259D%2520OR%2520%25E2%2580%259Clast%2520mile%25E2%2580%259D%2520OR%2520%25E2%2580%259Ccustomer%2520experience%25E2%2580%259D%2520OR%2520%25E2%2580%259Ccustomer%2520success%25E2%2580%259D%2520OR%2520%25E2%2580%259Clogistics%25E2%2580%259D%2520OR%2520%25E2%2580%259CTransportation%25E2%2580%259D%2520OR%2520%25E2%2580%259COmni-channel%25E2%2580%259D%2520OR%2520%25E2%2580%259CFinal%2520Mile%2520Delivery%25E2%2580%259D%2520OR%2520%25E2%2580%259CLast%2520mile%2520Delivery%25E2%2580%259D%2520OR%2520%25E2%2580%259CWhite%2520Glove%2520Delivery%25E2%2580%259D%2520OR%2520%25E2%2580%259CHome%2520Delivery%25E2%2580%259D%2520OR%2520%25E2%2580%259CSystems%25E2%2580%259D%2520OR%2520%25E2%2580%259CIntegrations%25E2%2580%259D%2520OR%2520%25E2%2580%259CProject%2520Management%25E2%2580%259D%2520OR%2520%25E2%2580%259CDelivery%25E2%2580%259D%2529%2529%2CselectionType%3AINCLUDED)%2C(text%3A%2528%25E2%2580%259Cmanager%25E2%2580%259D%2520AND%2520%2528%25E2%2580%259Cdigital%2520transformation%25E2%2580%259D%2520OR%2520%25E2%2580%259Coperations%25E2%2580%259D%2520OR%2520%25E2%2580%259Clogistics%25E2%2580%259D%2520OR%2520%25E2%2580%259Cfleet%25E2%2580%259D%2520OR%2520%25E2%2580%259Ctransportation%25E2%2580%259D%2529%2529%2520OR%2520%2528%25E2%2580%259Chead%25E2%2580%259D%2520AND%2520%2528%25E2%2580%259Clogistics%25E2%2580%259D%2520OR%2520%25E2%2580%259Clast%2520mile%25E2%2580%259D%2529%2529%2520OR%2520%2528%25E2%2580%259CVP%25E2%2580%259D%2520OR%2520%25E2%2580%259Cvice%2520president%25E2%2580%259D%2520AND%2520%2528%25E2%2580%259Clogistics%25E2%2580%259D%2520OR%2520%25E2%2580%259CTransportation%25E2%2580%259D%2520OR%2520%25E2%2580%259COmni-channel%25E2%2580%259D%2520OR%2520%25E2%2580%259CFinal%2520Mile%2520Delivery%25E2%2580%259D%2520OR%2520%25E2%2580%259CLast%2520mile%2520Delivery%25E2%2580%259D%2520OR%2520%25E2%2580%259CWhite%2520Glove%2520Delivery%25E2%2580%259D%2520OR%2520%25E2%2580%259CHome%2520Delivery%25E2%2580%259D%2520OR%2520%25E2%2580%259CSystems%25E2%2580%259D%2520OR%2520%25E2%2580%259CIntegrations%25E2%2580%259D%2520OR%2520%25E2%2580%259CProject%2520Management%25E2%2580%259D%2520OR%2520%25E2%2580%259CDelivery%25E2%2580%259D%2520OR%2520%25E2%2580%259Ce-commerce%25E2%2580%259D%2520OR%2520%25E2%2580%259CIT%25E2%2580%259D%2520OR%2520%25E2%2580%259Cinformation%2520technology%25E2%2580%259D%2529%2529%2CselectionType%3AINCLUDED)))%2C(type%3AREGION%2Cvalues%3AList((id%3A102221843%2Ctext%3ANorth%2520America%2CselectionType%3AINCLUDED)))))&sessionId=Jw%2BJm9jCTDC8NeUOv2pBoA%3D%3D"  #@param {type:"string"}

  base_P3 ="https://www.linkedin.com/sales/search/people?query=(recentSearchParam%3A(id%3A2744797746%2CdoLogHistory%3Atrue)%2Cfilters%3AList((type%3ACURRENT_COMPANY%2Cvalues%3AList((id%3Aurn%253Ali%253Aorganization%253AKEYWORD%2Ctext%3ARedwood%2520Logistics%2CselectionType%3AINCLUDED%2Cparent%3A(id%3A0))))%2C(type%3AREGION%2Cvalues%3AList((id%3A102221843%2Ctext%3ANorth%2520America%2CselectionType%3AINCLUDED)))%2C(type%3ACURRENT_TITLE%2Cvalues%3AList((text%3A%25E2%2580%259Cfounder%25E2%2580%259D%2520OR%2520%2528%25E2%2580%259Cdirector%25E2%2580%259D%2520AND%2520%25E2%2580%259Ctechnology%25E2%2580%259D%2529%2520OR%2520%25E2%2580%259Cpresident%25E2%2580%259D%2520OR%2520%25E2%2580%259CCEO%25E2%2580%259D%2520OR%2520%25E2%2580%259Cchief%2520executive%2520officer%25E2%2580%259D%2520OR%2520%25E2%2580%259CCOO%25E2%2580%259D%2520OR%2520%25E2%2580%259Cchief%2520operations%2520officer%25E2%2580%259D%2520OR%2520%25E2%2580%259CCTO%25E2%2580%259D%2520OR%2520%25E2%2580%259Cchief%2520technology%2520officer%25E2%2580%259D%2520OR%2520%25E2%2580%259Cchief%2520information%2520officer%25E2%2580%259D%2520OR%2520%25E2%2580%259CCIO%25E2%2580%259D%2520OR%2520%25E2%2580%259CCSO%25E2%2580%259D%2520OR%2520%25E2%2580%259Cchief%2520strategy%2520officer%25E2%2580%259D%2520OR%2520%2528%25E2%2580%259Chead%25E2%2580%259D%2520AND%2520%25E2%2580%259Ce-commerce%25E2%2580%259D%2529%2CselectionType%3AINCLUDED)))))&sessionId=Jw%2BJm9jCTDC8NeUOv2pBoA%3D%3D"  #@param {type:"string"}


  # Función para reemplazar "KEYWORD" en la base con el valor de mainCompanyID
  def replace_keyword_with_id_p2(row):
      return base_P2.replace("KEYWORD", str(row['mainCompanyID']))
  def replace_keyword_with_id_p3(row):
      return base_P3.replace("KEYWORD", str(row['mainCompanyID']))

  df_final['QueryP2'] = df_final.apply(replace_keyword_with_id_p2, axis=1)
  df_final['QueryP3'] = df_final.apply(replace_keyword_with_id_p3, axis=1)

  #-->PRINTING PART START<--
  df_final = df_final.applymap(sanitize_data)
  df_final.fillna('', inplace=True)
  data_dataframe = [df_final.columns.tolist()] + df_final.values.tolist()
  worksheet.clear()
  max_rows_per_request = 40000 // len(df_final.columns)
  for i in range(0, len(data_dataframe), max_rows_per_request):
      chunk = data_dataframe[i:i + max_rows_per_request]
      range_start = f'A{i + 1}'
      col_letter = get_excel_column_name(len(df_final.columns))
      range_end = f'{col_letter}{i + len(chunk)}'
      range_name = f'{range_start}:{range_end}'
      update_with_backoff(range_name, chunk)
  print("Data printed!")
  #-->PRINTING PART END<--

  new_worksheet = spreadsheet.add_worksheet(title='ContactsP2', rows='100', cols='20')
  new_worksheet2 = spreadsheet.add_worksheet(title='ContactsP3', rows='100', cols='20')

  sheetName = "Companies"
  columnName = "QueryP2"
  resultFileName = "result"
  max_pages = "4"

  #---------------------------------------------------------------------------
  max_pages = int(max_pages)
  #---------------------------------------------------------------------------

  from selenium import webdriver
  #from selenium.webdriver.chrome.options import Options
  from selenium.webdriver.chrome.service import Service
  from gspread_dataframe import set_with_dataframe
  from nameparser import HumanName


  #-->Get JSESSIONID, li_a and csrf_token start<--
  service = Service(executable_path=r'/usr/bin/chromedriver')
  options = webdriver.ChromeOptions()
  options.add_argument("--headless")
  options.add_argument("--disable-gpu")
  options.add_argument("--window-size=1920x1080")
  options.add_argument("--lang=en-US")
  options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
  options.add_argument('--no-sandbox')
  options.add_argument('--disable-dev-shm-usage')
  driver = webdriver.Chrome(service=service, options=options)

  driver.get("https://www.linkedin.com")
  driver.add_cookie({
      'name': 'li_at',
      'value': li_at,
      'domain': '.linkedin.com'
  })
  driver.get("https://www.linkedin.com/sales/home")
  time.sleep(5)
  driver.get("https://www.linkedin.com/sales/search/people?query=(filters%3AList((type%3ACURRENT_COMPANY%2Cvalues%3AList((id%3Aurn%253Ali%253Aorganization%253A18875652%2CselectionType%3AINCLUDED)))))")
  time.sleep(10)
  logs = driver.get_log('performance')
  for entry in logs:
      log = json.loads(entry['message'])['message']
      if log['method'] == 'Network.requestWillBeSent':
          request_url = log['params']['request']['url']
          if request_url.startswith("https://www.linkedin.com/sales-api/salesApiAccess"):
              data_str = json.dumps(log['params']['request'], indent=4)
              data = json.loads(data_str)
              csrf_token = data['headers']['Csrf-Token']
              break
  all_cookies = driver.get_cookies()
  linkedin_cookies = [cookie for cookie in all_cookies
                      if '.linkedin.com' in cookie['domain']
                      and cookie['name'] in ['JSESSIONID', 'li_a']]
  li_a = None
  JSESSIONID = None
  for cookie in linkedin_cookies:
      if cookie['name'] == 'li_a':
          li_a = cookie['value']
      elif cookie['name'] == 'JSESSIONID':
          JSESSIONID = cookie['value']
  driver.quit()
  #-->Get JSESSIONID, li_a and csrf_token end<--

  def sanitize_data(data):
      if isinstance(data, list):
          return [sanitize_data(item) for item in data]
      elif isinstance(data, dict):
          return {key: sanitize_data(value) for key, value in data.items()}
      elif isinstance(data, float):
        if np.isinf(data) or np.isnan(data):
            return ''
        return data
      else:
          return data

  def get_excel_column_name(n):
      string = ""
      while n > 0:
          n, remainder = divmod(n - 1, 26)
          string = chr(65 + remainder) + string
      return string

  def update_with_backoff(range_name, chunk):
      backoff_time = 1
      max_backoff_time = 64
      while True:
          try:
              worksheet.update(range_name, chunk, value_input_option='RAW')
              break
          except Exception as e:
              if "429" in str(e):
                  time.sleep(backoff_time)
                  backoff_time = min(2 * backoff_time, max_backoff_time)
              else:
                  raise

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

  def safe_extract(data, *keys):
    try:
      for key in keys:
        data = data[key]
      return data
    except (KeyError, IndexError, TypeError):
      return None

  #kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com
  from google.oauth2.service_account import Credentials
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
  worksheet = spreadsheet.worksheet(sheetName)
  data = worksheet.get_all_values()
  dataframe = pd.DataFrame(data[1:], columns=data[0])


  dataframe.drop_duplicates(subset=[columnName], inplace=True)

  columnName_values = dataframe[columnName].tolist()

  #sh donde se imprimirá
  spreadsheetcont = client.open_by_url(spreadsheetUrl)
  contact_tabname="ContactsP2"
  worksheetcont = spreadsheetcont.worksheet(contact_tabname)



  print("Search export scrape")
  progress_bar = tqdm(total = len(columnName_values))

  df_final = pd.DataFrame()

  too_many_requests = False

  for index, value in enumerate(columnName_values):
      if too_many_requests:
            break
      query_params = unquote(urlparse(value).query)
      request_url = f'https://www.linkedin.com/sales-api/salesApiLeadSearch?q=searchQuery&{query_params}&start=0&count=100&decorationId=com.linkedin.sales.deco.desktop.searchv2.LeadSearchResult-14'
      df_loop_final = pd.DataFrame()
      error = None
      loop_counter = 0

      #---------------------------------------------------------------------------
      page_counter = 0
      #---------------------------------------------------------------------------

      while True:
          df_loop_base = pd.DataFrame()
          response = requests.get(url=request_url, cookies=cookies, headers=headers)
          status_code = response.status_code
          if status_code == 429:
              print("Too many requests!")
              too_many_requests = True
              break
          if status_code == 200:
            data = response.json()
            totalDisplayCount = data.get('metadata', {}).get('totalDisplayCount', None)
            if totalDisplayCount:
                try:
                    totalDisplayCount = int(totalDisplayCount)
                except ValueError:
                    totalDisplayCount = 2500
            if totalDisplayCount and int(totalDisplayCount) > 0:
                contact_details = safe_extract(data['elements'])
                for index_contact in range(len(contact_details)):
                    contact_lastName = safe_extract(contact_details[index_contact], 'lastName')
                    contact_geoRegion = safe_extract(contact_details[index_contact], 'geoRegion')
                    contact_openLink = safe_extract(contact_details[index_contact], 'openLink')
                    contact_premium = safe_extract(contact_details[index_contact], 'premium')
                    currentPositions_tenureAtPosition_numYears = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'tenureAtPosition', 'numYears')
                    currentPositions_tenureAtPosition_numMonths = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'tenureAtPosition', 'numMonths')
                    year_suffix_tenureAtPosition = " year" if currentPositions_tenureAtPosition_numYears == 1 else " years"
                    month_suffix_tenureAtPosition = " month" if currentPositions_tenureAtPosition_numMonths == 1 else " months"
                    suffixes_tenureAtPosition = (year_suffix_tenureAtPosition, month_suffix_tenureAtPosition)
                    values_tenureAtPosition = (currentPositions_tenureAtPosition_numYears, currentPositions_tenureAtPosition_numMonths)
                    formatted_values_tenureAtPosition = [f"{value}{suffix}" for value, suffix in zip(values_tenureAtPosition, suffixes_tenureAtPosition) if value is not None]
                    if formatted_values_tenureAtPosition:
                        formatted_values_tenureAtPosition.append('in role')
                    durationInRole = ' '.join(formatted_values_tenureAtPosition) if formatted_values_tenureAtPosition else None
                    currentPositions_companyName = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'companyName')
                    currentPositions_title = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'title')
                    currentPositions_companyUrnResolutionResult_name = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'companyUrnResolutionResult', 'name')
                    currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_rootUrl = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'companyUrnResolutionResult', 'companyPictureDisplayImage', 'rootUrl')
                    currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_artifacts_0_fileIdentifyingUrlPathSegment = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'companyUrnResolutionResult', 'companyPictureDisplayImage', 'artifacts', 0, 'fileIdentifyingUrlPathSegment')
                    logo200x200 = currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_rootUrl + currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_artifacts_0_fileIdentifyingUrlPathSegment if currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_rootUrl is not None and currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_artifacts_0_fileIdentifyingUrlPathSegment is not None else None
                    currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_artifacts_1_fileIdentifyingUrlPathSegment = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'companyUrnResolutionResult', 'companyPictureDisplayImage', 'artifacts', 1, 'fileIdentifyingUrlPathSegment')
                    logo100x100 = currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_rootUrl + currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_artifacts_1_fileIdentifyingUrlPathSegment if currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_rootUrl is not None and currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_artifacts_1_fileIdentifyingUrlPathSegment is not None else None
                    currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_artifacts_2_fileIdentifyingUrlPathSegment = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'companyUrnResolutionResult', 'companyPictureDisplayImage', 'artifacts', 2, 'fileIdentifyingUrlPathSegment')
                    logo400x400 = currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_rootUrl + currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_artifacts_2_fileIdentifyingUrlPathSegment if currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_rootUrl is not None and currentPositions_companyUrnResolutionResult_companyPictureDisplayImage_artifacts_2_fileIdentifyingUrlPathSegment is not None else None
                    currentPositions_companyUrnResolutionResult_location = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'companyUrnResolutionResult', 'location')
                    currentPositions_companyUrn = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'companyUrn')
                    currentPositions_companyUrn = re.search(r'\d+', currentPositions_companyUrn).group() if currentPositions_companyUrn and re.search(r'\d+', currentPositions_companyUrn) else None
                    currentPositions_current = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'current')
                    currentPositions_tenureAtCompany_numYears = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'tenureAtCompany', 'numYears')
                    currentPositions_tenureAtCompany_numMonths = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'tenureAtCompany', 'numMonths')
                    year_suffix_tenureAtCompany = " year" if currentPositions_tenureAtCompany_numYears == 1 else " years"
                    month_suffix_tenureAtCompany = " month" if currentPositions_tenureAtCompany_numMonths == 1 else " months"
                    suffixes_tenureAtCompany = (year_suffix_tenureAtCompany, month_suffix_tenureAtCompany)
                    values_tenureAtCompany = (currentPositions_tenureAtCompany_numYears, currentPositions_tenureAtCompany_numMonths)
                    formatted_values_tenureAtCompany = [f"{value}{suffix}" for value, suffix in zip(values_tenureAtCompany, suffixes_tenureAtCompany) if value is not None]
                    if formatted_values_tenureAtCompany:
                        formatted_values_tenureAtCompany.append('in company')
                    durationInCompany = ' '.join(formatted_values_tenureAtCompany) if formatted_values_tenureAtCompany else None
                    currentPositions_startedOn_month = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'startedOn', 'month')
                    currentPositions_startedOn_year = safe_extract(contact_details[index_contact], 'currentPositions', 0, 'startedOn', 'year')
                    if currentPositions_startedOn_month is not None and currentPositions_startedOn_year is not None:
                        month_name_startedOn = calendar.month_abbr[currentPositions_startedOn_month]
                        startDate = f"{month_name_startedOn} {currentPositions_startedOn_year}"
                    elif currentPositions_startedOn_month is None and currentPositions_startedOn_year is not None:
                        startDate = str(currentPositions_startedOn_year)
                    else:
                        startDate = None
                    contact_entityUrn = safe_extract(contact_details[index_contact], 'entityUrn')
                    contact_entityUrn = re.search(r'\(([^,]+)', contact_entityUrn).group(1) if contact_entityUrn and re.search(r'\(([^,]+)', contact_entityUrn) else None
                    linkedInProfileUrl = 'https://www.linkedin.com/in/' + contact_entityUrn + '/' if contact_entityUrn is not None else None
                    contact_degree = safe_extract(contact_details[index_contact], 'degree')
                    contact_degree_mapping = {3: "3rd", 2: "2nd", 1: "1st", -1: "Out of network"}
                    contact_degree = contact_degree_mapping.get(contact_degree, contact_degree)
                    contact_fullName = safe_extract(contact_details[index_contact], 'fullName')
                    contact_firstName = safe_extract(contact_details[index_contact], 'firstName')
                    profileUrl = 'https://www.linkedin.com/sales/lead/' + contact_entityUrn + ',' if contact_entityUrn is not None else None
                    companyUrl = 'https://www.linkedin.com/sales/company/' + currentPositions_companyUrn + '/' if currentPositions_companyUrn is not None else None
                    regularCompanyUrl = 'https://www.linkedin.com/company/' + currentPositions_companyUrn + '/' if currentPositions_companyUrn is not None else None
                    current_timestamp = datetime.now()
                    timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    all_variables = locals()
                    selected_vars = {var: [all_variables[var]] for var in ['contact_lastName','contact_geoRegion','contact_openLink','contact_premium','contact_premium','durationInRole','currentPositions_companyName','currentPositions_title','currentPositions_companyUrnResolutionResult_name','logo200x200','logo100x100','logo400x400','currentPositions_companyUrnResolutionResult_location','currentPositions_companyUrn','currentPositions_current','durationInCompany','startDate','contact_entityUrn','contact_degree','contact_fullName','contact_firstName','linkedInProfileUrl','profileUrl','companyUrl','regularCompanyUrl','timestamp']}
                    df_loop_base = pd.DataFrame(selected_vars)
                    df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'error': [error]})
                    df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
                    df_final = pd.concat([df_final, df_loop_final])
            else:
                error = 'No result found'
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'error': [error]})
                df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
                df_final = pd.concat([df_final, df_loop_final])

            #---------------------------------------------------------------------------
            page_counter += 1
            if page_counter >= max_pages:
              break
            #---------------------------------------------------------------------------

            if totalDisplayCount == 2500 and loop_counter >= 25:
                break
            if int(urlparse(request_url).query.split('&start=')[1].split('&')[0]) + 100 < data['paging']['total']:
                start_val = int(urlparse(request_url).query.split('&start=')[1].split('&')[0]) + 100
                request_url = request_url.replace(f'start={start_val - 100}', f'start={start_val}')
                loop_counter += 1
            else:
                break
      index += 1
      progress_bar.update(1)
      progress_bar.refresh()

  columns_rename = {
      'contact_lastName': 'lastName',
      'contact_geoRegion': 'location',
      'contact_openLink': 'isOpenLink',
      'contact_premium': 'isPremium',
      'currentPositions_companyUrnResolutionResult_name': 'companyName',
      'currentPositions_title': 'title',
      'currentPositions_companyName': 'linkedinCompanyName',
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

  try:
      df_final = df_final[['query','error','profileUrl','linkedInProfileUrl','vmid','firstName','lastName','fullName','location','title','companyId','companyUrl','regularCompanyUrl','companyName','linkedinCompanyName','startDateInRole','durationInRole','durationInCompany','companyLocation','logo100x100','logo200x200','logo400x400','connectionDegree','isCurrent','isOpenLink','isPremium','timestamp']]
  except KeyError as e:
      df_final = dataframe


  #Reordenando los resultados
  fill_empty_columns = [
      'email', 'status', 'phone1', 'phone2', 'directPhone', 'personalPhone',
      'standardTitle', 'standardTitles', 'seniority', 'function', 'persona',
      'city', 'state', 'country','time zone','domain','Industry','HQ Location', 'CompanyName'
  ]


  # Haciendo una copia de df_final para evitar el warning
  df_final_julio = df_final.copy()

  for col in fill_empty_columns:
      df_final_julio[col] = ""

  df_final_julio['time zone'] = ''
  df_final_julio['Domain'] = ''
  df_final_julio['Industry'] = ''
  df_final_julio['HQ Location'] = ''
  df_final_julio['CompanyName'] = ''


  # Ordenando las columnas según lo especificado
  ordered_columns = [
      'query','fullName', 'firstName', 'lastName', 'email', 'status', 'linkedInProfileUrl',
      'vmid', 'phone1', 'phone2','title',
      'standardTitle', 'seniority', 'function', 'persona',
      'location', 'city', 'state', 'country', 'time zone', 'companyId', 'linkedinCompanyName',
      'CompanyName','Domain','Industry','HQ Location'
  ]

  df_final_julio = df_final_julio[ordered_columns]

  #Leyendo e importando los datos de lotions
  spreadsheet_Loc = client.open_by_url("https://docs.google.com/spreadsheets/d/1OZst6QPAzBgLi-lv5cHU5McOGwlUig1SHyMb6_ErHTs/edit#gid=1479286735")
  worksheet_Loc = spreadsheet_Loc.worksheet("Locations")

  data_Loc = worksheet_Loc.get_all_values()
  dataframe_Loc = pd.DataFrame(data_Loc[1:], columns=data_Loc[0])

  columnName_Loc= "CONTACT LOCATION"
  dataframe_Loc.drop_duplicates(subset=[columnName_Loc], inplace=True)

  columnName_values = dataframe_Loc[columnName_Loc].tolist()

  #Leyendo e importando los datos de TZ
  spreadsheet_TZ = client.open_by_url("https://docs.google.com/spreadsheets/d/1OZst6QPAzBgLi-lv5cHU5McOGwlUig1SHyMb6_ErHTs/edit#gid=568103393")
  worksheet_TZ = spreadsheet_TZ.worksheet("TIME ZONES")

  data_TZ = worksheet_TZ.get_all_values()
  dataframe_TZ = pd.DataFrame(data_TZ[1:], columns=data_TZ[0])

  columnName_TZ= "State"
  dataframe_TZ.drop_duplicates(subset=[columnName_TZ], inplace=True)

  columnName_values = dataframe_TZ[columnName_TZ].tolist()


  #Wrangling
  # Limpieza y preparación de los datos
  df_final_julio['location'] = df_final_julio['location'].apply(lambda x: str(x).rstrip(' .'))
  dataframe_Loc['CONTACT LOCATION'] = dataframe_Loc['CONTACT LOCATION'].apply(lambda x: str(x).strip())

  # Realizando el merge

  #Locations
  df_final_julio = pd.merge(df_final_julio, dataframe_Loc[['CONTACT LOCATION', 'City', 'State', 'Country']],
                left_on='location',
                right_on='CONTACT LOCATION',
                how='left')
  # Manejo de valores nulos para las nuevas columnas
  df_final_julio['city'] = df_final_julio['City'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df_final_julio['state'] = df_final_julio['State'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df_final_julio['country'] = df_final_julio['Country'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df_final_julio.drop(columns=['CONTACT LOCATION','City', 'State', 'Country'], inplace=True)

  #TimeZone
  dataframe_TZ['State'] = dataframe_Loc['State'].apply(lambda x: str(x).strip())
  df_final_julio = pd.merge(df_final_julio, dataframe_TZ[['State', 'Time Zone']],
                left_on='state',
                right_on='State',
                how='left')
  df_final_julio['time zone'] = df_final_julio['Time Zone'].fillna('')
  df_final_julio.drop(columns=['State', 'Time Zone'], inplace=True)

  #Company Fields
  df_final_julio = pd.merge(df_final_julio, dataframe[['mainCompanyID','companyName', 'domain','industry','headquarter_companyAddress']],
                left_on='companyId',
                right_on='mainCompanyID',
                how='left')
  # Manejo de valores nulos para las nuevas columnas
  df_final_julio['CompanyName'] = df_final_julio['companyName'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df_final_julio['Domain'] = df_final_julio['domain'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df_final_julio['Industry'] = df_final_julio['industry'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df_final_julio['HQ Location'] = df_final_julio['headquarter_companyAddress'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df_final_julio.drop(columns=['mainCompanyID','companyName', 'domain', 'industry','headquarter_companyAddress'], inplace=True)

  #Limpiar Nombres:
  def procesar_nombre(nombre_completo):
      nombre = HumanName(nombre_completo)
      return pd.Series([nombre.first, nombre.last, str(nombre)])

  # Aplicando la función a cada nombre completo en la columna 'fullName'
  df_final_julio['fullName'] = df_final_julio['fullName'].astype(str)
  df_final_julio[['firstName', 'lastName', 'cleanFullName']] = df_final_julio['fullName'].apply(procesar_nombre)
  df_final_julio['fullName'] = df_final_julio['cleanFullName']
  df_final_julio.drop(columns=['cleanFullName'], inplace=True)


  df_final_julio.drop_duplicates(subset=['linkedInProfileUrl'], inplace=True)

  df_final_julio['fullName'] = df_final_julio['fullName'].apply(lambda x: str(x).strip().title())
  df_final_julio['firstName'] = df_final_julio['firstName'].apply(lambda x: str(x).strip().title())
  df_final_julio['lastName'] = df_final_julio['lastName'].apply(lambda x: str(x).strip().title())

  #-->PRINTING PART START<--
  set_with_dataframe(worksheetcont, df_final_julio, include_index=False, resize=True, allow_formulas=True)

  #-->PRINTING PART END<--


import streamlit as st
import time


st.title("List Building Script")
("List Builder Script")

# Campos para ingresar los parámetros
spreadsheetUrl = st.text_input("Enter the Google Spreadsheet URL:")
li_at = st.text_input("Enter your LinkedIn li_at cookie:")

if st.button("Run Scraper"):
    if not spreadsheetUrl or not li_at:
        st.error("Please enter both the Spreadsheet URL and the LinkedIn li_at cookie.")
    else:
        with st.spinner("Running the scraper. This could take a few minutes depending of the list size..."):
            try:
                result = linkedin_comp_scrapper(spreadsheetUrl, li_at)
                st.success("Scraping completed!")
                st.write(result)  # Assuming result is a DataFrame or something displayable
            except Exception as e:
                st.error(f"An error occurred: {e}")
