from rest_framework.decorators import api_view
from django.http import JsonResponse
from bs4 import BeautifulSoup
import requests
import json
import os


CONST_YEAR = 2025


@api_view(['GET'])
def get_alpha_cpi(request):
    url = f'https://www.alphavantage.co/query?function=CPI&interval=monthly&apikey={os.getenv("API_KEY")}'
    r = requests.get(url)
    json_data = r.json()
    return JsonResponse(json_data)


@api_view(['GET'])
def get_alpha_unemployment(request):
    url = f'https://www.alphavantage.co/query?function=UNEMPLOYMENT&apikey={os.getenv("API_KEY")}'
    r = requests.get(url)
    json_data = r.json()
    return JsonResponse(json_data)


@api_view(['GET'])
def get_alpha_gdp(request):
    url = f'https://www.alphavantage.co/query?function=REAL_GDP&interval=annual&apikey={os.getenv("API_KEY")}'
    r = requests.get(url)
    json_data = r.json()
    return JsonResponse(json_data)


@api_view(['GET'])
def get_cpi(request):
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"registrationkey": os.getenv("REGISTRATION_KEY"),
                       "seriesid": ['CUUR0000SA0'],
                       "startyear": "2002",
                       "endyear": "2021",
                       "calculations": True,
                       "annualaverage": False})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
    result_data = json.loads(p.text)
    results = get_cpi_data(result_data)
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


def get_cpi_data(data):
    results = []
    for element in reversed(data['Results']['series'][0]['data']):
        if element['period'] != 'M13':
            results.append({'date': f"{element['year']}-{element['period'].replace('M', '')}",
                            'value': float(element['calculations']['pct_changes']['1'])})
    return results


@api_view(['GET'])
def get_unemployment(request):
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"registrationkey": os.getenv("REGISTRATION_KEY"),
                       "seriesid": ['LNS14000000'],
                       "startyear": "2002",
                       "endyear": "2021",
                       "annualaverage": True})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
    result_data = json.loads(p.text)
    results = get_unemployment_data(result_data)
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


def get_unemployment_data(data):
    results = []
    for element in reversed(data['Results']['series'][0]['data']):
        if element['period'] != 'M13':
            results.append({'date': f"{element['year']}-{element['period'].replace('M', '')}", 'value': float(element['value'])})
    return results


@api_view(['GET'])
def get_immigration(request):
    results = get_immigration_data(CONST_YEAR)
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


def get_immigration_data(year):
    try:
        page = requests.get(f'https://www.dhs.gov/immigration-statistics/yearbook/{year}/table1')
        soup = BeautifulSoup(page.content, 'html.parser')
        trs = soup.find('table').find_all('tr')
        results = []
        for tr in reversed(trs):
            th = tr.find('th')
            td = tr.find('td')
            try:
                results.append({'year': th.get_text()[0:4], 'value': int(td.get_text().replace(",", ""))})
            except AttributeError:
                pass
        return results
    except AttributeError:
        return get_immigration_data(year-1)

# https://www.dhs.gov/immigration-statistics/yearbook/2019/table39


@api_view(['GET'])
def get_deportation(request):
    results = get_deportation_data(CONST_YEAR)
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


def get_deportation_data(year):
    try:
        page = requests.get(f'https://www.dhs.gov/immigration-statistics/yearbook/{year}/table39')
        soup = BeautifulSoup(page.content, 'html.parser')
        trs = soup.find('table').find_all('tr')
        results = []
        for tr in reversed(trs):
            th = tr.find('th')
            td = tr.find('td')
            try:
                results.append({'year': th.get_text()[0:4], 'value': int(td.get_text().replace(",", ""))})
            except AttributeError:
                pass
        return results
    except AttributeError:
        return get_deportation_data(year-1)


@api_view(['GET'])
def get_immigration_deportation(request):
    all_data_dict = {}
    all_data_list = []
    immigration_data = get_immigration_data(CONST_YEAR)
    deportation_data = get_deportation_data(CONST_YEAR)
    for item in immigration_data:
        all_data_dict[item['year']] = {'immigration': item['value'], 'deportation': 0}
    for item in deportation_data:
        all_data_dict[item['year']]['deportation'] = item['value']
    for key in all_data_dict.keys():
        all_data_list.append({'year': key,
                              'immigration': all_data_dict[key]['immigration'],
                              'deportation': all_data_dict[key]['deportation']})
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': all_data_list}))
    return JsonResponse(json_data)



@api_view(['GET'])
def get_department_spending(request):
    url = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/statement_net_cost?sort=-record_date&format=json&page[size]=1000'
    r = requests.get(url)
    result_data = json.loads(r.text)
    results = get_department_spending_data(result_data['data'])
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


def get_department_spending_data(data):
    results = {}
    departments = []
    for element in data:
        if element['agency_nm'] != 'Total':
            department_name = element['agency_nm']
            department_name = clean_department_name(department_name)
            if department_name not in departments:
                departments.append(department_name)
    departments.sort()
    for department in departments:
        results[department] = []
    for element in data:
        if element['agency_nm'] != 'Total' and element['restmt_flag'] == "N":
            department_name = element['agency_nm']
            department_name = clean_department_name(department_name)
            results[department_name].append({'date': element['stmt_fiscal_year'],
                                             'cost_in_billions': element['net_cost_bil_amt']})
    for item in results:
        results[item].reverse()
    return results


def clean_department_name(department_name):
    department_name = department_name.lower()
    department_name = department_name.replace(".", "")
    department_name = department_name.replace("&", "and")
    department_name = department_name.replace(" the", "")
    department_name = department_name.replace("united states ", "")
    department_name = department_name.replace("us ", "")
    department_name = department_name.replace("s ", " ")
    return department_name


@api_view(['GET'])
def get_monthly_gas_prices(request):
    url = f'https://api.eia.gov/series/?api_key={os.getenv("KEY")}&series_id=PET.EMM_EPMR_PTE_NUS_DPG.M'
    r = requests.get(url)
    result_data = json.loads(r.text)
    results = get_monthly_gas_prices_data(result_data['series'][0]['data'])
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


def get_monthly_gas_prices_data(data):
    results = []
    for element in reversed(data):
        gas_date = f'{element[0][0:4]}-{element[0][4:6]}'
        gas_price = element[1]
        if gas_price is not None:
            results.append({'date': gas_date, 'price': gas_price})
    return results


@api_view(['GET'])
def get_executive_orders(request):
    results = get_executive_orders_data()
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


def get_executive_orders_data():
    page = requests.get(f'https://www.presidency.ucsb.edu/statistics/data/executive-orders')
    soup = BeautifulSoup(page.content, 'html.parser')
    trs = soup.find('table').find_all('tr')
    results = []
    for tr in trs:
        more_trs = tr.find_all('td')
        try:
            if "Total" in more_trs[1].get_text():
                data = more_trs[2].get_text().replace('\n', '')
                data = data.replace(',', '')
                results.append({'president': more_trs[0].get_text(),
                                "orders": data})
        except IndexError:
            pass
    return results


@api_view(['GET'])
def get_deficit(request):
    results = get_deficit_data()
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


def get_deficit_data():
    page = requests.get(f'https://www.presidency.ucsb.edu/statistics/data/federal-budget-receipts-and-outlays')
    soup = BeautifulSoup(page.content, 'html.parser')
    trs = soup.find('table').find_all('tr')
    results = []
    for tr in trs:
        more_trs = tr.find_all('td')
        try:
            if "estimate" in more_trs[0].get_text():
                break
            else:
                year = int(more_trs[1].get_text())
                data = more_trs[4].get_text().replace('\n', '')
                data = data.replace(',', '')
                data = data.replace(u'\xa0', u' ')
                if ' ' not in data and 'Outlays' not in data and year > 1975:
                    results.append({'year': year,
                                    'deficit': data})
        except (IndexError, ValueError):
            pass
    return results


@api_view(['GET'])
def get_initial_approval(request):
    results = get_approval_data("https://www.presidency.ucsb.edu/statistics/data/initial-presidential-job-approval-ratings")
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


@api_view(['GET'])
def get_final_approval(request):
    results = get_approval_data("https://www.presidency.ucsb.edu/statistics/data/final-presidential-job-approval-ratings")
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


def get_approval_data(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    trs = soup.find('table').find_all('tr')
    results = []
    for tr in trs:
        more_trs = tr.find_all('td')
        try:
            if "year" in more_trs[0].get_text():
                pass
            else:
                president = more_trs[2].get_text()
                approval = int(more_trs[3].get_text())
                disapproval = int(more_trs[4].get_text())
                results.append({'president': president, 'approval': approval, 'disapproval': disapproval})
        except (IndexError, ValueError):
            pass
    return results


@api_view(['GET'])
def get_temperature(request):
    year = 2025
    results = get_temperature_data(year)
    json_data = json.loads(json.dumps({'status': 'REQUEST_SUCCEEDED', 'data': results}))
    return JsonResponse(json_data)


def get_temperature_data(year):
    data_list = []
    r = requests.get(f'https://www.ncdc.noaa.gov/cag/global/time-series/globe/land_ocean/12/12/1880-{year}/data.json')
    if r.text == '':
        return get_temperature_data(year - 1)
    result_data = json.loads(r.text)
    for item in result_data['data']:
        data_list.append({'date': int(item), 'value': float(result_data['data'][item])})
    return data_list
