import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
load_dotenv()

BASE_URL = "https://sms.dhvsu.edu.ph"
LOGIN_URL = f"{BASE_URL}/auth/login"
GRADES_ENDPOINT = f"{BASE_URL}/students/grades/event" 


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': f"{BASE_URL}/#student/grades",
    'Accept': 'application/json, text/javascript, */*; q=0.01'
}

class StudentCredentials(BaseModel):
    student_number : int
    password : str

from typing import List, Optional

class StudentResponse(BaseModel):
    student_number: int
    year_level: int
    general_weighted_average: float  # Note: fix the typo 'weighet' in your DB/DataFrame
    academic_honors: str

app = FastAPI()
@app.post('/student', response_model=List[StudentResponse])
def StudentLogging(login_data : StudentCredentials):
    USERNAME = login_data.student_number
    PASSWORD = login_data.password
    clean_columns = [] 
    rows_data = []


    session = requests.Session()
    res = session.get(LOGIN_URL, headers=HEADERS)
    soup = BeautifulSoup(res.text, 'html.parser')
    token = soup.find('input', dict(name='_token'))['value']

    # USERNAME = student_number
    # PASSWORD = password

    login_payload = {
    'Username': USERNAME,
    'password': PASSWORD,
    '_token': token
    }

    login_response = session.post(LOGIN_URL, data=login_payload, headers=HEADERS)
    if login_response.url == LOGIN_URL: 
        raise HTTPException(401, 'WRONG PASSWORD')
    existing_credentials = []
    with psycopg.connect(
        host=os.getenv('DB_HOST'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')) as conn:
        with conn.cursor() as curr:
            credentials_select_query = 'SELECT student_number, password FROM student_credentials'
            curr.execute(credentials_select_query)
            data = curr.fetchall()
            for data in data:
                existing_credentials.append(data)

    existing_credentials2 =[]
    for row in existing_credentials:
        for entry in row:
            existing_credentials2.append(entry)
    term_mapping = {'V29mNXVxamJQb0xBQXdWMDlYTTRlQT09' : 20252,
                'a1h6ZEREdlZ5RFRQNENZNFc4blRHQT09' : 20251,
                'QVQvODYvQ1dTZjVuWFFzS25RSUptUT09' : 20242,
                'bTJka2NZTmkyYzdTV1M2MkNISU5MQT09' : 20241,
                'WDB3dU5pQWw2RFRodUJiTWd5N01GUT09' : 20232,
                'WTh6TjJzRlRJMllNNkZTU1lmOWgvdz09' : 20231,
                }

    terms = ['V29mNXVxamJQb0xBQXdWMDlYTTRlQT09',
            'a1h6ZEREdlZ5RFRQNENZNFc4blRHQT09',
            'QVQvODYvQ1dTZjVuWFFzS25RSUptUT09',
            'bTJka2NZTmkyYzdTV1M2MkNISU5MQT09',
            'WDB3dU5pQWw2RFRodUJiTWd5N01GUT09',
            'WTh6TjJzRlRJMllNNkZTU1lmOWgvdz09']
    existing_terms = []
    if USERNAME in existing_credentials2 :
        terms = []
        with psycopg.connect(
            host=os.getenv('DB_HOST'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')) as conn:
            with conn.cursor() as curr:
                curr.execute('SELECT term_code FROM student_aggregated_table WHERE student_number = %s', (USERNAME,))
                termcodes = curr.fetchall()
                for termcode in termcodes:
                    existing_terms.append(termcode[0])
                    existing_terms
                curr.execute("SELECT last_term FROM student_credentials WHERE student_number = %s", (USERNAME,))
                old_last_term_data = curr.fetchall()
                if old_last_term_data:
                    old_last_term = old_last_term_data

        for key, values in term_mapping.items():
            if values not in existing_terms and values > int(old_last_term[0][0]): 
                terms.append(key)
                print(f'Old User! Proceeding with extracting data from term/s: {values}')
    else : print('New User! Proceeding with extracting data from all terms!')

    if terms == ['V29mNXVxamJQb0xBQXdWMDlYTTRlQT09']:
        with psycopg.connect(
                host=os.getenv('DB_HOST'),
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                port=os.getenv('DB_PORT')) as conn:
                with conn.cursor() as curr:
                    curr.execute(f'SELECT student_number, year_level, general_weighted_average, academic_honors FROM global_student_table WHERE student_number = {USERNAME}')
                    data = curr.fetchall()
                    student_data = []
                    for row in data:
                        student_data.append(row)
                    student_showable = pd.DataFrame(student_data, columns=['student_number', 'year_level', 'general_weighted_average', 'academic_honors'])
                    return student_showable.to_dict(orient='records')

    term_dict = {
    'V29mNXVxamJQb0xBQXdWMDlYTTRlQT09' : ['2', '2025'],
    'a1h6ZEREdlZ5RFRQNENZNFc4blRHQT09' : ['1', '2025'],
    'QVQvODYvQ1dTZjVuWFFzS25RSUptUT09' : ['2', '2024'],
    'bTJka2NZTmkyYzdTV1M2MkNISU5MQT09' : ['1', '2024'],
    'WDB3dU5pQWw2RFRodUJiTWd5N01GUT09' : ['2', '2023'],
    'WTh6TjJzRlRJMllNNkZTU1lmOWgvdz09' : ['1', '2023'],
    }
    for term in terms:
        grades_payload = {
            "event": "grades",
            "term": term,
            "_token": token}
        try:
            response = session.post(GRADES_ENDPOINT, data=grades_payload, headers=HEADERS)
            data = response.json()

            if 'list' in data:
                grade_soup = BeautifulSoup(data['list'], 'html.parser')
                unclean_columns = grade_soup.find_all('th')
                clean_columns = [column.text.strip() for column in unclean_columns] + ['semester', 'school_year', 'student_number']
                term_data = term_dict.get(term, ['No Term Data'])
                all_rows = grade_soup.find_all('tr')

                for tr in all_rows:
                    cells = tr.find_all('td')
                    if len(cells) == 8:
                        first_entry = cells[0].get_text(strip=True)
                        if first_entry.endswith('.') and first_entry[:-1].isdigit():
                            new_row = [c.get_text(strip=True) for c in cells] + term_data + [USERNAME]
                            if new_row not in rows_data:
                                rows_data.append(new_row)
        except Exception as e:
            print(f'Error Found!: {e}')
    try:
        df = pd.DataFrame(rows_data, columns=clean_columns)
        df.columns = (df.columns
                        .str.strip()
                        .str.lower()
                        .str.replace(' ', '_'))
        df = df[df['final_average'] != '']
        df['term_code'] = df['school_year'].astype(str) + df['semester'].astype(str)


        program_data = grade_soup.find(class_="form-control").text.strip()
        program = 'Bachelor' + program_data.split('Bachelor')[-1]

        df['#'] = df['#'].str.replace('.', '').astype(int)
        df['section'] = program
        df = df.rename(columns={'#': 'subject_number', 'section': 'program'})
        df = df[['student_number', 'program', 'subject_number', 'code', 'descriptive', 'units', 'final_average', 'equivalent_grade', 'remarks', 'semester', 'school_year', 'term_code']]

        column_dtype_mapping = {
            'student_number' : int,
            'program' : str,
            'subject_number' : int,
            'code' : str,
            'descriptive' : str, 
            'units' : int,
            'final_average' : float,
            'equivalent_grade' : float,
            'remarks' : str,
            'semester' : int,
            'school_year' : int,
            'term_code' : int
        }

        df = df.astype(column_dtype_mapping)

        conn_url = f"postgresql+psycopg://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        engine = create_engine(conn_url, echo=False)
        df.to_sql(
            name='student_raw_staging',
            con = engine,
            if_exists='replace',
            index=False
        )
        last_term = df['term_code'].max()
        df1 = pd.DataFrame([[program, USERNAME, PASSWORD, last_term]], columns=['program','student_number', 'password', 'last_term'])
        df1.to_sql(
            name='student_credentials_staging',
            con = engine,
            if_exists='replace',
            index=False
        )
        print('Success')
    except Exception as e:
        if e == 'final_average' : print('No New Data Extracted')
    with psycopg.connect(
            host=os.getenv('DB_HOST'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')) as conn:
            with conn.cursor() as curr:
                curr.execute("CALL student_transformation_procedure();")
                conn.commit()
                curr.execute(f'SELECT student_number, year_level, general_weighted_average, academic_honors FROM global_student_table WHERE student_number = {USERNAME}')
                data = curr.fetchall()
                student_data = []
                for row in data:
                    student_data.append(row)
            student_showable = pd.DataFrame(student_data, columns=['student_number', 'year_level', 'general_weighted_average', 'academic_honors'])
            return student_showable.to_dict(orient='records')

