from ..extract.extract import extract_collection
import pandas as pd


def drop_missing_values(data_users: pd.DataFrame) -> pd.DataFrame:
    return data_users.dropna(subset=['firstName', 'lastName', 'username', 'email', 'password'])

def drop_duplicates_values(data_users: pd.DataFrame) -> pd.DataFrame:
    unique_fields = ['email', 'username', 'cpf', 'cnpj']
    return data_users.drop_duplicates(subset=unique_fields, keep='first')

def clean_first_names(data_users: pd.DataFrame) -> pd.DataFrame:
    data_users = data_users[~((data_users['firstName'].str.strip() == '') | (data_users['firstName'].str.len() < 2) | (~data_users['firstName'].str.match(r'^[A-Za-zÀ-ÿ\s]+$')))]
    data_users['firstName'] = data_users['firstName'].str.strip().str.capitalize()
    return data_users

def clean_last_names(data_users: pd.DataFrame) -> pd.DataFrame:
    data_users = data_users[~((data_users['lastName'].str.strip() == '') | (data_users['lastName'].str.len() < 2) | (~data_users['lastName'].str.match(r'^[A-Za-zÀ-ÿ\s]+$')))]
    data_users['lastName'] = data_users['lastName'].str.strip().str.capitalize()
    return data_users

def clean_maiden_names(data_users: pd.DataFrame) -> pd.DataFrame:
    data_users = data_users[~((~data_users['maidenName'].str.match(r'^[A-Za-zÀ-ÿ\s]+$')) & (data_users['maidenName'] != ''))]
    data_users['maidenName'] = data_users['maidenName'].str.strip().str.capitalize()
    return data_users

def drop_inconsistent_values(data_users: pd.DataFrame) -> pd.DataFrame:
    data_users =  data_users[(data_users['age'] >=0) & (data_users['age'] <= 120)]
    return data_users[(data_users['height'] >= 0) & (data_users['weight'] >= 0)]


def parse_gender(data_users: pd.DataFrame) -> pd.DataFrame:
    data_users['gender'] = data_users['gender'].str.strip().str.lower()
    data_users['gender'] = data_users['gender'].replace({
        'm' : 'male',
        'f' : 'female',
        'male' : 'male',
        'female' : 'female'
    })
    return data_users

def clean_phone_numbers(data_users: pd.DataFrame) -> pd.DataFrame:
    data_users['phone'] = data_users['phone'].str.strip()
    data_users = data_users[data_users['phone'].str.startswith('+')]
    return data_users


def clean_username(data_users: pd.DataFrame) -> pd.DataFrame:
    data_users['username'] = data_users['username'].str.strip()
    data_users['username'] = data_users['username'].str.lower()
    return data_users

def clean_user_fields(data_users: pd.DataFrame) -> pd.DataFrame:
    columns_to_strip = [
        'image', 'bloodGroup', 'eyeColor', 'ip', 'macAddress',
        'university', 'userAgent', 'role', 'cnpj'
    ]
    for col in columns_to_strip:
        if col in data_users.columns:
            data_users[col] = data_users[col].astype(str).str.strip()
    return data_users

def clean_user_email(data_users: pd.DataFrame) -> pd.DataFrame:
    data_users['email'] = data_users['email'].str.strip()
    data_users['email'] = data_users['email'].str.lower()
    email_pattern = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
    return data_users[data_users['email'].str.match(email_pattern)]

def clean_user_birthdate(data_users: pd.DataFrame) -> pd.DataFrame:
    data_users['birthDate'] = data_users['birthDate'].str.strip()
    data_users['birthDate'] = pd.to_datetime(data_users['birthDate'], format='%Y-%m-%d')
    return data_users



def run_etl_users() -> pd.DataFrame:
    data_users = extract_collection('users')
    data_users = drop_missing_values(data_users)
    data_users = drop_duplicates_values(data_users)
    data_users = clean_first_names(data_users)
    data_users = clean_last_names(data_users)
    data_users = clean_maiden_names(data_users)
    data_users = drop_inconsistent_values(data_users)
    data_users = parse_gender(data_users)
    data_users = clean_phone_numbers(data_users)
    data_users = clean_username(data_users)
    data_users = clean_user_fields(data_users)
    data_users = clean_user_email(data_users)
    data_users = clean_user_birthdate(data_users)
    return data_users