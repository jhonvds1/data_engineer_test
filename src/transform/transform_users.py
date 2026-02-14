from ..extract.extract import extract_collection
import pandas as pd

data_users = extract_collection('users')


# ---------- tirar dados faltantes

# data_users = data_users.dropna(subset=['firstName', 'lastName', 'username', 'email', 'password'])

# ----------- tratar dados duplicados

# unique_fields = ['email', 'username', 'cpf', 'cnpj']
# data_users = data_users.drop_duplicates(subset=unique_fields, keep='first')


# ---------- tratamento nome

# invalid_names = data_users[(data_users['firstName'].str.strip() == '') | (data_users['firstName'].str.len() < 2) | (~data_users['firstName'].str.match(r'^[A-Za-zÀ-ÿ\s]+$'))]
# print(f"a: {invalid_names}")
# data_users['firstName'] = data_users['firstName'].str.strip().str.capitalize

# ---------- tratamento sobrenome

# invalid_names = data_users[(data_users['lastName'].str.strip() == '') | (data_users['lastName'].str.len() < 2) | (~data_users['lastName'].str.match(r'^[A-Za-zÀ-ÿ\s]+$'))]
# print(f"a: {invalid_names}")
# data_users['lastName'] = data_users['lastName'].str.strip().str.capitalize


# tratamento maidenname

# invalid_names = data_users[(~data_users['maidenName'].str.match(r'^[A-Za-zÀ-ÿ\s]+$')) & (data_users['maidenName'] != '')]
# data_users['maidenName'] = data_users['maidenName'].str.strip().str.capitalize


# tratamentp idade

# data_users = data_users[(data_users['age'] >=0) & (data_users['age'] <= 120)]

# tratamento gender
# data_users['gender'] = data_users['gender'].str.strip().str.lower()

# data_users['gender'] = data_users['gender'].replace({
#     'm' : 'male',
#     'f' : 'female',
#     'male' : 'male',
#     'female' : 'female'
# })


# tratamento phone
# data_users['phone'] = data_users['phone'].str.strip()
# data_users = data_users[data_users['phone'].str.startswith('+')]

#tratamento username

# data_users['username'] = data_users['username'].str.strip()
# data_users['username'] = data_users['username'].str.lower()

# tratamento password

# data_users['password'] = data_users['password'].str.strip()

#tratamento image

# data_users['image'] = data_users['image'].str.strip()

# tratamento sangue

# data_users['bloodGroup'] = data_users['bloodGroup'].str.strip().str.upper()

# tratamento peso e a ltura

# data_users = data_users[(data_users['height'] >= 0) & (data_users['weight'] >= 0)]

# tratamento cor do olho 

# data_users['eyeColor'] = data_users['eyeColor'].str.strip().str.capitalize()

# tratamento ip

# data_users['ip'] = data_users['ip'].str.strip()

#tratamento macAddress

# data_users['macAddress'] = data_users['macAddress'].str.strip()

# tratamento university

# data_users['university'] = data_users['university'].str.strip().str.capitalize()

# tratamento useragent

# data_users['userAgent'] = data_users['userAgent'].str.strip()

# tratamento role

# data_users['role'] = data_users['role'].str.strip().str.capitalize()

#tratamento cpf

# data_users['cpf'] = data_users['cpf'].str.strip()

#tratamento cnpj

# data_users['cnpj'] = data_users['cnpj'].str.strip()

# tratamento bithdate
# data_users['birthDate'] = data_users['birthDate'].str.strip()
# data_users['birthDate'] = pd.to_datetime(data_users['birthDate'], format='%Y-%m-%d')

# tratamento email

# data_users['email'] = data_users['email'].str.strip()
# data_users['email'] = data_users['email'].str.lower()

# email_pattern = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
# data_users = data_users[data_users['email'].str.match(email_pattern)]


# print(data_users.keys())
