import logging
import pandas as pd
# Importa função de extração da coleção MongoDB
from ..extract.extract import extract_collection

# Configuração global de logs para o ETL de users
logging.basicConfig(
    level=logging.INFO,  # Nível INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Remove registros com valores obrigatórios ausentes
def drop_missing_values(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com valores ausentes obrigatórios")
    before = len(data_users)
    data_users = data_users.dropna(subset=['firstName', 'lastName', 'username', 'email', 'password', 'city', 'state', 'country'])
    removed = before - len(data_users)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por valores ausentes")
    logging.info(f"Registros restantes: {len(data_users)}")
    return data_users

# Remove duplicados com base em campos críticos
def drop_duplicates_values(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros duplicados (email, username, cpf, cnpj)")
    before = len(data_users)
    unique_fields = ['email', 'username', 'cpf', 'cnpj']
    data_users = data_users.drop_duplicates(subset=unique_fields, keep='first')
    removed = before - len(data_users)
    if removed > 0:
        logging.warning(f"{removed} registros duplicados removidos")
    logging.info(f"Registros restantes: {len(data_users)}")
    return data_users

# Limpa e padroniza firstName
def clean_first_names(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Limpando firstName")
    before = len(data_users)
    # Remove nomes vazios, com menos de 2 caracteres ou com caracteres inválidos
    data_users = data_users[~((data_users['firstName'].str.strip() == '') | 
                              (data_users['firstName'].str.len() < 2) | 
                              (~data_users['firstName'].str.match(r'^[A-Za-zÀ-ÿ\s]+$')))]
    removed = before - len(data_users)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por firstName inválido")
    # Padroniza capitalização e remove espaços
    data_users['firstName'] = data_users['firstName'].str.strip().str.capitalize()
    return data_users

# Limpa e padroniza lastName
def clean_last_names(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Limpando lastName")
    before = len(data_users)
    data_users = data_users[~((data_users['lastName'].str.strip() == '') | 
                              (data_users['lastName'].str.len() < 2) | 
                              (~data_users['lastName'].str.match(r'^[A-Za-zÀ-ÿ\s]+$')))]
    removed = before - len(data_users)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por lastName inválido")
    data_users['lastName'] = data_users['lastName'].str.strip().str.capitalize()
    return data_users

# Limpa maidenName (nome de solteira)
def clean_maiden_names(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Limpando maidenName")
    before = len(data_users)
    # Remove apenas registros com caracteres inválidos
    data_users = data_users[~((~data_users['maidenName'].str.match(r'^[A-Za-zÀ-ÿ\s]+$')) & (data_users['maidenName'] != ''))]
    removed = before - len(data_users)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por maidenName inválido")
    data_users['maidenName'] = data_users['maidenName'].str.strip().str.capitalize()
    return data_users

# Remove registros com valores inconsistentes de idade, altura ou peso
def drop_inconsistent_values(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Removendo registros com idade, altura ou peso inconsistentes")
    before = len(data_users)
    data_users = data_users[(data_users['age'] >= 0) & (data_users['age'] <= 120)]
    data_users = data_users[(data_users['height'] >= 0) & (data_users['weight'] >= 0)]
    removed = before - len(data_users)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por valores inconsistentes")
    logging.info(f"Registros restantes: {len(data_users)}")
    return data_users

# Padroniza campo gender
def parse_gender(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Padronizando campo gender")
    data_users['gender'] = data_users['gender'].str.strip().str.lower()
    data_users['gender'] = data_users['gender'].replace({'m': 'male', 'f': 'female', 'male': 'male', 'female': 'female'})
    return data_users

# Limpa números de telefone
def clean_phone_numbers(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Limpando phone numbers")
    before = len(data_users)
    data_users['phone'] = data_users['phone'].str.strip()
    data_users = data_users[data_users['phone'].str.startswith('+')]  # Garante padrão internacional
    removed = before - len(data_users)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por telefone inválido")
    return data_users

# Limpa username
def clean_username(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Limpando username")
    data_users['username'] = data_users['username'].str.strip().str.lower()
    return data_users

# Limpa campos opcionais do usuário
def clean_user_fields(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Limpando campos opcionais do usuário")
    columns_to_strip = ['image', 'bloodGroup', 'eyeColor', 'ip', 'macAddress',
                        'university', 'userAgent', 'role', 'cnpj', 'city', 'state', 'country']
    for col in columns_to_strip:
        if col in data_users.columns:
            data_users[col] = data_users[col].astype(str).str.strip()
    return data_users

# Valida e padroniza emails
def clean_user_email(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Limpando emails")
    before = len(data_users)
    data_users['email'] = data_users['email'].str.strip().str.lower()
    email_pattern = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
    data_users = data_users[data_users['email'].str.match(email_pattern)]
    removed = before - len(data_users)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por email inválido")
    return data_users

# Converte birthDate para datetime e remove inválidos
def clean_user_birthdate(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Convertendo birthDate para datetime")
    data_users['birthDate'] = data_users['birthDate'].str.strip()
    data_users['birthDate'] = pd.to_datetime(data_users['birthDate'], format='%Y-%m-%d', errors='coerce')
    before = len(data_users)
    data_users = data_users.dropna(subset=['birthDate'])
    removed = before - len(data_users)
    if removed > 0:
        logging.warning(f"{removed} registros removidos por birthDate inválido")
    return data_users

# Explode a coluna address em city, state e country
def explode_address(data_users: pd.DataFrame) -> pd.DataFrame:
    logging.info("Explodindo coluna address para city, state, country")
    address = pd.json_normalize(data_users['address'])
    cols = ['city', 'state', 'country']
    for col in cols:
        data_users[col] = address.get(col)
    return data_users

# Função principal do ETL de users
def run_etl_users() -> pd.DataFrame:
    logging.info("Iniciando ETL de users")
    try:
        # Extração da coleção MongoDB 'users'
        data_users = extract_collection('users')
        logging.info(f"{len(data_users)} registros extraídos da coleção 'users'")

        # Sequência completa de limpeza e transformação
        data_users = explode_address(data_users)
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

        logging.info(f"ETL de users concluído com {len(data_users)} registros válidos")
        return data_users
    except Exception as e:
        logging.error(f"Falha no ETL de users: {e}")
        raise
