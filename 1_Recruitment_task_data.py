import json, csv, re, ast, os, sqlite3
import pandas as pd
import xml.etree.ElementTree as ET


# Funkcja do wczytania danych JSON
def read_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)


# Funkcja do wczytania danych CSV
def read_csv(file_path):
    return pd.read_csv(file_path, delimiter=';', on_bad_lines='skip')


# Funkcja do wczytania i przetworzenia danych XML
def read_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    data = []
    for user in root.findall('user'):
        user_data = {
            'firstname': user.find('firstname').text,
            'telephone_number': user.find('telephone_number').text,
            'email': user.find('email').text,
            'password': user.find('password').text,
            'role': user.find('role').text,
            'created_at': user.find('created_at').text,
            # Przetwarzanie listy dzieci, jeśli istnieje
            'children': [(child.find('name').text, child.find('age').text) for child in user.findall('children/child')]
        }
        data.append(user_data)

    return pd.DataFrame(data)


# Definicja wyrażenia regularnego do walidacji adresów e-mail
email_regex = re.compile(r'^[^@]+@[^@]+\.[A-Za-z0-9]{1,4}$')


# Funkcja do walidacji e-maili
def validate_email(email):
    return bool(email_regex.match(email))


# Funkcja do przekształcania numeru telefonu
def transform_phone_number(phone):
    if pd.isna(phone):
        return ''
    elif isinstance(phone, float):
        phone = str(int(phone))  # Zamienia wartość float na int, a następnie na string
    elif not isinstance(phone, str):
        phone = str(phone)  # Zamienia inne typy na string

    # Usuwanie znaków specjalnych i wiodących zer
    phone_clean = re.sub(r'\D', '', phone)
    phone_clean = phone_clean.lstrip('0')

    # Upewnienie się, że numer ma dokładnie 9 cyfr
    if len(phone_clean) > 9:
        phone_clean = phone_clean[-9:]

    return phone_clean


def remove_duplicates(file_path, output_file_path):
    # Wczytanie danych z pliku CSV
    data = pd.read_csv(file_path)

    # Konwersja kolumny 'created_at' na format daty i czasu
    data['created_at'] = pd.to_datetime(data['created_at'])

    # Sortowanie danych według 'created_at' w porządku malejącym
    data = data.sort_values(by='created_at', ascending=False)

    # Usunięcie duplikatów dla 'telephone_number' i 'email' osobno
    data_no_duplicates_telephone = data.drop_duplicates(subset='telephone_number', keep='first')
    data_no_duplicates_email = data.drop_duplicates(subset='email', keep='first')

    # Łączenie wyników, zachowując unikalność telefonów i e-maili
    combined_data = (pd.concat([data_no_duplicates_telephone, data_no_duplicates_email])
                     .drop_duplicates(subset=['telephone_number', 'email'], keep='first'))

    # Zapisanie oczyszczonych danych do nowego pliku CSV
    combined_data.to_csv(output_file_path, index=False)


def normalize_children_data(row):
    if pd.isna(row) or row == '[]':
        return []

    try:
        children_list = ast.literal_eval(row)
        if isinstance(children_list, list):
            if all(isinstance(child, tuple) for child in children_list):
                return [{'name': child[0], 'age': int(child[1])} for child in children_list]
            elif all(isinstance(child, dict) for child in children_list):
                return children_list
    except:
        pass

    children_str_format = re.findall(r'(\w+) \((\d+)\)', row)
    if children_str_format:
        return [{'name': child[0], 'age': int(child[1])} for child in children_str_format]

    return []


def save_final_data():
    # Ścieżka do pliku źródłowego
    file_path = 'Data/valid_date.csv'

    # Wczytanie danych
    data = pd.read_csv(file_path)

    # Normalizacja danych w kolumnie 'children'
    data['children'] = data['children'].apply(normalize_children_data)

    # Ścieżka do zapisu nowego pliku
    output_file_path = 'Data/final_data.csv'

    # Zapisanie danych do nowego pliku CSV
    data.to_csv(output_file_path, index=False)

    print(f"Dane zostały zapisane do {output_file_path}")


# Funkcja do tworzenia bazy danych i tabel
def create_database():
    conn = sqlite3.connect('moja_baza.db')
    cursor = conn.cursor()

    # Tworzenie tabeli users
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            firstname TEXT,
            telephone_number TEXT,
            email TEXT,
            password TEXT,
            role TEXT,
            created_at TEXT
        )
    ''')

    # Tworzenie tabeli children
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS children (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            user_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')

    conn.commit()
    conn.close()


# Funkcja do importowania danych z pliku CSV
def import_data_from_csv(file_path):
    conn = sqlite3.connect('moja_baza.db')
    cursor = conn.cursor()

    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            # Dodanie użytkownika
            cursor.execute('''
                INSERT INTO users (firstname, telephone_number, email, password, role, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
            row['firstname'], row['telephone_number'], row['email'], row['password'], row['role'], row['created_at']))
            user_id = cursor.lastrowid

            # Dodanie dzieci użytkownika
            children = json.loads(row['children'].replace("'", "\""))  # Konwersja z formatu JSON
            for child in children:
                cursor.execute('''
                    INSERT INTO children (name, age, user_id)
                    VALUES (?, ?, ?)
                ''', (child['name'], child['age'], user_id))

    conn.commit()
    conn.close()


# Główna funkcja skryptu
def import_data_to_database():
    odpowiedz = input("Czy stworzyć nową bazę danych? T/N: ").strip().upper()
    if odpowiedz == 'T':
        create_database()
        import_data_from_csv('Data/final_data.csv')
        print("Baza danych i tabele zostały utworzone, dane zaimportowane.")
    else:
        print("Tworzenie bazy danych anulowane.")


# Ścieżki do plików
json_file_path = 'Data/users.json'
csv_file_path1 = 'Data/users_1.csv'
csv_file_path2 = 'Data/users_2.csv'
xml_file_path1 = 'Data/users_1.xml'
xml_file_path2 = 'Data/users_2.xml'

# Wczytywanie danych
json_data = pd.DataFrame(read_json(json_file_path))
csv_data_1 = read_csv(csv_file_path1)
csv_data_2 = read_csv(csv_file_path2)
xml_data_1 = read_xml(xml_file_path1)
xml_data_2 = read_xml(xml_file_path2)

# Połączenie danych
combined_data = pd.concat([json_data, csv_data_1, csv_data_2, xml_data_1, xml_data_2])

# Zapisanie połączonych danych do nowego pliku CSV
combined_data.to_csv('Data/date.csv', index=False)

# Ścieżka do oryginalnego pliku CSV
file_path = 'Data/date.csv'
# Wczytanie danych z pliku CSV
data = pd.read_csv(file_path)

# Stosowanie funkcji walidacji do adresów e-mail i zliczanie wyników
valid_count = 0
invalid_count = 0
for email in data['email']:
    if validate_email(email):
        valid_count += 1
    else:
        invalid_count += 1

# Wyświetlanie wyników
print(f"Liczba poprawnych e-maili: {valid_count}")
print(f"Liczba niepoprawnych e-maili: {invalid_count}")

# Filtrowanie danych tylko z poprawnymi e-mailami
valid_emails_data = data[data['email'].apply(validate_email)]

# Ścieżka do nowego pliku CSV, który będzie zawierał wyniki
output_file_path = 'Data/valid_mail.csv'

# Zapisanie wyników do nowego pliku CSV
valid_emails_data.to_csv(output_file_path, index=False)

print("Wiersze z poprawnymi e-mailami zostały zapisane w:", output_file_path)

# Ścieżka do oryginalnego pliku CSV
file_path = 'Data/valid_mail.csv'

# Wczytanie danych z pliku CSV
data = pd.read_csv(file_path)

# Przekształcanie numerów telefonów
data['telephone_number'] = data['telephone_number'].apply(transform_phone_number)

# Ścieżka do nowego pliku CSV
output_file_path = 'Data/valid_phone_number.csv'

# Zapisanie wyników do nowego pliku CSV
data.to_csv(output_file_path, index=False)

print("Przekształcenie numerów telefonów zakończone. Wyniki zapisane w:", output_file_path)

# Ścieżka do pliku, który zostanie przeczytany
file_path = 'Data/valid_phone_number.csv'

# Ścieżka do nowego pliku, który będzie zawierał wiersze z numerami telefonów
new_file_path = 'Data/only_with_number.csv'

# Wyrażenie regularne do wykrywania poprawnych numerów telefonów
phone_number_pattern = re.compile(r'\d+')

# Zmienne do liczenia wierszy z i bez numerów telefonów
rows_with_phone = 0
rows_without_phone = 0

# Lista do przechowywania wierszy z numerami telefonów
rows_with_phone_numbers = []

# Czytanie oryginalnego pliku i przetwarzanie każdego wiersza
with open(file_path, 'r') as file:
    reader = csv.reader(file)
    header = next(reader)  # Czytanie nagłówka
    rows_with_phone_numbers.append(header)  # Dodawanie nagłówka do nowego pliku

    for row in reader:
        if phone_number_pattern.search(row[1]):  # Sprawdzanie, czy kolumna z numerem telefonu zawiera poprawny numer
            rows_with_phone += 1
            rows_with_phone_numbers.append(row)
        else:
            rows_without_phone += 1

# Zapisywanie wierszy z numerami telefonów do nowego pliku
with open(new_file_path, 'w', newline='') as new_file:
    writer = csv.writer(new_file)
    writer.writerows(rows_with_phone_numbers)

# Wyświetlanie informacji na ekranie
print(f"Liczba wierszy z numerem telefonu: {rows_with_phone}")
print(f"Liczba wierszy bez numeru telefonu: {rows_without_phone}")

# Ścieżka do nowego pliku
print(f"Plik z wierszami zawierającymi numery telefonów: {new_file_path}")

# Ścieżki do plików
input_file_path = 'Data/only_with_number.csv'
output_file_path = 'Data/valid_date.csv'

remove_duplicates(input_file_path, output_file_path)

save_final_data()

import_data_to_database()

os.remove('Data/date.csv')
os.remove('Data/only_with_number.csv')
os.remove('Data/valid_date.csv')
os.remove('Data/valid_mail.csv')
os.remove('Data/valid_phone_number.csv')
# os.remove('Data/final_data.csv')
