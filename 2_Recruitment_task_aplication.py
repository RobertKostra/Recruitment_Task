import sqlite3
import sys

# Ścieżka do bazy danych
DB_PATH = 'moja_baza.db'


# Funkcje pomocnicze
def connect_db():
    """Nawiązuje połączenie z bazą danych."""
    return sqlite3.connect(DB_PATH)


def check_password(stored_password, provided_password):
    """Sprawdza, czy hasła są identyczne."""
    return stored_password == provided_password


# Klasa do autentykacji
class Authentication:
    def __init__(self, conn):
        self.conn = conn

    def login(self, login, password):
        """Logowanie użytkownika."""
        cursor = self.conn.cursor()
        user = None
        if "@" in login:  # Logowanie za pomocą emaila
            cursor.execute("SELECT * FROM users WHERE email = ?", (login,))
        else:  # Logowanie za pomocą numeru telefonu
            cursor.execute("SELECT * FROM users WHERE telephone_number = ?", (login,))
        user = cursor.fetchone()

        if user and check_password(user[4], password):
            return user
        else:
            return None


# Klasa do działań użytkownika
class UserActions:
    def __init__(self, conn, user):
        self.conn = conn
        self.user = user

    def print_all_accounts(self):
        """Wyświetla liczbę wszystkich kont, jeśli użytkownik ma rolę admina."""
        if self.user[5] != 'admin':
            return "Niepoprawne dane logowania"
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        return count

    def print_oldest_account(self):
        """Wyświetla najstarsze konto, jeśli użytkownik ma rolę admina."""
        if self.user[5] != 'admin':
            return "Niepoprawne dane logowania"

        cursor = self.conn.cursor()
        cursor.execute("SELECT firstname, email, created_at FROM users ORDER BY created_at LIMIT 1")
        oldest_account = cursor.fetchone()
        if oldest_account:
            return f"name: {oldest_account[0]}\nemail_address: {oldest_account[1]}\ncreated_at: {oldest_account[2]}"
        else:
            return "No accounts found"

    def group_by_age(self):
        """Grupuje dzieci według wieku, jeśli użytkownik ma rolę admina."""
        if self.user[5] != 'admin':
            return "Niepoprawne dane logowania"

        cursor = self.conn.cursor()
        cursor.execute("SELECT age, COUNT(*) as count FROM children GROUP BY age ORDER BY count DESC")
        age_groups = cursor.fetchall()
        return age_groups

    def print_children(self):
        """Wyświetla dzieci danego użytkownika."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT name, age FROM children WHERE user_id = ? ORDER BY name", (self.user[0],))
        children = cursor.fetchall()
        return children

    def find_similar_children_by_age(self):
        """Znajduje dzieci innych użytkowników w podobnym wieku."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT age FROM children WHERE user_id = ?", (self.user[0],))
        ages = [age[0] for age in cursor.fetchall()]

        similar_children = []
        for age in ages:
            cursor.execute("""
                SELECT u.firstname, u.telephone_number, c.name, c.age 
                FROM children c 
                JOIN users u ON c.user_id = u.id 
                WHERE c.age = ? AND u.id != ?
                """, (age, self.user[0]))
            similar_children.extend(cursor.fetchall())

        return similar_children


# Główny skrypt
def main():
    conn = connect_db()
    auth = Authentication(conn)

    # Parsowanie argumentów linii komend
    args = sys.argv[1:]
    if len(args) < 2:
        print("Usage: script.py <command> --login <login> --password <password>")
        return

    command = args[0]
    login_index = args.index('--login') + 1
    password_index = args.index('--password') + 1
    login = args[login_index]
    password = args[password_index]
    # Logowanie
    user = auth.login(login, password)
    if not user:
        print("Invalid Login")
        return
    user_actions = UserActions(conn, user)
    # Wykonywanie poleceń
    if command == "print-all-accounts":
        print(user_actions.print_all_accounts())
    elif command == "print-oldest-account":
        print(user_actions.print_oldest_account())
    elif command == "group-by-age":
        age_groups = user_actions.group_by_age()
        for age, count in age_groups:
            print(f"age: {age}, count: {count}")
    elif command == "print-children":
        children = user_actions.print_children()
        for child in children:
            print(f"name: {child[0]}, age: {child[1]}")
    elif command == "find-similar-children-by-age":
        similar_children = user_actions.find_similar_children_by_age()
        for parent, telephone, child_name, age in similar_children:
            print(f"parent: {parent}, telephone: {telephone}, child: {child_name}, age: {age}")
    else:
        print('Invalid command.')


if __name__ == "__main__":
    main()
