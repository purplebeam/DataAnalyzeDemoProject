# Name: Konstantinos
# Surname: Papageorgiou
# Project: Ανάλυση δεδομένων - Datalabs

import pandas as pd
import psycopg2


# Βήμα 1: Δημιουργία κλάσης Employee
# Δημιουργήστε μια κλάση Python με την ονομασία Employee που θα περιλαμβάνει τα εξής χαρακτηριστικά:
# - first_name (το όνομα του υπαλλήλου)
# - last_name (το επώνυμο του υπαλλήλου)
# - age (η ηλικία του υπαλλήλου)
# - salary (ο μισθός του υπαλλήλου)
# - department (το τμήμα όπου εργάζεται ο υπάλληλος)
# Υλοποιήστε μέσα στην κλάση Employee έναν μέθοδο που θα ονομάζεται display_info
# και θα επιστρέφει μια συμβολοσειρά με τα στοιχεία του υπαλλήλου στην εξής μορφή: \
# "Όνομα Επώνυμο, Ηλικία: Ηλικία Υπαλλήλου, Μισθός: $Μισθός Υπαλλήλου, Τμήμα: Τμήμα Υπαλλήλου."

class Employee:
    def __init__(self, first_name, last_name, age, salary, department):
        self.first_name = first_name
        self.last_name = last_name
        self.age = age
        self.salary = salary
        self.department = department

    def display_info(self):
        return f"Όνομα: {self.first_name} {self.last_name}, Ηλικία: {self.age}, Μισθός: ${self.salary}, Τμήμα: {self.department}"

    # Επεκτείνετε την κλάση Employee προσθέτοντας τις εξής μεθόδους:
    # give_bonus(self, bonus_amount):
    # Αυτή η μέθοδος πρέπει να δέχεται ως παράμετρο το ποσό του μπόνους
    # και να αυξάνει το μισθό του υπαλλήλου κατά αυτό το ποσό.
    def give_bonus(self, bonus_amount):
        self.salary += bonus_amount

    # change_department(self, new_department):
    # Αυτή η μέθοδος πρέπει να δέχεται το όνομα του νέου τμήματος και να ενημερώνει το τμήμα του υπαλλήλου
    def change_department(self, new_department):
        self.department = new_department


# Βήμα 2: Διάβασμα αρχείου csv
# Χρησιμοποιήστε τη βιβλιοθήκη pandas για να διαβάσετε τα δεδομένα
# από το αρχείο CSV "employee_data.csv" σε ένα DataFrame.

file_path = 'pythondata.csv'
df = pd.read_csv(file_path, delimiter=';')

# Βήμα 3: Εμφάνιση αρχικών δεδομένων σε Dataframe
# Εμφανίστε τις πρώτες γραμμές του DataFrame για να ελέγξετε τα αρχικά δεδομένα.
print("Αρχικά Δεδομένα:")
print(df.head())

# Βήμα 4: Καθαρισμός δεδομένων
# 5. Πραγματοποιήστε τον καθαρισμό των δεδομένων,
# αντικαθιστώντας τις απουσιάζουσες τιμές (NaN)
# στις στήλες 'ηλικία' και 'μισθός' με τις μέσες τιμές των αντίστοιχων στηλών.
df['age'].fillna(df['age'].mean(), inplace=True)
df['salary'].fillna(df['salary'].mean(), inplace=True)

# Βήμα 5: Στατιστικά στοιχεία
# Υπολογίστε και εμφανίστε στατιστικά στοιχεία για την 'ηλικία' και τον 'μισθό' χρησιμοποιώντας τη μέθοδο describe.
print("\nΣτατιστικά Στοιχεία:")
print(df[['age', 'salary']].describe())

# Βήμα 6: Δημιουργία αντικειμένων Employee
# Δημιουργήστε παραδείγματα της κλάσης Employee
# για κάθε υπάλληλο στο σύνολο δεδομένων και αποθηκεύστε τα σε μια λίστα.
employees = []
for index, row in df.iterrows():
    employee = Employee(row['first_name'], row['last_name'], row['age'], row['salary'], row['department'])
    employees.append(employee)

# Βήμα 7: Εμφάνιση πληροφοριών για κάθε υπάλληλο
# Χρησιμοποιήστε ένα βρόχο για να εμφανίσετε πληροφορίες
# για κάθε υπάλληλο καλώντας τη μέθοδο display_info των αντικειμένων Employee.
print("\nΠληροφορίες Υπαλλήλων:")
for employee in employees:
    print(employee.display_info())

# Βήμα 8: Φιλτράρισμα υπαλλήλων άνω των 30 ετών
# Εφαρμόστε συνθήκες φιλτραρίσματος για να δημιουργήσετε ένα DataFrame
# που θα περιέχει μόνο υπαλλήλους που είναι πάνω από 30 ετών. Εμφανίστε τα δεδομένα που προκύπτουν από το φίλτρο.
filtered_df = df[df['age'] > 30]
print("\nΥπάλληλοι άνω των 30 ετών:")
print(filtered_df)

# Bonus Βήμα 9: Σύνδεση σε βάση δεδομένων PostgreSQL και αποθήκευση καθαρισμένων δεδομένων
hostname = "localhost123",
database = 'datalabspython',
username = 'postgres',
pwd = 'admin',
port_id = 5432

# Συνδεθείτε σε μια βάση δεδομένων PostgreSQL χρησιμοποιώντας μια βιβλιοθήκη όπως η psycopg2.
conn = psycopg2.connect(
    host=hostname,
    dbname=database,
    user=username,
    password=pwd,
    port=port_id)

cursor = conn.cursor()

# Ορισμός Πίνακα στη βάση δεδομένων
# Δημιουργήστε έναν πίνακα στη βάση δεδομένων για την αποθήκευση των καθαρισμένων δεδομένων των υπαλλήλων.
table_creation_query = """
CREATE TABLE IF NOT EXISTS employees (
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INTEGER,
    salary FLOAT,
    department VARCHAR(50)
);
"""

cursor.execute(table_creation_query)
conn.commit()

# Εισάγετε τα καθαρισμένα δεδομένα των υπαλλήλων στη βάση δεδομένων PostgreSQL.
# Εισαγωγή καθαρισμένων δεδομένων στη βάση δεδομένων
for index, row in df.iterrows():
    insert_query = f"""
    INSERT INTO employees (first_name, last_name, age, salary, department)
    VALUES ('{row['first_name']}', '{row['last_name']}', {row['age']}, {row['salary']}, '{row['department']}');
    """
    cursor.execute(insert_query)

conn.commit()
conn.close()
