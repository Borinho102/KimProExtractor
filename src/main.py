import re

import numpy as np
import pandas as pd
import json
import datetime
import mysql.connector


mydb = mysql.connector.connect(host="localhost", user="root", password="QhmMmplFIdFLzV7RHnLA", database="kim_pro", port=3306)


COUNTRIES = {
    "Deutschland": 50,
    "Österreich": 173,
    "Schweiz": 197,
    "Polen": 182,
    "Niederlande": 163,
    "nan": 50
}


CURRENCIES = {
    "EUR": 34,
    "CHF": 124,
    "PLN": 159
}


def companies_exporter():
    mydb = mysql.connector.connect(host="localhost", user="root", password="12345678", database="kim_pro")
    mycursor = mydb.cursor()

    companies = []
    bank = []
    df = pd.read_csv('customerCompany.csv')
    for index, row in df.iterrows():
        data = {
            "bankdata_id": int(row['pkey']) + 10000,
            "bankdata_iban": str(row['iban']),
            "bankdata_bank_name": str(row['bankName']),
            "bankdata_vat": str(row['vat']),
            "bankdata_account_number": str(row['kontoNr']),
            "bankdata_bank_code_number": str(row['blz']),
            "bankdata_bic": str(row['blz']),
            "bankdata_is_active": 1
        }
        bank.append(data)
        # print(json.dumps(data, indent=4))

    # Insert Bank Data
    print("+--------------------- INSERT BANK DATA ----------------------+")
    for el in bank:
        delSQL = "DELETE FROM bankdata WHERE bankdata_id = %s"
        try:
            mycursor.execute(delSQL, [el['bankdata_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)

        sql = "INSERT INTO bankdata ("
        param = " VALUES ("
        data = list(el.keys())
        for i, elem in enumerate(data):
            if i >= len(data) - 1:
                sql += str(elem) + ")"
                param += "%s)"
            else:
                sql += str(elem) + ", "
                param += "%s, "
        query = sql + param
        val = list(el.values())
        try:
            mycursor.execute(query, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

    addressesData = []
    for index, row in df.iterrows():
        address = {
            "key": str(int(row['pkey']) + 20000),
            "street": str(row['street']),
            "zip": str(row['zip']),
            "city": str(row['city']),
            "country": str(row['country'])
        }
        addressesData.append(address)
        # print(json.dumps(address, indent=4), "\n")

    # Insert Addresses
    print("+--------------------- INSERT ADDRESS DATA ----------------------+")
    for el in addressesData:
        print("+---------------------- COUNTRY ( " + str(el['key']) + " )----------------------------+")
        delSQL = "DELETE FROM country WHERE country_id = %s"
        try:
            mycursor.execute(delSQL, [el['key']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        sql = "INSERT INTO country (country_id, country_name, country_short) VALUES(%s, %s, %s)"
        val = [
            el['key'],
            el['country'],
            el['country'][:2]
        ]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------------- CITY ( " + str(el['key']) + " ) ----------------------------+")
        delSQL = "DELETE FROM city WHERE city_id = %s"
        try:
            mycursor.execute(delSQL, [el['key']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        sql = "INSERT INTO city (city_id, city_name, city_zip) VALUES(%s, %s, %s)"
        val = [
            el['key'],
            el['city'],
            el['zip']
        ]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------------- CITY - COUNTRY ( " + str(el['key']) + " ) ----------------------------+")
        delSQL = "DELETE FROM city_country_rel WHERE country_id = %s AND city_id = %s"
        try:
            mycursor.execute(delSQL, [el['key'], el['key']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        sql = "INSERT INTO city_country_rel (country_id, city_id) VALUES(%s, %s)"
        val = [el['key'], el['key']]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------------- ADDRESS ( " + str(el['key']) + " ) ----------------------------+")
        delSQL = "DELETE FROM address WHERE address_id = %s"
        try:
            mycursor.execute(delSQL, [el['key']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)

        sql = "INSERT INTO address (address_id, address_street) VALUES(%s, %s)"
        val = [
            el['key'],
            el['street']
        ]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------------- CITY - ADDRESS ( " + str(el['key']) + " ) ----------------------------+")
        delSQL = "DELETE FROM address_city_rel WHERE address_id = %s AND city_id = %s"
        try:
            mycursor.execute(delSQL, [el['key'], el['key']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        sql = "INSERT INTO address_city_rel (address_id, city_id) VALUES(%s, %s)"
        val = [el['key'], el['key']]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")


    for index, row in df.iterrows():
        data = {
            "company_id": int(row['pkey']) + 10000,
            "company_name": row['name'],
            "company_shortname": str(row['name'])[:2] + str(row['pkey']),
            "company_template_coverletter_subject": str(row['anschreibenVorlageBetreff']),
            "company_template_coverletter_text": str(row['anschreibenVorlageText'])
        }
        companies.append(data)
        print(json.dumps(data, indent=4))

    # Insert Company
    print("+--------------------- INSERT COMPANY DATA ----------------------+")
    for el in companies:
        print("COMPANY ID: ", el['company_id'])
        delSQL = "DELETE FROM company WHERE company_id = %s"
        try:
            mycursor.execute(delSQL, [el['company_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)

        sql = "INSERT INTO company ("
        param = " VALUES ("
        data = list(el.keys())
        for i, elem in enumerate(data):
            if i >= len(data) - 1:
                sql += str(elem) + ")"
                param += "%s)"
            else:
                sql += str(elem) + ", "
                param += "%s, "
        query = sql + param
        val = list(el.values())
        try:
            mycursor.execute(query, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

    # Insert Company Related
    print("+--------------------- INSERT COMPANY RELATED DATA ----------------------+")
    for el in companies:
        delSQL = "DELETE FROM company_address_rel WHERE company_id = %s AND address_id = %s"
        try:
            mycursor.execute(delSQL, [el['company_id'], str(int(el['company_id']) + 10000)])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        sql = "INSERT INTO company_address_rel (company_id, address_id) VALUES(%s, %s)"
        val = [el['company_id'], str(int(el['company_id']) + 10000)]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        delSQL = "DELETE FROM company_bankdata_rel WHERE company_id = %s AND bankdata_id = %s"
        try:
            mycursor.execute(delSQL, [el['company_id'], str(int(el['company_id']))])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        sql = "INSERT INTO company_bankdata_rel (company_id, bankdata_id) VALUES(%s, %s)"
        val = [el['company_id'], str(int(el['company_id']))]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")


def user_exporter():
    mydb = mysql.connector.connect(host="localhost", user="root", password="12345678", database="kim_pro")
    mycursor = mydb.cursor()

    addressData = {}
    addressesData = []
    df = pd.read_csv('addressdata.csv')
    for index, row in df.iterrows():
        data = {
            str(int(row['pkey']) + 10000): {
                "name": str(row['name']),
                "firstname": str(row['firstname']),
                "fax": str(row['fax']),
                "mobile": str(row['mobile']),
                "phone": str(row['phone']),
                "role": str(row['role']),
                "shortName": str(row['shortName']),
                "title": "Mr" if row['title'] == 0 else "Mme"
            }
        }
        addressData.update(data)
        address = {
            "key": str(int(row['pkey']) + 10000),
            "street": str(row['street']),
            "zip": str(row['zip']),
            "city": str(row['city']),
            "country": str(row['country'])
        }
        addressesData.append(address)

    # Insert Addresses
    for el in addressesData:
        print("+---------------------- COUNTRY ( " + str(el['key']) + " )----------------------------+")
        delSQL = "DELETE FROM city_country_rel WHERE country_id = %s AND city_id = %s"
        try:
            mycursor.execute(delSQL, [el['key'], el['key']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        delSQL = "DELETE FROM address_city_rel WHERE address_id = %s AND city_id = %s"
        try:
            mycursor.execute(delSQL, [el['key'], el['key']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        delSQL = "DELETE FROM country WHERE country_id = %s"
        try:
            mycursor.execute(delSQL, [el['key']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        delSQL = "DELETE FROM city WHERE city_id = %s"
        try:
            mycursor.execute(delSQL, [el['key']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)

        sql = "INSERT INTO country (country_id, country_name, country_short) VALUES(%s, %s, %s)"
        val = [
            el['key'],
            el['country'],
            el['country'][:2]
        ]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------------- CITY ( " + str(el['key']) + " ) ----------------------------+")
        sql = "INSERT INTO city (city_id, city_name, city_zip) VALUES(%s, %s, %s)"
        val = [
            el['key'],
            el['city'],
            el['zip']
        ]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------------- CITY - COUNTRY ( " + str(el['key']) + " ) ----------------------------+")
        sql = "INSERT INTO city_country_rel (country_id, city_id) VALUES(%s, %s)"
        val = [el['key'], el['key']]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------------- ADDRESS ( " + str(el['key']) + " ) ----------------------------+")
        delSQL = "DELETE FROM address WHERE address_id = %s"
        try:
            mycursor.execute(delSQL, [el['key']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        sql = "INSERT INTO address (address_id, address_street) VALUES(%s, %s)"
        val = [
            el['key'],
            el['street']
        ]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------------- CITY - ADDRESS ( " + str(el['key']) + " ) ----------------------------+")
        sql = "INSERT INTO address_city_rel (address_id, city_id) VALUES(%s, %s)"
        val = [el['key'], el['key']]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

    users = []
    df = pd.read_csv('user.csv')
    for index, row in df.iterrows():
        key = int(row['pkey']) + 10000
        data = {
            "user_id": key,
            "user_email": row['email'],
            "user_password": row['passwort'],
            "user_creation_time": datetime.datetime.fromtimestamp(row['creationTime']),
            "user_wrong_password_counter": row['wrongPasswordCounter'],
            "user_password_reset_required": row['passwordResetRequired'],
            "user_lastname": addressData[str(int(row['addressData']) + 10000)]["name"],
            "user_firstname": addressData[str(int(row['addressData']) + 10000)]["firstname"],
            "user_salutation": addressData[str(int(row['addressData']) + 10000)]["title"],
            "user_function": addressData[str(int(row['addressData']) + 10000)]["role"],
            "user_abbreviation": addressData[str(int(row['addressData']) + 10000)]["shortName"][:5],
            "user_locked": 0,
            "user_token": "string",
            "user_mobile": addressData[str(int(row['addressData']) + 10000)]["mobile"],
            "user_fix_phonenumber": addressData[str(int(row['addressData']) + 10000)]["phone"],
            "user_fax": addressData[str(int(row['addressData']) + 10000)]["fax"],
            "address_id": str(int(row['addressData']) + 10000)
        }
        users.append(data)

    # Insert Users
    for el in users:
        delSQL = "DELETE FROM user WHERE user_id = %s"
        try:
            mycursor.execute(delSQL, [el['user_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)

        sql = "INSERT INTO user ("
        param = " VALUES ("
        data = list(el.keys())
        for i, elem in enumerate(data):
            if i >= len(data) - 1:
                sql += str(elem) + ")"
                param += "%s)"
            else:
                sql += str(elem) + ", "
                param += "%s, "
        query = sql + param
        val = list(el.values())
        try:
            mycursor.execute(query, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

    # Insert User - Address Relation
    for el in users:
        delSQL = "DELETE FROM address_user_rel WHERE user_id = %s AND address_id = %s"
        try:
            mycursor.execute(delSQL, [el['user_id'], el['address_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
        sql = "INSERT INTO address_user_rel (address_id, user_id) VALUES(%s, %s)"
        val = [
            el['user_id'], el['address_id']
        ]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")


def getLocationDesc(s):
    contents = re.findall(r'\((.*?)\)', s)
    return contents[0] if len(contents) > 0 else ""


def getAddressDesc(s):
    return s.split("(")[0].strip()


def contract_exporter():
    mycursor = mydb.cursor()

    contract = {}
    df = pd.read_csv('contract.csv')
    for index, row in df.iterrows():
        key = "V" + str(row['pkey'])
        data = {
            key: {
                "contract_id": int(row['pkey']) + 190000,
                "contract_name": "Contract V" + str(row['pkey']),
                "contract_creation_time": str(datetime.datetime.fromtimestamp(row['creationTime'])),
                "contract_last_change_time": str(datetime.datetime.fromtimestamp(row['lastChangeTime']))
            }
        }
        contract.update(data)
        # print(json.dumps(data, indent=4))

    contracts = []
    contracts_extra = {}
    df = pd.read_excel('contracts.xlsx')
    for index, row in df.iterrows():
        # key = int(row['ContractID']) + 190000
        elem = contract.get(str(row['ContractID']), None)
        if elem is not None:
            elem.update({
                "contract_end_date": str(datetime.datetime.fromtimestamp(row['end'] if not np.isnan(row['end']) else 0)),
                "contract_begin_date": str(datetime.datetime.fromtimestamp(row['start'] if not np.isnan(row['start']) else 0)),
                "contract_first_possible_end_date": str(datetime.datetime.fromtimestamp(row['possibleEnd'] if not np.isnan(row['possibleEnd']) else 0))
            })
            contracts_extra.update({
                elem['contract_id'] : {
                    "company": str(row['firma']),
                    "store": str(row['store']),
                    "location": str(row['location']),
                    "city": str(row['city']),
                    "country": COUNTRIES[str(row['country'])],
                    "currency": CURRENCIES[str(row['currency'])],
                    "rental": str(row['rental']),
                    "appointment": str(row['appointment'])
                }
            })
            contracts.append(elem)
    # print(json.dumps(contracts_extra, indent=4))

    # Insert Contracts
    un = 0
    for el in contracts:
        delSQL = "DELETE FROM contract_location_rel WHERE contract_id = %s AND location_id = %s"
        try:
            mycursor.execute(delSQL, [el['contract_id'], el['contract_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("")
            print("Error:", e)
        delSQL = "DELETE FROM location_address_rel WHERE address_id = %s AND location_id = %s"
        try:
            mycursor.execute(delSQL, [el['contract_id'], el['contract_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("")
            print("Error:", e)
        delSQL = "DELETE FROM address_city_rel WHERE address_id = %s AND city_id = %s"
        try:
            mycursor.execute(delSQL, [el['contract_id'], el['contract_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("")
            print("Error:", e)
        delSQL = "DELETE FROM city_country_rel WHERE country_id = %s AND city_id = %s"
        try:
            mycursor.execute(delSQL, [contracts_extra[el['contract_id']]['country'], el['contract_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("")
            print("Error:", e)

        print("+------------------------- CONTRACT (" + str(el['contract_id']) + ") ----------------------------+")
        delSQL = "DELETE FROM contract WHERE contract_id = %s"
        try:
            mycursor.execute(delSQL, [el['contract_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("")
            print("Error:", e)
        sql = "INSERT INTO contract ("
        param = " VALUES ("
        data = list(el.keys())
        for i, elem in enumerate(data):
            if i >= len(data) - 1:
                sql += str(elem) + ")"
                param += "%s)"
            else:
                sql += str(elem) + ", "
                param += "%s, "
        query = sql + param
        val = list(el.values())
        try:
            mycursor.execute(query, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+------------------------- LOCATION (" + str(el['contract_id']) + ") ----------------------------+")
        delSQL = "DELETE FROM location WHERE location_id = %s"
        try:
            mycursor.execute(delSQL, [el['contract_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("+-----------------------------+")
            print("Error:", e)
        data = contracts_extra[el['contract_id']]
        sql = "INSERT INTO location (location_id, location_name, location_objectdescription) VALUES(%s, %s, %s)"
        val = [
            el['contract_id'],
            str(un) + " - " + data['location'],
            getLocationDesc(data['location'])
        ]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")
        un += 1

        print("+------------------------- ADDRESS (" + str(el['contract_id']) + ") ----------------------------+")
        data = contracts_extra[el['contract_id']]
        delSQL = "DELETE FROM address WHERE address_id = %s"
        try:
            mycursor.execute(delSQL, [el['contract_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("+-----------------------------+")
            print("Error:", e)
        sql = "INSERT INTO address (address_id, address_street) VALUES(%s, %s)"
        val = [
            el['contract_id'],
            getAddressDesc(data['location'])
        ]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------------- CITY ( " + str(el['contract_id']) + " ) ----------------------------+")
        delSQL = "DELETE FROM city WHERE city_id = %s"
        try:
            mycursor.execute(delSQL, [el['contract_id']])
            mydb.commit()
            print("record deleted.")
        except mysql.connector.IntegrityError as e:
            print("+-------:------+")
            print("Error:", e)
        sql = "INSERT INTO city (city_id, city_name) VALUES(%s, %s)"
        val = [
            el['contract_id'],
            data['city']
        ]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")


        print("+---------------- Country - City ( " + str(el['contract_id']) + " ) --------------------+")
        sql = "INSERT INTO city_country_rel (country_id, city_id) VALUES(%s, %s)"
        val = [data['country'], el['contract_id']]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")


        print("+---------------- City - Address ( " + str(el['contract_id']) + " ) --------------------+")
        sql = "INSERT INTO address_city_rel (address_id, city_id) VALUES(%s, %s)"
        val = [el['contract_id'], el['contract_id']]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------- Address - Location ( " + str(el['contract_id']) + " ) --------------------+")
        sql = "INSERT INTO location_address_rel (address_id, location_id) VALUES(%s, %s)"
        val = [el['contract_id'], el['contract_id']]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")

        print("+---------------- Location - Contract ( " + str(el['contract_id']) + " ) --------------------+")
        sql = "INSERT INTO contract_location_rel (contract_id, location_id) VALUES(%s, %s)"
        val = [el['contract_id'], el['contract_id']]
        try:
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "record inserted.")
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Record already exists in the database.")


if __name__ == '__main__':
    # companies_exporter()
    # user_exporter()
    contract_exporter()
