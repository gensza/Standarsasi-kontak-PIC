import re
import mysql.connector
import json
from mysql.connector import errorcode

try:
    conx = mysql.connector.connect(
        user='root', password='', host='localhost', database='wanvolution_dev_backup')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Mungkin salah username dan password')
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print('database tidak ada')
    else:
        print(err)
else:
    print('Koneksi ke database berhasil')

# Create a cursor
cursor = conx.cursor()

# Execute a query
query = 'SELECT id_remote, pet_it FROM v_all_remote'
cursor.execute(query)

# Fetch data
rows = cursor.fetchall()

# Print data
if rows:
    data_array = []
    for row in rows:  # looping data
        if row[1] is not None:  # proses data pic_pinca is not null
            result = re.findall(r'\d+', row[1])  # find number in string
            phone_number = ''.join(result)  # merge phone number spacing
            new_phone_number = phone_number.replace("0", str('62'), 1)

            # result_grp = re.search(r'[a-zA-Z\s]+', row[1])  # get name only
            name_pic_split = row[1]
            name_replace = name_pic_split.replace("|", str('/'), 1)
            result_split = re.split('/', name_replace)
            result_grp = re.search(r'[a-zA-Z\s]+', result_split[0])

            count_num_phone = len(new_phone_number)  # count long number
            if result_grp != None:
                if count_num_phone <= 14:  # if one number phone
                    result_grp = re.search(r'[a-zA-Z\s]+', result_split[0])
                    name_pic = result_grp.group()
                    data = [
                        {
                            "nama_pic": name_pic,
                            "nomer_pic": [new_phone_number]
                        }
                    ]
                    data_array.append(data)
                elif count_num_phone <= 28:  # if two number phone
                    result_grp = re.search(r'[a-zA-Z\s]+', result_split[0])
                    result_grp_1 = re.search(r'[a-zA-Z\s]+', result_split[1])
                    name_pic = result_grp.group()
                    name_pic_1 = result_grp_1.group()
                    new_phone_number2 = phone_number.replace(
                        "08", str('628'), 2)
                    items = new_phone_number2.split("628")
                    data = [
                        {
                            "nama_pic": name_pic,
                            "nomer_pic": ['628'+items[1]]
                        },
                        {
                            "nama_pic": name_pic_1,
                            "nomer_pic": ['628'+items[2]]
                        },
                    ]

                    data_array.append(data)
                else:  # if three number phone
                    result_grp = re.search(r'[a-zA-Z\s]+', result_split[0])
                    name_pic = result_grp.group()
                    new_phone_number3 = phone_number.replace(
                        "08", str('628'), 3)
                    items = new_phone_number3.split("628")
                    data = [
                        {
                            "nama_pic": name_pic,
                            "nomer_pic": ['628'+items[1]]
                        },
                        {
                            "nama_pic": name_pic,
                            "nomer_pic": ['628'+items[2]]
                        },
                        {
                            "nama_pic": name_pic,
                            "nomer_pic": ['628'+items[3]]
                        }
                    ]
                    data_array.append(data)

    marge_json = data_array
    print(marge_json)

    with open("data.json", "w") as file:
        json.dump(marge_json, file)
else:
    print('Not Found!')

# Close the cursor and the connection
cursor.close()
conx.close()
