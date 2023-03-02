import re
import mysql.connector
import json
import xlsxwriter
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
query = 'SELECT kode_kanca, nama_kanca, pic_pinca FROM tb_kanca'
cursor.execute(query)

# Fetch data
rows = cursor.fetchall()

# Print data
if rows:
    data_array = []
    for row in rows:  # looping data
        if row[2] is not None:  # proses data pic_pinca is not null
            result = re.findall(r'\d+', row[2])  # find number in string
            phone_number = ''.join(result)  # merge phone number spacing
            new_phone_number = phone_number.replace("0", str('62'), 1)
            result_grp = re.search(r'[a-zA-Z\s]+', row[2])  # get name only
            count_num_phone = len(new_phone_number)  # count long number
            if result_grp != None:
                name_pic = result_grp.group()
                if count_num_phone <= 14:  # if one number phone
                    data = [
                        {
                            'nama_pic': name_pic,
                            'nomer_pic': [new_phone_number]
                        }
                    ]
                    data_array.append(data)
                elif count_num_phone <= 28:  # if two number phone
                    new_phone_number2 = phone_number.replace(
                        "08", str('628'), 2)
                    items = new_phone_number2.split("628")
                    data = [
                        {
                        "nama_pic": name_pic,
                        "nomer_pic": ['628'+items[1], '628'+items[2]]
                        }
                    ]
                    data_array.append(data)
                elif count_num_phone <= 42:  # if three number phone
                    new_phone_number3 = phone_number.replace(
                        "08", str('628'), 3)
                    items = new_phone_number3.split("628")
                    data = [
                        {
                        "nama_pic": name_pic,
                        "nomer_pic": ['628'+items[1], '628'+items[2], '628'+items[3]]
                        }
                    ]
                    data_array.append(data)

            # json_array_update = json.dumps(data)
            # # Define the SQL query to update data
            # sql_update = "UPDATE tb_kanca SET pic_pinca = %s WHERE kode_kanca = %s"

            # # Define the values to be updated
            # values_update = (json_array_update,  row[0])

            # # Execute the SQL query with values
            # cursor.execute(sql_update, values_update)

            # # Commit the changes to the database
            # conx.commit()

            # # Print the number of rows affected by the update
            # print(cursor.rowcount, "record(s) updated")

    # marge_json = data_array
    # print(marge_json)

    # with open("data.json", "w") as file:
    #     json.dump(marge_json, file)

    # # Create a new Excel file
    # workbook = xlsxwriter.Workbook('new_format_pic_pinca.xlsx')

    # # Add a new worksheet to the file
    # worksheet = workbook.add_worksheet()

    # # Write the data to the worksheet
    # for row_num, row_data in enumerate(marge_json):
    #     for col_num, value in enumerate(row_data):
    #         worksheet.write(row_num, col_num, value)

    # # Save the workbook and close it
    # workbook.close()
else:
    print('Not Found!')

# Close the cursor and the connection
cursor.close()
conx.close()
