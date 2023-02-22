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
query = 'SELECT kode_kanca, pet_it FROM tb_kanca'
cursor.execute(query)

# Fetch data
rows = cursor.fetchall()

# Print data
if rows:
    data_array = []
    data_array_else = []
    for row in rows:  # looping data
        if row[1] is not None:  # proses data pic_pinca is not null
            result = re.findall(r'\d+', row[1])  # find number in string
            phone_number = ''.join(result)  # merge phone number spacing
            new_phone_number = phone_number.replace("0", str('62'), 1)

            name_pic_split = row[1]
            name_replace = name_pic_split.replace("|", str('/'))
            result_split = re.split('/', name_replace)
            result_grp = re.search(r'[a-zA-Z\s]+', result_split[0])

            count_num_phone = len(new_phone_number)  # count long number
            if result_grp != None:
                if count_num_phone <= 14:  # if one number phone
                    result_grp = re.search(r'[a-zA-Z\s]+', result_split[0])
                    name_pic = result_grp.group()
                    data_json = [
                        {
                            'name':name_pic,
                            'number':[new_phone_number]
                        }
                    ]
                    y_json = json.dumps(data_json)
                    data = [row[0],y_json]
                    
                    data_array.append(data)
                elif count_num_phone <= 28:
                    result_grp = re.search(r'[a-zA-Z\s]+', result_split[0])
                    name_pic = result_grp.group()

                    number_pic = re.findall(r'\d+', result_split[0])
                    number_pic_merge = ''.join(number_pic)
                    new_phone_number = number_pic_merge.replace("08", str('628'), 1)

                    # second
                    if len(new_phone_number) < 14 and result_split[1] != "":
                        result_grp_2 = re.search(r'[a-zA-Z\s]+', result_split[1])
                        if result_grp_2 is not None:

                            name_pic_2 = result_grp_2.group()

                            number_pic_2 = re.findall(r'\d+', result_split[1])
                            number_pic_merge_2 = ''.join(number_pic_2)
                            new_phone_number_2 = number_pic_merge_2.replace("08", str('628'), 1)

                            data_json = [
                                {
                                    'name':name_pic,
                                    'number':[new_phone_number]
                                },
                                {
                                    'name':name_pic_2,
                                    'number':[new_phone_number_2]
                                }
                            ]
                            y_json = json.dumps(data_json)
                            data = [row[0],y_json]

                            data_array.append(data)
                        else:
                            print("(2.2)>>> " + name_replace)
                            data_json_else = name_replace
                            y_json_else = json.dumps(data_json_else)
                            data_else = [row[0],y_json_else]
                            data_array_else.append(data_else)

                    else:
                        print("(2.1)>>> " + name_replace)
                        data_json_else =  name_replace
                        y_json_else = json.dumps(data_json_else)
                        data_else = [row[0],y_json_else]
                        data_array_else.append(data_else)

                elif count_num_phone <= 42:
                    result_grp = re.search(r'[a-zA-Z\s]+', result_split[0])
                    name_pic = result_grp.group()

                    number_pic = re.findall(r'\d+', result_split[0])
                    number_pic_merge = ''.join(number_pic)
                    new_phone_number = number_pic_merge.replace("08", str('628'), 1)

                    # second
                    if len(new_phone_number) < 14 and result_split[1] != "":
                        result_grp_2 = re.search(r'[a-zA-Z\s]+', result_split[1])
                        if result_grp_2 is not None:

                            name_pic_2 = result_grp_2.group()
                            number_pic_2 = re.findall(r'\d+', result_split[1])
                            number_pic_merge_2 = ''.join(number_pic_2)
                            new_phone_number_2 = number_pic_merge_2.replace("08", str('628'), 1)

                            if len(new_phone_number_2) < 14 and result_split[2] != "":
                                result_grp_3 = re.search(r'[a-zA-Z\s]+', result_split[2])
                                if result_grp_3 is not None:

                                    name_pic_3 = result_grp_3.group()
                                    number_pic_3 = re.findall(r'\d+', result_split[1])
                                    number_pic_merge_3 = ''.join(number_pic_3)
                                    new_phone_number_3 = number_pic_merge_2.replace("08", str('628'), 1)

                                    data_json = [
                                        {
                                            'name':name_pic,
                                            'number':[new_phone_number]
                                        },
                                        {
                                            'name':name_pic_2,
                                            'number':[new_phone_number_2]
                                        },
                                        {
                                            'name':name_pic_3,
                                            'number':[new_phone_number_3]
                                        }
                                    ]
                                    y_json = json.dumps(data_json)
                                    data = [row[0],y_json]

                                    data_array.append(data)
                            else:
                                print("(3.2)>>> " + name_replace)
                                data_json_else = name_replace
                                y_json_else = json.dumps(data_json_else)
                                data_else = [row[0],y_json_else]
                                data_array_else.append(data_else)

                        else:
                            print("(3.1)>>> " + name_replace)
                            data_json_else = name_replace
                            y_json_else = json.dumps(data_json_else)
                            data_else = [row[0],y_json_else]
                            data_array_else.append(data_else)

                    else:
                        print("(3.0)>>> " + name_replace)
                        data_json_else = name_replace
                        y_json_else = json.dumps(data_json_else)
                        data_else = [row[0],y_json_else]
                        data_array_else.append(data_else)

                elif count_num_phone <= 56:
                    result_grp = re.search(r'[a-zA-Z\s]+', result_split[0])
                    name_pic = result_grp.group()

                    number_pic = re.findall(r'\d+', result_split[0])
                    number_pic_merge = ''.join(number_pic)
                    new_phone_number = number_pic_merge.replace("08", str('628'), 1)

                    # second
                    if len(new_phone_number) < 14 and result_split[1] != "":
                        result_grp_2 = re.search(r'[a-zA-Z\s]+', result_split[1])
                        if result_grp_2 is not None:

                            name_pic_2 = result_grp_2.group()
                            number_pic_2 = re.findall(r'\d+', result_split[1])
                            number_pic_merge_2 = ''.join(number_pic_2)
                            new_phone_number_2 = number_pic_merge_2.replace("08", str('628'), 1)

                            if len(new_phone_number_2) < 14 and result_split[2] != "" and name_pic_2 != "":
                                result_grp_3 = re.search(r'[a-zA-Z\s]+', result_split[2])
                                if result_grp_3 is not None:

                                    name_pic_3 = result_grp_3.group()
                                    number_pic_3 = re.findall(r'\d+', result_split[1])
                                    number_pic_merge_3 = ''.join(number_pic_3)
                                    new_phone_number_3 = number_pic_merge_2.replace("08", str('628'), 1)

                                    if len(new_phone_number_3) < 14 and result_split[3]:
                                        result_grp_4 = re.search(r'[a-zA-Z\s]+', result_split[3])
                                        if result_grp_4 is not None:

                                            name_pic_4 = result_grp_4.group()
                                            number_pic_4 = re.findall(r'\d+', result_split[1])
                                            number_pic_merge_4 = ''.join(number_pic_4)
                                            new_phone_number_4 = number_pic_merge_2.replace("08", str('628'), 1)
                                        
                                            data_json = [
                                                {
                                                    'name':name_pic,
                                                    'number':[new_phone_number]
                                                },
                                                {
                                                    'name':name_pic_2,
                                                    'number':[new_phone_number_2]
                                                },
                                                {
                                                    'name':name_pic_3,
                                                    'number':[new_phone_number_3]
                                                },
                                                {
                                                    'name':name_pic_4,
                                                    'number':[new_phone_number_4]
                                                }
                                            ]
                                            y_json = json.dumps(data_json)
                                            data = [row[0],y_json]

                                            data_array.append(data)
                                            # print(data_json)
                                        else:
                                            print("(4.5)>>> " + name_replace)
                                            data_json_else = name_replace
                                            y_json_else = json.dumps(data_json_else)
                                            data_else = [row[0],y_json_else]
                                            data_array_else.append(data_else)
                                    else:
                                        print("(4.4)>>> " + name_replace)
                                        data_json_else = name_replace
                                        y_json_else = json.dumps(data_json_else)
                                        data_else = [row[0],y_json_else]
                                        data_array_else.append(data_else)
                                else:
                                    print("(4.3)>>> " + name_replace)
                                    data_json_else = name_replace
                                    y_json_else = json.dumps(data_json_else)
                                    data_else = [row[0],y_json_else]
                                    data_array_else.append(data_else)
                            else:
                                print("(4.2)>>> " + name_replace)
                                data_json_else = name_replace
                                y_json_else = json.dumps(data_json_else)
                                data_else = [row[0],y_json_else]
                                data_array_else.append(data_else)

                        else:
                            print("(4.1)>>> " + name_replace)
                            data_json_else = name_replace
                            y_json_else = json.dumps(data_json_else)
                            data_else = [row[0],y_json_else]
                            data_array_else.append(data_else)
                            
                    else:
                        print("(4.0)>>> " + name_replace)
                        data_json_else = name_replace
                        y_json_else = json.dumps(data_json_else)
                        data_else = [row[0],y_json_else]
                        data_array_else.append(data_else)

                else:
                    print("(Other) " + name_replace)
                    data_json_else = name_replace
                    y_json_else = json.dumps(data_json_else)
                    data_else = [row[0],y_json_else]
                    data_array_else.append(data_else)

    marge_json = data_array + data_array_else
    print(marge_json)

    with open("data.json", "w") as file:
        json.dump(marge_json, file)

        # Create a new Excel file
    workbook = xlsxwriter.Workbook('new_format_pet_it.xlsx')

    # Add a new worksheet to the file
    worksheet = workbook.add_worksheet()

    # Write the data to the worksheet
    for row_num, row_data in enumerate(marge_json):
        for col_num, value in enumerate(row_data):
            worksheet.write(row_num, col_num, value)

    # Save the workbook and close it
    workbook.close()

else:
    print('Not Found!')

# Close the cursor and the connection
cursor.close()
conx.close()
