import xlsxwriter

# Create a new Excel file
workbook = xlsxwriter.Workbook('example.xlsx')

# Add a new worksheet to the file
worksheet = workbook.add_worksheet()

# Define the data that we want to write to the worksheet
data = [['Name', 'Age', 'Country'],
        ['John Doe', 30, 'USA'],
        ['Jane Doe', 28, 'Canada']]

print(data)

# Write the data to the worksheet
for row_num, row_data in enumerate(data):
    for col_num, value in enumerate(row_data):
        worksheet.write(row_num, col_num, value)

# Save the workbook and close it
workbook.close()
