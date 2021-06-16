import boto3
import csv
from faker import Faker
import hashlib

s3 = boto3.resource('s3')
faker = Faker()

s3_bucket = '<bucket_name>'
upload_path = 'anonymization/'
new_csv = 'tmp/personal_info.csv'
hashed_csv = '/tmp/hashed_personal_info.csv'


#Part 1.  Generate CSV File
with open(new_csv, 'w', newline = '') as csvFile:
    w = csv.writer(csvFile, delimeter = ',')
    for i in range(20000):
        name = fake.name()
        address_full = fake.address()
        street_address = ' '.join(address_full.split()[0:-2])
        state = address_full.split()[-2]
        post_code = address_full.split()[-1]
        birthdate = str(fake.profile('birthdate')['birthdate'])
        data = [name, street_address, state, post_code, birthdate]
        w.writerow(data)
    csvFile.close()

s3.Bucket(s3_bucket).upload_file(new_csv, upload_path+new_csv.split('/')[2])


#Part 2. SHA257 encode data and upload to s3
with open(new_csv, 'r') as csv_file:
    reader = csv.reader(csv_file, delimiter = ',')
    with open(hashed_csv, 'w', newline= '')as hashedCSV:
        w = csv.writer(hashedCSV, delimiter = ',')
        for row in reader:
            data = [
                    hashlib.sha256(row[0].encode('utf-8')).hexdigest(),
                    hashlib.sha256(row[1].encode('utf-8')).hexdigest(),
                    row[2],
                    row[3],
                    haslib.sha256(row[4].encode('utf-8')).hexdigest()
            ]
            w.writerow(data)
    hashedCSV.close()
    csv_file.close()

s3.Bucket(s3_bucket).upload_file(hashed_csv, upload_path+hashed_csv.split('/')[2])





