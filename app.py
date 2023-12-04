import json
import mysql.connector
from decimal import Decimal
import requests
import json
from datetime import datetime,timedelta
import boto3

def retrieve_secret(secret_name):
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager')

    try:
        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)
        
        # Extract the secret string from the response
        secret = response['SecretString']
        return secret
        
    except Exception as e:
        print(f"Error retrieving secret: {str(e)}")
        return None


def lambda_handler(event,context):
	secret_name = 'webapp'
	retrieved_secret = eval(retrieve_secret(secret_name))
	mysql_password = retrieved_secret['MysqlPassword']
	auth_key = retrieved_secret['AuthKey']
	
	host = 'localhost'
	user = 'root'
	password = mysql_password
	database = 'jobs'

	# Establish MySQL connection
	conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
	cursor = conn.cursor()

	current_date = datetime.utcnow() - timedelta(days=1)
	keywords = ['accountant','software','data','aws','cloud','database','project','manager','sap','teacher','ui','hr','Engineer']
	
	count = 0
	# keywords  = ['accountant']
	for keyword in keywords:
		print(f'Searching job in domain {keyword}')
		url = f"https://www.reed.co.uk/api/1.0/search?keywords={keyword}"

		payload={}
		headers = {
		  'Authorization': f'Basic {auth_key}',
		  'Cookie': '__cf_bm=Pc7PAUyH4bXBQhMFeCax.NmN_7wgBg1R_ey.qL7Q.oc-1700443776-0-AW0LMuWJHu/AIFDoYti/klI9N6RZZ71LGOQf2PL/KwE6kHrIbb313la0JAzm8MCq6T9oPVInscrugC60bD0Pjsk=; __cfruid=0976808dbf9883c5dec1fd9fd5d622dbd697dd35-1700441855; .ASPXANONYMOUS=k0pdtL-IyKARELQXEbVjjUNpTYCsoKC0efJIotY4fuED_0ujCErZwoxPZh6QUH2giC5SO53wcPaimYQXVf2X94iM86F8tKMmwLRsgmBvaV0rV4fpBf-r0dMon1X0k045wgg6Og2'
		}

		response = requests.request("GET", url, headers=headers, data=payload)

		json_data = json.loads(response.text)
		for result in json_data['results']:
			provided_date = datetime.strptime(result['date'], "%d/%m/%Y")
			if str(provided_date.date()) == str(current_date.date()):
				count = count +1 
			# Insert data into the 'jobs' table
				insert_query = '''
				INSERT INTO jobs (
					jobId, employerId, employerName, employerProfileId, employerProfileName,
					jobTitle, locationName, minimumSalary, maximumSalary, currency,
					expirationDate, date, jobDescription, applications, jobUrl
				) VALUES (
					%(jobId)s, %(employerId)s, %(employerName)s, %(employerProfileId)s, %(employerProfileName)s,
					%(jobTitle)s, %(locationName)s, %(minimumSalary)s, %(maximumSalary)s, %(currency)s,
					%(expirationDate)s, %(date)s, %(jobDescription)s, %(applications)s, %(jobUrl)s
				)
				'''
				cursor.execute(insert_query, result)
	
	conn.commit()
	cursor.close()
	conn.close()
	
	print(f'Added {count} records ')


	
lambda_handler('','')