from databricks import sql
import os
import keyring

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
print(server_hostname)
http_path = os.getenv("DATABRICKS_HTTP_PATH")
print(http_path)
access_token = os.getenv("DATABRICKS_TOKEN")
print(access_token)

with sql.connect(
                server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token    = os.getenv("DATABRICKS_TOKEN")) as connection:
    cursor = connection.cursor()
    cursor.execute('SELECT :param `p`, * FROM RANGE(10)', {"param": "foo"})
    result = cursor.fetchall()
    for row in result:
        print(row)
    cursor.close()
#connection.close()

print("test completed.")