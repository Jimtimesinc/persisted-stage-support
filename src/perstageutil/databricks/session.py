# sesson class is a wrapper over the connection object and handles opening and closing a connection similar to the way 
# pyspark SparkSesson object.  This little bit of extra code is to make the code more Databricks pyspark and Snowflake Snowpark like so it can
# be ported to those use cases.

from databricks import sql
import os
from logging import Logger



class Session():
    """
    Docstring for Session
    """
    conn : sql.Connection
    def __init__(self, logger : Logger, server_hostname : str, http_path : str, **kwargs):
        """
        A Databricks session picks up the local environmental parameters for a connection for this simple version.
        
        :param self: Description
        :param logger: Description
        """
        if kwargs.get('access_token', False):
            access_token = kwargs['access_token']
        else:
            raise Exception("Access token is the only authentication method supported at this time.")
        
        self.logger = logger
        try:
            self.conn = sql.connect(
                server_hostname = server_hostname,
                http_path       = http_path,
                access_token    = access_token)
        except Exception as err:
            self.logger.error(err)
            raise err
        
    def close(self):
        """
        Docstring for close
        
        :param self: Description
        """
        self.conn.close()
        self.logger.debug("Connection closed.")