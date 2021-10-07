import snowflake.connector
import os
import pandas as pd
from snowflake.connector.errors import DatabaseError
import logging
import sys

class SnowflakeQuery():

    def fetch_query(self, query) -> pd.DataFrame:
        
        try:
            ctx = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PW'],
            account=os.environ['SNOWFLAKE_ACCOUNT']
            )
            cs = ctx.cursor()
            cs.execute(query)
            results = cs.fetch_pandas_all()

        except DatabaseError as err:
            logging.error('A database error occured when querying Snowflake: {0}'.format(err))
            return None
        except:
            logging.error("Unexpected Snowflake error:", sys.exc_info()[0])
            return None

        finally:
            cs.close()
        ctx.close()
        return results
        