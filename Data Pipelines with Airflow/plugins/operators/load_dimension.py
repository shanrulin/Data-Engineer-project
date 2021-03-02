from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    truncate_table = """
    TRUNCATE TABLE {} 
    """
 
    insert_sql = """
        INSERT INTO {}
        {}
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_insert="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_insert = sql_insert
        self.append_data = append_data

    def execute(self, context):
        self.log.info('LoadDimensionOperator start...')
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info("Load Dimension table")
        if self.append_data == True:
            self.log.info("Use Append to Insert Data")
            redshift.run(LoadDimensionOperator.insert_sql.format(self.table, self.sql_insert))
        
        else:
            self.log.info("Use Truncate to Insert Data")         
            redshift.run(LoadDimensionOperator.truncate_table.format(self.table))
            redshift.run(LoadDimensionOperator.insert_sql.format(self.table, self.sql_insert))
            
        
        
