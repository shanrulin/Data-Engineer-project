from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_primary_dict={},
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_primary_dict = table_primary_dict
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        self.log.info('DataQualityOperator start')
        redshift = PostgresHook(self.redshift_conn_id)
        for table, primary_key in self.table_primary_dict.items():
            self.log.info(f"Check data quality on table {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
        
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contains 0 row")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            
            self.log.info(f"Check Null value of primary key column {primary_key} on table {table}")
            null_records = redshift.get_records(f"SELECT COUNT(*) FROM {table} WHERE {primary_key} is null")
            null_count = null_records[0][0]
            if null_count != 0:
                raise ValueError(f"Column {primary_key} contains null value")
            self.log.info(f"Null value check of column {primary_key} on table {table} passed")
            
            # check duplicate records
            self.log.info(f"Check duplicate value in column {primary_key} on table {table}")
            duplicate_sql = """
                SELECT count_total
                FROM (
                SELECT {}, SUM(1) AS count_total
                FROM {}
                GROUP BY 1
                )
                WHERE count_total > 1
                LIMIT 1
                """
            
            duplicate_records = redshift.get_records(duplicate_sql.format(primary_key, table))
            if len(duplicate_records) > 1:
                raise ValueError(f"column {primary_key} has duplicate value")
            self.log.info("Duplicate check on column {primary_key} in table {table} passed")
           

           
            
            
            
            
        
        