from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import logging

class Transform():
    def __init__(self, engine_mysql, engine_postgre):
        self.engine_mysql   = engine_mysql
        self.engine_postgre = engine_postgre

    def get_data_mysql(self, dimension=None):
        if dimension == 'province':
            sql = "SELECT DISTINCT kode_prov AS province_id, nama_prov AS province_name FROM mysql.covid_data"
        elif dimension == 'district':
            sql = "SELECT DISTINCT kode_kab AS district_id, kode_prov AS province_id, nama_kab AS district_name FROM mysql.covid_data"
        else:
            sql = "SELECT * FROM mysql.covid_data"

        df = pd.read_sql(sql, con=self.engine_mysql)
        logging.info('Succes get data from MySQL')
        return df
    
    def get_data_postgre(self, table='district_daily'):
        sql = f"SELECT * FROM {table}"
        df = pd.read_sql(sql, con=self.engine_postgre)
        logging.info('Succes get data from PostgreSQL')
        return df
    
    def create_dim_table(self, dimension):
        table_name_mapping = {
            'province': 'dim_province',
            'district': 'dim_district',
            'case'    : 'dim_case'
        }

        if dimension not in table_name_mapping:
            logging.warning(f"Unsupported dimension: {dimension}. No data inserted.")
            return

        table_name = table_name_mapping[dimension]

        if dimension == 'province':
            # Transform data for province dimension
            df_dimension = self.get_data_mysql('province')
            
        elif dimension == 'district':
            # Transform data for district dimension
            df_dimension = self.get_data_mysql('district')
            
        elif dimension == 'case':
            df = self.get_data_mysql()
            # Transform data for case dimension
            init_column = ['suspect_diisolasi', 'suspect_discarded','suspect_meninggal', 'closecontact_dikarantina', 
                            'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 
                            'confirmation_sembuh', 'confirmation_meninggal', 'closecontact_meninggal', 'probable_meninggal']
            final_column = ['case_id', 'status_name', 'status_detail', 'status']

            df = df[init_column]
            df = df.melt(var_name = "status", value_name = "total")
            df = df.drop_duplicates("status").sort_values("status").reset_index()
            df[['status_name', 'status_detail']] = df["status"].str.split('_', n=1, expand=True)
            df['case_id'] = df.index + 1
            df_dimension = df[final_column]

        # Insert data into PostgreSQL
        try:
            # Execute to Insert Data
            df_dimension.to_sql(con=self.engine_postgre, name=table_name, if_exists='replace', index=False)
            logging.info(f"Successfully inserted data into {table_name}")
        
        except SQLAlchemyError as e:
            logging.error(f"Error inserting data into {table_name}: {e}")


    def insert_daily(self, dimension):
        df          = self.get_data_mysql()
        df_case_dim = self.get_data_postgre('dim_case')

        raw_id_mapping     = {'province': 'kode_prov', 'district': 'kode_kab'}
        final_id_mapping   = {'province': 'province_id', 'district': 'district_id' }
        table_name_mapping = {'province': 'province_daily', 'district': 'district_daily' }

        col_raw     = raw_id_mapping[dimension]
        col_final   = final_id_mapping[dimension]
        table_name  = table_name_mapping[dimension]

        init_column = [ col_raw, 'tanggal', 'suspect_diisolasi', 'suspect_discarded','suspect_meninggal', 'closecontact_dikarantina', 
                       'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh',
                       'confirmation_meninggal', 'closecontact_meninggal', 'probable_meninggal']

        final_column = ['date', col_final, 'status', 'total']

        df_daily = df[init_column]
        df_daily = df_daily.melt(id_vars = ['tanggal', col_raw], var_name='status', value_name='total').sort_values(['tanggal', col_raw, 'status', 'total'])
        df_daily = df_daily.groupby(by=['tanggal', col_raw, 'status']).sum()
        df_daily = df_daily.reset_index()

        df_daily.columns = final_column
        df_daily['id'] = df_daily.index + 1

        df_daily = pd.merge(df_daily, df_case_dim, how='inner', on='status')
        df_daily = df_daily[['id', col_final, 'case_id', 'date', 'total']]
        df_daily = df_daily.sort_values('id')
         # insert to postgres
        df_daily.to_sql(con=self.engine_postgre, name=table_name, if_exists='replace', index=False)
