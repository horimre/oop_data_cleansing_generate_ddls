import pandas as pd
from csvToImport import CsvToImport
from datetime import datetime


class WishlistCsv(CsvToImport):
    def __init__(self, file_name: str, arrival_date=datetime.today().strftime('%Y%m%d'), table_name='Wishlist', separator=','):
        super().__init__(
            file_name, arrival_date, table_name, separator
        )

    def _extract_csv(self):
        # print(self.arrival_date)
        df = pd.read_csv(f"{super().file_dir}/source_files/{self.file_name}", sep=self.separator, on_bad_lines='warn', dtype={'customer_no': object, 'product_id': object})
        # print(df.head)
        return df

    def _transform_csv(self, df: pd.DataFrame):
        df = df.sort_values(by='customer_no')
        # print(df.head)
        return df

    def _create_insert_script(self, df: pd.DataFrame):

        #print(self.table_name)

        # create lists from csv records
        records = [list(x) for x in df.values]

        # create insert script, no date needed
        with open(f"{super().file_dir}/insert_scripts/insert_{self.file_name.rstrip('.csv')}.sql", 'w') as file:
            for record in records:
                var = f"INSERT INTO {self.table_name} VALUES ("
                for token in record:
                    if type(token) != int and type(token) != float:
                        token = f"'{token}'"
                    # replace NaN with NULL in the script
                    if pd.isna(token):
                        token = 'NULL'
                    var = var + str(token).upper() + ', '
                var = var.rstrip(', ') + ");"
                file.write(var + '\n')
