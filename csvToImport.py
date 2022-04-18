import pandas as pd
import os


class CsvToImport:

    file_dir = os.path.dirname(os.path.realpath('__file__'))
    print(file_dir)

    def __init__(self, file_name, arrival_date, table_name, separator=','):
        self._file_name = file_name
        self._arrival_date = arrival_date
        self._table_name = table_name
        self._separator = separator

    @property
    def file_name(self):
        return self._file_name

    @property
    def arrival_date(self):
        return self._arrival_date

    @property
    def table_name(self):
        return self._table_name

    @property
    def separator(self):
        return self._separator

    # @staticmethod
    '''
    def _is_int(self, x):
        try:
            int(x)
        except ValueError:
            print(self._file_name, 'int check failed:', x)
            return False
        return True
   '''
    '''
    @staticmethod
    def _clean_text(row):
        return [r.encode('ascii', 'ignore').decode() for r in row]
    '''
    # @staticmethod
    def _is_bool(self, x):
        try:
            bool(x)
        except ValueError:
            print(self._file_name, ': bool check failed. value:', x)
            return False
        return True

    # @staticmethod
    def _is_float(self, x):
        try:
            float(x)
        except ValueError:
            print(self._file_name, ': float check failed. value:',  x)
            return False
        return True

    def _create_insert_script(self, df: pd.DataFrame):

        # create lists from csv records
        records = [list(x) for x in df.values]

        # create insert script
        with open(f"{__class__.file_dir}/insert_scripts/insert_{self._file_name.rstrip('.csv')}.sql", 'w') as file:
            for record in records:
                var = f"INSERT INTO {self._table_name} VALUES ("
                for token in record:
                    if type(token) != int and type(token) != float:
                        token = f"'{token}'"
                    # replace NaN with NULL in the script
                    if pd.isna(token):
                        token = 'NULL'
                    var = var + str(token).upper() + ', '
                var = var + f"'{self._arrival_date}');"
                file.write(var + '\n')

    def _extract_csv(self):
        df = pd.read_csv(f"{__class__.file_dir}/source_files/{self.file_name}", sep=self._separator, on_bad_lines='warn')
        print(df.head)
        return df

    def _transform_csv(self, df: pd.DataFrame):
        return df

    def _load_csv(self, df: pd.DataFrame):
        self._create_insert_script(df)
        # load to sqllite db

    def etl(self):
        # print(self._file_name)
        df = self._extract_csv()
        df_cleansed = self._transform_csv(df)
        self._load_csv(df_cleansed)
