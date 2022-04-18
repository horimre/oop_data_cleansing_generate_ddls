import pandas as pd
from csvToImport import CsvToImport


class ProductsCsv(CsvToImport):
    def __init__(self, file_name: str, arrival_date, table_name='Products',   separator=';'):
        super().__init__(
            file_name, arrival_date, table_name, separator
        )

    def _clean_file(self, df: pd.DataFrame):

        orig_rownum = len(df)

        # df = df[df.item.apply(self._clean_text)]
        # df = df[df.title.apply(self._clean_text)]

        # remove duplicates. whislist only tracks item (product_id)
        df = df.drop_duplicates(subset=['item'], keep='last')

        print(orig_rownum, len(df))

        if orig_rownum != len(df):
            # log
            pass

        '''
        # item should be numeric but there are records where it is not
        if df.item.dtype != int:
            # remove row where item is not numeric
            df = df[df.item.apply(self._is_int)]

            # change item datatype to int
            df = df.astype({"item": int})

            # print(df.item.dtype)
            # print(df.shape)
        '''

        # print(df.dtypes)

        if df.available.dtype != bool:
            # remove row where available is not bool
            df = df[df.available.apply(self._is_bool)]

            # change item datatype to bool
            df = df.astype({"available": bool})

            # print(df.available.dtype)
            # print(df.shape)

        if df.price.dtype != float:
            # remove row where price is not float
            df = df[df.price.apply(self._is_float)]

            # change price datatype to float
            df = df.astype({"price": float})

            # print(df.price.dtype)
            # print(df.shape)

        # print(df.dtypes)
        print(len(df))
        return df

    def _extract_csv(self):
        #print('aa', self.file_name)
        df = pd.read_csv(f"{super().file_dir}/source_files/{self.file_name}", sep=self.separator, on_bad_lines='warn', dtype={'item': object})
        #print(df.head)
        return df

    def _transform_csv(self, df: pd.DataFrame):
        df = df.sort_values(by='item')
        # print(df.head)

        df_cleansed = self._clean_file(df)
        # print(df_cleansed.head)
        return df_cleansed

