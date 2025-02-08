import clickhouse_connect
import numpy
import pandas

class clickhouse:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
                            host='iebo4f3e4z.asia-southeast1.gcp.clickhouse.cloud',
                            user='default',
                            password='I8_AuX5WqWwGC',
                            secure=True)
    def data_cleaner(self, data):
        for i in zip(data.columns,data.dtypes):
            if i[1]=='O':
                data[i[0]] = data[i[0]].fillna('Unknown')
            else:
                data[i[0]] = data[i[0]].fillna(0)
        return data

    def table_creater(self, data, database_name):
        columns = ""
        for i in zip(data.columns,data.dtypes):
            if i[1]=='O':
                columns += f'{i[0]} String, '
            else:
                columns += f'{i[0]} Float64, '
       
        self.client.command(f'CREATE TABLE {database_name} ({columns}) Engine MergeTree ORDER BY ID')
        self.client.insert(database_name, data.to_numpy(), column_names=list(data.columns))
        clickhouse.null_values_remover(self, database_name)

    def data_reconstructer(self, tabels):
        columns_, joint_, data_ = '', '', []
        for i in tabels:
            columns_ += f'{i}.{i}, '
        for i in tabels[1:]:
            joint_ += f'FULL OUTER JOIN {i} ON {tabels[0]}.ID = {i}.ID '
        data = self.client.command(f'SELECT {tabels[0]}.ID, {columns_} FROM {tabels[0]} {joint_};')
        [data_.append(i) if '\n' not in i else [data_.append(i) for i in i.split('\n')] for i in data]

        return numpy.array(data_).reshape(int(len(data_)/(len(tabels)+1)), len(tabels)+1)

    def data_reconstructer_right(self, tabels):
        columns_, joint_, data_ = '', '', []
        for i in tabels:
            columns_ += f'{i}.{i}, '
        for i in tabels[1:]:
            joint_ += f'RIGHT JOIN {i} ON {tabels[0]}.ID = {i}.ID '
        data = self.client.command(f'SELECT {tabels[0]}.ID, {columns_} FROM {tabels[0]} {joint_};')
        [data_.append(i) if '\n' not in i else [data_.append(i) for i in i.split('\n')] for i in data]

        return numpy.array(data_).reshape(int(len(data_)/(len(tabels)+1)), len(tabels)+1)

    def data_reconstructer_left(self, tabels):
        columns_, joint_, data_ = '', '', []
        for i in tabels:
            columns_ += f'{i}.{i}, '
        for i in tabels[1:]:
            joint_ += f'LEFT JOIN {i} ON {tabels[0]}.ID = {i}.ID '
        data = self.client.command(f'SELECT {tabels[0]}.ID, {columns_} FROM {tabels[0]} {joint_};')
        [data_.append(i) if '\n' not in i else [data_.append(i) for i in i.split('\n')] for i in data]

        return numpy.array(data_).reshape(int(len(data_)/(len(tabels)+1)), len(tabels)+1)

    def data_reconstructer_inner(self, tabels):
        columns_, joint_, data_ = '', '', []
        for i in tabels:
            columns_ += f'{i}.{i}, '
        for i in tabels[1:]:
            joint_ += f'INNER JOIN {i} ON {tabels[0]}.ID = {i}.ID '
        data = self.client.command(f'SELECT {tabels[0]}.ID, {columns_} FROM {tabels[0]} {joint_};')
        [data_.append(i) if '\n' not in i else [data_.append(i) for i in i.split('\n')] for i in data]

        return numpy.array(data_).reshape(int(len(data_)/(len(tabels)+1)), len(tabels)+1)

    def null_values_remover(self, database_name):
        querie = f"DELETE FROM your_table where"
        added = [f'{i} IS NULL' for i in clickhouse.column_extracter(self, database_name)]
        for  i in range(len(added)):
            if len(added)-1 < i: 
                querie += added[i] + 'AND'
            else:
                querie += added[i]

    def restart(self):
        tabels = self.client.command(f'SHOW TABLES;')
        try:
            tabels = tabels.split('\n')
            for i in tabels:
                self.client.command(f'DROP TABLE {i};')
        except:
            pass
    def column_reader(self, column_name, database_name = 'Sales'):
        train = {} 
        train = self.client.command(f'SELECT top 100 {column_name} FROM {database_name}').split('\n')
        if train[0].isdigit() or '.' in train[0]:
            train = [float(i) if i!='nan' else 0 for i in train ]

        return train
        
    def view_tabels(self):
        return self.client.command(f'SHOW TABLES;').split('\n')

    def column_extracter(self, database_name):
        columns = [i for i in self.client.command(f'DESC {database_name}') if i!='' and i!='String' and i!='Float64']
        columns = [columns[0]]+[i.split('\n')[1] for i in columns[1:]]
        return columns

data = pandas.read_csv('sales_train.csv')[:100]
house = clickhouse()
house.restart()

data['ID'] = data['date']

data = data.drop(columns=['date', 'unique_id'])
for i in data.columns:
    if i!='ID':
        house.table_creater(data[['ID',i]], i)

print(house.data_reconstructer_inner(house.view_tabels()))
