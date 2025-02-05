from flask import Flask, render_template, request,send_file
import warnings

import numpy
import json 
import clickhouse_connect
import matplotlib.pyplot as plt

warnings.simplefilter(action='ignore')
plt.style.use('dark_background') 

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
        #print(data.dtypes)
        #print(f'CREATE TABLE {database_name} ({columns}) Engine MergeTree ORDER BY ID')
        self.client.command(f'CREATE TABLE {database_name} ({columns}) Engine MergeTree ORDER BY ID')
        self.client.insert(database_name, data.to_numpy(), column_names=list(data.columns))

    def table_reader(self, column_name, database_name = 'Sales'):
        train = {} 
        train = self.client.command(f'SELECT top 10000 {column_name} FROM {database_name}').split('\n')
        if train[0].isdigit() or '.' in train[0]:
            train = [float(i) if i!='nan' else 0 for i in train ]
        if column_name=='sales':
            print(train)

        return train 

class data_ploter:
    def __new__(self, labels, type_):
        plt.figure(figsize=(12, 7))
        
        self.data = {}
        for i in labels:
            self.data[i] = house.table_reader(i) 
        if type_=='single_label_plot':
            label = data_ploter.label_extracter(self, labels[0])
            return json.dumps([list(label.keys()), list(label.values())])
            
        elif type_=='double_label_plot':            
            label = data_ploter.two_label_extracter(self, labels[0],labels[1])

            if type(list(label.keys())[0])==str:
                return json.dumps([list(label.keys()), list(label.values())])
            else:
                return json.dumps([list(label.keys()), list(label.values())])
            
        elif type_=='normal_plot': 
            if len(labels)==1:
                plt.title(f'{labels[0]}')
                plt.plot(self.data[labels[0]].to_numpy())
                
            elif len(labels)==2:     
                label = data_ploter.two_label_extracter(self, labels[0],labels[1])  
                return json.dumps([list(label.keys()), list(label.values())])
            
    def label_extracter(self, label):
        label_count = {}
        for i in self.data[label]:
            if i not in label_count.keys():
                label_count[i]=1
            else:
                label_count[i]+=1
        return label_count
        
    def two_label_extracter(self, label_1, label_2):
        label_count = {}
        for i in zip(self.data[label_1],self.data[label_2]):
            if i[0] not in label_count.keys():
                label_count[i[0]]=i[1]
            else:
                label_count[i[0]]+=i[1]
        myKeys = list(label_count.keys())
        myKeys.sort()

        return {i: label_count[i] for i in myKeys}

def data_cleaner(data):
        for i in zip(data.columns,data.dtypes):
            if i[1]=='O':
                data[i[0]] = data[i[0]].fillna('Unknown')
            else:
                data[i[0]] = data[i[0]].fillna(0)
        return data       

house = clickhouse()
main=Flask(__name__)

main.jinja_env.filters['zip'] = zip

@main.route("/",methods=['POST','GET'])
def main_div():
    plot = {'total_orders per date' : data_ploter(['date', 'total_orders'], 'normal_plot')}

    plot_1 = {'sales per date' : data_ploter(['date', 'sales'], 'normal_plot'),
              'type_1_discount per date' : data_ploter(['date', 'type_1_discount'], 'normal_plot'),
              'type_2_discount per date' : data_ploter(['date', 'type_2_discount'], 'normal_plot'),
              'type_3_discount per date' : data_ploter(['date', 'type_3_discount'], 'normal_plot'),
              'type_4_discount per date' : data_ploter(['date', 'type_4_discount'], 'normal_plot'),
              'type_5_discount per date' : data_ploter(['date', 'type_5_discount'], 'normal_plot'),
              'type_6_discount per date' : data_ploter(['date', 'type_6_discount'], 'normal_plot'),
              'availability as per date' : data_ploter(['date', 'availability'], 'double_label_plot')}

    plot_2 = {'warehouse usage' : data_ploter(['warehouse'], 'single_label_plot'),
              'type_1_discount as per warehouse' : data_ploter(['warehouse', 'type_1_discount'], 'double_label_plot'),
              'type_2_discount as per warehouse' : data_ploter(['warehouse', 'type_2_discount'], 'double_label_plot'),
              'type_3_discount as per warehouse' : data_ploter(['warehouse', 'type_3_discount'], 'double_label_plot'),
              'type_4_discount as per warehouse' : data_ploter(['warehouse', 'type_4_discount'], 'double_label_plot'),
              'type_5_discount as per warehouse' : data_ploter(['warehouse', 'type_5_discount'], 'double_label_plot'),
              'type_6_discount as per warehouse' : data_ploter(['warehouse', 'type_6_discount'], 'double_label_plot'),
              'availability as per warehouse' : data_ploter(['warehouse', 'availability'], 'double_label_plot')}  

    return render_template("index.html", plot = plot, itrater = zip(plot.keys(),['chart'+str(i) for i in range(len(plot.keys()))]),
                                         plot_1 = plot_1, itrater_1 = zip(plot_1.keys(),['chart_'+str(i) for i in range(len(plot_1.keys()))]),
                                         plot_2 = plot_2, itrater_2 = zip(plot_2.keys(),['char_'+str(i) for i in range(len(plot_2.keys()))]))

if __name__ == '__main__':
    main.run(debug=True) #host="2409:408d:eb9:9fa0:c8e7:473b:d1c5:81ea"
