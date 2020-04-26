import pyrebase
import re

config = {
    'apiKey': "AIzaSyBPFupD840P9rnGqojKfVlxyOnz5Ev8ODA",
    'authDomain': "inf551-project-msl-wqd.firebaseapp.com",
    'databaseURL': "https://inf551-project-msl-wqd.firebaseio.com",
    'projectId': "inf551-project-msl-wqd",
    'storageBucket': "inf551-project-msl-wqd.appspot.com",
    'messagingSenderId': "745003088267",
    'appId': "1:745003088267:web:81bdbebfeb22467a47578e",
    'measurementId': "G-FDHJE9DX3H"
}

firebase = pyrebase.initialize_app(config)

db = firebase.database()

# db.child("names").child("name").update({"name":"winston234"})
# db.child("names").child("name").remove()
# users = db.child("names").child("name").get()
# print(users.val())

from flask import *

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def basic():
    if request.method == 'POST':
        # first give index to selection box
        if request.form['selection'] == 'WCC':
            data_index = 'World'
            database_search_index = "World_index"
            dictionary = db.child("World_schema").get().val()
        elif request.form['selection'] == 'FA':
            data_index = 'FilmsActors'
            database_search_index = "FilmsActors_index"
            dictionary = db.child("FilmsActors_schema").get().val()
        elif request.form['selection'] == 'COD':
            data_index = 'CustomersOrder'
            database_search_index = "CustomersOrder_index"
            dictionary = db.child("CustomersOrder_schema").get().val()
        else:
            return render_template('index.html', result={'Please select a database!': ''},
                                   dictionary={'Please select a database!': ''})

        # Case insensitive and split the search box and remove "" and duplicate
        index_box = []
        index_box_pre = re.sub(r'[^A-Za-z0-9 ]+', '', request.form['name'].lower()).strip()
        index_box_pre = re.split(r'\s+', index_box_pre)
        index_box_pre = list(set(index_box_pre))
        for i in index_box_pre:
            if i != '':
                index_box.append(i)
        # by case to get the index from firebase
        result_to_find = {}
        for index in index_box:
            if request.form['selection'] == 'WCC':
                todo = db.child("World_index").child(index).get()
            elif request.form['selection'] == 'FA':
                todo = db.child("FilmsActors_index").child(index).get()
            elif request.form['selection'] == 'COD':
                todo = db.child("CustomersOrder_index").child(index).get()
            to = todo.val()
            if to:
                result_to_find[index] = to

        # order it by times appear, if all of the key word appears in a column, it should show first.
        # The tuples may come from different tables and should be ordered by the number of keywords appearing in the tuple.
        # Note that it is possible that different keywords appear in the values of different attributes in a tuple.
        # In this case, the tuple should be ranked after the tuple which contains all keywords in the same attribute (or a smaller number of attributes).
        times = {}
        index_dic = {}
        for j in result_to_find:
            for i in result_to_find[j]:
                if i['PRIMARYKEY'] not in times:
                    times[i['PRIMARYKEY']] = [1, dict(), dict()]
                    index_dic[i['PRIMARYKEY']] = i
                    times[i['PRIMARYKEY']][1][j] = 1
                    times[i['PRIMARYKEY']][2][i['COLUMN']] = set()
                    times[i['PRIMARYKEY']][2][i['COLUMN']].add(j)

                else:
                    times[i['PRIMARYKEY']][0] = times[i['PRIMARYKEY']][0] + 1
                    if j in times[i['PRIMARYKEY']][1]:
                        times[i['PRIMARYKEY']][1][j] = times[i['PRIMARYKEY']][1][j] + 1
                    else:
                        times[i['PRIMARYKEY']][1][j] = 1
                    if i['COLUMN'] not in times[i['PRIMARYKEY']][2]:
                        times[i['PRIMARYKEY']][2][i['COLUMN']] = set()
                        times[i['PRIMARYKEY']][2][i['COLUMN']].add(j)
                    else:
                        times[i['PRIMARYKEY']][2][i['COLUMN']].add(j)
        big_num = 100
        first_layer = []
        second_layer = []
        third_layer = []
        for i in times:
            if len(times[i][1]) == len(index_box):
                for j in times[i][2]:
                    if len(times[i][2][j]) == len(index_box):
                        first_layer.append([times[i][0], i])
                        break
                else:
                    second_layer.append([big_num - len(times[i][1]), times[i][0], i])
            else:
                third_layer.append([times[i][0], i])

        first_layer.sort(reverse=True)
        second_layer.sort(reverse=True)
        third_layer.sort(reverse=True)

        result_sort = []
        for i in first_layer:
            result_sort.append(index_dic[i[1]])
        for i in second_layer:
            result_sort.append(index_dic[i[2]])
        for i in third_layer:
            result_sort.append(index_dic[i[1]])

        # save the result as a dict with a list of dict. format: {Table1:[{},{},{}],Table2:[{},{},{}]}
        result_dic = {}
        for json_data in result_sort:
            if json_data['TABLE'] not in result_dic:
                result_dic[json_data['TABLE']] = []
            result_item = db.child(data_index).child(json_data['TABLE']).child(json_data['PRIMARYKEY']).get()
            result_item = result_item.val()
            result_dic[json_data['TABLE']].append(result_item)
        if not result_dic:
            return render_template('index.html', result={'No Result!': ''}, dictionary={'No Result!': ''})
        return render_template('index.html', result=result_dic, data_index=database_search_index, dictionary=dictionary)
    return render_template('index.html', result={}, dictionary={})


@app.route('/primary-key', methods=['GET', 'POST'])
def fk_index():
    # get the item from the table which is a fk
    index = str(request.form.get("id")).replace("_index", "").split('+')
    # split into 3 part, first part is the fk word, second is the table, third is the database
    data_index = index[2]
    result = db.child(data_index).child(index[1]).child(index[0].replace('_', '')).get().val()
    dictionary = db.child(data_index + "_schema").get().val()
    return render_template('fk_index.html', result={index[1]: [result]}, dictionary=dictionary, data_index=data_index, index= db.child(data_index).child(index[1]).get().val())

@app.route('/foreign-key', methods=['GET', 'POST'])
def pk_index():
    # get the item from the table which is a fk
    index = str(request.form.get("id")).replace("_index", "").split('+')
    # split into 3 part, first part is the pk word, second is the table, third is the database
    data_index = index[2]   
    dic_schema = db.child(data_index + "_schema").child(index[1]).child('referenced_key').get().val()
    dictionary = db.child(data_index + "_schema").get().val()
    todo = db.child(data_index).get().val()
    result_dic = {}
    if dic_schema:
        for key in dic_schema:
            for item in dic_schema[key]:
                temp = []
                what = item
                what2 = todo[item['referenced_table']]
                if type(todo[item['referenced_table']]) == dict:
                    for dic in todo[item['referenced_table']]:
                        new_dic = todo[item['referenced_table']][dic]
                        for key in new_dic:
                            if new_dic and item['referenced_column'] in new_dic and str(new_dic[item['referenced_column']]).lower() == index[0].lower() and new_dic not in temp:
                                temp.append(new_dic)
                else:
                    for dic in todo[item['referenced_table']]:
                        if dic and item['referenced_column'] in dic and str(dic[item['referenced_column']]).lower() == index[0].lower() and dic not in temp:
                            temp.append(dic)
                if temp:
                    result_dic[item['referenced_table']] = temp
        
    return render_template('index.html', result=result_dic, dictionary=dictionary, data_index=data_index, index = index)

if __name__ == '__main__':
    app.run(debug=True)