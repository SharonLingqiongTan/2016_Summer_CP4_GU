# -*- coding: utf-8 -*-
import certifi,os,yaml,sys,re
from elasticsearch import Elasticsearch

def query_retrival(query_path): #parse the json query file into a list of queries
    f = open(query_path)
    lines = f.readlines()
    f.close()
    queries = yaml.load("".join(line.strip("\n") for line in lines))
    query_list = []
    for query_type in queries:
        for query_id in queries[query_type]:
            query = queries[query_type][query_id]["parsed"].copy()
            query["type"] = query_type.lower()
            query["id"] = query_id
            query_list.append(query)
    return query_list

def query_parse(query):
    skin_color_list = ["white","yellow","black"]
    search_ignore_list = ["obfuscation","number_of_individuals","gender"]
    where = query["where"]
    clauses = where["clauses"]
    filters = {}
    if "filters" in where:
        filters = where["filters"]
    parsed_dic = {}
    must_search_field = {}
    should_search_field = {}
    must_not_field = {}
    required_match_field = {}
    optional_match_field = {}
    answer_field = {}
    for clause in clauses:
        predicate = re.sub("[^\w]","",clause["predicate"])
        if "constraint" in clause:  #constraint indicates given condition
            if predicate in search_ignore_list:
                pass
            elif predicate == "ethnicity":
                if clause["constraint"] in skin_color_list:
                    should_search_field["ethnicity"] = clause["constraint"]
                else:
                    must_search_field["ethnicity"] = clause["constraint"]
            elif predicate == "drug_use":
                should_search_field["drug_use"] = "drug"
            elif predicate == "seed":
                if "@" in clause["constraint"]:
                    must_search_field["email"] = clause["constraint"]
                    required_match_field["email"] = clause["constraint"]
                elif clause["constraint"].isdigit():
                    must_search_field["phone"] = clause["constraint"]
                    required_match_field["phone"] = clause["constraint"]
                else:
                    required_match_field["physical_address"] = clause["constraint"]
                continue
            else:
                must_search_field[predicate] = clause["constraint"]
            if predicate != "seed":
                required_match_field[predicate] = clause["constraint"]
        else:
            if clause["isOptional"]:
                optional_match_field[predicate] = clause["variable"]
            else:
                if predicate == "ad":
                    pass
                else:
                    if predicate == "hair_color":
                        must_search_field[predicate] = "hair"
                    if predicate == "eye_color":
                        must_search_field[predicate] = "hair"
                    answer_field[predicate] = clause["variable"]
    #filters
    if filters:
        for filter in filters:
            filter_dic = {}
            operator = ""
            if "operator" in filter:
                operator = filter["operator"][0].upper()
            for clause in filter["clauses"]:
                if "variable" in clause:
                    if clause["variable"] in filter_dic:
                        filter_dic[clause["variable"]]["content"] += " "+operator+" "+re.sub("[^\w\s]","",clause["constraint"])
                    else:
                        filter_dic[clause["variable"]] = {}
                        filter_dic[clause["variable"]]["operator"] = clause["operator"]
                        filter_dic[clause["variable"]]["content"] = re.sub("[^\w\s]","",clause["constraint"])
                else: #image case,ignore for now
                    pass
            for variable in filter_dic:
                for key,value in answer_field.items():
                    if value == variable:
                        del answer_field[key]
                        if filter_dic[variable]["operator"] == "!=":
                            must_not_field[key] = filter_dic[variable]["content"]
                        elif filter_dic[variable]["operator"] == "=":
                            must_search_field[key] = filter_dic[variable]["content"]
                            required_match_field[key] = filter_dic[variable]["content"].split(operator)
    parsed_dic["must_search_field"] = must_search_field
    parsed_dic["should_search_field"] = should_search_field
    parsed_dic["must_not_field"] = must_not_field
    parsed_dic["required_match_field"] = required_match_field
    parsed_dic["optional_match_field"] = optional_match_field
    parsed_dic["answer_field"] = answer_field
    return parsed_dic

def query_body_build(parsed_query):
    must_list = []
    should_list = []
    must_not_list = []
    must_search_dic = parsed_query["must_search_field"]
    should_search_dic = parsed_query["should_search_field"]
    must_not_dic = parsed_query["must_not_field"]
    #month_dic = {"01":"(Jan OR January OR 1)","02":"(Feb OR February OR 2)","03":"(March OR Mar OR 3","04":"April OR Apr OR 4","05":"May OR 5","06":"June OR Jun OR 6"}
    for condition in must_search_dic:
        if condition == "phone":
            must_list.append(must_search_dic[condition][:3])
            must_list.append(must_search_dic[condition][3:6])
            must_list.append(must_search_dic[condition][6:])
        elif condition == "posting_date":
            calendar = must_search_dic[condition].split("-")
            if len(calendar) == 3: #year,month,day are all included
                must_list.append(calendar[0])
                #must_list.append(month_dic[calendar[1]])
                must_list.append(calendar[2])
            elif len(calendar) == 2:
                #must_list.append(month_dic[calendar[0]])
                must_list.append(calendar[1])
        elif condition == "eye_color":
            if must_search_dic[condition] == "eyes":
                must_list.append("eyes")
            else:
                must_list.append(must_search_dic[condition]+"eyes")
        elif condition == "hair_color":
            if must_search_dic[condition] == "hair":
                must_list.append("hair")
            else:
                must_list.append(must_search_dic[condition]+"hair")
        else:
            must_list.append(must_search_dic[condition])

    for condition in should_search_dic:
        should_list.append(should_search_dic[condition])

    for condition in must_not_dic:
        must_not_list.append(must_not_dic[condition])

    should_arr = []
    must_str = " AND ".join(must_list)
    must_not_str = " AND ".join(must_not_list)
    for word in should_list:
        query_dic = {}
        query_string_dic = {}
        query_string_dic["query"] = word
        query_dic["query_string"] = query_string_dic
        should_arr.append(query_dic)
    size = 10
    body = {"size":size,"query":{"bool":{"must":{"match":{"extracted_text": must_str}}, "must_not":{"match": {"extracted_text": must_not_str}}, "should": should_arr}}}
    return body

def elastic_search(query_body):
    es = Elasticsearch(
        ['https://cdr-es.istresearch.com:9200/memex-qpr-cp4-2'],
        http_auth=('cdr-memex', 's7Zhd71r3VD8ojRj'),
        port=9200,
        use_ssl=True,
        verify_certs = True,
        ca_certs=certifi.where(),
    )
    response = es.search(body=query_body)
    documents = response["hits"]["hits"]
    return documents

def annotation(text):
    f = open("tmp.txt","w")
    f.write(text)
    f.close()
    inputPath = "tmp.txt"
    shell_cmd = "java -mx600m -cp \"stanford-ner-2015-12-09/*:stanford-ner-2015-12-09/lib/*\" edu.stanford.nlp.ie.crf.CRFClassifier -loadClassifier stanford-ner-2015-12-09/classifiers/english.all.3class.distsim.crf.ser.gz -outputFormat inlineXML -textFile %s" % inputPath
    annotated_text = os.popen(shell_cmd).read()
    os.system("rm \"tmp.txt\"")
    return annotated_text

