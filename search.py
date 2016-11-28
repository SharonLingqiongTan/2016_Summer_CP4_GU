# -*- coding: utf-8 -*-
import certifi,os,yaml,sys,re,json
from elasticsearch import Elasticsearch,RequestsHttpConnection
import extraction

def query_retrival(query_path): #parse the json query file into a list of queries
    f = open(query_path)
    lines = f.readlines()
    query_list = []
    for line in lines:
        query_list.append(json.loads(line,strict = False))
    f.close()
    return query_list
    # f = open(query_path)
    # lines = f.readlines()
    # f.close()
    # queries = yaml.load("".join(line.strip("\n") for line in lines))
    # query_list = []
    # for query_type in queries:
    #     for query_id in queries[query_type]:
    #         query = queries[query_type][query_id]["parsed"].copy()
    #         query["type"] = query_type.lower()
    #         query["id"] = query_id
    #         query_list.append(query)
    # return query_list

# def query_parse(query):
#     skin_color_list = ["white","yellow","black"]
#     search_ignore_list = ["obfuscation","multiple_providers","gender"]
#     where = query["where"]
#     clauses = where["clauses"]
#     filters = {}
#     if "filters" in where:
#         filters = where["filters"]
#     parsed_dic = {} #Dictory to store the parsed query
#     must_search_field = {} #Correspond to the must field in ElasticSearch Query
#     should_search_field = {} #Correspond to the should field in ElasticSearch Query
#     must_not_field = {} #Correspond to the must not field in ElasticSearch Query
#     required_match_field = {} #Validation fields after document retrieval
#     optional_match_field = {} #Optional fields after document retrieval
#     answer_field = {}
#     for clause in clauses:
#         predicate = re.sub("[^\w]","",clause["predicate"])
#         if "constraint" in clause:  #constraint indicates given condition
#             if predicate in search_ignore_list:
#                 pass
#             elif predicate == "ethnicity":
#                 if clause["constraint"] in skin_color_list:
#                     should_search_field["ethnicity"] = clause["constraint"]
#                 else:
#                     must_search_field["ethnicity"] = clause["constraint"]
#             # elif predicate == "drug_use":
#             #     should_search_field["drug_use"] = "drug"
#             elif predicate == "location": #location = city,state(which is not necesary
#                 location = clause["constraint"].split(",")
#                 if location:
#                     should_search_field["location"] = clause["constraint"]
#                     if len(location)>1:
#                         if location[1].lower().capitalize() in extraction.nationality_list:
#                             country = location[1]
#                             if location[1].lower() == "thailand":
#                                 country = "thai"
#                             must_search_field["location"] = location[0]+" AND "+country
#                         else:
#                             must_search_field["location"] = location[0]
#                     else:
#                         must_search_field["location"] = location[0]
#             elif predicate == "seed":
#                 if "@" in clause["constraint"]:
#                     must_search_field["email"] = clause["constraint"]
#                     required_match_field["email"] = clause["constraint"]
#                 elif clause["constraint"].isdigit():
#                     must_search_field["phone"] = clause["constraint"]
#                     required_match_field["phone"] = clause["constraint"]
#                 else:
#                     required_match_field["street_address"] = clause["constraint"]
#                 continue
#             else:
#                 must_search_field[predicate] = clause["constraint"]
#             if predicate != "seed":
#                 required_match_field[predicate] = clause["constraint"]
#         else:
#             if clause["isOptional"]:
#                 optional_match_field[predicate] = clause["variable"]
#             else:
#                 if predicate == "ad":
#                     pass
#                 else:
#                     if predicate == "hair_color":
#                         must_search_field[predicate] = "hair"
#                     if predicate == "eye_color":
#                         must_search_field[predicate] = "hair"
#                     if predicate == "ethnicity": #Add "ethinitity" to should search field to get relavant documents ahead in the result.
#                         should_search_field[predicate] = "ethnicity"
#                     if predicate == "review_site":
#                         should_search_field[predicate] = "review"
#                     answer_field[predicate] = clause["variable"]
#     #filters
#     if filters:
#         for filter in filters:
#             filter_dic = {}
#             operator = ""
#             if "operator" in filter:
#                 operator = filter["operator"][0].upper()
#             for clause in filter["clauses"]:
#                 if "variable" in clause:
#                     if clause["variable"] in filter_dic:
#                         filter_dic[clause["variable"]]["content"] += " "+operator+" "+re.sub("[^\w\s]","",clause["constraint"])
#                     else:
#                         filter_dic[clause["variable"]] = {}
#                         filter_dic[clause["variable"]]["operator"] = clause["operator"]
#                         filter_dic[clause["variable"]]["content"] = re.sub("[^\w\s]","",clause["constraint"])
#                 else: #image case,ignore for now
#                     pass
#             for variable in filter_dic:
#                 for key,value in answer_field.items():
#                     if value == variable:
#                         if filter_dic[variable]["operator"] == "!=":
#                             must_not_field[key] = filter_dic[variable]["content"]
#                         elif filter_dic[variable]["operator"] == "=":
#                             del answer_field[key]
#                             must_search_field[key] = filter_dic[variable]["content"]
#                             required_match_field[key] = filter_dic[variable]["content"].split(operator)
#     parsed_dic["must_search_field"] = must_search_field
#     parsed_dic["should_search_field"] = should_search_field
#     parsed_dic["must_not_field"] = must_not_field
#     parsed_dic["required_match_field"] = required_match_field
#     parsed_dic["optional_match_field"] = optional_match_field
#     parsed_dic["answer_field"] = answer_field
#     return parsed_dic

def query_parse(query):  # input query - json
    skin_color_list = ["white","yellow","black"]
    # search_ignore_list = ["obfuscation","multiple_provider"] # No gender feature
    query_id = query['id']
    query_type = query['type']
    sparql = query['SPARQL'][0]
    lines = sparql.split('\n')
    parsed_dic = {}
    ans_field = {}
    must_search = {}
    should_search = {}
    must_not_search = {}
    must_match = {} #Validation fields after document retrieval
    should_match = {} #Optional fields after document retrieval
    group = {}
    filter_condition = {}
    for line in lines:
        line = line.strip()
        words = line.split(' ')
        if line.startswith('PREFIX'):
            continue
        if line.startswith('SELECT'):
            pattern = "\?([A-Za-z_]+)"
            fields = re.findall(pattern,line)
            if len(fields) == 2:
                for item in fields:
                    if item != "ad":
                        ans_field[item]	= "?"+item
        if line.startswith('qpr:'):
            line = line[:-1].strip() #remove the punctuation at the end
            words = line.split(" ",1)
            if not words[1].startswith("?"): #Given conditions
                words[1] = words[1][1:-1]
                predicate = words[0][4:]
                constraint = words[1]
                # Need to refine search_ignore_list
                # if predicate in search_ignore_list:
                    # pass
                if predicate == 'ethnicity':
                    if constraint in skin_color_list:
                        should_search['ethnicity'] = constraint
                    else:
                        if constraint.lower() not in extraction.continent_dic:
                            must_search['ethnicity'] = constraint
                        else:
                            should_search["ethnicity"] = constraint
                    must_match[predicate] = constraint
                # search directly
                elif predicate == 'phone':
                    words = query["question"].split()
                    if "number" in words:
                        number_index = words.index("number")+1
                        if number_index < len(words) and re.findall("\d",words[number_index]):
                            phone = ""
                            while len(re.findall("\d",phone))<8:
                                phone += words[number_index]+" "
                                number_index += 1
                            must_search["phone"] = re.sub(r"[^\d\(\)-\+ ]","",phone.strip())
                            must_match["phone"] = re.sub(r"[^\d\(\)-\+ ]","",phone.strip())
                        else:
                            should_search['phone'] = constraint
                            must_match[predicate] = constraint
                            must_search[predicate] = constraint
                elif predicate == 'location':
                    location = constraint.split(',')
                    if location:
                        should_search['location'] = constraint
                        if len(location)>1:
                            if location[1].lower().capitalize() in extraction.nationality_list:
                                country = location[1]
                                if location[1].lower() == 'thailand':
                                    country = 'thai'
                                    must_search['location'] = location[0]+' AND '+country
                                else:
                                    must_search['location'] = location[0]
                            else: #city
                                must_search["location"] = constraint
                        else:
                            must_search['location'] = location[0]
                        must_match['location'] = constraint
                # elif predicate = 'tatoos':
                elif predicate == 'multiple_providers':
                    should_search['multiple_providers'] = constraint
                    must_match[predicate] = constraint
                elif predicate == 'hair_color':
                    should_search['hair_color'] = constraint
                    must_match[predicate] = constraint
                    must_search[predicate] = constraint
                elif predicate == 'eye_color':
                    should_search['eye_color'] = constraint
                    must_match[predicate] = constraint
                    must_search[predicate] = constraint
                elif predicate == "height":
                    he = re.findall("\d",constraint)
                    if len(he) == 1:
                        must_search["height"] = he[0]+"'"
                        should_search["height"] = he[0]+"\""
                        must_match["height"] = he[0]+"'"
                    elif len(he) == 2:
                        must_search["height"] = he[0]+"'"+he[1]+"\""
                        must_match["height"] = he[0]+"'"+he[1]+"\""
                elif predicate == "post_date":
                    should_search["post_date"] = constraint
                    must_match["post_date"] = constraint
                # cluster query
                elif predicate == 'seed':
                    if "@" in constraint:
                        must_search['email'] = constraint
                        must_match['email'] = constraint
                    elif constraint.isdigit():
                        must_search['phone'] = constraint
                        must_match['phone'] = constraint
                else:
                    must_search[predicate] = constraint
                    must_match[predicate] = constraint # email, street_address, social_media_id, review_site_id, age, price, services, height, weight, post_date


        if line.startswith('GROUP BY'):
            ans_pattern = '(?:\?)([a-z]+)'
            for word in words:
                group_variable = re.findall(ans_pattern, word)
                group['group_by'] = group_variable
        if line.startswith('ORDER BY'):
            for word in words:
                if 'DESC' in word:
                    group['order_by'] = 'DESC'
                elif 'ASEC' in word:
                    group['order_by'] = 'ASEC'
        if line.startswith("LIMIT"):
            group["limit"] = int(line.split()[1])
        if line.startswith("FILTER"):
            filterPattern = "'(.*?)'"
            filter_constraint = re.findall(filterPattern, line)
            if filter_constraint:
                filter_constraint = filter_constraint[0]
            if "content" in line:
                must_search["content"] = filter_constraint
                must_match["content"] = filter_constraint
            elif "title" in line:
                must_search["title"] = filter_constraint
                must_match["title"] = filter_constraint


    parsed_dic["type"] = query["type"]
    parsed_dic['answer_field'] = ans_field
    parsed_dic['must_search_field'] = must_search
    parsed_dic['should_search_field'] = should_search
    parsed_dic['must_not_field'] = must_not_search
    parsed_dic['required_match_field'] = must_match
    parsed_dic['optional_match_field'] = should_match
    parsed_dic['group'] = group
    return parsed_dic


def query_body_build(parsed_query):
    must_list = []
    should_list = []
    must_not_list = []
    must_search_dic = parsed_query["must_search_field"]
    should_search_dic = parsed_query["should_search_field"]
    must_not_dic = parsed_query["must_not_field"]
    answer_field = parsed_query["answer_field"]
    #month_dic = {"01":"(Jan OR January OR 1)","02":"(Feb OR February OR 2)","03":"(March OR Mar OR 3","04":"April OR Apr OR 4","05":"May OR 5","06":"June OR Jun OR 6"}
    for condition in must_search_dic:
        if condition == "phone": #phone number usually can not be searched directly
            if len(must_search_dic[condition])>= 12: #phone number greater than 12 digits are regarded as international phone number.
                must_list.append(must_search_dic[condition][:2])
                must_list.append(must_search_dic[condition][3:])
            else:
                must_list.append(must_search_dic[condition][:3])
                must_list.append(must_search_dic[condition][3:6])
                must_list.append(must_search_dic[condition][6:])
                should_list.append(must_search_dic[condition][:3]+"-"+must_search_dic[condition][3:6]+"-"+must_search_dic[condition][6:])
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
            must_list.append("eye")
        elif condition == "hair_color":
            must_list.append("hair")
        elif condition == "ethnicity":
            should_list.append("ethnicity")
        elif condition == "nationality":
            should_list.append("nationality")
        else:
            must_list.append(must_search_dic[condition])

    for condition in should_search_dic:
        should_list.append(should_search_dic[condition])

    feature_should_search_map = {"tattoos":"tattoo","name":"name","street_address":"address","age":"age","hair_color":"hair","eye_color":"eye","nationality":"nationality","ethnicity":"ethnicity","review_site_id":"review","email":"email","phone":"phone","location":"location","price":"","multiple_providers":"","social_media_id":"","services":"","height":"height","weight":"weight","post_date":"posted"}
    for field in answer_field:
        if feature_should_search_map[field]:
            should_list.append(feature_should_search_map[field])

    for condition in must_not_dic:
        must_not_list.append(must_not_dic[condition])
    should_arr = []
    must_str = " AND ".join(must_list)
    must_not_str = " AND ".join(must_not_list)
    for word in should_list:
        query_dic = {}
        query_dic["match"] = {}
        query_dic["match"]["extracted_text"] = word
        should_arr.append(query_dic)
    size = 3000
    if parsed_query["type"] == "Cluster Identification":
        size = 500
    body = {"size":size,"query":{"bool":{"must":{"match":{"extracted_text": must_str}}, "must_not":[{"match": {"extracted_text": must_not_str}},{"match": {"raw_content": must_not_str}}], "should": should_arr}}}
    return body

def elastic_search(query_body):
    # es = Elasticsearch(
    #     ['https://cdr-es.istresearch.com:9200/memex-qpr-cp4-2'],
    #     http_auth=('cdr-memex', '5OaYUNBhjO68O7Pn'),
    #     port=9200,
    #     use_ssl=True,
    #     verify_certs = True,
    #     ca_certs=certifi.where(),
    # )
    es = Elasticsearch(['search-memex-7kthxwtfdr3yvzrdpjlcwordou.us-east-1.es.amazonaws.com:80/gt',],connection_class= RequestsHttpConnection)
    #es = Elasticsearch()
    response = es.search(body=query_body,request_timeout=60)
    documents = response["hits"]["hits"]
    return documents

def annotation(text):
    f = open("tmp7.txt","w")
    f.write(text)
    f.close()
    inputPath = "tmp7.txt"
    shell_cmd = "java -mx5g -cp \"stanford-ner-2015-12-09/*:stanford-ner-2015-12-09/lib/*\" edu.stanford.nlp.ie.crf.CRFClassifier -loadClassifier stanford-ner-2015-12-09/classifiers/english.all.3class.distsim.crf.ser.gz -outputFormat inlineXML -textFile %s" % inputPath
    annotated_text = os.popen(shell_cmd).read()
    os.system("rm \"tmp7.txt\"")
    return annotated_text

