__author__ = 'infosense'
import sys,json,datetime,os
import yaml
import fuzzywuzzy as fuzz
import search,extraction,ebola_html_dealer

def main():
    reload(sys)
    sys.setdefaultencoding("utf-8")
    query_path = "sparql-queries-parsed-2016-07-23T11-11.json"
    query_list = search.query_retrival(query_path)
    answer_dic = []
    for query in query_list:
        if query["type"] == "aggregate" and query["id"] == "1519":
            print(datetime.datetime.now())
            filepath = "aggregate/"+query["id"]
            parsed_query_dic = search.query_parse(query)
            query_body = search.query_body_build(parsed_query_dic)
            documents = search.elastic_search(query_body)
            annotated_raw_contents,annotated_clean_contents =  annotator(documents)
            for i in range(len(documents)):
                documents[i]["annotated_raw_content"] = annotated_raw_contents[i]
                documents[i]["annotated_clean_content"] = annotated_clean_contents[i]
                output_filepath = "/Users/infosense/Desktop/test"
                document_path = os.path.join(output_filepath,str(i))
                w = open(document_path,"w")
                extractions = {}
                for func_name,func in extraction.functionDic.items():
                    extractions["raw_"+func_name] = func(documents[i],True)
                    extractions[func_name] = func(documents[i],False)
                documents[i]["indexing"] = extractions
                json.dump(documents[i],w)
                w.close()
            print(datetime.datetime.now())
            #     if validate(document,parsed_query_dic):
            #         answer = answer_extraction(document,parsed_query_dic)
            #         #print(answer)
            #         #if len(answer)>0:
            #         dic = {}
            #         dic["id"] = document["_id"]
            #         dic["validation_score"] = document["validation_score"]
            #         dic["els_score"] = document["_score"]
            #         dic["extraction_score"] = answer["extraction_score"]
            #         dic["feature"] = extraction.generate_feature_score(document)
            #         dic.update(answer)
            #         result.append(dic)
            # final_result = generate_formal_answer(query,result)
            # answer_dic.append(final_result)
            # #print(answer_dic)
            # f = open(filepath,"w")
            # json.dump(answer_dic,f)
            # f.close()

def annotator(documents):
    #print(datetime.datetime.now())
    para_size = 300 #how many documents are annotated every time
    para_num = len(documents)/para_size
    separator = "wjxseparator" #used to join raw_content from different documents, combine them and annotate at one time
    indexed_raw_result = []
    indexed_clean_result = []
    for i in range(para_num):
        raw_contents = []
        clean_contents = []
        for j in range(i*para_size,(i+1)*para_size):
            raw_content = documents[j]["_source"]["raw_content"]
            raw_contents.append(raw_content)
            clean_content = ""
            if "extracted_text" in documents[j]["_source"] and documents[j]["_source"]["extracted_text"]:
                clean_content = documents[j]["_source"]["extracted_text"]
            else:
                clean_content = ebola_html_dealer.make_clean_html(raw_content)
            clean_contents.append(clean_content)
        raw_indexed = search.annotation(separator.join(raw_contents))
        clean_indexed = search.annotation(separator.join(clean_contents))
        indexed_raw_result += raw_indexed.split(separator)
        indexed_clean_result += clean_indexed.split(separator)
    return (indexed_raw_result,indexed_clean_result)
    #print(datetime.datetime.now())

def modify_query(parsed_query, field, seed):  # Already test
    new_query = parsed_query.copy()
    connected_field = ["email", "phone", "address"]
    for connect in connected_field:
        if connect in new_query["must_search_field"]:
            del new_query["must_search_field"][connect]
    new_query["must_search_field"][field] = seed
    return new_query

def cluster(query, search_round):
    need_to_search = []
    res = []
    searched_email = []
    searched_phone = []
    searched_address = []
    searched_ads = []
    ########## Base search ##############
    parsed_query = search.query_parse(query)
    #print(parsed_query)
    parsed_must_field = parsed_query["must_search_field"]
    if "email" in parsed_must_field:
        searched_email.append(parsed_must_field["email"])
    if "phone" in parsed_must_field:
        searched_phone.append(parsed_must_field["phone"])
    if "address" in parsed_must_field:
        searched_address.append(parsed_must_field["address"])
    query_body = search.query_body_build(parsed_query)
    # print query_body
    search_result = search.elastic_search(query_body)
    # print search_result
    dicts = build_dictionary(search_result, parsed_query, searched_ads)
    if dicts:
        need_to_search.extend(dicts)
    ########### Rounds search #############
    i = 1
    while i < search_round:
        if need_to_search:
            for j in range(len(need_to_search)):     # seed ads list in this round
                # for item in need_to_search[j]:			# Each ads
                email_to_search = []
                phone_to_search = []
                address_to_search = []
                ad_id,value = need_to_search[0].items()[0]
                #print value
                if "email" in value:
                    j_email = list(set(value["email"]))
                    #print(j_email)
                    if j_email:
                        for email in j_email:
                            if email not in searched_email:
                                email_to_search.append(email)
                        for email in email_to_search:
                            new_query = modify_query(parsed_query, "email", email)
                            #print parsed_query, new_query
                            email_query_body = search.query_body_build(new_query)
                            email_search_result = search.elastic_search(email_query_body)
                            new_dicts = build_dictionary(email_search_result, new_query, searched_ads)
                            if new_dicts:
                                need_to_search.extend(new_dicts)
                            searched_email.append(email)
                if "phone" in value:
                    j_phone = list(set(value["email"]))
                    #print(j_phone)
                    if j_phone:
                        for phone in j_phone:
                            if phone not in phone_to_search:
                                phone_to_search.append(phone)
                        for phone in phone_to_search:
                            new_query = modify_query(parsed_query, "phone", phone)
                            #print parsed_query, new_query
                            phone_query_body = search.query_body_build(new_query)
                            phone_search_result = search.elastic_search(phone_query_body)
                            new_dicts = build_dictionary(phone_search_result, new_query, searched_ads)
                            if new_dicts:
                                need_to_search.extend(new_dicts)
                            searched_phone.append(phone)
                if "address" in value:
                    j_address = list(set(value["address"]))
                    #print(j_address)
                    if j_address:
                        for add in j_address:
                            for searched in searched_address:
                                if fuzz.ratio(add, searched) < 80:
                                    address_to_search.append(add)
                        for add in address_to_search:
                            new_query = modify_query(parsed_query, "address", add)
                            # print parsed_query, new_query
                            address_query_body = search.query_body_build(new_query)
                            add_search_result = search.elastic_search(address_query_body)
                            new_dicts = build_dictionary(add_search_result, new_query, searched_ads)
                            if new_dicts:
                                need_to_search.extend(new_dicts)
                            searched_address.append(add)
                res.append(need_to_search.pop(0))
        i += 1
    return res

def build_dictionary(search_result, current_query, searched_ads):
    functionDic = extraction.functionDic
    dicts = []
    for result in search_result:
        dic = {}
        adID = result["_id"]
        if adID not in searched_ads:
            searched_ads.append(adID)
            extracted_text = extraction.get_text(result)
            #print(current_query)
            if validate(result, current_query):
                # print("Pass validation")
                email = list(set(extraction.email_recognition(result)))
                phone = list(set(extraction.phone_recognition(result)))
                address = list(set(extraction.physical_address_recognition(result)))
                dic[adID] = {}
                dic[adID]["email"] = email
                dic[adID]["phone"] = phone
                dic[adID]["address"] = address
                dic[adID]["validation_score"] = result["validation_score"]
                dic[adID]["els_score"] = result["_score"]
                dic[adID]["extraction_score"] = 0
                if email:
                    dic[adID]["extraction_score"] += 3
                if phone:
                    dic[adID]["extraction_score"] += 3
                if address:
                    dic[adID]["extraction_score"] += 3
                dicts.append(dic)
    return dicts

def generate_formal_answer(query,result):
    final_result = {}
    final_result["question_id"] = query["id"]
    validate_coeff = 0.6
    extraction_coeff = 0.4
    els_coeff = 0.2
    if query["type"] == "cluster":
        ad_list = []
        for ad in result:
            ad_dic = {}
            ad_dic["?cluster"] = get_cluster_seed(query)
            key,value = ad.items()[0]
            ad_dic["?ad"] = key
            ad_dic["score"] = validate_coeff*ad[key]["validation_score"]+extraction_coeff*ad[key]["extraction_score"]+els_coeff*ad[key]["els_score"]
            ad_list.append(ad_dic)
        ad_list.sort(key= lambda k:k["score"],reverse=True)
        final_result["answers"] = ad_list
    elif query["type"] == "pointfact":
        select_answer_number = len(result)
        # if "group-by" in query:
        #     group_by = query["group-by"]
        #     for item in result: #sort the answer in every document
        #         if "sorted-order" in group_by:
        #             if group_by["sorted-order"] == "desc": #descending
        #                 item[group_by["order-variable"][1:]].sort(reverse=True)
        #         else:#ascending
        #             for item in result:
        #                 item[group_by["order-variable"][1:]].sort()
        #     if "sorted-order" in group_by: #sort the document
        #             if group_by["sorted-order"] == "desc": #descending
        #                 result.sort(key = lambda k:k[group_by["order-variable"][1:]][0],reverse =True)
        #     else:
        #         result.sort(key = lambda k:k[group_by["order-variable"][1:]][0])
        #     # if "limit" in group_by:
        #     #     select_answer_number = min(len(result),group_by["limit"])
        #print(result)
        final_result["answers"] = []
        answer_field = ""
        score_list = ["extraction_score","validation_score","id","els_score"]
        if len(result)>0:
            for key,value in result[0].items():
                if key not in score_list:
                    answer_field = key
        for i in range(select_answer_number):
            answer_dic = {}
            answer_dic["?ad"] = result[i]["id"]
            answer_dic["score"] = validate_coeff*result[i]["validation_score"]+extraction_coeff*result[i]["extraction_score"]+els_coeff*result[i]["els_score"]
            try:
                if result[i][answer_field]:
                    answer_field_value = result[i][answer_field][0]
                    if answer_field == "posting_date":
                        answer_field_value_str = str(answer_field_value)
                        answer_field_value = str(answer_field_value_str)[0:4]+"-"+str(answer_field_value_str)[4:6]+"-"+str(answer_field_value_str)[6:]
                    elif answer_field == "age":
                        answer_field_value = str(answer_field_value)
                    answer_dic["?"+answer_field] = answer_field_value
                final_result["answers"].append(answer_dic)
            except Exception as e:
                pass
        final_result["answers"].sort(key= lambda k:k["score"],reverse=True)
    else:
        group_by = query["group-by"]
        group_variable = group_by["group-variable"]
        feature = group_variable[1:]
        dic = {}
        f = open("feature.txt","w")
        for i in range(len(result)):
            result[i]["score"] = validate_coeff*result[i]["validation_score"]+extraction_coeff*result[i]["extraction_score"]+els_coeff*result[i]["els_score"]
            f.write(extraction.write_feature_score(result[i]["feature"],query["id"],result[i]["id"]))
        result.sort(key= lambda k:k["score"],reverse=True)
        for ad in result:
            if feature in ad:
                for item in ad[feature]:
                    if item in dic:
                        if ad["id"] not in dic[item]:
                            dic[item].append(ad["id"])
                    else:
                        dic[item] = [ad["id"]]
        answer = dic.items()
        if "sorted-order" in group_by:
            if group_by["sorted-order"] == "desc": #descending
                answer.sort(key=lambda k:len(k[1]),reverse=True)
        else:#ascending
            answer.sort(key=lambda k:len(k[1]))
        select_answer_number = len(answer)
        # if "limit" in group_by:
        #     select_answer_number = min(group_by["limit"],len(result))
        # else:
        #     select_answer_number = len(answer)
        final_result["answers"] = []
        for i in range(select_answer_number):
            answer_dic = {}
            answer_dic[group_variable] = answer[i][0]
            answer_dic["?count"] = len(answer[i][1])
            answer_dic["?ads"] = ",".join(answer[i][1])
            final_result["answers"].append(answer_dic)
    return final_result

def get_cluster_seed(query):
    clauses = query["where"]["clauses"]
    for clause in clauses:
        if clause["predicate"] == "seed":
            return clause["constraint"]

def answer_extraction(document,parsed_query_dic):
    extraction_result = {}
    extraction_result["extraction_score"] = 0
    answer_field = parsed_query_dic["answer_field"]
    match_frequency = 0.0
    for feature in answer_field:
        #check if feature is in the metadata_extraction
        if extraction.is_metadata(document):
            if feature in document["_source"]["extracted_metadata"]:
                document["meta_text_percentage"] += 1
        #look for the extraction in raw content
        raw_result = extraction.functionDic[feature](document,True)
        extraction_result[feature] = raw_result
        match_frequency += len(raw_result)
        if raw_result:
            document["raw_content_percentage"] += 1.0
            extraction_result["extraction_score"] += 3
        #look for the extraction in extracted text
        result = extraction.functionDic[feature](document,False)
        match_frequency += len(result)
        if result:
            document["extract_text_percentage"] += 1.0
    matchword = parsed_query_dic["required_match_field"]
    document["match_frequency"] = match_frequency/2
    document["raw_content_percentage"] /= (len(answer_field)+len(matchword))
    document["extract_text_percentage"] /= (len(answer_field)+len(matchword))
    return extraction_result

def validate(document, parsed_query): # Need to write
    text = extraction.get_raw_content(document)
    extract_text = extraction.get_text(document)
    matchword = parsed_query["required_match_field"]  # dict
    lower_text = text.lower()
    lower_extract_text = extract_text.lower()
    if "validation_score" not in document:
        document["validation_score"] = 0
    score = 0
    extract_score = 0
    meta_datascore = 0
    for feature in matchword:
        if extraction.is_metadata(document):
            if feature in document["_source"]["extracted_metadata"]:
                meta_datascore += 1.0
        if type(matchword[feature]) is list:
            #isValid = False
            for item in matchword[feature]:
                if item.lower() in lower_text:
                    # isValid = True
                    score += 1.0   #add 3 points if one of the attributes validates
                    break
            for item in matchword[feature]:
                if item.lower() in lower_extract_text:
                    # isValid = True
                    extract_score += 1.0   #add 3 points if one of the attributes validates
                    break
        else:
            if feature == "location":
                location_fields = [field.strip() for field in matchword[feature].split(",")]
                #isValid = False
                for i in range(len(location_fields)):
                    if location_fields[i].lower() in lower_text:
                        score += 1.0/len(location_fields)
                    if location_fields[i].lower() in lower_extract_text:
                        extract_score += 1.0/len(location_fields)
                        #return True
                        # isValid = True
                #     else:
                #         return False
                # if isValid:
                #     return True
                # else:
                #     return False
            else:
                if matchword[feature].lower() in lower_text:
                    score += 1.0
                if matchword[feature].lower() in lower_extract_text:
                    extract_score += 1.0
        if score == 0:
            results = extraction.functionDic[feature](document,True)
            #print(results)
            if results:
                #isValid = False
                #print(results)
                for result in results:
                    if fuzz.ratio(str(result),matchword[feature])>=80:
                        score += 1.0
                        break
                #         isValid = True
                #         break
                # if isValid:
                #     continue
                # else:
                #     return False
            # else:
            #     return False
        if extract_score == 0 and extract_text:
            results = extraction.functionDic[feature](document,False)
            if results:
                #isValid = False
                #print(results)
                for result in results:
                    if fuzz.ratio(str(result),matchword[feature])>=80:
                        score += 1.0
                        break
    #document["validation_score"] += score
    document["raw_content_percentage"] = score
    document["extract_text_percentage"] = extract_score
    document["meta_text_percentage"] = meta_datascore
    if score+extract_score+meta_datascore>0 :
        return True
    else:
        return False

if __name__ == "__main__":
    main()