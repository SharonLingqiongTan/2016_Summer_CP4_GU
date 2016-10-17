__author__ = 'infosense'
import sys,json,yaml,os,certifi
import extraction,search,ebola_html_dealer
from collections import Counter
from elasticsearch import Elasticsearch


def searchDocument(id):
    es = Elasticsearch(
        ['https://cdr-es.istresearch.com:9200/memex-qpr-cp4-2'],
        http_auth=('cdr-memex', '5OaYUNBhjO68O7Pn'),
        port=9200,
        use_ssl=True,
        verify_certs = True,
        ca_certs=certifi.where(),
    )
    query_body = {
        "query":{
          "bool": {
              "must": {
                 "match": {
                     "_id":id
                 }
                }
           }
       }
    }
    response = es.search(body=query_body,request_timeout=60)
    document = response["hits"]["hits"]
    if document:
        return document[0]
    else:
        return document

def readAnswer(filepath):
    f = open(filepath)
    lines = f.readlines()
    f.close()
    answer = yaml.load(lines[0])[0]
    return [item["?ad"] for item in answer["answers"]]

def readGT(filepath): #read the whole ground truth
    f = open(filepath)
    GT = json.load(f)
    return GT

def getGT(query_id,GT): #get ground truth documents for specific query
    query_map = {"1634.1818":"1818","1636.1818":"1820","1638.1822":"1822","1646.1842":"1842","1657.1825":"1825","1661.1856":"1856","1664.1857":"1857","1668.1828":"1828","1674.1832":"1832","1676.1864":"1864","1686.1869":"1869","1694.1887":"1887","22":"22","30":"30","41":"41","43":"43","50":"50","52":"52",
                 "58":"58","61":"61","82":"82","83":"83","1486":"1486","1493":"1493","1505":"1505","1519":"1519","1564":"1564","1566":"1566","1575":"1575","1577":"1577","1584":"1584","1592":"1592","1595":"1595","1602":"1602","1608":"1608","1612":"1612"}
    for query in GT:
        if query["question_id"] == query_map[query_id]:
            ads = []
            for ans in query['answers']:
                for url in ans['urls']:
                    # Generate ground truth answer key
                    ads.append(url['ad_id'])
            return ads

def getQuery(query_id,query_list):#get the query for the documents
    for query in query_list:
        if query["id"] == query_id:
            return query

# def generate_feature(document,parsed_dic):
#     extraction.validate(document,parsed_dic)
#     extraction.answer_extraction(document,parsed_dic)
#     feature = extraction.generate_feature_score(document)
#     return feature

def generate_GT_training_data(GT,parsed_query_dic,query_id):
    train_filepath = "/Users/infosense/Desktop/answer/train_"+query_id
    f = open(train_filepath,"w")
    for ad in GT:
        document = searchDocument(ad)
        if document:
            raw_content = extraction.get_raw_content(document)
            document["annotated_raw_content"] = search.annotation(raw_content)
            clean_content = ""
            if "extracted_text" in document["_source"] and document["_source"]["extracted_text"]:
                clean_content = document["_source"]["extracted_text"]
            else:
                clean_content = ebola_html_dealer.make_clean_html(raw_content)
            document["annotated_clean_content"] = search.annotation(clean_content)
            extractions = {}
            for func_name,func in extraction.functionDic.items():
                extractions["raw_"+func_name] = func(document,True)
                extractions[func_name] = func(document,False)
            document["indexing"] = extractions
            feature_dic = generate_feature(document)
            feature = "1 "+extraction.write_feature_score(feature_dic,query_id,ad)
            f.write(feature+"\n")
    f.close()

def generate_sample_training_data(GT,sample,parsed_query_dic,query_id):
    train_filepath = "/Users/infosense/Desktop/answer/train_"+query_id
    f = open(train_filepath,"a")
    for document in sample:
        feature_dic = generate_feature(document)
        feature = "0 "+extraction.write_feature_score(feature_dic,query_id,document["_id"])
        f.write(feature+"\n")
    f.close()

def generate_testdata(GT,test,parsed_query_dic,query_id):
    train_filepath = "/Users/infosense/Desktop/answer/test_"+query_id
    f = open(train_filepath,"w")
    for document in test:
        feature_dic = generate_feature(document)
        feature = "0 "+extraction.write_feature_score(feature_dic,query_id,document["_id"])
        f.write(feature+"\n")
    f.close()

def generate_feature(document):
    feature_dic = {}
    count_list = ["physical_address","name","email","phone","location","posting_date","price","review_id","title","business","business_type","business_name","hyperlink","multiple_phone","top_level_domain"]
    dic_list = ["hair_color","eye_color","gender","nationality","ethnicity","review_site","services"]
    numerical_list = ["age","number_of_individuals"]
    for index,feature in enumerate(extraction.feature_list):
        feature_result = document["indexing"][feature]
        if type(feature_result) == dict:
            feature_result = feature_result.values()
        for i in range(len(feature_result)):
            if type(feature_result[i]) is list:
                feature_result[i] = feature_result[i][0]
        count_result = Counter(feature_result).most_common()
        if feature in count_list:
            feature_dic[index+1] = len(feature_result)
        elif feature in numerical_list:
            if count_result:
                feature_dic[index+1] = int(count_result[0][0])
            else:
                feature_dic[index+1] = 0
        elif feature == "gender":
            gender_list = ["boys","gentlemen","female","girl","male","transsexual"]
            if count_result:
                if count_result[0][0] not in gender_list:
                    feature_dic[index+1] = 0
                else:
                    feature_index = gender_list.index(count_result[0][0])
                    if feature_index >1:
                        feature_dic[index+1] = 0
                    else:
                        feature_dic[index+1] = feature_index+1
            else:
                feature_dic[index+1] = 0
        elif feature == "hair_color" or feature == "eye_color":
            if count_result:
                if count_result[0][0].split()[-1] not in extraction.color_list:
                    feature_dic[index+1] = len(extraction.color_list)
                else:
                    feature_dic[index+1] = extraction.color_list.index(count_result[0][0].split()[-1])
            else:
                feature_dic[index+1] = len(extraction.color_list)
        elif feature == "nationality" or feature == "ethnicity":
            if count_result:
                if count_result[0][0] in extraction.nationality_list:
                    feature_index = extraction.nationality_list.index(count_result[0][0])
                    feature_dic[index+1] = feature_index
                else:
                    feature_dic[index+1] = len(extraction.nationality_list)
            else:
                feature_dic[index+1] = len(extraction.nationality_list)
        elif feature == "services":
            if count_result:
                if count_result[0][0] in extraction.service_list:
                    feature_index = extraction.service_list.index(count_result[0][0])
                    feature_dic[index+1] = feature_index
                else:
                    feature_dic[index+1] = len(extraction.service_list)
            else:
                feature_dic[index+1] = len(extraction.service_list)
        elif feature == "review_site":
            if count_result:
                if count_result[0][0] in extraction.review_site_list:
                    feature_index = extraction.review_site_list.index(count_result[0][0])
                    feature_dic[index+1] = feature_index
                else:
                    feature_dic[index+1] = len(extraction.review_site_list)
            else:
                feature_dic[index+1] = len(extraction.review_site_list)
    #feature_dic[len(extraction.feature_list)] =
    return feature_dic
if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding("utf-8")
    document_dir = "/Users/infosense/Desktop/indexed_documents"
    query_path = "sparql-queries-parsed-2016-07-23T11-11.json"
    GT_path = "CP4_GT.json"
    rank_path = "/Users/infosense/Desktop/answer/"
    GT = readGT(GT_path)
    query_list = search.query_retrival(query_path)
    for category in os.listdir(document_dir):#iterate over all documents
        if not category.startswith("."): #exclude .DS_store file
            type_path = os.path.join(document_dir,category)
            for query_dir in os.listdir(type_path):
                if not query_dir.startswith("."):
                    gt_ads = getGT(query_dir,GT)
                    query_path = os.path.join(type_path,query_dir)
                    query = getQuery(query_dir,query_list)
                    parsed_query = search.query_parse(query)
                    documents = []
                    for file in os.listdir(query_path):
                        if not file.startswith("."):
                            document_path = os.path.join(query_path,file)
                            f = open(document_path)
                            document = json.load(f)
                            documents.append(document)
                    generate_GT_training_data(gt_ads,parsed_query,query_dir)
                    generate_sample_training_data(gt_ads,documents[-10:],parsed_query,query_dir)
                    generate_testdata(gt_ads,documents[:-10],parsed_query,query_dir)
                    train_cmd = "java -jar RankLib-2.1-patched.jar -train %s -test %s -ranker 6 -metric2t NDCG@10 -save %s" % (rank_path+"train_"+query_dir,rank_path+"test_"+query_dir,rank_path+"model_"+query_dir+".txt")
                    os.system(train_cmd)
                    score_cmd = "java -jar RankLib-2.1-patched.jar -load %s -rank %s -score %s" % (rank_path+"model_"+query_dir+".txt",rank_path+"test_"+query_dir,rank_path+"score_"+query_dir)
                    os.system(score_cmd)
                    f = open(rank_path+"score_"+query_dir)
                    indexed_documents = f.readlines()
                    ads = []
                    for document in indexed_documents:
                        parts = document.split()
                        if parts:
                            ads.append(parts[0])
                    result_dic = {}
                    result_dic["answers"] = {"?ads":",".join(ads)}
                    result_dic["question_id"] = query_dir
                    result = []
                    result.append(result_dic)
                    output_path = rank_path+"ranked_"+query_dir
                    f = open(output_path,"w")
                    json.dump(result,f)
                    f.close()