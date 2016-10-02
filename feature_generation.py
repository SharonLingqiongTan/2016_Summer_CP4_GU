# -*- coding: utf-8 -*-
__author__ = 'Moon'
import search,yaml,certifi,json,os,extraction,sys
from elasticsearch import Elasticsearch
import ebola_html_dealer as html_cleaner

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
    query_map = {"1634.1818":"1818","1636.1818":"1820","1638.1822":"1822","1646.1842":"1842","1657.1825":"1825","1661.1856":"1856","1664.1857":"1857","1668.1828":"1828","1674.1832":"1832","1676.1864":"1864","1686.1869":"1869","1694.1887":"1887"}
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

def generate_feature(document,parsed_dic):
    extraction.validate(document,parsed_dic)
    extraction.answer_extraction(document,parsed_dic)
    feature = extraction.generate_feature_score(document)
    return feature

def generate_GT_training_data(GT,parsed_query_dic,query_id):
    train_filepath = "cluster/train_"+query_id
    f = open(train_filepath,"w")
    for ad in GT:
        document = searchDocument(ad)
        if document:
            feature_dic = generate_feature(document,parsed_query_dic)
            feature = "1 "+extraction.write_feature_score(feature_dic,query_id,ad)
            f.write(feature+"\n")
    f.close()

def generate_sample_training_data(GT,sample,parsed_query_dic,query_id):
    train_filepath = "cluster/train_"+query_id
    f = open(train_filepath,"a")
    for ad in sample:
        if ad not in GT:
            document = searchDocument(ad)
            if document :
                feature_dic = generate_feature(document,parsed_query_dic)
                feature = "0 "+extraction.write_feature_score(feature_dic,query_id,ad)
                f.write(feature+"\n")
    f.close()

def generate_test_data(GT,test_data,parsed_query_dic,query_id):
    test_path = "cluster/test_"+query_id
    f = open(test_path,"w")
    for ad in test_data:
        document = searchDocument(ad)
        if document:
            feature_dic = generate_feature(document,parsed_query_dic)
            feature = ""
            if document in GT:
                feature = "1 "+extraction.write_feature_score(feature_dic,query_id,ad)
            else:
                feature = "0 "+extraction.write_feature_score(feature_dic,query_id,ad)
            f.write(feature+"\n")
    f.close()



if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding("utf-8")
    document_dir = "/Users/Moon/Desktop/inforsense/CP4/cluster"
    query_path = "sparql-queries-parsed-2016-07-23T11-11.json"
    GT_path = "CP4_GT.json"
    GT = readGT(GT_path)
    query_list = search.query_retrival(query_path)
    for file in os.listdir(document_dir):#iterate over all documents
        if not file.startswith(".") and file == "1638.1822": #exclude .DS_store file
            gt_ads = getGT(file,GT)
            filepath = os.path.join(document_dir,file)
            answer = readAnswer(filepath)
            query = getQuery(file,query_list)
            parsed_query = search.query_parse(query)
            generate_GT_training_data(gt_ads,parsed_query,file)
            generate_sample_training_data(gt_ads,answer[:10],parsed_query,file)
            generate_test_data(gt_ads,answer,parsed_query,file)
            train_cmd = "java -jar Rank/RankLib-2.1-patched.jar -train %s -test %s -ranker 6 -metric2t NDCG@10 -save %s" % ("cluster/train_"+file,"cluster/test_"+file,"cluster/model_"+file+".txt")
            os.system(train_cmd)
            score_cmd = "java -jar Rank/RankLib-2.1-patched.jar -load %s -rank %s -score %s" % ("cluster/model_"+file+".txt","cluster/test_"+file,"cluster/score_"+file)
            os.system(score_cmd)
            f = open("cluster/score_"+file)
            documents = f.readlines()
            ranked_documents = []
            for document in documents:
                parts = document.split()
                if parts:
                    dic = {}
                    dic["?ads"] = parts[0]
                    dic["?cluster"] = ""
                    ranked_documents.append(dic)
            result_dic = {}
            result_dic["answers"] = ranked_documents
            result_dic["question_id"] = file
            result = []
            result.append(result_dic)

            output_path = "cluster/ranked_"+file
            f = open(output_path,"w")
            json.dump(result,f)
            f.close()




