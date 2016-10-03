import extraction,os,json,sys
reload(sys)
sys.setdefaultencoding("utf-8")
input_path = "/Users/infosense/Desktop/documents"
output_path = "/Users/infosense/Desktop/indexed_documents"
for dir in os.listdir(input_path):
    if not dir.startswith("."):
        #create the output directory for each category of question with the same name as input category
        indexed_category_path = os.path.join(output_path,dir)
        if not os.path.exists(indexed_category_path):
            mkdir = "mkdir "+indexed_category_path
            os.system(mkdir)
        #do extration for each file in each category and then save the result in corresponding output path with same name
        category = os.path.join(input_path,dir)
        for query in os.listdir(category):
            if not query.startswith("."):
                input_filepath = os.path.join(category,query)
                output_filepath = os.path.join(indexed_category_path,query)
                f = open(input_filepath)
                documents = json.load(f)
                f.close()
                w = open(output_filepath,"w")
                indexed_documents = []
                for document in documents:
                    extractions = {}
                    for func_name,func in extraction.functionDic.items():
                        extractions["raw_"+func_name] = func(document,True)
                        extractions[func_name] = func(document,False)
                    document["indexing"] = extractions
                    indexed_documents.append(document)
                json.dump(indexed_documents,w)
                w.close()
