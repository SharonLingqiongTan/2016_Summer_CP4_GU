import extraction,os

input_path = "/Users/infosense/Desktop/documents"
output_path = "/Users/infosense/Desktop/indexed_documents"
for dir in os.listdir(input_path):
    category = os.path.join(input_path,dir)
    for question in os.listdir(category):
        print(question)