# -*- coding: utf-8 -*-
import re,sys,json,yaml,os,webcolors,search
from fuzzywuzzy import fuzz
from nltk.corpus import stopwords
from datetime import date,timedelta
import ebola_html_dealer as html_cleaner


#Feature_list [1:is_extract_text,2:is_meta_data,3:raw_content_match percentage, 4:extracted_text_match percentage 5:meta_data_match percentage
#6:raw_content_len, 7:extract_len, 8:match_frequency, 9:elastic_score, 10:raw_content_ave_distance, 11:extract_text_ave_dis
def is_extract_text(document):
    if get_text(document):
        return 1
    else:
        return 0

def is_metadata(document): #extraction from elastic search
    if "extracted_metadata" in document["_source"]:
        return 1
    else:
        return 0

def raw_content_length(document):
    if "raw_content" in document["_source"]:
        return len(document["_source"]["raw_content"].split())
    else:
        return 0

def extract_content_length(document):
    return len(get_text(document).split())

def elastic_score(document):
    return document["_score"]

def generate_feature_score(document):
    feature = {}
    feature[1] = is_extract_text(document)
    feature[2] = is_metadata(document)
    feature[3] = document["raw_content_percentage"]
    feature[4] = document["extract_text_percentage"]
    feature[5] = document["meta_text_percentage"]
    feature[6] = raw_content_length(document)
    feature[7] = extract_content_length(document)
    feature[8] = document["match_frequency"]
    #feature[9] = elastic_score(document)
    return feature

def write_feature_score(feature_dic,query_id,document_id):
    feature = "qid:"+document_id+" "
    #print(feature_dic)
    for i in range(len(feature_dic)):
        feature += str(i+1)+":"+str(feature_dic[i+1])+" "
    feature += "#docid = "+document_id
    return feature

#extraction

def get_text(document):
    if "extracted_text" in document["_source"]:
        extract_text = document["_source"]["extracted_text"]
        if extract_text:
            return extract_text
    try:
        extract_text = html_cleaner.make_clean_html(get_raw_content(document))
    except Exception as e:
        extract_text = ""
    document["_source"]["extracted_text"] = extract_text
    return extract_text

def get_raw_content(document):
    return document["_source"]["raw_content"]

def age_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    age_pattern = r"(?:^|\D)([1-6]\d)(?:\D|$)"
    words = re.sub(r'[^\w\s]',' ',text).split()
    result = []
    match_range = 5
    pre_match_words = ["i","am","this","age","aged","my"]
    post_match_words = ["year","years","old","yrs"]
    for i in range(len(words)):
        tmp = re.findall(age_pattern,words[i])
        if len(tmp)>0:  #check if the words appear around the age_pattern indicates it is age
            age = tmp[0]
            is_validate_age = False
            post_match_field = words[i+1:i+match_range]
            if i>0 and int(age)%10 == 0:  # check if it satisfies the pattern: early,mid,late 30s
                if "early" in words[i-1].lower():
                    #result.append([age,str(int(age)+1),str(int(age)+2),str(int(age)+3)])
                    result.append(int(age)+2)
                    break
                if "mid" in words[i-1].lower():
                    #result.append([str(int(age)+4),str(int(age)+5),str(int(age)+6)])
                    result.append(int(age)+5)
                    break
                if "late" in words[i-1].lower():
                    result.append([str(int(age)+7),str(int(age)+8),str(int(age)+9)])
                    result.append(int(age)+8)
                    break
            for word in post_match_field:
                if word.lower() in post_match_words:
                    is_validate_age = True
                    break
            pre_match_field = words[i-match_range:i]
            for word in pre_match_field:
                if word.lower() in pre_match_words:
                    is_validate_age = True
                    break
            if is_validate_age:
                result.append(int(age))
    birthday_pattern = re.compile(r"(?i)birthday[^A-Za-z0-9]{1,3}((?:19[0-9]{2})|(?:20[01][0-9]))")  #pattern of bodyrubresumes.com
    birthday_pattern_result = re.findall(birthday_pattern,text)
    for item in birthday_pattern_result:
        result.append(2016-int(item))
    if "extractions" in document["_source"]:
        crawl_extractions = document["_source"]["extractions"]
        if "age" in crawl_extractions:
            for age in crawl_extractions["age"]["results"]:
                if age.isdigit():
                    if int(age) not in result:
                        result.append(age)
    return result
    # age_pattern1 = re.compile(r"(?i)age[^A-Za-z0-9]{1,3}([1-6][0-9])[^A-Za-z0-9]")
    # age_pattern2 = re.compile(r"((?i)(?:i'm|im|i am)?[^A-Za-z0-9]?[1-6][0-9])(?:[^A-Za-z0-9]?(?i)(?:years|yrs|year)[^A-Za-z0-9](?:old)?)")
    # #early: 1,2,3; mid: 4,5,6; late: 7,8,9
    # age_pattern3 = re.compile(r"((early|mid|late) ([1-9]0)'?s)")
    # age_pattern1_result = re.findall(age_pattern1,text)
    # age_pattern2_result = re.findall(age_pattern2,text)
    # age_pattern3_result = re.findall(age_pattern3,text)
    # for item in age_pattern1_result+age_pattern2_result+age_pattern3_result:
    #     result.append(item[0])
    # return result

def hair_color_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    normalized_color = ["blonde", "brown", "black", "red", "auburn", "chestnut", "gray", "white","dark"]
    color_dic = webcolors.CSS3_NAMES_TO_HEX
    for color in normalized_color:
        if color not in color_dic:
            color_dic[color] = "1"
    text_result = []
    # raw_content_result = []
    text_without_quotation = re.sub(r'[^\w\s]','',text)
    # raw_content_without_quotation = re.sub(r'[^\w\s]','',raw_content)
    words = text_without_quotation.split()
    # raw_words = raw_content_without_quotation.split()
    for i in range(len(words)):
        if fuzz.ratio(words[i].lower(),"hair")>=80: #judge if word and hair are similar
            color_str = ""
            eye_color = False
            for j in range(i+1,i+6): #look for color vocabulary after hair
                if words[j].lower() in color_dic:
                    color_str = words[j].lower()
                if fuzz.ratio(words[i].lower(),"eyes")>=80: #check if eyes color is around
                    eye_color = True
            if color_str:
                if eye_color:
                    hair_color_str = ""
                    for j in range(i-5,i):
                        if words[j].lower() in color_dic:
                            hair_color_str = words[j].lower()
                    if hair_color_str:
                        text_result.append(hair_color_str)
                    else:
                        text_result.append(color_str)
                else:
                    text_result.append(color_str)
            else:
                hair_color_str = ""
                for j in range(i-5,i):
                    if words[j].lower() in color_dic:
                        hair_color_str = words[j].lower()
                if hair_color_str:
                    text_result.append(hair_color_str)

    # for i in range(len(raw_words)):
    #     if fuzz.ratio(raw_words[i].lower(),"hair")>=80: #judge if word and hair are similar
    #         color_str = ""
    #         eye_color = False
    #         for j in range(i+1,i+6): #look for color vocabulary after hair
    #             if raw_words[j].lower() in color_dic:
    #                 color_str = raw_words[j].lower()
    #             if fuzz.ratio(raw_words[i].lower(),"eyes")>=80: #check if eyes color is around
    #                 eye_color = True
    #         if color_str:
    #             if eye_color:
    #                 hair_color_str = ""
    #                 for j in range(i-5,i):
    #                     if raw_words[j].lower() in color_dic:
    #                         hair_color_str = raw_words[j].lower()
    #                 if hair_color_str:
    #                     raw_content_result.append(hair_color_str)
    #                 else:
    #                     raw_content_result.append(color_str)
    #             else:
    #                 raw_content_result.append(color_str)
    #         else:
    #             hair_color_str = ""
    #             for j in range(i-5,i):
    #                 if raw_words[j].lower() in color_dic:
    #                     hair_color_str = raw_words[j].lower()
    #             if hair_color_str:
    #                 raw_content_result.append(hair_color_str)
    # if len(text_result)>len(raw_content_result):
    #     return text_result
    # else:
    #     return raw_content_result
    return text_result

def eye_color_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    normalized_color = ["blue", "brown", "green", "hazel", "gray", "amber"]
    color_dic = webcolors.CSS3_NAMES_TO_HEX
    for color in normalized_color:
        if color not in color_dic:
            color_dic[color] = "1"
    text_result = []
    #raw_content_result = []
    text_without_quotation = re.sub(r'[^\w\s]','',text)
    #raw_content_without_quotation = re.sub(r'[^\w\s]','',raw_content)
    words = text_without_quotation.split()
    #raw_words = raw_content_without_quotation.split()
    for i in range(len(words)):
        if fuzz.ratio(words[i].lower(),"eyes")>=80: #judge if word and hair are similar
            color_str = ""
            hair_color = False
            for j in range(i+1,i+6): #look for color vocabulary after eyes
                if words[j].lower() in color_dic:
                    color_str = words[j].lower()
                if fuzz.ratio(words[i].lower(),"hair")>=80: #check if eyes color is around
                    hair_color = True
            if color_str:
                if hair_color:
                    eye_color_str = ""
                    for j in range(i-5,i):
                        if words[j].lower() in color_dic:
                            eye_color_str = words[j].lower()
                    if eye_color_str:
                        text_result.append(eye_color_str)
                    else:
                        text_result.append(color_str)
                else:
                    text_result.append(color_str)
            else:
                eye_color_str = ""
                for j in range(i-5,i):
                    if words[j].lower() in color_dic:
                        eye_color_str = words[j].lower()
                if eye_color_str:
                    text_result.append(eye_color_str)

    # for i in range(len(raw_words)):
    #     if fuzz.ratio(raw_words[i].lower(),"eyes")>=80: #judge if word and hair are similar
    #         color_str = ""
    #         hair_color = False
    #         for j in range(i+1,i+6): #look for color vocabulary after eyes
    #             if raw_words[j].lower() in color_dic:
    #                 color_str = raw_words[j].lower()
    #             if fuzz.ratio(raw_words[i].lower(),"hair")>=80: #check if eyes color is around
    #                 hair_color = True
    #         if color_str:
    #             if hair_color:
    #                 eye_color_str = ""
    #                 for j in range(i-5,i):
    #                     if raw_words[j].lower() in color_dic:
    #                         eye_color_str = raw_words[j].lower()
    #                 if eye_color_str:
    #                     raw_content_result.append(eye_color_str)
    #                 else:
    #                     raw_content_result.append(color_str)
    #             else:
    #                 raw_content_result.append(color_str)
    #         else:
    #             eye_color_str = ""
    #             for j in range(i-5,i):
    #                 if raw_words[j].lower() in color_dic:
    #                     eye_color_str = raw_words[j].lower()
    #             if eye_color_str:
    #                 raw_content_result.append(eye_color_str)
    # if len(text_result)>len(raw_content_result):
    #     return text_result
    # else:
    #     return raw_content_result
    return text_result

def nationality_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    nationality_filepath = "./resource/nationality"
    with open(nationality_filepath) as f:
        nationality_list = ','.join(f.readlines()).split(",")
        f.close()
        words = text.split()
        #raw_words = raw_content.split()
        text_result = []
        #raw_content_result = []
        for word in words:
            word_norm = word.lower().capitalize()
            if word_norm in nationality_list:
                text_result.append(result_normalize(word_norm))
        # for word in raw_words:
        #     word_norm = word.lower().capitalize()
        #     if word_norm in nationality_list:
        #         raw_content_result.append(result_normalize(word_norm))
        # if len(text_result)>len(raw_content_result):
        #     return text_result
        # else:
        #     return raw_content_result
        return text_result


def ethnicity_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    nationality_filepath = "./resource/nationality"
    ethnicity_arr = ["caucasian", "hispanic", "asian", "african american", "caribbean", "pacific islander", "middle eastern", "biracial", "south asian", "native american"]
    result = []
    f = open(nationality_filepath)
    nationality_list = ','.join(f.readlines()).split(",")
    f.close()
    words = text.split()
    for word in words:
            word_norm = word.lower().capitalize()
            if word_norm in nationality_list:
                result.append(word_norm)
    lowercase_text = text.lower()
    for word in ethnicity_arr:
        if word in lowercase_text:
            result.append(word)
    return result

def review_site_recognition(document,is_raw_content):
    #url_pattern = re.compile(r'(http[s]?://)|(www.)(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    review_site_list = ["eccie", "TER", "preferred411", "eccie.net", "theeroticreview",]
    review_site = []
    hyperlinks = hyperlink_recognition(document,is_raw_content)
    if hyperlinks:
        for link in hyperlinks:
            for site in review_site_list:
                if site in link:
                    if site == "eccie.net":
                        site = "eccie"
                    if site == "theeroticreview":
                        site = "TER"
                    review_site.append(site)
    return review_site

def email_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    regex = re.compile(("([a-z0-9!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`"
                    "{|}~-]+)*(@|\sat\s)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(\.|"
                    "\sdot\s))+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)"))
    result = []
    text_result = re.findall(regex,text)
    #raw_content_result = re.findall(regex,raw_content)
    # if len(text_result)>=len(raw_content_result):
    #     for email in text_result:
    #         if not email[0].startswith('//'):
    #             result.append(email[0].lower())
    # else:
    #     for email in raw_content_result:
    #         if not email[0].startswith('//'):
    #             result.append(email[0].lower())
    for email in text_result:
        if not email[0].startswith('//'):
            result.append(email[0].lower())
    return result

def phone_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    number_pattern = r"(?:^|\D)([0-9]{3})[^A-Za-z0-9]{0,2}([0-9]{3})[^A-Za-z0-9]{0,2}([0-9]{3,6})(?:\D|$)"
    text_result = re.findall(number_pattern,text)
    #raw_content_result = re.findall(number_pattern,raw_content)
    result = []
    # if len(text_result)>=len(raw_content_result):
    #     for item in text_result:
    #         result.append("".join(item))
    # else:
    #     for item in raw_content_result:
    #         result.append("".join(item))
    for item in text_result:
        result.append("".join(item))
    return result

def name_recognition(document,is_raw_content):
    # text = ""
    # if is_raw_content:
    #     text = get_raw_content(document)
    # else:
    #     text = get_text(document)
    annotated_text = ""
    if is_raw_content:
        annotated_text = document["annotated_raw_content"]
    else:
        annotated_text = document["annotated_clean_content"]
    name_pattern = re.compile(r"\<PERSON\>(.*?)\</PERSON>")
    name_pattern_result = re.findall(name_pattern,annotated_text)
    result = []
    if len(name_pattern_result)>0:
        for item in name_pattern_result:
            result.append(result_normalize(item))
    return result

def location_recognition(document,is_raw_content):
    # text = ""
    # if is_raw_content:
    #     text = get_raw_content(document)
    # else:
    #     text = get_text(document)
    annotated_text = ""
    if is_raw_content:
        annotated_text = document["annotated_raw_content"]
    else:
        annotated_text = document["annotated_clean_content"]
    location_arr = re.findall(r"\<LOCATION\>(.*?)\</LOCATION\>",annotated_text)
    #print(document)
    result = []
    # if len(location_arr) == 0:
    #     state_pattern = re.compile(r"in ([A-Z]{2})")
    #     state_pattern_result = re.findall(state_pattern,document)
    #     if len(state_pattern_result)>0:
    #         start_index = 0
    #         for item in state_pattern_result:
    #             str_index = document[start_index:].index(item)
    #             subdocument = document[:str_index]
    #             word_index = len(subdocument.split())
    #             result.append(word_index)
    #             start_index = start_index+str_index+len(item)
    if len(location_arr) > 0:
        # words = annotated_text.split()
        # for i in range(len(words)):
        #     if "<LOCATION>" in words[i]:
        #         result.append(i)
        for location in location_arr:
            result.append(result_normalize(location))
    #print(result)
    return result

def organization_recognition(document,is_raw_content):
    # text = ""
    # if is_raw_content:
    #     text = get_raw_content(document)
    # else:
    #     text = get_text(document)
    annotated_text = ""
    if is_raw_content:
        annotated_text = document["annotated_raw_content"]
    else:
        annotated_text = document["annotated_clean_content"]
    organization_pattern = re.compile(r"\<ORGANIZATION\>(.*?)\</ORGANIZATION>")
    organization_pattern_result = re.findall(organization_pattern,annotated_text)
    result = []
    if len(organization_pattern_result)>0:
        for item in organization_pattern_result:
            result.append(result_normalize(item))
    return result

#return all the extracted dates in dictioanry format -- date_dic = {day:int month:int year: int}, if date is not exact(more than a week ago), use an interval(int_low,int_high) instead
def posting_date_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    #digit date pattern like 8/7/2000, 2000/7/8
    month = r"((?:0?[1-9])|(?:1[0-2]))"
    day = r"((?:0?[1-9])|(?:[12][0-9])|(?:[3][01]))"
    year = r"((?:19[0-9]{2})|(?:20[01][0-9]))"
    conjunction = r"[^A-Za-z0-9]"
    month_day_year = "("+month+conjunction+day+conjunction+year+")"
    day_month_year = "("+day+conjunction+month+conjunction+year+")"
    year_month_day = "("+year+conjunction+month+conjunction+day+")"
    digit_date_pattern = month_day_year+"|"+day_month_year+"|"+year_month_day
    digit_date_pattern_result = re.findall(digit_date_pattern,text)
    result = []
    for item in digit_date_pattern_result:
        dic = {}
        if len(item[0])>0:
            dic["month"] = item[1]
            dic["day"] = item[2]
            dic["year"] = item[3]
        elif len(item[4])>0:
            dic["day"] = item[5]
            dic["month"] = item[6]
            dic["year"] = item[7]
        elif len(item[8])>0:
            dic["year"] = item[9]
            dic["month"] = item[10]
            dic["day"] = item[11]
        if len(dic)>0:
            date_int = int(dic["year"])*(10**4)+int(dic["month"])*(10**2)+int(dic["day"])
            result.append(date_int)

    #str_digit pattern like Jan 8th 2001
    month_str = r"(?i)(January|Jan|February|Feb|March|Mar|April|Apr|May|June|Jun|July|Jul|August|Aug|September|Sep|October|Oct|November|Nov|December|Dec)"
    day_str = r"((?:[1-3]?1(?i)(?:st)?)|(?:[1-2]?2(?i)(?:nd)?)|(?:[1-2]?3(?i)(?:rd)?)|(?:[1-3]?[04-9](?i)(?:th)?))"
    month_day_pattern = "("+month_str+r"[^A-Za-z0-9]"+day_str+r"[^A-Za-z0-9]{1,2}"+year+"(?:[^A-Za-z0-9])"+")"
    day_month_pattern = "("+day_str+r"[^A-Za-z0-9]"+month_str+r"[^A-Za-z0-9]{1,2}"+year+"(?:[^A-Za-z0-9])"+")"
    str_date_pattern = month_day_pattern+"|"+day_month_pattern
    str_date_pattern_result = re.findall(str_date_pattern,text)
    month_dic = {"jan":1,"january":1, "feb": 2, "february": 2, "mar": 3, "march": 3, "apr": 4, "april": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7, "august": 8, "aug": 8, "september": 9, "sep": 9, "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12}
    for item in str_date_pattern_result:
        dic = {}
        if item[0]:
            dic["month"] = month_dic[item[1].lower()]
            dic["day"] = re.sub("[A-Za-z]","",item[2])
            dic["year"] = item[3]
        else:
            dic["month"] = month_dic[item[6].lower()]
            dic["day"] = re.sub("[A-Za-z]","",item[5])
            dic["year"] = item[7]
        if len(dic)>0:
            date_int = int(dic["year"])*(10**4)+int(dic["month"])*(10**2)+int(dic["day"])
            result.append(date_int)
            
    #relative date pattern like 10 months ago, more than a week a ago
    # number_str = r"((?i)(?:[1-3]?[0-9])|a|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)"
    # number_dic = {"a":1,"one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,"eight":8,"nine":9,"ten":10,"eleven":11,"twelve":12,"thirteen":13,"fourteen":14,"fifteen":15,"sixteen":16,"seventeen":17,"eighteen":18,"nineteen":19,"twenty":20}
    # relative_date_pattern = r"(?i)(more than|less than|over)?[^A-Za-z0-9]"+ number_str+r"[^A-Za-z0-9](day|week|month|year)(?:s)?[^A-Za-z0-9]ago"
    # relative_date_pattern_result = re.findall(relative_date_pattern,text)
    # current_date = date.today()
    # result = []
    # for item in relative_date_pattern_result:
    #     dic = {}
    #     if item[1] in number_dic:
    #         time_interval = number_dic[item[1]]
    #     else:
    #         time_interval = int(item[1])
    #     if len(item[0]) == 0:
    #         if item[2] == "day":
    #             post_date = current_date - timedelta(days = time_interval)
    #         elif item[2] == "week":
    #             post_date = current_date - timedelta(weeks = time_interval)
    #         elif item[2] == "month":
    #             post_date = current_date - timedelta(days = time_interval*30)
    #         else:
    #             post_date = current_date - timedelta(days = time_interval*365)
    #         dic["day"] = post_date.day
    #         dic["month"] = post_date.month
    #         dic["year"] = post_date.year
    #     else:
    #         if item[0] == "more than" or item[0] == "over":
    #             if item[2] == "day":
    #                 post_date_high = current_date - timedelta(days = time_interval-1)
    #                 post_date_low = current_date - timedelta(days = time_interval)
    #             elif item[2] == "week":
    #                 post_date_high = current_date - timedelta(weeks = time_interval-1)
    #                 post_date_low = current_date - timedelta(weeks = time_interval)
    #             elif item[2] == "month":
    #                 post_date_high = current_date - timedelta(days = (time_interval-1)*30)
    #                 post_date_low = current_date - timedelta(days = (time_interval)*30)
    #             else:
    #                 post_date_high = current_date - timedelta(days = (time_interval-1)*365)
    #                 post_date_low = current_date - timedelta(days = (time_interval)*365)
    #             dic["day"] = (post_date_low.day,post_date_high.day)
    #             dic["month"] = (post_date_low.month,post_date_high.month)
    #             dic["year"] = (post_date_low.year,post_date_high.year)
    #         else:
    #             if item[2] == "day":
    #                 post_date_high = current_date - timedelta(days = time_interval)
    #                 post_date_low = current_date - timedelta(days = time_interval+1)
    #             elif item[2] == "week":
    #                 post_date_high = current_date - timedelta(weeks = time_interval)
    #                 post_date_low = current_date - timedelta(weeks = time_interval+1)
    #             elif item[2] == "month":
    #                 post_date_high = current_date - timedelta(days = time_interval*30)
    #                 post_date_low = current_date - timedelta(days = (time_interval+1)*30)
    #             else:
    #                 post_date_high = current_date - timedelta(days = time_interval*365)
    #                 post_date_low = current_date - timedelta(days = (time_interval+1)*365)
    #             dic["day"] = (post_date_low.day,post_date_high.day)
    #             dic["month"] = (post_date_low.month,post_date_high.month)
    #             dic["year"] = (post_date_low.year,post_date_high.year)
    #     result.append(dic)
    return result



def gender_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    result = []
    gender_list = ["male","female","transsexual"]
    if "extractions" in document["_source"]:
        crawl_extractions = document["_source"]["extractions"]
        if "gender" in crawl_extractions:
            genders = crawl_extractions["gender"]["results"]
            for item in genders:
                for gender in gender_list:
                    if fuzz.ratio(item,gender)>=80:
                        result.append(gender)
    if len(result) == 0:
        male_words = ["ladies","girls","boy"]
        female_words = ["boys","gentlemen","girl"]
        for word in female_words:
            if word in text:
                result.append(word)
    return result


def number_of_individuals_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    if "twin" in text:
        return [2]
    names = name_recognition(document,is_raw_content)
    for i in range(len(names)):
        names[i] = result_normalize(names[i])
    names = list(set(names))
    eye_colors = eye_color_recognition(document,is_raw_content)
    hair_colors = hair_color_recognition(document,is_raw_content)
    ages = age_recognition(document,is_raw_content)
    nationalities = nationality_recognition(document,is_raw_content)
    ethnicities = ethnicity_recognition(document,is_raw_content)
    number_list = [names,eye_colors,hair_colors,ages,nationalities,ethnicities]
    #print(number_list)
    number_list.sort(key=lambda k:len(k))
    result = 0
    for item in number_list:
        if len(item) >0:
            result = len(item)
            break
    if result == 0:
        return [1]
    else:
        return [result]

def review_id_recognition(document,is_raw_content):
    url = document["_source"]["cleaned_url"] + "/"      # Add a non-num and non-alph character in case review id is right at the end of url
    pattern = "(?:[^A-Za-z0-9])([0-9]{5,})(?:[^A-Za-z0-9])"
    review_id = re.findall(pattern, url)
    return review_id

def title_recognition(document,is_raw_content):
    result = []
    if "extractions" in document["_source"]:
        crawl_extractions = document["_source"]["extractions"]
        if "title" in crawl_extractions:
            if "results" in crawl_extractions["title"]:
                result = crawl_extractions["title"]["results"][:]
                for i in range(len(result)):
                    result[i] = result_normalize(result[i])
    return result

def business_recognition(document,is_raw_content):
    text = get_text(document)
    business = []
    business_name = business_name_recognition(document,is_raw_content)
    business_address = physical_address_recognition(document,is_raw_content)
    if business_name:
        for name in business_name:
            name = result_normalize(name)
            business.append(name)
    if business_address:
        for address in business_address:
            address = result_normalize(address)
            business.append(address)
    return business

def business_type_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    business_type_found = []
    business_type = ["massage", "spa", "escort agency", "escort-agency"]
    for business in business_type:
        pattern = "(?:[^A-Za-z])(?i)(" + business + ")(?:$|[^A-Za-z])"
        results = re.findall(pattern, text)
        if results:
            for res in results:
                business_type_found.append(result_normalize(res))
    return business_type_found

def business_name_recognition(document,is_raw_content):
    return organization_recognition(document,is_raw_content)

def result_normalize(result):
    normedResult = ""
    if type(result) is str:
        normedResult = re.sub("[^\w\s]"," ",result.lower())
    return normedResult

def services_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    service_list_path = "resource/serviceList.txt"
    sex_service = []
    with open(service_list_path, "r") as inputFile:
        services = inputFile.readlines()
        for i in range(len(services)):
            services[i] = services[i].strip("\n")
    for service in services:
        pattern = "(?i)(" + service + ")"
        results = re.findall(pattern, text)
        if results:
            for res in results:
                sex_service.append(result_normalize(res))
    return sex_service

def hyperlink_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    pattern = "href=\"(.*?)\""
    hyperlinks = re.findall(pattern, text)
    return hyperlinks

def drug_use_recognition(document,is_raw_content):
    result = []
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    cleaned_text = re.sub("[^\w\s]"," ",text)
    words = cleaned_text.split()
    for i in range(len(words)):
        if fuzz.ratio(words[i].lower(),"drug")>=80:
            drug_use = "true"
            for word in words[i-3:i+4]:
                if word.lower == "no":
                    result.append("false")
                    drug_use = "false"
                    break
                if drug_use:
                    result.append("true")
    return result

def multiple_phone_recognition(document,is_raw_content):
    result = phone_recognition(document,is_raw_content)
    return list(set(result))

def top_level_domain_recognition(document,is_raw_conent):
    path = "resource/Seed_TLDs_7.15.2016.txt"
    parentUrl = document["_source"]["cleaned_url"]
    findTLD = False
    result = []
    with open(path) as inputFile:
        TLDs = inputFile.readlines()
        for TLD in TLDs:
            TLD = TLD.strip("\n")
            if parentUrl.find(TLD) != -1:
                findTLD = True
                result.append(TLD)
                break
            else:
                continue
        if findTLD == False:
            if parentUrl.startswith("http://"):
                url = parentUrl[len("http://"):]
            elif parentUrl.startswith("https://"):
                url = parentUrl[len("https://")]
            else:
                url = parentUrl
            url = url[:url.find("/")]
            url_parts = url.split(".")
            TLD = url_parts[-2] + "." + url_parts[-1]
            result.append(TLD)
    return result

def image_with_phone_recognition(document):
    return []

def image_with_email_recognition(document):
    return []

def obfuscation_recognition(document):
    return []

def image_with_review_id_recognition(document):
    return []

def image_with_tattoo_recognition(document):
    return []

def image_in_hotel_motel_room_recognition(document):
    return []

def image_without_professional_lighting_recognition(document):
    return []

def price_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    price1 = "(?:\d+\.)?\d+,\d+"
    price2 = "(^(\$|€|¥|£|$|Fr|¥|kr|Ꝑ|ք|₩|R|(R$)|₺|₹)\d+)"
    units = "(Z|zero)|(O|one)|(T|two)|(T|three)|(F|four)|(F|five)|(S|six)|(S|seven)|(E|eight)|(N|nine)|(T|ten)|(E|eleven)|(T|twelve)|(T|thirteen)|(F|fourteen)|(F|fifteen)|(S|sixteen)|(S|seventeen)|(E|eighteen)|(N|nineteen)"
    tens = "(T|ten)|(T|twenty)|(T|thirty)|(F|forty)|(F|fourty)|(F|fifty)|(S|sixty)|(S|seventy)|(E|eighty)|(N|ninety)"
    hundred = "(H|hundred)"
    thousand = "(T|thousand)"
    OPT_DASH = "-?"
    price3 = "(" + units + OPT_DASH + "(" + thousand + ")?" + OPT_DASH + "(" + units + OPT_DASH + hundred + ")?" + OPT_DASH + "(" + tens + ")?" + ")" + "|" + "(" + tens + OPT_DASH + "(" + units + ")?" + ")"
    price4 = "\d+"
    preDollarPrice = [price1, price3, price4]
    split = text.split(" ")
    priceList = {}
    for i in range(len(split)):
        if split[i] == "dollar" or split[i] == "dollars":
            for pricePat in preDollarPrice:
                price = re.findall(pricePat, split[i - 1])
                if price:
                    priceList[i-1] = split[i - 1] + " " + split[i]
                    #print(priceList[i-1])
        else:
            price = re.findall(price2, split[i])
            if price:
                priceList[i] = price[0][0]
                #print(priceList[i])
    return priceList

def height_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    height_pattern = re.compile(r"(([1-9])'(([0-9])\")?)")
    height_pattern_result = re.findall(height_pattern,text)
    if len(height_pattern_result)>0:
        result = []
        for item in height_pattern_result:
            result.append(item[0])
        return result
    return ""

def color_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    color_dic = webcolors.CSS3_NAMES_TO_HEX
    text_without_quotation = re.sub(r'[^\w\s]',' ',text)
    words = text_without_quotation.split()
    result = {}
    for i in range(len(words)):
        if words[i].lower() in color_dic:
            result[i] = words[i]
    return result


def physical_address_recognition(document,is_raw_content):
    text = ""
    if is_raw_content:
        text = get_raw_content(document)
    else:
        text = get_text(document)
    #raw_content = get_raw_content(document)
    text_without_quotation = re.sub(r'[^\w\s]','',text)
    #raw_content_without_quotation = re.sub(r'[^\w\s]','',raw_content)
    streetNumber = "([1-9][0-9]{1,3} )"
    nsew = "(((N|S|E|W|North|South|East|West|NW|NE|SW|SE) )?)"
    nsewString = "North|South|East|West|NW|NE|SW|SE|"
    streetTypeString = "Street|St|ST|Boulevard|Blvd|Lane|Ln|Road|Rd|Avenue|Ave|Circle|Cir|Cove|Cv|Drive|Dr|Parkway|Pkwy|Court|Ct|Square|Sq|Loop|Lp|"
    roomString = "Suite|suite|Ste|ste|Apt|apt|Apartment|apartment|Room|room|Rm|rm|#|suitenumber"
    streetName_pattern1 = r"(((?!(?:"+nsewString+streetTypeString+roomString+r")\b)[A-Z][a-z]+(?: (?!(?:"+nsewString+streetTypeString+roomString+r")\b)[A-Z][a-z]+){0,2})|((\d+)(st|ST|nd|ND|rd|RD|th|TH)))"
    #streetName_pattern2 = r"((\d+)(st|ST|nd|ND|rd|RD|th|TH))"
    streetName = streetName_pattern1 #+ "|" + streetName_pattern2
    #streetName = "((?!(?:Apt)\b)[A-Z][a-z]+(?: (?!(?:Apt)\b)[A-Z][a-z]+){0,2})"
    streetType = "((Street|St|ST|Boulevard|Blvd|Lane|Ln|Road|Rd|Avenue|Ave|Circle|Cir|Cove|Cv|Drive|Dr|Parkway|Pkwy|Court|Ct|Square|Sq|Loop|Lp) )?"
    room = "(((Suite|suite|Ste|ste|Apt|apt|Apartment|apartment|Room|room|Rm|rm|#|suitenumber) ([0-9]{1,4}([A-Za-z]?)) )?)"
    city_state = "((((([A-Z][a-z]+)|([A-Z]+)) ){1,2}[A-Z]{2} )?)"
    zip_code = "([0-9]{5} )?"
    addree_pattern = re.compile(r"("+streetNumber+nsew+streetName_pattern1+" "+streetType+nsew+room+city_state+zip_code+")")
    text_result= re.findall(addree_pattern,text_without_quotation)
    #raw_content_result= re.findall(addree_pattern,raw_content_without_quotation)
    result = []
    # if len(text_result)>=len(raw_content_result):
    #     for item in text_result:
    #         address_parts = item[0].split()
    #         if len(address_parts)>2:   #although only street number and streeName are required in the pattern, address consists of at least three parts.
    #             isValid = False
    #             for part in address_parts:
    #                 if part.lower() in streetTypeString.lower() or part.lower() in nsew.lower():
    #                     isValid = True
    #             if isValid:
    #                 result.append(result_normalize(item[0]))
    # else:
    #     for item in raw_content_result:
    #         address_parts = item[0].split()
    #         if len(address_parts)>2:   #although only street number and streeName are required in the pattern, address consists of at least three parts.
    #             isValid = False
    #             for part in address_parts:
    #                 if part.lower() in streetTypeString.lower() or part.lower() in nsew.lower():
    #                     isValid = True
    #             if isValid:
    #                 result.append(result_normalize(item[0]))
    for item in text_result:
        address_parts = item[0].split()
        if len(address_parts)>2:   #although only street number and streeName are required in the pattern, address consists of at least three parts.
            isValid = False
            for part in address_parts:
                if part.lower() in streetTypeString.lower() or part.lower() in nsew.lower():
                    isValid = True
            if isValid:
                result.append(result_normalize(item[0]))
    return result


if __name__ != "__main__":
    global functionDic
    functionDic = {"physical_address": physical_address_recognition,"age":age_recognition,
                   "name":name_recognition, "hair_color":hair_color_recognition,"eye_color":eye_color_recognition,"nationality":nationality_recognition,
                   "ethnicity":ethnicity_recognition,"review_site":review_site_recognition,"email": email_recognition,"phone": phone_recognition,
                   "location":location_recognition,"posting_date":posting_date_recognition,"price":price_recognition,"number_of_individuals": number_of_individuals_recognition,
                   "gender":gender_recognition,"review_id":review_id_recognition,"title":title_recognition,"business":business_recognition,"business_type":business_type_recognition,
                   "business_name":business_name_recognition,"services":services_recognition,"hyperlink":hyperlink_recognition,"drug_use":drug_use_recognition,
                   "multiple_phone":multiple_phone_recognition,"top_level_domain":top_level_domain_recognition
                   }
