def phone_recognition(text,country_abbr_list):
    result = []
    for country in country_abbr_list:
        for match in phonenumbers.PhoneNumberMatcher(text,country):
            if match.raw_string not in result:
                result.append(match.raw_string)
    for i in range(len(result)):
        result[i] = re.sub(r"[\D]","",result[i])
    return list(set(result))

