from extraction import top_level_domain_pattern
from bs4 import BeautifulSoup
import re
#input: document, is_raw_content

def TLD_specific_search(document, is_raw_content):
	TLD = top_level_domain_pattern(document, is_raw_content)
	raw_content = document["raw_content"]
	# raw_content = document
	if TLD and raw_content:
		soup = BeautifulSoup(raw_content, 'html.parser')
		if TLD == "escortcafe.com":
			content = soup.find_all("div", class_="details")
	# print type(content)
	# print re.findall('Blonde', content)
		elif TLD == "classifriedads.com":
			content = soup.find_all(id="contentcell")
		elif TLD == "slixa.com":
			content = soup.find_all("div", class_="span9 profile-content") + soup.find_all("aside", class_="profile-sidebar span3")
		# elif TLD == "allsexyescort.com":
		elif TLD == "escort-ads.com":
			content = soup.findall("div", class_="container main-content vip-content")
		# elif TLD == "liveescortreviews.com":
		# elif TLD == "escort-europe.com":
		elif TLD == "find-escorts.com":
			content = soup.findall(id="contentcell")
		elif TLD == "escortserv.com":
			content = soup.findall(id="index")
		elif TLD == "slixa.ca":
			content = soup.find_all("div", class_="span9 profile-content") + soup.find_all("aside", class_="profile-sidebar span3")
		elif TLD == "escortpost.com":
			content = soup.findall(id="content")
		elif TLD == "privateescorts.ro":
			content = soup.findall("tbody")
		elif TLD == "adultsearch.com":
			content = soup.findall(id="ad")

		return str(content)	
	else:
			return ""

path = "/Users/Sharon/Desktop/bs_test.txt"
f = open(path)
data = f.read().replace('\n', '')
print TLD_specific_search(data, True)