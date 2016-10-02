#!/usr/bin/env python
from __future__ import print_function
import json
import math
import sys

# how to use: python CP4_eval_script.py ground_truth_sample_CP4.json submission_sample_CP4.json output_sample_CP4.txt
output_file = open(sys.argv[3], "w")

################################################
# Obtain full list of ground truth ads and generate ground truth answer key
with open(sys.argv[1]) as gt_file:
	gt_list = json.load(gt_file)

gt_ads = []
gt_key = {}
for entry in gt_list:
	entry_ads = []
	for ans in entry['answers']:
		for url in ans['urls']:
			# Generate ground truth answer key
			entry_ads.append(url['ad_id'])
			# Obtain full list of ground truth ads
			gt_ads.append(url['ad_id'])
	gt_key[entry['question_id']] = entry_ads

################################################
# submission data
with open(sys.argv[2]) as sub_file:
	sub_list = json.load(sub_file)
sub_rank = {}

for entry in sub_list:
	#print(type(entry))
	# Correctly parse question id
	q_id = entry['question_id'].split('.')[-1]
	# Hand jam correction
	if entry['question_id'] == '1636.1818':
		q_id = '1820'

	# Only evaluate questions for which there is ground truth
	if q_id in gt_key.keys():
		ads = []
		uniq_ads = []
		for answer in entry['answers']:
			if '?ad' in answer.keys():
				ads.append(answer['?ad'])
			elif '?ads' in answer.keys():
				ads.extend(answer['?ads'].split(','))
		# Make de-duped list while preserving order (INEFFICIENT)
		for ad in ads:
			# Only consider ads found in the overall ground truth set
			if ad in gt_ads:
				if ad not in uniq_ads:
					uniq_ads.append(ad)
		sub_rank[q_id] = uniq_ads

################################################
# DCG Calculations:
for question in sub_rank.keys():
	sub_ads = sub_rank[question]
	ans_ads = gt_key[question]
	if question == "1486":
		print(ans_ads)
	#print(question)
	#print(ans_ads)
#	# DEBUG ###################
#	if question == '83':
#		same = set(sub_ads) & set(ans_ads)
#		print(question)
#		print(sub_ads)
#		print(len(ans_ads))
#		print(len(same))
	#############################


	# Here we use:
	# DCG = SUM( (2^rel_i - 1) / (log_2(i+1)) ) = SUM( 1 / (log_2(i+1)) )
	# where:
	# rel_i = 1 for right answers
	# i = rank of answer

	# Calculate discounted cumulative gain (DCG).
	# Given binary relevance weighting, DCG depends only on
	# the rank of each correct answer.
	DCG = 0.0
	i = 1
	for ad in sub_ads:
		if ad in ans_ads:
			DCG += 1 / (math.log(i+1,2))
		i += 1

	# Calculate max (a/k/a ideal) discounted cumulative gain (DCG).
	# Given binary relevance weighting, max DCG can be calculated from
	# the number of correct ad ids.
	maxDCG = 0.0
	i = 1
	for ad in ans_ads:
		maxDCG += 1 / (math.log(i+1,2))
		i += 1
	if maxDCG > 0:
	# Normalized DCG
		nDCG = DCG/maxDCG

		# Print to output file
		print("Question: ", question, file=output_file)
		print("Normalized Discounted Cumulative Gain (DCG): ", nDCG, file=output_file)
		print("", file=output_file)