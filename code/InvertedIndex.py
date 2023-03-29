# from pyspark import SparkContext, SparkConf
# import os
# import re
# import unicodedata

# if __name__ == "__main__":

#     sc = SparkContext()

#     input_dir = "data 2/devdata"
#     output_dir_uni = "result/unigram_index.txt"
#     output_dir_bi = "result/bigram_index.txt"

#     allowedBigrams = [
#     "computer science",
#     "information retrieval",
#     "power politics",
#     "los angeles",
#     "bruce willis"
#     ]
    
#     regexStr = "[^a-zA-Z]"

#     indexRDD = sc.parallelize([])

#     files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]

#     # create RDDs from the input files
#     rdd_list = []
#     for file in files:
#         rdd = sc.textFile(file)
#         rdd_list.append(rdd)

#     # for rdd in rdd_list:
#     #     doc_rdd = rdd.map(lambda x: (x.split("\t")[0], x.split("\t")[1])).collect()
#     #     docID, contents = doc_rdd[0], " ".join(doc_rdd[1:]).lower()
#     #     contents = re.sub("[^a-zA-Z]", " ", contents)
#     #     contents = contents.split(" ")
#     #     bigrams = []
#     #     for i in range(len(contents) - 1):
#     #         bigrams.append(" ".join([contents[i], contents[i + 1]]))
        
#     #     contentsRDD = sc.parallelize(bigrams)
#     #     contentsRDD = contentsRDD\
#     #     .map(lambda word: (word, (docID, 1)))\
#     #     .reduceByKey(lambda x, y: (x[0], x[1] + y[1]))\
#     #     .filter(lambda x: x[0] in allowedBigrams)\
#     #     .map(lambda x: (x[0], {x[1][0]: x[1][1]}))\
#     #     .cache()

#     #     print(contentsRDD.take(10))
        
#     #     indexRDD = indexRDD.union(contentsRDD).cache()

#     # indexRDD = indexRDD.reduceByKey(lambda x, y: {**x, **y}).sortByKey().cache()

#     # print("indexRDD:", indexRDD.take(10))

#     # with open(output_dir_bi, "w") as file:
#     #     for row in indexRDD.collect():
#     #         word = row[0]
#     #         doc_counts = " ".join([f"{doc}:{count}" for doc, count in row[1].items()])
#     #         file.write(f"{word}\t{doc_counts}\n")

#     final_rdd = sc.union(rdd_list)

#     doc_rdd = final_rdd.map(lambda x: (x.split("\t")[0], x.split("\t")[1]))
#     preprocessed_rdd = doc_rdd.mapValues(lambda x: ''.join(c if c.isalpha() and not unicodedata.name(c).startswith('GREEK') else ' ' for c in x).lower())
#     # text_rdd = preprocessed_rdd.map(lambda x: x[1]).collect()
    
#     # for row in preprocessed_rdd.collect():
#     #     docID, contents = row
#     #     contents = contents.split(" ")
#     #     bigrams = []
#     #     for i in range(len(contents) - 1):
#     #         bigrams.append(" ".join([contents[i], contents[i + 1]]))
        
#     #     contentsRDD = sc.parallelize(bigrams)
#     #     contentsRDD = contentsRDD\
#     #     .map(lambda word: (word, (docID, 1)))\
#     #     .reduceByKey(lambda x, y: (x[0], x[1] + y[1]))\
#     #     .filter(lambda x: x[0] in allowedBigrams)\
#     #     .map(lambda x: (x[0], {x[1][0]: x[1][1]}))\
#     #     .cache()

#     #     print(contentsRDD.take(10))
#     #     indexRDD = indexRDD.union(contentsRDD).cache()

    


#     words_rdd = preprocessed_rdd.flatMapValues(lambda x: x.split())

#     ## Unigram ##   
#     # words_rdd = preprocessed_rdd.flatMapValues(lambda x: x.split())
#     # word_doc_rdd = words_rdd.map(lambda x: ((x[1], x[0]), 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], (x[0][1], x[1])))
#     # df_rdd = word_doc_rdd.groupByKey().mapValues(lambda x: dict(x)).sortByKey()
    
#     # df_rdd.map(lambda x: x[0] + "\t" + str(x[1]))

#     # with open(output_dir_uni, "w") as file:
#     #     for row in df_rdd.collect():
#     #         word = row[0]
#     #         doc_counts = " ".join([f"{doc}:{count}" for doc, count in row[1].items()])
#     #         file.write(f"{word}\t{doc_counts}\n")
    

#     ## Bigram ##
#     bigram_rdd = words_rdd.map(lambda x: (x[1], x[0])).reduceByKey(lambda x, y: x + " " + y)
#     print("bigramsRDD1:", bigram_rdd.take(10))
#     bigram_rdd = bigram_rdd.flatMap(lambda x: [(bigram, x[0]) for bigram in [bigram_pair for bigram_pair in zip(x[1].split()[:-1], x[1].split()[1:]) if " ".join(bigram_pair) in allowedBigrams]])
#     print("bigramsRDD2:", bigram_rdd.take(10))
#     bigram_doc_rdd = bigram_rdd.map(lambda x: ((x[1], x[0]), 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], (x[0][1], x[1])))
#     print("bigram_doc_rdd:", bigram_doc_rdd.take(10))
#     df_rdd = bigram_doc_rdd.groupByKey().mapValues(lambda x: dict(x)).sortByKey()
#     print("df_rdd:", df_rdd.take(10))

#     # df_rdd.map(lambda x: x[0] + "\t" + str(x[1]))

#     # print(df_rdd.collect())

#     with open(output_dir_bi, "w") as file:
#         for row in df_rdd.collect():
#             word = row[0]
#             doc_counts = " ".join([f"{doc}:{count}" for doc, count in row[1].items()])
#             file.write(f"{word}\t{doc_counts}\n")

from pyspark import SparkContext, SparkConf
import os
import re

if __name__ == "__main__":

    # Initialize Spark
    conf = SparkConf().setAppName("BigramIndex")
    sc = SparkContext(conf=conf)

    # Define input and output paths
    input_dir = "data 2/devdata"
    output_dir = "result/bigram_index.txt"

    # Define list of allowed bigrams
    allowed_bigrams = [
        "computer science",
        "information retrieval",
        "power politics",
        "los angeles",
        "bruce willis"
    ]

    # Read input files into RDD
    rdd_list = []
    for file in os.listdir(input_dir):
        if os.path.isfile(os.path.join(input_dir, file)):
            rdd = sc.textFile(os.path.join(input_dir, file))
            rdd_list.append(rdd)
    final_rdd = sc.union(rdd_list)

    # Split input into docID and contents
    doc_rdd = final_rdd.map(lambda x: (x.split("\t")[0], x.split("\t")[1]))

    # Preprocess contents: remove non-alpha characters and convert to lowercase
    preprocessed_rdd = doc_rdd.mapValues(lambda x: re.sub("[^a-zA-Z ]", "", x).lower())

    # Split contents into words and create bigrams
    bigrams_rdd = preprocessed_rdd.flatMapValues(lambda x: zip(x.split()[:-1], x.split()[1:])).map(lambda x: (" ".join(x[1]), x[0]))

    # Count occurrences of each bigram in each document
    bigram_doc_rdd = bigrams_rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda x, y: x + y)

    # Filter out bigrams that are not allowed
    filtered_bigram_doc_rdd = bigram_doc_rdd.filter(lambda x: x[0][0] in allowed_bigrams)

    # Group by bigram and create dictionary of document frequencies
    df_rdd = filtered_bigram_doc_rdd.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(lambda x: dict(x))

    # Write output to file
    with open(output_dir, "w") as file:
        for row in df_rdd.collect():
            bigram = row[0]
            doc_counts = " ".join([f"{doc}:{count}" for doc, count in row[1].items()])
            file.write(f"{bigram}\t{doc_counts}\n")

