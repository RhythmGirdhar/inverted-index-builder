from pyspark import SparkContext, SparkConf
import os
import unicodedata

if __name__ == "__main__":

    sc = SparkContext()

    input_dir = "data 2/fulldata"
    output_dir_uni = "result/unigram_index.txt"

    files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]

    # create RDDs from the input files
    rdd_list = []
    for file in files:
        rdd = sc.textFile(file)
        rdd_list.append(rdd)

    final_rdd = sc.union(rdd_list)

    doc_rdd = final_rdd.map(lambda x: (x.split("\t", 2)[0], x.split("\t", 2)[1]))
    preprocessed_rdd = doc_rdd.mapValues(lambda x: ''.join(c if c.isalpha() and not unicodedata.name(c).startswith('GREEK') else ' ' for c in x).lower())
    words_rdd = preprocessed_rdd.flatMapValues(lambda x: x.split())

    ## Unigram ##   
    words_rdd = preprocessed_rdd.flatMapValues(lambda x: x.split())
    word_doc_rdd = words_rdd.map(lambda x: ((x[1], x[0]), 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], (x[0][1], x[1])))
    df_rdd = word_doc_rdd.groupByKey().mapValues(lambda x: dict(x)).sortByKey()
    
    df_rdd.map(lambda x: x[0] + "\t" + str(x[1]))

    with open(output_dir_uni, "w") as file:
        for row in df_rdd.collect():
            word = row[0]
            doc_counts = " ".join([f"{doc}:{count}" for doc, count in row[1].items()])
            file.write(f"{word}\t{doc_counts}\n")
    