import os
from pyspark import SparkContext
import re

if __name__ == "__main__":

  sc = SparkContext()

  input_dir = "/data 2/devdata"
  output_dir_bi = "result/selected_bigram_index.txt"
  subFolderPath = os.getcwd() + input_dir

  allowedBigrams = [
    "computer science",
    "information retrieval",
    "power politics",
    "los angeles",
    "bruce willis"
  ]

  regexStr = "[^a-zA-Z]"

  indexRDD = sc.parallelize([])

  for filename in os.listdir(subFolderPath):
    with open(os.path.join(subFolderPath, filename), 'r') as f:
      fileData = f.read().split("\t")
      docID, contents = fileData[0], re.split(regexStr, fileData[1].lower())
  
      bigrams = [" ".join(contents[i:i+2]) for i in range(len(contents)-1)]
      bigramsRDD = sc.parallelize(bigrams)
      
      filteredRDD = bigramsRDD.filter(lambda bigram: bigram in allowedBigrams)
      countedRDD = filteredRDD.map(lambda bigram: (bigram, (docID, 1))) \
                               .reduceByKey(lambda a, b: (a[0], a[1]+b[1])) \
                               .map(lambda x: (x[0], {x[1][0]: x[1][1]}))

      indexRDD = indexRDD.union(countedRDD).cache()
      
  indexRDD = indexRDD.reduceByKey(lambda x, y: {**x, **y}).sortByKey().cache()
      
  with open(output_dir_bi, "w") as file:
        for row in indexRDD.collect():
            word = row[0]
            doc_counts = " ".join([f"{doc}:{count}" for doc, count in row[1].items()])
            file.write(f"{word}\t{doc_counts}\n")