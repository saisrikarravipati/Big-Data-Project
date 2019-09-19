from kafka import KafkaConsumer
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import NaiveBayes
from nltk.corpus import stopwords
import requests


def get_data(u):
    json_data = requests.get(u).json()
    d = []

    for ind in range(len(json_data["response"]['results'])):
        headline = json_data["response"]['results'][ind]['fields']['headline']
        body_text = json_data["response"]['results'][ind]['fields']['bodyText']
        headline += ". "
        headline += body_text
        label = json_data["response"]['results'][ind]['sectionName']
        temp = list()
        temp.append(label)
        temp.append(headline)
        d.append(temp)

    return d


if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    consumer = KafkaConsumer(
        'srikar',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test',
        api_version=(0, 10),
        consumer_timeout_ms=1000,
        value_deserializer=lambda x: x.decode('utf-8'))

    data = []
    for i in consumer:
        tmp = list()
        tmp.append(i.value.split("||")[0])
        tmp.append(i.value.split("||")[1])
        data.append(tmp)

    print(len(data))
    df = sqlContext.createDataFrame(data, schema=["category", "text"])

    # regular expression tokenizer
    regex_tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")

    # stop words
    stop_words = list(set(stopwords.words('english')))

    stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filters").setStopWords(stop_words)


    count_vectors = CountVectorizer(inputCol="filters", outputCol="features", vocabSize=10000, minDF=5)
    label_string_index = StringIndexer(inputCol="category", outputCol="label")
    label_string_index.setHandleInvalid("keep")

    pipeline = Pipeline(stages=[regex_tokenizer, stop_words_remover, count_vectors, label_string_index])
    (training_data, test_data) = df.randomSplit([0.8, 0.2], seed=100)
    pipeline_fit = pipeline.fit(training_data)
    pipeline_fit.write().overwrite().save("pipelining")

    training_datas = pipeline_fit.transform(training_data)




    print("Training: " + str(training_datas.count()))
    print("Test: " + str(test_data.count()))

    lr = LogisticRegression(maxIter=100, regParam=0.2, elasticNetParam=0)
    lrmodel = lr.fit(training_datas)
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
    model = nb.fit(training_datas)

    test_datas = pipeline_fit.transform(test_data)
    predictions = lrmodel.transform(test_datas)
    predictions1 = model.transform(test_datas)


    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    percent = evaluator.evaluate(predictions)
    print("LR accuracy"+str(percent * 100))
    percent = evaluator.evaluate(predictions1)
    print("NB accuracy"+str(percent * 100))

    model.write().overwrite().save("model.model")


    lrmodel.write().overwrite().save("lrm.model")


