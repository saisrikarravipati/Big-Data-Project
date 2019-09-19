import requests
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.classification import NaiveBayesModel
# import sys


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

    lr_model = LogisticRegressionModel.load("lrm.model")
    model = NaiveBayesModel.load("model.model")




    key = "00254a08-1426-4547-b54f-bc0137d9d547"
    from_date = "2018-02-01"
    to_date = "2018-02-12"

    url = 'http://content.guardianapis.com/search?from-date=' + from_date + '&to-date=' + to_date + \
          '&order-by=newest&show-fields=all&page-size=200&%20num_per_section=10000&api-key=' + key

    data = get_data(url)
    df = sqlContext.createDataFrame(data, schema=["category", "text"])
    pipeline_fit = PipelineModel.load("pipelining")
    dataset = pipeline_fit.transform(df)

    predictions = lr_model.transform(dataset)
    predictions1 = model.transform(dataset)
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    percent = evaluator.evaluate(predictions)
    print("accuracy of lr model is"+str(percent * 100))
    percent = evaluator.evaluate(predictions1)
    print("accuracy of NB model is"+ str(percent * 100))
