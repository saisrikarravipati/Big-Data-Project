# Big-Data-Project
Building a classifier over stream of Data


Name of Source files: 

1.Stream_producer.py

2.model.py

3.stream.py





Funtionality of Sorce Files:

Stream Producer - This fetches data from the guardian api and publishes it to the Kafka producer 


model.py- This program is written in python and it consumes the data from the Kafka producer and create's a data-frame with it.

Pipeline model is developed with 3 stages - stop words ,Tokenizer, labeler. 

It is fitted on the training data and saved for later predictions of incoming data also choose logistic regression and naive bayes as my classifiers which are fitted on training data and saved to be loaded later.

stream.py - This program load's the saved pipeline and classifier models and transform's it on the data we collected from Url 
            and creating a data frame. Finally, We calculate the accuracy for the classifiers.


