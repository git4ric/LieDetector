from nltk.corpus import stopwords
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import GaussianNB
import pickle
import os

contradictingTweets = []
stopwords = stopwords.words('english')
allTweets = []
train = []

def populateGeneralStatements():
    data = open("alltweets.txt")
    for line in data:
        # line = unicode(line, "utf-8")
        content = [w for w in line.split() if w.lower() not in stopwords]
        content = " ".join(content).strip().replace(","," ")
        allTweets.append(content + "," + "no")

def populateContradictions():
    data = open("Contradiction.txt")
    for line in data:
        ht = {}
        # line = unicode(line, "utf-8")
        content = [w for w in line.split() if w.lower() not in stopwords]
        content = " ".join(content).strip().replace(","," ")
        contradictingTweets.append(content + "," + "yes")

def writePositive():
    res = open('positive.txt', 'w')
    for a in contradictingTweets:
        res.write(a + '\n')
    res.close()

def writeNegative():
    res = open('negative.txt', 'w')
    for a in allTweets:
        res.write(a + '\n')
    res.close()

def generateTrain():
    res = open('train.txt', 'w')
    train = allTweets
    train.extend(contradictingTweets)
    for a in train:
        res.write(str(a) + '\n')
    res.close()

def trainClassifier():
    data = pd.read_csv('train.txt', header=0,encoding="latin1")
    data.head()

    # changed column names
    data.columns = ['data', 'label']
    text = data['data']
    label = data['label']

    tfidf_vectorizer = CountVectorizer(min_df=2)

    # Step you were missing : vectorization
    train_vectors = tfidf_vectorizer.fit_transform(text)
    trainingData = train_vectors.toarray()

    # gaussian naive bayes
    nbc = GaussianNB()
    nbc.fit(trainingData, label)

    # test = [""]
    # test_vectors = tfidf_vectorizer.transform(test)
    # test = test_vectors.toarray()
    #
    # prediction = nbc.predict(test)
    # print(prediction)

    with open('model.pkl', 'wb') as f:
        pickle.dump((tfidf_vectorizer, nbc), f)

if __name__== '__main__':
    print(os.getcwd())
    os.chdir(os.getcwd() + "/../data")
    populateContradictions()
    writePositive()

    populateGeneralStatements()
    writeNegative()

    generateTrain()
    trainClassifier()


