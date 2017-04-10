import pickle, sys
from nltk.corpus import stopwords
import warnings


def classify(test, modelToUse):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        mystopwords = stopwords.words('english')
        test = test.strip()
        testList = [w for w in test.split() if w.lower() not in mystopwords]
        test = " ".join(testList)
        testList = [test]
        if "T" == modelToUse:
            with open('../data/model.pkl', 'rb') as f:
                vectorizer, nbc = pickle.load(f, encoding='bytes')
                test_vectors = vectorizer.transform(testList)
                test = test_vectors.toarray()
                prediction = nbc.predict(test)
                return prediction

        elif "B" == modelToUse:
            with open('../data/modelBernie.pkl', 'rb') as f:
                vectorizer, nbc = pickle.load(f, encoding='bytes')
                test_vectors = vectorizer.transform(testList)
                test = test_vectors.toarray()
                prediction = nbc.predict(test)
                return prediction
