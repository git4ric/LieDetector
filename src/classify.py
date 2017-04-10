import pickle, sys
from nltk.corpus import stopwords

stopwords = stopwords.words('english')

def classify(test, modelToUse):
    test = test.strip()
    testList = [w for w in test.split() if w.lower() not in stopwords]
    test = " ".join(testList)
    testList = [test]
    if "T" == modelToUse:
        with open('../data/model.pkl', 'rb') as f:
            vectorizer, nbc = pickle.load(f,encoding='bytes')
            test_vectors = vectorizer.transform(testList)
            test = test_vectors.toarray()
            prediction = nbc.predict(test)
            return prediction


    elif "B" == modelToUse:
        with open('../data/modelBernie.pkl', 'rb') as f:
            vectorizer, nbc = pickle.load(f,encoding='bytes')
            test_vectors = vectorizer.transform(testList)
            test = test_vectors.toarray()
            prediction = nbc.predict(test)
            return prediction
	
    
