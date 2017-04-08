import pickle, sys

def classify(test):
    testList = [test]
    with open('../src/model.pkl', 'rb') as f:
        vectorizer, nbc = pickle.load(f,encoding="bytes")
        test_vectors = vectorizer.transform(testList)
        test = test_vectors.toarray()
        prediction = nbc.predict(test)
        return prediction	
    
