#-*- coding:utf-8 -*-
import lda
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
class  iniModel:
    def bulidModel(self,filename,topicwordnum):
        corpus = []
        vocab = []
        for line in open(filename, 'r').readlines():
            print line
            corpus.append(line.strip())
            vocab +=line.split(" ")
            #print corpus
            #将文本中的词语转换为词频矩阵 矩阵元素a[i][j] 表示j词在i类文本下的词频
        vectorizer = CountVectorizer()
        print vocab
        print vectorizer
        X = vectorizer.fit_transform(corpus)
        analyze = vectorizer.build_analyzer()
        weight = X.toarray()
        print len(weight)
        print (weight[:5, :5])

        # n_topics是想得到的topic类型的数量，n_iter是循环的次数，用于自主训练        
        model = lda.LDA(n_topics=9, n_iter=500, random_state=1)
        model.fit(np.asarray(weight))
        topic_word = model.topic_word_
        n_top_words = topicwordnum

        for i, topic_dist in enumerate(topic_word):
            topic_words = np.array(vocab)[np.argsort(topic_dist)][:-(n_top_words+1):-1]
            print('Topic {}: {}'.format(i, ' '.join(topic_words)))
        doc_topic = model.doc_topic_
        print("type(doc_topic): {}".format(type(doc_topic)))
        print("shape: {}".format(doc_topic.shape))
        label = []

        # range中的数字是电影信息的总数，如一共10条信息，则range为10
        for n in range(21):
            topic_most_pr = doc_topic[n].argmax()
            label.append(topic_most_pr)
            print("doc: {} topic: {}".format(n, topic_most_pr))


p = iniModel()

# 第二个参数是topic的数量，如想每条电影信息保留下30条topic，则为30
p.bulidModel("example.txt",30)

