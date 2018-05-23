
#-*- coding:utf-8 -*-

import codecs
from gensim.models import LdaModel
from gensim import corpora
from gensim.corpora import Dictionary
from gensim.models import LdaModel
from gensim.models import word2vec

# stopwords = codecs.open('stopwords.txt','r',encoding='utf8').readlines()
# stopwords = [ w.strip() for w in stopwords ]
#  
# train = []
# fp = codecs.open('doubanComment.txt','r',encoding='utf8')
# for line in fp:
#     line = line.split()
#     train.append([ w for w in line if w not in stopwords ])
# dictionary = corpora.Dictionary(train)
# corpus = [ dictionary.doc2bow(text) for text in train ]
# lda = LdaModel(corpus=corpus, id2word=dictionary, num_topics=9,
#                 alpha='auto', eval_every=30)
# fp.close()
#  
# for i in range(0, lda.num_topics):
#     print "Topic"+str(i)+" :"+lda.print_topic(i)
#   
# fp = codecs.open('doubanComment.txt','r',encoding='utf8')
#  
# for line in fp:
#     print line
#     newTrain = [];
#     line = line.split()
#     newTrain.append([ w for w in line if w not in stopwords ])
#     newCorpus = [ dictionary.doc2bow(text) for text in newTrain ]
#     t = lda.get_document_topics(newCorpus, minimum_probability=None, 
#                         minimum_phi_value=None, per_word_topics=True)
#     print t[0]
# fp.close()

nID = raw_input("")
if len(nID) != len("13222319810101****"):
    print 'wring length of id,input again'
else:
    print "lala"