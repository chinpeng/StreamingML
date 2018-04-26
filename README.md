# Flink-streamingML
基于Flink实现流式机器学习算法

| 分类    | 算法                                       | 微批增量更新 | 在线增量更新 | 在线概要更新 |
| ----- | ---------------------------------------- | ------ | ------ | ------ |
| 分类    | Naïve Bayes                              |        | √      |        |
| 分类    | Streaming variationalBayes[1]            |        | √      |        |
| 分类    | Logistic Regressionwith FTRL[2]          |        | √      |        |
| 分类    | Very Fast DecisionTree(VFDT)[3]          |        |        | √     |
| 分类    | Streaming LogisticRegression with SGD[4] | √      |        |       |
| 分类    | Perceptron[5]                            |        | √      |       |
| 分类    | One-Pass SVMs[6]                         |        |        | √     |
| 回归    | Linear Regression withFTRL               |        | √      |       |
| 聚类    | StreamKM++[7]                            |        |        | √     |
| 聚类    | CluStream[8]                             |        |        | √     |
| 聚类    | Mini-batch *k*-means[9]                  |        | √      |       |
| 聚类    | Streaming k-means[10]                    | √      |        |       |
| 降维    | Incremental PCA[11]                      |        | √      |       |
| 降维    | Streaming SVD[12]                        |        |        | √     |
| 概率图模型 | Incremental Skip-gram[13]                |        |        | √   |  



 **参考文献：** 

[1]    Tamara Broderick, Nicholas Boyd, Andre Wibisono, Ashia C. Wilson, Michael I. Jordan: Streaming Variational Bayes. NIPS 2013: 1727-1735.  
[2]    H. Brendan McMahan, Gary Holt, David Sculley, et al.: Ad click prediction: a view from the trenches. KDD 2013: 1222-1230.  
[3]    Domingos P, Hulten G. Mining high-speed data streams[C]//Proceedings of the sixth ACM SIGKDD international conference on Knowledge discovery and data mining. ACM, 2000: 71-80.  
[4]    Mu Li, Tong Zhang, Yuqiang Chen, Alexander J. Smola: Efficient mini-batch training for stochastic optimization. KDD 2014: 661-670.  
[5]    Rosenblatt F. The perceptron: a probabilistic model for information storage and organization in the brain. Psychological review, 1958, 65(6): 386.  
[6]    Piyush Rai, Hal Daumé III, Suresh Venkatasubramanian: Streamed Learning: One-Pass SVMs. IJCAI 2009: 1211-1216.  
[7]    Marcel R. Ackermann, Marcus Märtens, Christoph Raupach, et al.: StreamKM++: A clustering algorithm for data streams. ACM Journal of Experimental Algorithmics 17(1) (2012).  
[8]    Charu C. Aggarwal, Jiawei Han, Jianyong Wang, Philip S. Yu: A Framework for Clustering Evolving Data Streams. VLDB 2003: 81-92.  
[9]    Sculley D. Web-scale k-means clustering[C]//Proceedings of the 19th international conference on World wide web. ACM, 2010: 1177-1178.  
[10]    Streaming k-means. https://spark.apache.org/docs/2.2.0/mllib-clustering.html#streaming-k-means.  
[11]    Artac M, Jogan M, Leonardis A. Incremental PCA for on-line visual learning and recognition[C]//Pattern Recognition, 2002. Proceedings. 16th international conference on. IEEE, 2002, 3: 781-784.  
[12]    Huang H, Kasiviswanathan S P. Streaming anomaly detection using randomized matrix sketching[J]. Proceedings of the VLDB Endowment, 2015, 9(3): 192-203.  
[13]    Kaji N, Kobayashi H. Incremental skip-gram model with negative sampling[J]. arXiv preprint arXiv:1704.03956, 2017.  



