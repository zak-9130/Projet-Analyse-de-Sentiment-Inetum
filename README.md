<H1>Analyse de sentiments sur des données Twitter en continu à l'aide de Spark Structured Streaming et de Python. JELTI Zakaria 04/2022 </H1>

Ce projet est un bon point de départ pour ceux qui ont peu ou pas d'expérience avec <b>Apache Spark Streaming</b>.</b>. Nous utilisons les données Twitter car Twitter fournit aux développeurs une API facile d'accès..
Nous présentons une architecture de bout en bout sur la façon de diffuser les données de Twitter, de les nettoyer et d'appliquer un modèle simple d'analyse des sentiments pour détecter la polarité et la subjectivité de chaque tweet..

<b> Input data:</b> Live tweets with a keyword <br>
<b>Main model:</b> Data preprocessing and apply sentiment analysis on the tweets <br>


<img align="center"  width="50%" src="https://github.com/zak-9130/Projet-Analyse-de-Sentiment-Inetum.git/blob/master/architec_twitter.png">


J'utilise Python version 3.9.1 et Spark version 3.2.1. Nous devons être prudents sur les versions que nous utilisons car les différentes versions de Spark nécessitent une version différente de Python. 

## Main Libraries
<b> tweepy:</b> interagir avec l'API Twitter Streaming et créer un pipeline de streaming de données en direct avec Twitter <br>
<b> pyspark: </b>prétraiter les données twitter (bibliothèque Spark de Python) <br>
<b> textblob:</b> appliquer l'analyse des sentiments sur les données textuelles de Twitter <br>
<b> textblob:</b> envoi des donnée à Elasticsearch et visualisation avec Kibana <br>
## Instructions
First, run the <b>Part 1:</b> <i>Kafka_producer.py</i> and let it continue running. <br>
Then, run the <b>Part 2:</b> <i>consumer_pyspark</I> from a different IDE. 

