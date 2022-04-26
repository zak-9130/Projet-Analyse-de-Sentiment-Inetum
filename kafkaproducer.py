import tweepy
import json
from pykafka import KafkaClient
#import findspark
from time import sleep
bearer_token="AAAAAAAAAAAAAAAAAAAAAIdJaAEAAAAAxr6J9uiTxhiUBNXlJm%2FcuGw2EDA%3DTPBUi5ZZhLBaWu5EiRi4b0J5jikE7OBOTRLKHRCpvd7fXuOy3T"

# Class IDPrinter pour l'écriture des tweets

class IDPrinter(tweepy.StreamingClient):

    def on_data(self, tweet):
        dataJson = tweet.decode('utf-8').replace("'","")#permet d'enlever les bit et retiré les quote inutile

        with open('dataJson.json', 'a', encoding="utf-8") as fd: # ecriture du json
                 fd.write(dataJson)
        dataTweets=json.loads(dataJson)['data']
        dataTweets=json.dumps(dataTweets)
        with open('dataTweets.json', 'a', encoding="utf-8") as fd: # ecriture du json que avec les tweets
                 fd.write(dataTweets)

        client = get_kafka_client()
        topic = client.topics['twitter']
        producer = topic.get_sync_producer()
        producer.produce(dataTweets.encode('utf-8'))
        #sleep(5)
        print(dataTweets)
    def on_errors(self, status):
       print(status)
def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')

stream = IDPrinter(bearer_token)
# Nettoyage des valeurs existantes
rules_ids = []
result=[]
result = stream.get_rules()
for rule in result.data:
    print(f"rule marked to delete :{rule.id} - {rule.value}")
    rules_ids.append(rule.id)
    stream = IDPrinter(bearer_token)

if(len(rules_ids) > 0):
    stream.delete_rules(rules_ids)
else:
    print('No rules to delete')

#filterTweet= tweepy.StreamRule(value="handicap lang:fr")

filterTweet = tweepy.StreamRule(value=" bank lang:en")
stream.add_rules(filterTweet)
stream.filter()