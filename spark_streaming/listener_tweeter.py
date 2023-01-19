import socket
import tweepy
import os
from dotenv import load_dotenv

load_dotenv()


HOST = os.environ['HOST']
PORT = int(os.environ['PORT'])

# print(os.environ['PORT'])

s = socket.socket()
s.bind((HOST, PORT))
print(f'aguardando conexao com o client na porta: {PORT}')

s.listen(5)

connection, address = s.accept()
print(f'Recebendo solicitação de {address}')

token = os.environ['TOKEN']
keyword = 'Messi'

class GetTweets(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        print(tweet.text)
        print("="*50)
        connection.send(tweet.text.encode('utf-8', 'ignore'))

printer = GetTweets(token)
print(printer.get_rules())
# printer.delete_rules(['1615490694960877568'])
printer.add_rules(tweepy.StreamRule(keyword))
# connection.close()
# a = 1/0
printer.filter()

# printer.filter(track=[f'{keyword}'])


connection.close()
