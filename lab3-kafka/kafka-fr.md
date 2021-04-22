# Kafka
## Qu'est-ce qu'un système de messaging?
Un système de messaging (Messaging System) est responsable du transfert de données d'une application à une autre, de manière à ce que les applications puissent se concentrer sur les données sans s'inquiéter de la manière de les partager ou de les collecter. Le messaging distribué est basé sur le principe de file de message fiable. Les messages sont stockés de manière asynchrone dans des files d'attente entre les applications clientes et le système de messaging.

Deux types de patrons de messaging existent: Les systèmes "point à point" et les systèmes "publish-subscribe".

1. Systèmes de messaging Point à Point
Dans un système point à point, les messages sont stockés dans une file. un ou plusieurs consommateurs peuvent consommer les message dans la file, mais un message ne peut être consommé que par un seul consommateur à la fois. Une fois le consommateur lit le message, ce dernier disparaît de la file.

![Point to point](./resources/point-to-point.gif)

2. Systèmes de messaging Publish/Subscribe¶
Dans un système publish-subscribe, les messages sont stockés dans un "topic". Contrairement à un système point à point, les consommateurs peuvent souscrire à un ou plusieurs topics et consommer tous les messages de ce topic.

![Pub Sub](./resources/pub-sub.gif)

## Présentation de Kafka
Apache Kafka est une plateforme de streaming qui bénéficie de trois fonctionnalités:

1. Elle vous permet de publier et souscrire à un flux d'enregistrements. Elle ressemble ainsi à une file demessage ou un système de messaging d'entreprise.
2. Elle permet de stocker des flux d'enregistrements d'une façon tolérante aux pannes.
3. Elle vous permet de traiter (au besoin) les enregistrements au fur et à mesure qu'ils arrivent.
![Pub Sub](./resources/kafka.png)

Les principaux avantages de Kafka sont:

1. La fiablitié: Kafka est distribué, partitionné, répliqué et tolérent aux fautes.
2. La scalabilité: Kafka se met à l'échelle facilement et sans temps d'arrêt.
3. La durabilité: Kafka utilise un commit log distribué, ce qui permet de stocker les messages sur le disque le plus vite possible.
4. La performance: Kafka a un débit élevé pour la publication et l'abonnement.
## Architecture de Kafka
Pour comprendre le fonctionnement de Kafka, il faut d'abord se familiariser avec le vocabulaire suivant:

1. Topic: Un flux de messages appartenant à une catégorie particulière. Les données sont stockées dans des topics.
2. Partitions: Chaque topic est divisé en partitions. Pour chaque topic, Kafka conserve un minimum d'une partition. Chaque partition contient des messages dans une séquence ordonnée immuable. Une partition est implémentée comme un ensemble de sègments de tailles égales.
3. Offset: Les enregistrements d'une partition ont chacun un identifiant séquentiel appelé offset, qui permet de l'identifier de manière unique dans la partition.
4. Répliques: Les répliques sont des backups d'une partition. Elles ne sont jamais lues ni modifiées par les acteurs externes, elles servent uniquement à prévenir la perte de données.
5. Brokers: Les brokers (ou courtiers) sont de simples systèmes responsables de maintenir les données publiées. Chaque courtier peut avoir zéro ou plusieurs partitions par topic. Si un topic admet N partitions et N courtiers, chaque courtier va avoir une seule partition. Si le nombre de courtiers est plus grand que celui des partitions, certains n'auront aucune partition de ce topic.
6. Cluster: Un système Kafka ayant plus qu'un seul Broker est appelé cluster Kafka. L'ajout de nouveau brokers est fait de manière transparente sans temps d'arrêt.
7. Producers: Les producteurs sont les éditeurs de messages à un ou plusieurs topics Kafka. Ils envoient des données aux courtiers Kafka. Chaque fois qu'un producteur publie un message à un courtier, ce dernier rattache le message au dernier sègment, ajouté ainsi à une partition. Un producteur peut également envoyer un message à une partition particulière.
8. Consumers: Les consommateurs lisent les données à partir des brokers. Ils souscrivent à un ou plusieurs topics, et consomment les messages publiés en extrayant les données à partir des brokers.
9. Leaders: Le leader est le noeud responsable de toutes les lectures et écritures d'une partition donnée. Chaque partition a un serveur jouant le rôle de leader.
10. Follower: C'est un noeud qui suit les instructions du leader. Si le leader tombe en panne, l'un des followers deviendra automatiquement le nouveau leader.
La figure suivante montre un exemple de flux entre les différentes parties d'un système Kafka:

![architecture](./resources/archi.jpg)

Dans cet exemple, un topic est configuré en trois partitions.

En supposant que, si le facteur de réplication du topic est de 3, alors Kafka va créer trois répliques identiques de chaque partition et les placer dans le cluster pour les rendre disponibles pour toutes les opérations. L'identifiant de la réplique est le même que l'identifiant du serveur qui l'héberge. Pour équilibrer la charge dans le cluster, chaque broker stocke une ou plusieurs de ces partitions. Plusieurs producteurs et consommateurs peuvent publier et extraire les messages au même moment.

## Kafka et Zookeeper
Zookeeper est un service centralisé permettant de maintenir l'information de configuration, de nommage, de synchronisation et de services de groupe. Ces services sont utilisés par les applications distribuées en général, et par Kafka en particulier. Pour éviter la complexité et difficulté de leur implémentation manuelle, Zookeeper est utilisé.

![architecture zookeeper kafka](./resources/zookeeper-kafka.png)

Un cluster Kafka consiste typiquement en plusieurs courtiers (Brokers) pour maintenir la répartition de charge. Ces courtiers sont stateless, c'est pour cela qu'ils utilisent Zookeeper pour maintenir l'état du cluster. Un courtier peut gérer des centaines de milliers de lectures et écritures par seconde, et chaque courtier peut gérer des téra-octets de messages sans impact sur la performance.

Zookeeper est utilisé pour gérer et coordonner les courtiers Kafka. Il permet de notifier les producteurs et consommateurs de messages de la présence de tout nouveau courtier, ou de l'échec d'un courtier dans le cluster.

Installation¶
Kafka a été installé sur le même cluster que les deux TP précédents. Suivre les étapes décrites dans la partie Installation du TP1 pour télécharger l'image et exécuter les trois contenaires. Si cela est déjà fait, il suffit de lancer vos machines grâce aux commandes suivantes:

```
  docker start hadoop-master hadoop-slave1 hadoop-slave2
```
puis d'entrer dans le contenaire master:

```
    docker exec -it hadoop-master bash
```
Lancer ensuite les démons yarn et hdfs:
```
  ./start-hadoop.sh
```
Lancer Kafka et Zookeeper en tapant :
```
  ./start-kafka-zookeeper.sh
```

# Première utilisation de Kafka
## Création d'un topic
Pour gérer les topics, Kafka fournit une commande appelée kafka-topics.sh.
Dans un nouveau terminal, taper la commande suivante pour créer un nouveau topic appelé "Hello-Kafka".

```
  kafka-topics.sh --create --zookeeper localhost:2181
                  --replication-factor 1 --partitions 1
                  --topic Hello-Kafka
```

Pour afficher la liste des topics existants, il faudra utiliser:

```
  kafka-topics.sh --list --zookeeper localhost:2181
```
Le résultat devrait être (parmi un grand nombre de lignes d'INFO):

```
  Hello-Kafka
```
## Exemple Producteur Consommateur
Kafka fournit un exemple de producteur standard que vous pouvez directement utiliser. Il suffit de taper:

```
  kafka-console-producer.sh --broker-list localhost:9092 --topic Hello-Kafka
```
Tout ce que vous taperez dorénavant sur la console sera envoyé à Kafka. L'option --broker-list permet de définir la liste des courtiers auxquels vous enverrez le message. Pour l'instant, vous n'en disposez que d'un, et il est déployé à l'adresse localhost:9092.

Pour lancer un consommateur, utiliser:

```
  kafka-console-consumer.sh --zookeeper localhost:2181 —topic Hello-Kafka --from-beginning
```
Le résultat devra ressembler au suivant:

![producer consumers](./resources/prod-cons.png)

## Configuration de plusieurs brokers¶
Dans ce qui précède, nous avons configuré Kafka pour lancer un seul broker. Pour créer plusieurs brokers, il suffit de dupliquer le fichier $KAFKA_HOME/config/server.properties autant de fois que nécessaire. Dans notre cas, nous allons créer deux autre fichiers: server-one.properties et server-two.properties, puis nous modifions les paramètres suivants comme suit:
```
  ### config/server-one.properties
  broker.id = 1
  listeners=PLAINTEXT://localhost:9093
  log.dirs=/tmp/kafka-logs-1

  ### config/server-two.properties
  broker.id = 2
  listeners=PLAINTEXT://localhost:9094
  log.dirs=/tmp/kafka-logs-2
```

Pour démarrer les différents brokers, il suffit d'appeler kafka-server-start.sh avec les nouveaux fichiers de configuration.

```
 kafka-server-start.sh $KAFKA_HOME/config/server.properties &
 kafka-server-start.sh $KAFKA_HOME/config/server-one.properties &
 kafka-server-start.sh $KAFKA_HOME/config/server-two.properties &
```
Lancer jps pour voir les trois serveurs s'exécuter.
