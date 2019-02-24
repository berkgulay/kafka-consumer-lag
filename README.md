# KafkaConsumerLagReceiver
Scheduled Receiver Service for Monitoring Lag of Kafka Consumers with JAVA
![](https://user-images.githubusercontent.com/20780894/53306658-f3ef9b80-38a0-11e9-8e20-b4d77e929cdd.JPG)


![](https://user-images.githubusercontent.com/20780894/53306688-5c3e7d00-38a1-11e9-849c-de4e58ca1535.png)

**_Description:_** Scheduled service as Java application to monitor lag state of each consumer through bootstrap server of Kafka. Gets all consumer groups in server, finds all consumers of each consumer group and receives their lag state with other required informations by sending paralel Api requests. _(GROK pattern was provided in "**grok_pattern.txt**", before monitoring via ELK to parse log message which contains lag state and other required informations like topic or consumer details in Logstash)_

**_Extras:_** Received lag state can be regularly monitored using ELK. Logstash "Grok" plugin can be utilized to parse received logs in Logstash. Just "Logger" configuration is needed in order to send printed logs directly into Logstash!

![](https://user-images.githubusercontent.com/20780894/53306672-2f8a6580-38a1-11e9-9794-d4c7b9d23192.jpg)


**Contact with me:** 
- [Berk Gulay](https://www.linkedin.com/in/berk-gulay97/)
