# KafkaConsumerLagReceiver
Scheduled Receiver Service for Monitoring Lag of Kafka Consumers

![](https://cdn1.imggmi.com/uploads/2019/2/25/c76118c1d6a82413527a9d444d5d80f2-full.jpg)


**_Description:_** Scheduled service in Java to monitor lag state of each consumer through bootstrap server of Kafka. Gets all consumer groups in server, finds all consumers of each consumer group and receives their lag state with other required informations by sending paralel Api requests. (GROK pattern was provided in "_grok_pattern.txt_", before monitoring via ELK to parse log message which contains lag state and other required informations like topic or consumer details in Logstash)

**_Extras:_** Received lag state can be regularly monitored using ELK. Logstash "Grok" plugin can be utilized to parse received logs in Logstash. Just "Logger" configuration is needed in order to send printed logs directly into Logstash!

**Contact with me:** 
- [Berk Gulay](https://www.linkedin.com/in/berk-gulay97/)
