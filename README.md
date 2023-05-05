

## Real Time Analysis of Bitcoin Network Congestion
## Using Spark Structured Streaming

## Group 8
- Tejaskumar Pareshbhai Patel
- Dhruv Ashokkumar Dhorajiya

## Steps to Run the Application 
1.	Make virtual environment and install the dependencies, i.e requirements.txt.
	- pip install -r requirement.txt
2.	Start services:
a.	   Zookeeper 
i.	.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
b.	Kafka
i.	.\bin\windows\kafka-server-start.bat .\config\server.properties

3.	Start capturing the real time bitcoin transactions with web socket and publishing to kafka topic “bitcoin-1”
a.	Python web-socket-kafka.py

4.	Submit the spark streaming application to run in local mode:
a.	Spark-submit spark-stream-btc.py

5.	Start the Flask interface application for spark job to sink the output via REST APIs.
a.	Python app.py

6.	Start the react application, for real time dashboard:
a.	cd amplify-react
b.	npm start


