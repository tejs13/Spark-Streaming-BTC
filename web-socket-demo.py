import json

import websocket
import time

from kafka import KafkaProducer
import kafka.errors



w_f = open('sample_btc_trans_4.txt', 'w')
cnt = 0

print('start of FILE *******************************')

while True:
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092',api_version=(3, 2, 3))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)



def on_message(ws, message):
    global cnt
    tx = json.loads(message)
    # print(t)
    print("========================", cnt, type(message))
    w_f.write(json.dumps(tx))
    w_f.write(",")
    cnt += 1
    # send_transactions(tx)
    try:
        # pass
        producer.send('bitcoin-1', str.encode(message))
    except Exception as e:
        print(str(e), "PRODUCER EXCEPTION !!!!!!!!!!!!!!!!!!!!!!!!%%%%%%%%%%%%%%%%%%%%%%%%%")
        # producer.flush()
        ws.close()



def on_error(ws, error):
    print(error, "WEB SCOKET ERROR")
    # producer.flush()
    # w_f.close()

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")
    # producer.flush()

    # w_f.close()

def on_open(ws):
    print("Opened connection")
    d = {
        "op": "unconfirmed_sub"
        }
    ws.send(json.dumps(d))


if __name__ == "__main__":



    websocket.enableTrace(True)


    ws = websocket.WebSocketApp("wss://ws.blockchain.info/inv",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    try:
        # Start the WebSocket connection
        ws.run_forever()
    finally:
        pass
        # Close the producer after use
        # producer.flush(timeout=300)
        # producer.close()
