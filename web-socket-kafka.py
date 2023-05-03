import json

import websocket
import time

from kafka import KafkaProducer
import kafka.errors

w_f = open('sample_btc_trans_4.txt', 'w')
json_dataset_file = open('BTC_Dataset_April_2.json', 'w', encoding='utf-8')
cnt = 0
dataset = []

while True:
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(3, 2, 3))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)



def on_message(ws, message):
    global cnt
    tx = json.loads(message)
    print("========================", cnt, type(message))

    #####################################################
    # write to text file
    #####################################################
    # w_f.write(json.dumps(tx))
    # w_f.write(",")

    #####################################################
    # write to json file
    #####################################################
    # json.dump(tx, json_dataset_file, ensure_ascii=False, indent=4)
    # json_dataset_file.write(",")
    cnt += 1

    # send BTC trnsaction to KAFKA, KAFKA active
    try:
        producer.send('bitcoin-1', str.encode(message))
    except Exception as e:
        print(str(e), "PRODUCER EXCEPTION !!!!!!!!!!!!!!!!!!!!!!!!%%%%%%%%%%%%%%%%%%%%%%%%%")
        producer.flush()
        ws.close()
def on_open(ws):
    print("Opened connection")
    d = {
        "op": "unconfirmed_sub"
        }
    ws.send(json.dumps(d))




def on_error(ws, error):
    print(error, "WEB SCOKET ERROR")
    # producer.flush()
    # w_f.close()

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")
    # producer.flush()
    # w_f.close()


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
        #Close the producer after use
        producer.flush(timeout=300)
        producer.close()
