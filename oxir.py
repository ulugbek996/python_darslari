import mysql.connector
import paho.mqtt.client as mqtt
import json
import pytz
import datetime as DT
import pymysql.cursors

client = mqtt.Client()
client.connect('185.196.214.190', 1883)
client.username_pw_set(username="emqx", password="12345")
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             database='mqtt')
cursor = connection.cursor()

def on_connect(client, userdata, flags, rc):
    print("Connected to a broker!")
    client.subscribe("M/+/+/+/info")
    # W/1/TOSHKENT/867857033397873/data


def on_message(client, userdata, message):
    try:
        a = message.payload.decode()
    except:
        print("xato")
    try:
        y = json.loads(a)
    except:
        print("1xato")
    try:
        stid = y["i"]
        if stid:
            viloyat = y["p1"]
            tuman = y["p2"]
            nomi = y["p3"]
            nomer = y["p5"]
            sid = y["p16"]
            proshivka = y["p10"]
            anomer = y["p5"]
            jonatish = y["p14"]
            strisd = y['t']
            batery = y['p8']
            time1 = strisd.split(',')
            tim2 = time1[0].split('/')
            tim3 = time1[1].split(':')
            tim4 = '20' + tim2[0]
            tim5 = int(tim4)
            tim6 = int(tim2[1])
            tim7 = int(tim2[2])
            tim8 = int(tim3[0])
            tim9 = int(tim3[1])
            naive = DT.datetime(tim5, tim6, tim7, tim8, tim9)
            utc = pytz.utc
            gmt5 = pytz.timezone('Etc/GMT-5')
            time10 = utc.localize(naive).astimezone(gmt5)
            time11 = str(time10)
            time12 = time11.split(' ')
            time13 = time12[0].split('-')
            time14 = time12[1].split(':')
            vaqt = str((time13[0] + time13[1] + time13[2] + time14[0] + time14[1]))
            location = y["p6"]
            location1 = location.split(',')
            long = location1[3]
            lotut = location1[4]
            sql = "SELECT * FROM mqtt_info WHERE imei = %s"
            adr = (stid,)
            cursor.execute(sql, adr)
            myresult = cursor.fetchall()
            if myresult:
                print()
            else:
                sql = "INSERT INTO `mqtt_info`( `imei`, `viloyat`, `tuman`, `nomi`, `nomer`, `long`, `lotut`, `sid`, `jonatish`, `batery`, `vaqt`, `anomer`, `proshivka`) VALUES (%s, %s,%s, %s,%s, %s , %s, %s,%s, %s,%s, %s , %s)"
                val = (stid, viloyat, tuman, nomi, nomer, long, lotut, sid, jonatish, batery, vaqt, anomer, proshivka)
                cursor.execute(sql, val)
                connection.commit()
    except:
        print()








while True:
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_connect1 = on_connect
    client.on_message1 = on_message
    client.loop_forever()
