from influxdb import InfluxDBClient
from Documents.Configurations.Topics import Topics
from Documents.Configurations.DataBaseConfigurations import DataBase, DataBaseHOST, DataBasePORT, UserName, Password 
from datetime import datetime
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool
import paho.mqtt.client as mqtt
import time

def main():
	"""
	Nome: main
	Entradas: Sem parâmetros de entrada.
	Função: É a principal função do programa principal. Realiza o manejo de informações
	entre o inversor de frequência, e salva os dados no InfluxDB.
	Objetos:
	Client_ModBus (Objeto responsável pela comunicação ModBus TCP/IP).
	Client_MQTT (Objeto responsável pela comunicação com o Banco de Dados na rede externa, no CPD)
	Variáveis:
		
		Terminal de Potência | Especificação do Terminal |     Tipo de Variável    | Momento de medição | Unidade |              Nome da Variável
			  Grid           |         3Phase            |      DeliveredEnergy    |     LastReset      |   kWh   | Grid_3Phase_DeliveredEnergy_LastReset_kWh
			  Grid           |         3Phase            |        DaylyEnergy      |       Today        |   kWh   | Grid_3Phase_DaylyEnergy_Today_kWh
			  Grid           |         Phase1            |        RMSVoltage       |      Instant       |    V    | Grid_Phase1_RMSVoltage_Instant_V 
			  Grid           |         Phase2            |        RMSVoltage       |      Instant       |    V    | Grid_Phase2_RMSVoltage_Instant_V
			  Grid           |         Phase3            |        RMSVoltage       |      Instant       |    V    | Grid_Phase3_RMSVoltage_Instant_V
			  Grid           |         3Phase            | Delivered_Aparent_Power |      Instant       |    VA   | Grid_3Phase_Delivered_Aparent_Power_Instant_VA
			  Grid           |         3Phase            |    OutputActivePower    |      Instant       |    W    | Grid_3Phase_OutputActivePower_Instant_W
		    PV_Input         |         Total             |       InputCurrent      |      Instant       |    A    | PV_Input_Total_InputCurrent_Instant_A
		      Grid           |         Phase1            |        RMSCurrent       |      Instant       |    A    | Grid_Phase1_RMSCurrent_Instant_A
		      Grid           |         Phase2            |        RMSCurrent       |      Instant       |    A    | Grid_Phase2_RMSCurrent_Instant_A
		      Grid           |         Phase3            |        RMSCurrent       |      Instant       |    A    | Grid_Phase3_RMSCurrent_Instant_A
		      Grid           |         3Phase            |    OutputReactiveower   |     LastReset      |   VAr   | Grid_3Phase_OutputReactivePower_LastReset_VAr
		      Grid           |         3Phase            |    DaylyReactiveEnergy  |       Today        |  kVArh  | Grid_3Phase_DaylyReactiveEnergy_Today_kVArh
		   PVDCInput         |         String1           |       InputCurrent      |      Instant       |    A    | PVDCInput_String1_InputCurrent_Instant_A
		   PVDCInput         |         String2           |       InputCurrent      |      Instant       |    A    | PVDCInput_String2_InputCurrent_Instant_A
		   PVDCInput         |         String3           |       InputCurrent      |      Instant       |    A    | PVDCInput_String3_InputCurrent_Instant_A
		   PVDCInput         |         String4           |       InputCurrent      |      Instant       |    A    | PVDCInput_String4_InputCurrent_Instant_A
		   PVDCInput         |         String5           |       InputCurrent      |      Instant       |    A    | PVDCInput_String5_InputCurrent_Instant_A
		   PVDCInput         |         String6           |       InputCurrent      |      Instant       |    A    | PVDCInput_String6_InputCurrent_Instant_A
		   PVDCInput         |         String7           |       InputCurrent      |      Instant       |    A    | PVDCInput_String7_InputCurrent_Instant_A
		   PVDCInput         |         String8           |       InputCurrent      |      Instant       |    A    | PVDCInput_String8_InputCurrent_Instant_A
		   PVDCInput         |         String9           |       InputCurrent      |      Instant       |    A    | PVDCInput_String9_InputCurrent_Instant_A
		   PVDCInput         |         String10          |       InputCurrent      |      Instant       |    A    | PVDCInput_String10_InputCurrent_Instant_A
		   PVDCInput         |         String11          |       InputCurrent      |      Instant       |    A    | PVDCInput_String11_InputCurrent_Instant_A
		   PVDCInput         |         String12          |       InputCurrent      |      Instant       |    A    | PVDCInput_String12_InputCurrent_Instant_A
		   PVDCInput         |         String13          |       InputCurrent      |      Instant       |    A    | PVDCInput_String13_InputCurrent_Instant_A
		   PVDCInput         |         String14          |       InputCurrent      |      Instant       |    A    | PVDCInput_String14_InputCurrent_Instant_A
		   PVDCInput         |         String15          |       InputCurrent      |      Instant       |    A    | PVDCInput_String15_InputCurrent_Instant_A
		   PVDCInput         |         String16          |       InputCurrent      |      Instant       |    A    | PVDCInput_String16_InputCurrent_Instant_A
           PV_Input          |          Total            |       InputVoltage      |      Instant       |   Vdc   | PV_Input_Total_InputVoltage_Instant_Vdc

	"""
	Client_MQTT = mqtt.Client("CPD")
	Client_MQTT.on_connect = on_connect
	Client_MQTT.on_message = on_message
	try:
		Client_MQTT.connect("mqtt.eclipse.org",1883,60)
		for topic in range(len(Topics)):
			Client_MQTT.subscribe(Topics[topic])
		while True:
			Client_MQTT.loop_start()
			time.sleep(30)
	except:
		print("Broker ou InfluxDB Offline")

def timeout(seconds):
	"""
	Responsável por gerar o timeout das funções
	Entradas:
		seconds: Valor em segundos do Timeout desejado
	"""
	def decorator(function):
		def wrapper(*args, **kwargs):
			pool = ThreadPool(processes=1)
			result = pool.apply_async(function, args=args, kwds=kwargs)
			try:
				return result.get(timeout=seconds)
			except TimeoutError as error:
				return error
		return wrapper
	return decorator

@timeout(5)
def send_data_to_influx_db(Client_InfluxDB,Measurement_Name, Measurement_Value):
	"""
	Recebe o nome da Measurement e o seu valor
	Entradas:
		Measurement_Name: Nome da Medição realizada a ser salva no banco de dados
		Measurement_Value: Valor da Medição realizada a ser salva no banco de dados
	Função: Elaborar um Json com o nome e valor da Measurement, e salvar os dados no InfluxDB
	"""
	Json_Body_Message =[
		{
			"measurement" : Measurement_Name,
			"fields" : {
					"value" : float(Measurement_Value),
			}
		}
	]
	Client_InfluxDB.write_points(Json_Body_Message)
	print("Dados Salvos no Banco de Dados")

def on_connect(client, userdata, rc):
    print("Connected with result code "+str(rc))

#Função on_message, responsável por exibir os valores enviados
def on_message(client, userdata, msg):
	Client_InfluxDB = InfluxDBClient(DataBaseHOST,DataBasePORT,UserName,Password,DataBase)
	send_data_to_influx_db(Client_InfluxDB, msg.topic, msg.payload.decode("utf-8"))
	print(msg.topic+" "+str(msg.payload.decode("utf-8")))



