# Programa Principal, onde será rodado o servidor
# Importações:
from pyModbusTCP.client import ModbusClient
from Documents.Configurations.ModBusHost import HOST
from Documents.Configurations.Topics import Topics 
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
	Inverter_Registers_Address = [6,7,12,13,24,25,26,28,29,32,21,22,23,30,38,39,40,41,42,43,44,45,33] # Endereço requisitado ao inversor
	Inverter_Registers_Length =  [2,2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1] # Dimensão do registrador
	Control_Flag = False
	Grid_3Phase_DaylyReactiveEnergy_Today_kVArh = 0
	Ts = 60
	Initial_Time = time.time()
	Client_MQTT = mqtt.Client("IngeTeam 100kW")
	Client_MQTT.on_connect = on_connect
	Client_MQTT.on_message = on_message
	Client_MQTT.connect("mqtt.eclipse.org",1883,60)
	while True:
		print(time.time())
		Client_MQTT.loop_start()
		Input_Registers = []
		#Client_ModBus.debug(True) 
		try:
			Client_ModBus = ModbusClient(host = HOST, port = 502, auto_open = True, auto_close = True)
			Client_ModBus.open()
			for Address in range(len(Inverter_Registers_Address)):
				if Inverter_Registers_Length[Address] == 2 and not(Control_Flag):
					Input_Registers.append(Client_ModBus.read_input_registers(Inverter_Registers_Address[Address],Inverter_Registers_Length[Address]))
					Control_Flag = True
				elif Inverter_Registers_Length[Address] == 2 and Control_Flag: 
					Control_Flag = False
				else:
					Input_Registers.append(Client_ModBus.read_input_registers(Inverter_Registers_Address[Address],Inverter_Registers_Length[Address]))
			Grid_3Phase_DeliveredEnergy_LastReset_kWh = convert_input_register_value_to_real_value(convert_parameters_uint_32(Input_Registers[0][0],Input_Registers[0][1]), Scale_Factor = 0.01)
			Grid_3Phase_DaylyEnergy_Today_kWh = convert_input_register_value_to_real_value(convert_parameters_uint_32(Input_Registers[1][0], Input_Registers[1][1]), Scale_Factor = 0.01)
			Grid_Phase1_RMSVoltage_Instant_V = convert_input_register_value_to_real_value(Input_Registers[2][0], Scale_Factor = 0.1)
			Grid_Phase2_RMSVoltage_Instant_V = convert_input_register_value_to_real_value(Input_Registers[3][0], Scale_Factor = 0.1)
			Grid_Phase3_RMSVoltage_Instant_V = convert_input_register_value_to_real_value(Input_Registers[4][0], Scale_Factor = 0.1)
			Grid_3Phase_Instant_Delivered_Aparent_Power_VA = convert_input_register_value_to_real_value(Input_Registers[5][0], Scale_Factor = 10)
			Grid_3Phase_OutputActivePower_Instant_W = convert_input_register_value_to_real_value(Input_Registers[6][0], Scale_Factor = 10)
			PV_Input_TotalCurrent_A = convert_input_register_value_to_real_value(Input_Registers[7][0], Scale_Factor = 0.01)
			Grid_Phase1_RMSCurrent_Instant_A = convert_input_register_value_to_real_value(Input_Registers[8][0], Scale_Factor = 0.01)
			Grid_Phase2_RMSCurrent_Instant_A = convert_input_register_value_to_real_value(Input_Registers[9][0], Scale_Factor = 0.01)
			Grid_Phase3_RMSCurrent_Instant_A = convert_input_register_value_to_real_value(Input_Registers[10][0], Scale_Factor = 0.01)
			Grid_3Phase_OutputReactivePower_LastReset_VAr = convert_input_register_value_to_real_value(Input_Registers[11][0], Scale_Factor = 10)
			Grid_3Phase_DaylyReactiveEnergy_Today_kVArh = grid_3Phase_dayly_energy_today_kVArh(Grid_3Phase_DaylyReactiveEnergy_Today_kVArh,Grid_3Phase_OutputReactivePower_LastReset_VAr, Ts)
			PVDCInput_String1_InputCurrent_Instant_A,  PVDCInput_String2_InputCurrent_Instant_A  = convert_parameters_uint_16_to_8bits_8bits(Input_Registers[12][0])		
			PVDCInput_String3_InputCurrent_Instant_A,  PVDCInput_String4_InputCurrent_Instant_A  = convert_parameters_uint_16_to_8bits_8bits(Input_Registers[13][0])
			PVDCInput_String5_InputCurrent_Instant_A,  PVDCInput_String6_InputCurrent_Instant_A  = convert_parameters_uint_16_to_8bits_8bits(Input_Registers[14][0])
			PVDCInput_String7_InputCurrent_Instant_A,  PVDCInput_String8_InputCurrent_Instant_A  = convert_parameters_uint_16_to_8bits_8bits(Input_Registers[15][0])
			PVDCInput_String9_InputCurrent_Instant_A,  PVDCInput_String10_InputCurrent_Instant_A = convert_parameters_uint_16_to_8bits_8bits(Input_Registers[16][0])
			PVDCInput_String11_InputCurrent_Instant_A, PVDCInput_String12_InputCurrent_Instant_A = convert_parameters_uint_16_to_8bits_8bits(Input_Registers[17][0])
			PVDCInput_String13_InputCurrent_Instant_A, PVDCInput_String14_InputCurrent_Instant_A = convert_parameters_uint_16_to_8bits_8bits(Input_Registers[18][0])
			PVDCInput_String15_InputCurrent_Instant_A, PVDCInput_String16_InputCurrent_Instant_A = convert_parameters_uint_16_to_8bits_8bits(Input_Registers[19][0])
			PV_Input_Total_InputVoltage_Vdc = convert_input_register_value_to_real_value(Input_Registers[20][0], Scale_Factor =1)
			mqtt_publish(Client_MQTT, Topics[0], Grid_3Phase_DeliveredEnergy_LastReset_kWh)
			mqtt_publish(Client_MQTT, Topics[1], Grid_3Phase_DaylyEnergy_Today_kWh)
			mqtt_publish(Client_MQTT, Topics[2], Grid_Phase1_RMSVoltage_Instant_V)
			mqtt_publish(Client_MQTT, Topics[3], Grid_Phase2_RMSVoltage_Instant_V)
			mqtt_publish(Client_MQTT, Topics[4], Grid_Phase3_RMSVoltage_Instant_V)
			mqtt_publish(Client_MQTT, Topics[5], Grid_3Phase_Instant_Delivered_Aparent_Power_VA)
			mqtt_publish(Client_MQTT, Topics[6], Grid_3Phase_OutputActivePower_Instant_W)
			mqtt_publish(Client_MQTT, Topics[7], PV_Input_TotalCurrent_A)
			mqtt_publish(Client_MQTT, Topics[8], Grid_Phase1_RMSCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[9], Grid_Phase2_RMSCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[10], Grid_Phase3_RMSCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[11], Grid_3Phase_OutputReactivePower_LastReset_VAr)
			mqtt_publish(Client_MQTT, Topics[12], Grid_3Phase_DaylyReactiveEnergy_Today_kVArh)
			mqtt_publish(Client_MQTT, Topics[13], PVDCInput_String1_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[14], PVDCInput_String2_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[15], PVDCInput_String3_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[16], PVDCInput_String4_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[17], PVDCInput_String5_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[18], PVDCInput_String6_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[19], PVDCInput_String7_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[20], PVDCInput_String8_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[21], PVDCInput_String9_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[22], PVDCInput_String10_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[23], PVDCInput_String11_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[24], PVDCInput_String12_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[25], PVDCInput_String13_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[26], PVDCInput_String14_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[27], PVDCInput_String15_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[28], PVDCInput_String16_InputCurrent_Instant_A)
			mqtt_publish(Client_MQTT, Topics[29], PV_Input_Total_InputVoltage_Vdc)
			Client_ModBus.close()
		except:
			print("Except")
			for topic in range(len(Topics)):
				mqtt_publish(Client_MQTT, Topics[topic], 0)
			Grid_3Phase_DaylyReactiveEnergy_Today_kVArh = 0
		Final_Time = time.time()
		CPU_Process_Time = Final_Time - Initial_Time
		print("Tempo de Processamento: %f"%(CPU_Process_Time))
		time.sleep(Ts - CPU_Process_Time)
		print(time.time())
		Initial_Time = Initial_Time+60

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

		
def convert_parameters_uint_32(Uint_32_Input_Registers_Most_Significant_Bits, Uint_32_Input_Registers_Less_Significant_Bits):
    """
    Recebe os parâmetro lidos do registrador do inversor
	
	Estradas: 
		Uint_32_Input_Registers_Most_Significant_Bits: Conjunto de Bits mais significativos da variável (de 32 a 16)
		Uint_32_Input_Registers_Less_Significant_Bits: Conjunto de Bits menos significativos da variável (de 15 a 0)
	
	Função: Converter o valor dos registradores do inversor em valores do tipo inteiro
    """
    return Uint_32_Input_Registers_Most_Significant_Bits * 65536 + Uint_32_Input_Registers_Less_Significant_Bits

def convert_parameters_uint_16_to_8bits_8bits(Uint_16_Input_Registers):
        '''Essa função separa os dados de corrente das String. O registrador armazena os dados de corrente de duas Strings em um único registrador Uint16.
        Essa função converte o valor inteiro retornado pelo protocolo em valor binário, sepera em dois grupos de 8 bits, converte novamente para inteiro e devolve a função principal'''
        Convert_Decimal_To_Binary = bin(Uint_16_Input_Registers) # Converte um valor em decimal para binário
        if(len(Convert_Decimal_To_Binary) > 10):
        	Length_Date                = len(Convert_Decimal_To_Binary) - 8                                
        	First_String            = int(Convert_Decimal_To_Binary[2:Length_Date],2) # Converte para inteiro
        	Second_String             = int(Convert_Decimal_To_Binary[Length_Date:len(Convert_Decimal_To_Binary)],2)
        else:
        	First_String = 0
        	Second_String = int(Convert_Decimal_To_Binary,2)
        return convert_input_register_value_to_real_value(First_String, 0.1), convert_input_register_value_to_real_value(Second_String, 0.1)  
        

def convert_input_register_value_to_real_value(Input_Register_Value, Scale_Factor):
	"""
	Recebe o parâmetro lido pelo inversor, já convertido de uint32 (se necessário)
	Entradas:
		Input_Register_Value: Valor enviado pelo inversor, sem conversão para valores reais
		Scale_Factor: Fator de escala necessário para converter o número em um número com valor real (Tensão, Corrente, Potência, Energia, etc)
	Função: Dado um número do tipo inteiro, e um fator de escala, essa função realiza a conversão deste número para valores de grandezas reais, como tensão em Volts, Corrente em Amperes, etc.
	"""
	return Input_Register_Value*Scale_Factor

def grid_3Phase_dayly_energy_today_kVArh(Grid_3Phase_DaylyReactiveEnergy_Today_kVArh,Grid_3Phase_OutputReactivePower_LastReset_VAr, Ts):
        """
        Calcula a energia reativa em KVArh durante um dia, utilizando Ts = 60 segundos
        Entradas: 
        	Grid_3Phase_DaylyReactiveEnergy_Today_kVArh: Valor da Energia reativa relativo à iteração anterior
        	Grid_3Phase_OutputReactivePower_LastReset_VAr: Valor da Potência Reativa Instantânea
        	Ts: Período de amostragem do Sinal
        Função: Retornar ao programa principal o valor da energia da rede, em KVArh
        """
        Real_Time = datetime.now()                     # Horário atual
        Hour       = Real_Time.hour
        Minute     = Real_Time.minute
        
        if Hour == 0 and Minute == 0:
            Grid_3Phase_DaylyReactiveEnergy_Today_kVArh = 0
        return Grid_3Phase_DaylyReactiveEnergy_Today_kVArh + Ts * Grid_3Phase_OutputReactivePower_LastReset_VAr/3.6e6

#Função responsável por realizar a conexão com o broker
def on_connect(Client_MQTT, userdata, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("Supervisorio/Inversor1/Corrente")

#Função on_message, responsável por exibir os valores enviados
def on_message(Client_MQTT, userdata, msg):
    print(msg.topic+" "+str(msg.payload.decode("utf-8")))

@timeout(2)
def mqtt_publish(Client_MQTT, Topic, Value):
	Client_MQTT.publish(Topic, Value)