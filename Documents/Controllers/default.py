# Programa Principal, onde será rodado o servidor
# Importações:
from pyModbusTCP.client import ModbusClient
from influxdb import InfluxDBClient
from Documents.Configurations.ModBusHost import HOST
from Documents.Configurations.DataBaseConfigurations import DataBase, DataBaseHOST, DataBasePORT, UserName, Password 
from datetime import datetime
import time


def main():
	"""
	Nome: main

	Entradas: Sem parâmetros de entrada.

	Função: É a principal função do programa principal. Realiza o manejo de informações
	entre o inversor de frequência, e salva os dados no InfluxDB.

	Objetos:
	Client_ModBus (Objeto responsável pela comunicação ModBus TCP/IP).
	Client_InfluxDB (Objeto responsável pela comunicação com o Banco de Dados)

	Variáveis:
	Inverter_Register_Addres: Endereço do registrador requisitado ao inversor
	Inverter_Register_Length: Dimensão do Registrador a ser lido, considerando-se registradore de 16 bits.
	Control_Flag: flag de controle, responsável por verificar qual endereço do registrador está sendo acessado no momento,
		e se esse endereço está sendo lido de maneira correta

	"""
	Inverter_Registers_Address = [6,7,12,13,24,25,26,28,29,32,21,22,23,30,38,33] # Endereço requisitado ao inversor
	Inverter_Registers_Length =  [2,2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1] # Dimensão do registrador
	Control_Flag = False
	Grid_3Phase_DaylyEnergy_Today_kVArh = 0
	Client_ModBus = ModbusClient(host = HOST, port = 502, auto_open = True, auto_close = True)
	Client_InfluxDB = InfluxDBClient(DataBaseHOST,DataBasePORT,'root','root',DataBase)
	Ts = 60
	while True:
		Initial_Time = time.clock()
		Input_Registers = []
		#Client_ModBus.debug(True) 
		try:
			Client_ModBus.open()
			for Address in range(len(Inverter_Registers_Address)):
				if Inverter_Registers_Length[Address] == 2 and not(Control_Flag):
					Input_Registers.append(Client_ModBus.read_input_registers(Inverter_Registers_Address[Address],Inverter_Registers_Length[Address]))
					Control_Flag = True
				elif Inverter_Registers_Length[Address] == 2 and Control_Flag: 
					Control_Flag = False
				else:
					Input_Registers.append(Client_ModBus.read_input_registers(Inverter_Registers_Address[Address],Inverter_Registers_Length[Address]))
			Grid_3Phase_DeliveredEnergy_LastReset_kWh = convert_input_register_value_to_real_value(converter_parameters_uint_32(Input_Registers[0][0],Input_Registers[0][1]), Scale_Factor = 0.01)
			Grid_3Phase_DaylyEnergy_Today_kWh = convert_input_register_value_to_real_value(converter_parameters_uint_32(Input_Registers[1][0], Input_Registers[1][1]), Scale_Factor = 0.01)
			Grid_Phase1_RMSVoltage_Instant_V = convert_input_register_value_to_real_value(Input_Registers[2][0], Scale_Factor = 0.1)
			Grid_Phase2_RMSVoltage_Instant_V = convert_input_register_value_to_real_value(Input_Registers[3][0], Scale_Factor = 0.1)
			Grid_Phase3_RMSVoltage_Instant_V = convert_input_register_value_to_real_value(Input_Registers[4][0], Scale_Factor = 0.1)
			Grid_3Phase_Instant_Delivered_Aparent_Power_VA = convert_input_register_value_to_real_value(Input_Registers[5][0], Scale_Factor = 10)
			Grid_3Phase_Instant_Delivered_Real_Power_W = convert_input_register_value_to_real_value(Input_Registers[6][0], Scale_Factor = 10)
			PV_Input_TotalCurrent_A = convert_input_register_value_to_real_value(Input_Registers[7][0], Scale_Factor = 0.01)
			Grid_Phase1_RMSCurrent_Instant_A = convert_input_register_value_to_real_value(Input_Registers[8][0], Scale_Factor = 0.01)
			Grid_Phase2_RMSCurrent_Instant_A = convert_input_register_value_to_real_value(Input_Registers[9][0], Scale_Factor = 0.01)
			Grid_Phase3_RMSCurrent_Instant_A = convert_input_register_value_to_real_value(Input_Registers[10][0], Scale_Factor = 0.01)
			Grid_3Phase_Instant_Delivered_Reative_Power_VAr = convert_input_register_value_to_real_value(Input_Registers[11][0], Scale_Factor = 10)
			Grid_3Phase_DaylyEnergy_Today_kVArh = grid_3Phase_dayly_energy_today_kVArh(Grid_3Phase_DaylyEnergy_Today_kVArh,Grid_3Phase_Instant_Delivered_Reative_Power_VAr, Ts)
			print("Valor INPUT PV 1 e 2: ")
			print(Input_Registers[12])
			PV_Input_TotalVoltage_Vdc = convert_input_register_value_to_real_value(Input_Registers[13][0], Scale_Factor =1)
			#print(Grid_3Phase_DeliveredEnergy_LastReset_kWh)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DeliveredEnergy_LastReset_kWh", Grid_3Phase_DeliveredEnergy_LastReset_kWh)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DaylyEnergy_Today_kWh", Grid_3Phase_DaylyEnergy_Today_kWh)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase1_RMSVoltage_Instant_V", Grid_Phase1_RMSVoltage_Instant_V)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase2_RMSVoltage_Instant_V", Grid_Phase2_RMSVoltage_Instant_V)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase3_RMSVoltage_Instant_V", Grid_Phase3_RMSVoltage_Instant_V)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_Instant_Delivered_Aparent_Power_VA", Grid_3Phase_Instant_Delivered_Aparent_Power_VA)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_Instant_Delivered_Real_Power_W", Grid_3Phase_Instant_Delivered_Real_Power_W)
			send_data_to_influx_db(Client_InfluxDB,"PV_Input_TotalCurrent_A", PV_Input_TotalCurrent_A)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase1_RMSCurrent_Instant_A", Grid_Phase1_RMSCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase2_RMSCurrent_Instant_A", Grid_Phase2_RMSCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase3_RMSCurrent_Instant_A", Grid_Phase3_RMSCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_Instant_Delivered_Reative_Power_VAr", Grid_3Phase_Instant_Delivered_Reative_Power_VAr)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DaylyEnergy_Today_kVArh", Grid_3Phase_DaylyEnergy_Today_kVArh)
		except:
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DeliveredEnergy_LastReset_kWh", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DaylyEnergy_Today_kWh", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase1_RMSVoltage_Instant_V", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase2_RMSVoltage_Instant_V", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase3_RMSVoltage_Instant_V", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_Instant_Delivered_Aparent_Power_VA", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_Instant_Delivered_Real_Power_W", 0)
			send_data_to_influx_db(Client_InfluxDB,"PV_Input_TotalCurrent_A", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase1_RMSCurrent_Instant_A", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase2_RMSCurrent_Instant_A", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase3_RMSCurrent_Instant_A", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_Instant_Delivered_Reative_Power_VAr", 0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DaylyEnergy_Today_kVArh", 0)
			Grid_3Phase_DaylyEnergy_Today_kVArh = 0

		Client_ModBus.close()
		Final_Time = time.clock()
		CPU_Process_Time = Final_Time - Initial_Time
		print("Tempo de processamento: %f"%CPU_Process_Time)
		time.sleep(Ts - CPU_Process_Time)
		
def converter_parameters_uint_32(Uint_32_Input_Registers_Most_Significant_Bits, Uint_32_Input_Registers_Less_Significant_Bits):
    """
    Recebe os parâmetro lidos do registrador do inversor
	
	Estradas: 

		Uint_32_Input_Registers_Most_Significant_Bits: Conjunto de Bits mais significativos da variável (de 32 a 16)
		Uint_32_Input_Registers_Less_Significant_Bits: Conjunto de Bits menos significativos da variável (de 15 a 0)
	
	Função: Converter o valor dos registradores do inversor em valores do tipo inteiro

    """
    return Uint_32_Input_Registers_Most_Significant_Bits * 65536 + Uint_32_Input_Registers_Less_Significant_Bits


def convert_input_register_value_to_real_value(Input_Register_Value, Scale_Factor):
	"""
	Recebe o parâmetro lido pelo inversor, já convertido de uint32 (se necessário)

	Entradas:
		Input_Register_Value: Valor enviado pelo inversor, sem conversão para valores reais
		Scale_Factor: Fator de escala necessário para converter o número em um número com valor real (Tensão, Corrente, Potência, Energia, etc)

	Função: Dado um número do tipo inteiro, e um fator de escala, essa função realiza a conversão deste número para valores de grandezas reais, como tensão em Volts, Corrente em Amperes, etc.
	"""
	return Input_Register_Value*Scale_Factor

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
					"value" : Measurement_Value,
			}
		}
	]
	try:
		Client_InfluxDB.write_points(Json_Body_Message)
	except:
		print("Erro ao enviar os dados ao banco de dados")

def grid_3Phase_dayly_energy_today_kVArh(Grid_3Phase_DaylyEnergy_Today_kVArh,Grid_3Phase_Instant_Delivered_Reative_Power_VAr, Ts):
        '''Essa função calcula a energia reativa em KVArh durante um dia, utilizando Ts = 60 segundos'''
        hora_agora = datetime.now()                     # Horário atual
        hora       = hora_agora.hour
        minuto     = hora_agora.minute
        
        if hora == 0 and minuto == 0:
            Grid_3Phase_DaylyEnergy_Today_kVArh = 0
        return Grid_3Phase_DaylyEnergy_Today_kVArh + Ts * Grid_3Phase_Instant_Delivered_Reative_Power_VAr/3.6e6
