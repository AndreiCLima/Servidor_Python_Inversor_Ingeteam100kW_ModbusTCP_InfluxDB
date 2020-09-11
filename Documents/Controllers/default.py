# Programa Principal, onde será rodado o servidor
# Importações:
from pyModbusTCP.client import ModbusClient
from influxdb import InfluxDBClient
from Documents.Configurations.ModBusHost import HOST
from Documents.Configurations.DataBaseConfigurations import DataBase, DataBaseHOST, DataBasePORT, UserName, Password 
from datetime import datetime
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool
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
	Client_InfluxDB = InfluxDBClient(DataBaseHOST,DataBasePORT,UserName,Password,DataBase)
	Ts = 60
	Initial_Time = time.clock()
	while True:
		print(time.clock())
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
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DeliveredEnergy_LastReset_kWh", Grid_3Phase_DeliveredEnergy_LastReset_kWh)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DaylyEnergy_Today_kWh", Grid_3Phase_DaylyEnergy_Today_kWh)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase1_RMSVoltage_Instant_V", Grid_Phase1_RMSVoltage_Instant_V)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase2_RMSVoltage_Instant_V", Grid_Phase2_RMSVoltage_Instant_V)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase3_RMSVoltage_Instant_V", Grid_Phase3_RMSVoltage_Instant_V)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_Instant_Delivered_Aparent_Power_VA", Grid_3Phase_Instant_Delivered_Aparent_Power_VA)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_OutputActivePower_Instant_W", Grid_3Phase_OutputActivePower_Instant_W)
			send_data_to_influx_db(Client_InfluxDB,"PV_Input_TotalCurrent_A", PV_Input_TotalCurrent_A)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase1_RMSCurrent_Instant_A", Grid_Phase1_RMSCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase2_RMSCurrent_Instant_A", Grid_Phase2_RMSCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase3_RMSCurrent_Instant_A", Grid_Phase3_RMSCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_OutputReactivePower_LastReset_VAr", Grid_3Phase_OutputReactivePower_LastReset_VAr)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DaylyReactiveEnergy_Today_kVArh", Grid_3Phase_DaylyReactiveEnergy_Today_kVArh)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String1_InputCurrent_Instant_A",PVDCInput_String1_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String2_InputCurrent_Instant_A",PVDCInput_String2_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String3_InputCurrent_Instant_A",PVDCInput_String3_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String4_InputCurrent_Instant_A",PVDCInput_String4_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String5_InputCurrent_Instant_A",PVDCInput_String5_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String6_InputCurrent_Instant_A",PVDCInput_String6_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String7_InputCurrent_Instant_A",PVDCInput_String7_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String8_InputCurrent_Instant_A",PVDCInput_String8_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String9_InputCurrent_Instant_A",PVDCInput_String9_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String10_InputCurrent_Instant_A",PVDCInput_String10_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String11_InputCurrent_Instant_A",PVDCInput_String11_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String12_InputCurrent_Instant_A",PVDCInput_String12_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String13_InputCurrent_Instant_A",PVDCInput_String13_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String14_InputCurrent_Instant_A",PVDCInput_String14_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String15_InputCurrent_Instant_A",PVDCInput_String15_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String16_InputCurrent_Instant_A",PVDCInput_String16_InputCurrent_Instant_A)
			send_data_to_influx_db(Client_InfluxDB, "PV_Input_Total_InputVoltage_Vdc", PV_Input_Total_InputVoltage_Vdc)
			Client_ModBus.close()
		except:
			print("Except")
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DeliveredEnergy_LastReset_kWh", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DaylyEnergy_Today_kWh", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase1_RMSVoltage_Instant_V", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase2_RMSVoltage_Instant_V", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase3_RMSVoltage_Instant_V", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_Instant_Delivered_Aparent_Power_VA", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_OutputActivePower_Instant_W", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"PV_Input_TotalCurrent_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase1_RMSCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase2_RMSCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_Phase3_RMSCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_OutputReactivePower_LastReset_VAr", 0.0)
			send_data_to_influx_db(Client_InfluxDB,"Grid_3Phase_DaylyReactiveEnergy_Today_kVArh", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String1_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String2_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String3_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String4_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String5_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String6_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String7_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String8_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String9_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String10_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String11_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String12_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String13_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String14_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String15_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PVDCInput_String16_InputCurrent_Instant_A", 0.0)
			send_data_to_influx_db(Client_InfluxDB, "PV_Input_Total_InputVoltage_Vdc", 0.0)
			Grid_3Phase_DaylyReactiveEnergy_Today_kVArh = 0
		Final_Time = time.clock()
		CPU_Process_Time = Final_Time - Initial_Time
		print("Tempo de Processamento: %f"%(CPU_Process_Time))
		time.sleep(Ts - CPU_Process_Time)
		print(time.clock())
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
