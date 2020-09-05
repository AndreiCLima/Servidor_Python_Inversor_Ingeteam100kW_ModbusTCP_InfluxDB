# Programa Principal, onde será rodado o servidor
# Importações:
from pyModbusTCP.client import ModbusClient
from Documents.Configurations.ModBusHost import HOST 
import time

def main():
	"""
	Nome: main

	Entradas: Sem parâmetros de entrada.

	Função: É a principal função do programa principal. Realiza o manejo de informações
	entre o inversor de frequência, e salva os dados no InfluxDB.

	Objetos:
	Client_ModBus (Objeto responsável pela comunicação ModBus TCP/IP).

	Variáveis:
	Inverter_Register_Addres: Endereço do registrador requisitado ao inversor
	Inverter_Register_Length: Dimensão do Registrador a ser lido, considerando-se registradore de 16 bits.
	Control_Flag: flag de controle, responsável por verificar qual endereço do registrador está sendo acessado no momento,
		e se esse endereço está sendo lido de maneira correta

	"""
	Inverter_Registers_Address = [6,7,12,13,24,25,26,28,29,32] # Endereço requisitado ao inversor
	Inverter_Registers_Length = [2,2,2,2,1,1,1,1,1,1] # Dimensão do registrador
	Input_Registers = []
	Control_Flag = False
	Client_ModBus = ModbusClient(host = HOST, port = 502, auto_open = True, auto_close = True)
	while True:
		Client_ModBus.open()
		#Client_ModBus.debug(True)
		for Address in range(len(Inverter_Registers_Address)):
			if Inverter_Registers_Length[Address] == 2 and not(Control_Flag):
				Input_Registers.append(Client_ModBus.read_input_registers(Inverter_Registers_Address[Address],Inverter_Registers_Length[Address]))
				Control_Flag = True
			elif Inverter_Registers_Length[Address] == 2 and Control_Flag: 
				Control_Flag = False
			else:
				Input_Registers.append(Client_ModBus.read_input_registers(Inverter_Registers_Address[Address],Inverter_Registers_Length[Address]))
		Grid_3Phase_DeliveredEnergy_LastReset_kWh = convert(converter_parameters_uint_32(Input_Registers[0][0],Input_Registers[0][1]), Scale_Factor = 0.01)
		Grid_3Phase_DaylyEnergy_Instant_kWh = convert(converter_parameters_uint_32(Input_Registers[1][0], Input_Registers[1][1]), Scale_Factor = 0.01)
		Grid_Phase1_RMSVoltage_Instant_V = convert(Input_Registers[2], Scale_Factor = 0.1)
		Grid_Phase2_RMSVoltage_Instant_V = convert(Input_Registers[3], Scale_Factor = 0.1)
		Grid_Phase3_RMSVoltage_Instant_V = convert(Input_Registers[4], Scale_Factor = 0.1)
		Grid_3Phase_Instant_Delivered_Aparent_Power_VA = convert(Input_Registers[5], Scale_Factor = 10)
		Grid_3Phase_Instant_Delivered_Real_Power_W = convert(Input_Registers[6], Scale_Factor = 10)
		#print(Grid_3Phase_DeliveredEnergy_LastReset_kWh)
		Client_ModBus.close()
		time.sleep(60)
		
		
def converter_parameters_uint_32(Uint_32_Input_Registers_Most_Significant_Bits, Uint_32_Input_Registers_Less_Significant_Bits):
    """
    Recebe os parâmetro lidos do registrador do inversor
	
	Estradas: 

		Uint_32_Input_Registers_Most_Significant_Bits: Conjunto de Bits mais significativos da variável (de 32 a 16)
		Uint_32_Input_Registers_Less_Significant_Bits: Conjunto de Bits menos significativos da variável (de 15 a 0)
	
	Função: Converter o valor dos registradores do inversor em valores do tipo inteiro

    """
    return Uint_32_Input_Registers_Most_Significant_Bits * 65536 + Uint_32_Input_Registers_Less_Significant_Bits


def convert_input_register_value_in_real_value(Input_Register_Value, Scale_Factor):
	"""
	Recebe o parâmetro lido pelo inversor, já convertido de uint32 (se necessário)

	Entradas:
		Input_Register_Value: Valor enviado pelo inversor, sem conversão para valores reais
		Scale_Factor: Fator de escala necessário para converter o número em um número com valor real (Tensão, Corrente, Potência, Energia, etc)

	Função: Dado um número do tipo inteiro, e um fator de escala, essa função realiza a conversão deste número para valores de grandezas reais, como tensão em Volts, Corrente em Amperes, etc.
	"""
    return Input_Register_Value*Scale_Factor
