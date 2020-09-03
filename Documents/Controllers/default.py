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


	"""
	Inverter_Registers_Address = [6,7,12,13,24,25,26,28,29,32] # Endereço requisitado ao inversor
	Inverter_Registers_Length = [2,2,2,2,1,1,1,1,1,1] # Dimensão do registrador
	Input_Registers = []
	control_flag = False
	Client_ModBus = ModbusClient(host = HOST, port = 502, auto_open = True, auto_close = True)
	while True:
		Client_ModBus.open()
		#Client_ModBus.debug(True)
		for Address in range(len(Inverter_Registers_Address)):
			if Inverter_Registers_Length[Address] == 2 and not(control_flag):
				Input_Registers.append(Client_ModBus.read_input_registers(Inverter_Registers_Address[Address],Inverter_Registers_Length[Address]))
				control_flag = True
			elif Inverter_Registers_Length[Address] == 2 and control_flag: 
				control_flag = False
			else:
				Input_Registers.append(Client_ModBus.read_input_registers(Inverter_Registers_Address[Address],Inverter_Registers_Length[Address]))
		print(Input_Registers)
		time.sleep(60)
		Client_ModBus.close()
		
		
def converter_parameters(Input_Registers):
    '''Recebe os parâmetro lidos pela main e converte.....'''
    Partial_Energy_delivered_the_unit_since_user_reset_value = Input_Registers[0][0] x 65536 + Input_Registers[0][1]
    Daily_energy_value       = (Input_Registers[1][0] x 65536 + Input_Registers[1][1]) / 100
    Grid_RMS_voltage_phase_1 = Input_Registers[2] / 10
    Grid_RMS_voltage_phase_2 = Input_Registers[3] / 10
    Grid_RMS_voltage_phase_3 = Input_Registers[4] / 10
    Output_apparent_power    = Input_Registers[5] * 10
    Output_active_power      = Input_Registers[6] * 10
    Input_current            = Input_Registers[7] / 100
