# Programa Principal, onde será rodado o servidor
# Importações:
from pyModbusTCP.client import ModbusClient
from Configurations.ModBusHost import HOST 

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
	Client_ModBus = ModbusClient(host = HOST, port = 502, auto_open = True, auto_close = True)
	Client_ModBus.open()
	Client_ModBus.debug(True)
	Registradores = Client_ModBus.read_holding_registers(1001,2)
	if Registradores:
		print(Registradores)
	else:
		print("Erro de leitura")
