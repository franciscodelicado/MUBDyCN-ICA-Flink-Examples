# !/usr/bin/env python3 
#
# Ejecución:
#       simulate_climate_devices.py --file <ruta_fichero> [--host <host_mqtt>] [--port <puerto_mqtt>]
#
# 
# Donde:
#  --file (-f): Ruta del archivo de texto a procesar. Este parámetro es obligatorio.
#  --host (-h): Nombre del host o dirección IP del servidor MQTT. Este parámetro es opcional y por defecto será "localhost".
#  --port (-p): Puerto del servidor MQTT. Este parámetro es opcional y por defecto será 1883.

#  El fichero de texto contendrá varias líneas, y cada una de ellas tendrá la siguiente estructura:
#     <api_key>:<id>:<p>

# Donde:
#   - api_key: es una cadena de texto que representa una clave de API.
#   - id: es una cadena de texto que representa un identificador de dispositivo.
#   - p: es un número que representa la probabilidad de enviar un valor erróneo (entre 0 y 1).

# El programa lee el archivo de texto línea por línea, y para cada línea hace lo siguiente:
#   1. Parsear la línea para extraer api_key, id y p.
#   2. Generar un hilo separado que simula un aparato de climatización que envía datos a un servidor #      MQTT y recibe comandos.
#   3. El hilo envía datos de temperatura y humedad cada segundo al servidor MQTT utilizando la 
#      <api_key> y el identificador de dispositivo correspondientes. El tópico MQTT tiene la 
#      siguiente estructura: "/ul/<api_key>/<id>/attrs". Y el payload es un string con el siguiente
#      formato "t|<temperatura>|h|<humedad>", donde <temperatura> y <humedad> son valores numéricos.
#       * La temperatura se genera según la siguiente regla:
#         - Temp_0 es la temperatura inicial, que es un valor aleatorio entre 15 y 25 grados Celsius.
#         - Temp_n = Temp_(n-1) + delta, donde delta es un valor de -0.1 o 0.1 grados Celsius.
#         - El valor inicial de delta se elige aleatoriamente.
#       * La humedad se genera según la siguiente regla:
#         - Hum_0 es la humedad inicial, que es un valor aleatorio entre 30% y 70%.
#         - Hum_n = Hum_(n-1) + delta, donde delta es un valor de -0.5 o 0.5%.
#         - El valor inicial de delta se elige aleatoriamente.
#   5. El hilo también está subscrito a un tópico MQTT para recibir comandos. El tópico de comandos 
#      tiene la siguiente estructura: "/<api_key>/<id>/cmd". Y su payload es un string con el
#      siguiente formato: "<id>@<command>|<value>", donde <command> puede ser "AC" o "HEAT", y <value> 
#      puede ser "ON" o "OFF".
#   6. Al recibir un comando, el hilo imprime en consola el comando recibido junto con el
#      identificador del dispositivo. Y realiza lo siguiente:
#           - Si el id del comando no coincide con el id del dispositivo, ignora el comando.
#           - Si el comando es "AC|ON", actualiza el valor de delta = -abs(delta).
#           - Si el comando es "AC|OFF", no hace nada.
#           - Si el comando es "HEAT|ON", actualiza el valor de delta = abs(delta).
#           - Si el comando es "HEAT|OFF", no hace nada.
#           - Cualquier otro comando será ignorado.
#           - Además tras la actualización del delta, imprime en consola el nuevo valor de delta. 
#             Y responde al tópico "/ul/<api_key>/<id>/cmdexe" con el payload "<id>@<command>|<value>".
#   7. Los hilos se ejecutan de manera concurrente, permitiendo que múltiples aparatos de
#      climatización funcionen al mismo tiempo. Y lo hacen indefinidamente hasta que el programa es
#      interrumpido manualmente.
#   8. El programa principal maneja adecuadamente la finalización de los hilos al recibir una
#      señal de interrupción (por ejemplo, Ctrl+C), asegurándose de que todos los hilos se cierren
#      correctamente antes de salir del programa.


import argparse
import threading
import time
import random
import signal
import sys
import paho.mqtt.client as mqtt

# Thread class for each device
class DeviceThread(threading.Thread):
    def __init__(self, api_key, device_id, p, host, port):
        super().__init__()
        self.api_key = api_key
        self.device_id = device_id
        self.p = float(p)
        self.host = host
        self.port = port
        self.stop_event = threading.Event()
        self.temp = random.uniform(15, 25)
        self.hum = random.uniform(30, 70)
        self.delta_temp = random.choice([-0.1, 0.1])
        self.delta_hum = random.choice([-0.5, 0.5])
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_message = self.on_message
        self.client.connect(self.host, self.port)
        self.client.subscribe(f"/{self.api_key}/{self.device_id}/cmd")
        self.client.loop_start()

    def on_message(self, client, userdata, msg):
        payload = msg.payload.decode()
        print(f"[{self.device_id}] Received MQTT message on topic {msg.topic}: {payload}")
        try:
            id_cmd, cmd_val = payload.split('@')
            command, value = cmd_val.split('|')
        except Exception:
            print(f"[{self.device_id}] Invalid command format: {payload}")
            return
        print(f"[{self.device_id}] Received command: {command}|{value}")
        if id_cmd != self.device_id: #Check that command is for this device
            print(f"[{self.device_id}] Command ID mismatch: {id_cmd}")
            return
        if command == "AC" and value == "ON":
            self.delta_temp = -abs(self.delta_temp)
            print(f"[{self.device_id}] delta_temp updated: {self.delta_temp}")
        elif command == "Heat" and value == "ON":
            self.delta_temp = abs(self.delta_temp)
            print(f"[{self.device_id}] delta_temp updated: {self.delta_temp}")
        # Respond to command execution
        self.client.publish(f"/ul/{self.api_key}/{self.device_id}/cmdexe", f"{self.device_id}@{command}|{value}")

    def run(self):
        topic = f"/ul/{self.api_key}/{self.device_id}/attrs"
        while not self.stop_event.is_set():
            # Simulate error with probability p
            # if random.random() < self.p:
            #     temp = random.choice([random.uniform(-80, -70), random.uniform(61, 80)])
            #     hum = random.choice([random.uniform(-10, 0), random.uniform(101, 120)])
            # else:
            #     temp = self.temp
            #     hum = self.hum
            temp = self.temp
            hum = self.hum
            print(f"[{self.device_id}] Publishing: t={temp:.2f}, h={hum:.2f}")
            payload = f"t|{temp:.2f}|h|{hum:.2f}"
            self.client.publish(topic, payload)
            # Update values
            self.temp += self.delta_temp
            self.hum += self.delta_hum
            # Clamp values to normal range
            self.temp = max(min(self.temp, 60), -70)
            self.hum = max(min(self.hum, 100), 0)
            time.sleep(1)
        self.client.loop_stop()
        self.client.disconnect()

    def stop(self):
        self.stop_event.set()


def parse_file(file_path):
    devices = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(':')
            if len(parts) != 3:
                print(f"Invalid line: {line}")
                continue
            devices.append(parts)
    return devices


def main():
    parser = argparse.ArgumentParser(description="Simulate climate devices sending MQTT data.")
    parser.add_argument('--file', '-f', required=True, help='Path to input text file')
    parser.add_argument('--host', '-H', default='localhost', help='MQTT server host')
    parser.add_argument('--port', '-p', type=int, default=1883, help='MQTT server port')
    args = parser.parse_args()

    devices = parse_file(args.file)
    threads = []
    for api_key, device_id, p in devices:
        t = DeviceThread(api_key, device_id, p, args.host, args.port)
        t.start()
        threads.append(t)

    def signal_handler(sig, frame):
        print("\nStopping all devices...")
        for t in threads:
            t.stop()
        for t in threads:
            t.join()
        print("All devices stopped. Exiting.")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    print("Simulation running. Press Ctrl+C to stop.")
    signal.pause()

if __name__ == "__main__":
    main()
