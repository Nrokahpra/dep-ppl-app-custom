import datetime
import json
import threading
import asyncio
import os
from nats.aio.client import Client as NATS

class Pplapp:
    
    def __init__(self, ipAddress, username, password):
        self.measurements = {}
        self.ipAddress = ipAddress
        self.username = username
        self.password = password
        self.connectToNats = True
        self.connection = NATS()

        threading.Thread(target=asyncio.run, args=(self.natsConnect(),)).start()

    def stop(self):
        self.connectToNats = False

    async def retryOperation(self, operation, errorMessage, successMessage=None, retryInterval=5):
        while True:
            try:
                await operation()
                if successMessage:
                    print(successMessage)
                return
            except asyncio.TimeoutError as e:
                print(f"{errorMessage}: Timeout error. Retrying in {retryInterval} seconds...")
                await asyncio.sleep(retryInterval)
            except Exception as e:
                print(f"{errorMessage}: {e}. Retrying in {retryInterval} seconds...")
                await asyncio.sleep(retryInterval)

    async def natsConnect(self):
        async def connect():
            await asyncio.wait_for(self.connection.connect(servers=[f"nats://{self.ipAddress}:4222"], user=self.username, password=self.password, connect_timeout=5), timeout=3)
            await self.connection.subscribe("nats_dialog", cb=self.processMessage)
            await self.sendMessageAsync("request", "reportMeasurements", "all", "1")
            while self.connectToNats:
                await asyncio.sleep(1)
            await self.natsDisconnect()

        await self.retryOperation(
            connect,
            "Error connecting to NATS server",
            "Successfully connected to NATS server"
        )

    async def natsDisconnect(self):
        try:
            await self.connection.close()
        except Exception as e:
            print(f"Error disconnecting from NATS server: {e}")

    async def processMessage(self, messageRaw):
        try:
            message = json.loads(messageRaw.data.decode())
            messageType = message.get("msg_type", "0")
            messageId = message.get("msg_id", "0")
            deviceId = message.get("device_id", "0")
            payload = message.get("payload", "0")

            if messageType == "reply" and messageId == "reportMeasurements" and deviceId == "all" and isinstance(payload, dict):
                self.writeMeasurements(payload)
            if messageType == "reply" and messageId == "getLogs":
                self.saveLogFile(payload)
        except Exception as e:
            print(f"Error processing message: {e}")

    async def sendMessageAsync(self, messageType, messageId, deviceId, command):
        try:
            message = {
                "timestamp": self.__getCurrentTimestamp(),
                "msg_type": messageType,
                "msg_id": messageId,
                "device_id": deviceId,
                "command": command
            }
            await self.connection.publish("nats_dialog", json.dumps(message).encode())
        except Exception as e:
            print(f"Error sending message: {e}")

    def __getCurrentTimestamp(self):
        currentTime = datetime.datetime.utcnow()
        return currentTime.strftime("[%Y.%m.%d_%H:%M:%S]")

    def writeMeasurements(self, payload):
        try:
            for deviceId, measurements in payload.items():
                if not self.__deviceExists(deviceId):
                    self.measurements[deviceId] = {}
                self.measurements[deviceId].update(measurements)
        except Exception as e:
            print(f"Error writing measurements: {e}")

    def saveLogFile(self, content):
        lines = content.splitlines()

        filename = lines[0].split('/')[-1]
        logContent = "\n".join(lines[1:])

        logDirectory = 'logs'

        if not os.path.exists(logDirectory):
            os.makedirs(logDirectory)

        filepath = os.path.join(logDirectory, filename)

        with open(filepath, 'w') as file:
            file.write(logContent)

    def __deviceExists(self, deviceId):
        return deviceId in self.measurements
    
    def getAllMeasurements(self):
        return self.measurements

    def getMeasurements(self, deviceId, measurement):
        return self.measurements.get(deviceId, {}).get(measurement)
    
    def getLogs(self):
        self.sendMessage("request", "getLogs", "", "")
        
    def sendTelegram(self, message, level="INFO"):
        self.sendMessage("request", "sendTelegram", message, level)
        
    def setCommands(self, deviceId, commands):
        self.sendMessage("request", "setCommands", deviceId, commands)

    def sendMessage(self, messageType, messageId, deviceId, commands):
        threading.Thread(target=asyncio.run, args=(self.sendMessageAsync(messageType, messageId, deviceId, commands),)).start()
