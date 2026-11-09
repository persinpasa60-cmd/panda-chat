import asyncio
import websockets
import json
import datetime
import os

connected_clients = {}
chat_history = []

async def notify_users():
    users_list = list(connected_clients.values())
    message = json.dumps({"type": "users", "data": users_list})
    if connected_clients:
        await asyncio.gather(*[client.send(message) for client in connected_clients], return_exceptions=True)

async def broadcast(message):
    if connected_clients:
        await asyncio.gather(*[client.send(message) for client in connected_clients], return_exceptions=True)

async def handle_client(websocket):
    try:
        init_msg = await websocket.recv()
        data = json.loads(init_msg)
        
        if data.get("type") == "join":
            username = data.get("name", "Аноним")
            connected_clients[websocket] = username
            
            await websocket.send(json.dumps({"type": "history", "data": chat_history}))
            await notify_users()
            
            sys_msg = {"type": "message", "name": "Система", "text": f"{username} присоединился к чату", "time": datetime.datetime.now().strftime("%H:%M")}
            chat_history.append(sys_msg)
            await broadcast(json.dumps(sys_msg))

        async for message in websocket:
            data = json.loads(message)
            if data.get("type") == "message":
                msg = {"type": "message", "name": connected_clients[websocket], "text": data.get("text"), "time": datetime.datetime.now().strftime("%H:%M")}
                chat_history.append(msg)
                if len(chat_history) > 100: chat_history.pop(0)
                await broadcast(json.dumps(msg))
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if websocket in connected_clients:
            username = connected_clients[websocket]
            del connected_clients[websocket]
            await notify_users()
            sys_msg = {"type": "message", "name": "Система", "text": f"{username} покинул чат", "time": datetime.datetime.now().strftime("%H:%M")}
            chat_history.append(sys_msg)
            await broadcast(json.dumps(sys_msg))

async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handle_client, "0.0.0.0", port):
        print(f"Сервер запущен на порту {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())