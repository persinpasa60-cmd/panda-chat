import asyncio
import websockets
import json
import datetime
import os

DB_FILE = "db.json"
connected_clients = {}
private_messages = {}

def load_db():
    global private_messages
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                private_messages = json.load(f)
            print(f"Loaded chats: {len(private_messages)}")
        except Exception as e:
            print(f"DB load error: {e}")

def save_db():
    try:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(private_messages, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"DB save error: {e}")

def get_chat_key(u1, u2):
    return "|".join(sorted([u1, u2]))

async def notify_users():
    users_list = [{"name": name, "status": "online"} for name in connected_clients.values()]
    message = json.dumps({"type": "users", "data": users_list})
    if connected_clients:
        await asyncio.gather(*[client.send(message) for client in connected_clients], return_exceptions=True)

async def handle_client(websocket):
    try:
        init_msg = await websocket.recv()
        data = json.loads(init_msg)
        
        if data.get("type") == "join":
            username = data.get("name", "Anon")
            connected_clients[websocket] = username
            await websocket.send(json.dumps({"type": "auth", "status": "success"}))
            await notify_users()

        async for message in websocket:
            data = json.loads(message)
            
            if data.get("type") == "message":
                receiver = data.get("to")
                sender = connected_clients[websocket]
                text = data.get("text")
                
                chat_key = get_chat_key(sender, receiver)
                msg_obj = {
                    "type": "message",
                    "from": sender,
                    "to": receiver,
                    "text": text,
                    "time": datetime.datetime.now().strftime("%H:%M")
                }
                
                if chat_key not in private_messages:
                    private_messages[chat_key] = []
                private_messages[chat_key].append(msg_obj)
                
                if len(private_messages[chat_key]) > 100:
                    private_messages[chat_key].pop(0)
                
                save_db()

                for ws, name in connected_clients.items():
                    if name == receiver or name == sender:
                        await ws.send(json.dumps(msg_obj))
            
            elif data.get("type") == "get_history":
                other_user = data.get("with")
                sender = connected_clients[websocket]
                chat_key = get_chat_key(sender, other_user)
                history = private_messages.get(chat_key, [])
                await websocket.send(json.dumps({"type": "history", "data": history}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if websocket in connected_clients:
            del connected_clients[websocket]
            await notify_users()

async def main():
    load_db()
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handle_client, "0.0.0.0", port):
        print(f"Server started on port {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
