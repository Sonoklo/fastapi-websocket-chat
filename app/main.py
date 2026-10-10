from fastapi import FastAPI, WebSocket, status, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
from app.socket_manager import manager

app = FastAPI()
BASE_DIR = Path(__file__).resolve().parent

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

@app.get("/")
def index():
    return FileResponse(BASE_DIR / "static" / "index.html")

@app.websocket("/ws/{room_id}")
async def chat(websocket: WebSocket, room_id:int, user:str):
    if len(user) > 4:
        await websocket.accept()
        await manager.add_to_room(room_id,websocket,user)
        await manager.user_connect_to_room(room_id,websocket,user)
        try:
            while True:
                msg = await websocket.receive_json()
                if msg["type"] == "message":
                    await manager.broadcast(websocket, room_id, message=msg["text"], user=user)
                if msg["type"] == "typing":
                    await manager.user_typing(websocket=websocket,user=user,room_id=room_id, is_typing=msg["is_typing"])
        except WebSocketDisconnect:
            await manager.delete_user(room_id,websocket,user)
        except Exception as ex:
            await manager.log_error(websocket,ex,user)
    else:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid username")



