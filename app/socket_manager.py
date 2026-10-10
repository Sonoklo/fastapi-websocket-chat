import datetime  
from fastapi import WebSocket
import asyncio

type Connections = dict[WebSocket, dict[str, WebSocket]]
type Rooms = dict[int, dict[WebSocket, dict[str, WebSocket]]]
type Errors = dict[str, str]
type Message_time = dict[WebSocket,str]

class RoomManager:
    def __init__(self, send_message, process_errors):
        self.__rooms: Rooms = {}
        self._send_message = send_message
        self.process_errors = process_errors

    @property
    def rooms(self):
        return self.__rooms

    async def _update_users_in_room(self, room: dict, room_id: int, user: str) -> None:
        users = [websocket["name"] for websocket in room.values()]
        results = await asyncio.gather(*(self._send_message(websocket=websocket["connect"], users=users, type="users") for websocket in room.values()), return_exceptions=True)
        await self.process_errors(results, room_id, user=user)

    async def add_to_room(self, room_id: int, websocket: WebSocket, user: str) -> None:
        if self.__rooms.get(room_id, None) is None:
            self.__rooms[room_id] = {}
        self.__rooms[room_id][websocket] = {"name": user, "connect": websocket}
        await self._update_users_in_room(self.__rooms[room_id], room_id, user)

    async def user_typing(self, websocket: WebSocket, user: str, room_id: int, is_typing: bool) -> None:
        websockets = self.__rooms.get(room_id)
        senders = websockets.copy()
        senders.pop(websocket)
        results = await asyncio.gather(*(self._send_message(websocket=ws["connect"], is_typing=is_typing, user=user, type="typing") for ws in senders.values()), return_exceptions=True)
        await self.process_errors(results, room_id, user=user)

    async def user_connect_to_room(self, room_id: int, websocket: WebSocket, user: str) -> None:
        websockets: Connections = self.__rooms[room_id]
        await websocket.send_json({"type": "system", "text": f"Добро пожаловать, {user}"})
        senders = websockets.copy()
        senders.pop(websocket)
        results = await asyncio.gather(*(self._send_message(websocket=ws["connect"], user=user, text=f"{user}, присоединился к чату", type="system") for ws in senders.values()), return_exceptions=True)
        await self.process_errors(results, room_id, user=user)

    async def delete_user(self, room_id: int, websocket: WebSocket, user: str) -> None:
        self.__rooms[room_id].pop(websocket)
        await self._update_users_in_room(self.__rooms[room_id], room_id, user)
        results = await asyncio.gather(*(self._send_message(websocket=ws["connect"], text=f"{user}, покинул группу", type="system") for ws in self.__rooms[room_id].values()), return_exceptions=True)
        await self.process_errors(results, room_id, user=user)

class MessageValidator:
    def __init__(self):
        self.__message_times: Message_time = {}
        self.COUNT_MESSAGES = 10
        self.TIME_LIMIT = 10

    def _check_time_message_limit(self, websocket: WebSocket) -> bool:
        now = datetime.datetime.now()

        if websocket not in self.__message_times:
            self.__message_times[websocket] = []

        times = self.__message_times[websocket]
        times = [time for time in times if now - time <= datetime.timedelta(seconds=self.TIME_LIMIT)]

        if len(self.__message_times[websocket]) >= self.COUNT_MESSAGES:
            return False

        times.append(now)
        self.__message_times[websocket] = times

        return True

    def _validate_message(self, message: str):
        FILE_PATH = ""
        if len(message) > 1000:
            return "key_lenght"
        
        with open(FILE_PATH, "r") as file:
            ban_words = file.read().split('\n')
            for ban_word in ban_words:
                if ban_word in message:
                    return "key_ban_word"
            return None

class MessageManager:
    def __init__(self, rooms, process_errors, message_validator, delete_user):
        self.__rooms = rooms
        self.process_errors = process_errors
        self.message_validator = message_validator
        self.delete_user = delete_user

    async def _send_message(self, type: str, websocket: WebSocket, user=None, text=None, users=None, is_typing=None) -> None | WebSocket:
        try:
            if type == "message":
                if user is not None and text is not None:
                    await websocket.send_json({"type": "message", "user": user, "text": text, "timestamp": datetime.datetime.now()})
            elif type == "system":
                if text is not None:
                    await websocket.send_json({"type": "system", "text": text})
            elif type == "error":
                if text is not None:
                    await websocket.send_json({"type": "error", "detail": text})
            elif type == "users":
                if users is not None:
                    await websocket.send_json({"type": "users", "users": users})
            elif type == "typing":
                if is_typing is not None:
                    await websocket.send_json({"type": "typing", "user": user, "is_typing": is_typing})
        except:
            return websocket

    async def broadcast(self, sender_websocket: WebSocket, room_id: int, message: str, user: str) -> None:
        websockets: Connections = self.__rooms[room_id]
        error_detail = ""
        if not self.message_validator._check_time_message_limit(sender_websocket):
            error_detail = f"{user}, нарушил правила превысив лимит сообщений, за это он был выгнан из группы"
            await sender_websocket.send_json({"type": "error", "detail": "Сообщения не отправленно из-за превышения лимита сообщений"})
        else:
            output = self.message_validator._validate_message(message)
            if output:
                if output == "key_ban_word":
                    error_detail = f"{user}, нарушил правила и написал запретное слово, за это он был выгнан из группы"
                    await sender_websocket.send_json({"type": "error", "detail": "Сообщения не отправленно из за наличия в нем запрещенного слова"})
                elif output == "key_lenght":
                    error_detail = f"{user}, нарушил правила превысив длину сообщения, за это он был выгнан из группы"
                    await sender_websocket.send_json({"type": "error", "detail": "Сообщения превысило длинну в 1000 символов"})
        if error_detail:
            senders = websockets.copy()
            senders.pop(sender_websocket)
            await asyncio.gather(*(self._send_message(websocket=ws["connect"], text=error_detail, type="system") for ws in senders.values()), return_exceptions=True)
            await self.delete_user(room_id, sender_websocket, user)
        else:
            results = await asyncio.gather(*(self._send_message(websocket=ws["connect"], user=user, text=message, type="message") for ws in websockets.values()), return_exceptions=True)
            await self.process_errors(results, room_id, user=user)

class Manager:
    def __init__(self):
        self.__errors: Errors = {}
        self.message_validator = MessageValidator()
        self.room_manager = RoomManager(send_message=self._send_message, process_errors=self.process_errors)
        self.message_manager = MessageManager(rooms=self.room_manager.rooms, process_errors=self.process_errors, message_validator=self.message_validator, delete_user=self.delete_user)

    async def _send_message(self, *args, **kwargs):
        return await self.message_manager._send_message(*args, **kwargs)

    async def log_error(self, websocket: WebSocket, ex: Exception, user: str) -> None:
        if self.__errors.get(ex, None) is None:
            self.__errors[ex] = {}
        self.__errors[ex] = {"name": user, "connect": websocket}
        print(f"Ошибка, {ex} у {user}")

    async def process_errors(self, results, room_id: int, user: str) -> None:
        room = self.room_manager.rooms[room_id]
        for websocket in results:
            if websocket is not None:
                room.pop(websocket, None)
                await asyncio.gather(*(self._send_message(websocket=ws["connect"], text=f"{user}, покинул группу", type="system") for ws in room.values()), return_exceptions=True)

    async def add_to_room(self, room_id: int, websocket: WebSocket, user: str) -> None:
        await self.room_manager.add_to_room(room_id, websocket, user)

    async def user_typing(self, websocket: WebSocket, user: str, room_id: int, is_typing: bool) -> None:
        await self.room_manager.user_typing(websocket, user, room_id, is_typing)

    async def user_connect_to_room(self, room_id: int, websocket: WebSocket, user: str) -> None:
        await self.room_manager.user_connect_to_room(room_id, websocket, user)

    async def delete_user(self, room_id: int, websocket: WebSocket, user: str) -> None:
        await self.room_manager.delete_user(room_id, websocket, user)

    async def broadcast(self, sender_websocket: WebSocket, room_id: int, message: str, user: str) -> None:
        await self.message_manager.broadcast(sender_websocket, room_id, message, user)

manager = Manager()