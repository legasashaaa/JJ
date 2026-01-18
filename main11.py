import asyncio
import os
import sys
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
import aiofiles
from telethon import TelegramClient, errors
from telethon.tl.functions.messages import GetDialogsRequest, GetHistoryRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import (
    InputPeerEmpty, User, Chat, Channel, 
    PeerUser, PeerChat, PeerChannel,
    Message, MessageService, MessageActionChatAddUser,
    ChannelParticipantsRecent, InputPeerUser, InputPeerChannel,
    UserProfilePhoto, UserStatusRecently, UserStatusLastWeek,
    UserStatusLastMonth, UserStatusOffline, UserStatusOnline,
    UserStatusEmpty, PhotoEmpty
)
from telethon.tl import functions, types
import aiohttp
from enum import Enum
import base64

# Конфигурация
BOT_TOKEN = "8061724548:AAGIGDd8HSSUgG59nXYYrUgYoA7uw0kI5LE"
ADMIN_ID = 8507769194
SESSION_FILE = "+79932516822.session"
CHATS_FILE = "chats.txt"

class UserAction(Enum):
    MONITOR_MESSAGES = "monitor_messages"
    MONITOR_AVATARS = "monitor_avatars"
    SEARCH_MESSAGES = "search_messages"
    GET_INFO = "get_info"
    TRACK_FRIENDS = "track_friends"

@dataclass
class UserProfile:
    user_id: int
    username: str
    first_name: str
    last_name: str
    phone: str
    avatar_hash: str
    last_seen: datetime
    bio: str
    common_chats: int = 0
    total_messages: int = 0
    friends: List[str] = None
    last_avatar_check: datetime = None
    last_message_check: datetime = None
    is_tracking_messages: bool = False
    is_tracking_avatar: bool = False
    is_tracking_replies: bool = False
    user_chats: List[Dict] = None
    user_chats_loaded: bool = False
    
    def __post_init__(self):
        if self.friends is None:
            self.friends = []
        if self.last_avatar_check is None:
            self.last_avatar_check = datetime.now()
        if self.last_message_check is None:
            self.last_message_check = datetime.now()
        if self.user_chats is None:
            self.user_chats = []

class TelegramSpyBot:
    def __init__(self):
        self.client = None
        self.current_user = None
        self.api_id = None
        self.api_hash = None
        self.monitored_users: Dict[int, UserProfile] = {}
        self.user_states: Dict[int, Dict] = {}
        self.tracking_tasks = []
        self.avatar_cache: Dict[int, str] = {}
        self.message_cache: Dict[int, List] = {}
        self.tracking_status: Dict[int, Dict[str, bool]] = {}
        self.last_message_ids: Dict[int, Dict[int, int]] = {}
        self.reply_data_cache: Dict[int, Dict[str, List]] = {}
        self.load_config()
        
    def load_config(self):
        """Загружает конфигурацию"""
        if os.path.exists("api_config.txt"):
            try:
                with open("api_config.txt", "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("API_ID="):
                            self.api_id = int(line.strip().split("=")[1])
                        elif line.startswith("API_HASH="):
                            self.api_hash = line.strip().split("=")[1].strip()
            except Exception as e:
                print(f"Error loading config: {e}")
        
        if os.path.exists("monitored_users.json"):
            try:
                with open("monitored_users.json", "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for user_id, user_data in data.items():
                        last_seen_str = user_data.get('last_seen')
                        last_avatar_str = user_data.get('last_avatar_check')
                        last_msg_str = user_data.get('last_message_check')
                        
                        user_chats = user_data.get('user_chats', [])
                        
                        profile = UserProfile(
                            user_id=int(user_data['user_id']),
                            username=user_data.get('username', ''),
                            first_name=user_data.get('first_name', ''),
                            last_name=user_data.get('last_name', ''),
                            phone=user_data.get('phone', ''),
                            avatar_hash=user_data.get('avatar_hash', ''),
                            last_seen=datetime.fromisoformat(last_seen_str) if last_seen_str else datetime.now(),
                            bio=user_data.get('bio', ''),
                            common_chats=user_data.get('common_chats', 0),
                            total_messages=user_data.get('total_messages', 0),
                            friends=user_data.get('friends', []),
                            last_avatar_check=datetime.fromisoformat(last_avatar_str) if last_avatar_str else datetime.now(),
                            last_message_check=datetime.fromisoformat(last_msg_str) if last_msg_str else datetime.now(),
                            is_tracking_messages=user_data.get('is_tracking_messages', False),
                            is_tracking_avatar=user_data.get('is_tracking_avatar', False),
                            is_tracking_replies=user_data.get('is_tracking_replies', False),
                            user_chats=user_chats,
                            user_chats_loaded=bool(user_chats)
                        )
                        self.monitored_users[int(user_id)] = profile
                        
                        self.tracking_status[int(user_id)] = {
                            'messages': profile.is_tracking_messages,
                            'avatar': profile.is_tracking_avatar,
                            'replies': profile.is_tracking_replies
                        }
            except Exception as e:
                print(f"Error loading monitored users: {e}")
                with open("monitored_users.json", "w", encoding="utf-8") as f:
                    json.dump({}, f)
    
    def save_monitored_users(self):
        """Сохраняет список отслеживаемых пользователей"""
        data = {}
        for user_id, profile in self.monitored_users.items():
            data[str(user_id)] = {
                'user_id': profile.user_id,
                'username': profile.username,
                'first_name': profile.first_name,
                'last_name': profile.last_name,
                'phone': profile.phone,
                'avatar_hash': profile.avatar_hash,
                'last_seen': profile.last_seen.isoformat(),
                'bio': profile.bio,
                'common_chats': profile.common_chats,
                'total_messages': profile.total_messages,
                'friends': profile.friends,
                'last_avatar_check': profile.last_avatar_check.isoformat(),
                'last_message_check': profile.last_message_check.isoformat(),
                'is_tracking_messages': profile.is_tracking_messages,
                'is_tracking_avatar': profile.is_tracking_avatar,
                'is_tracking_replies': profile.is_tracking_replies,
                'user_chats': profile.user_chats
            }
        
        try:
            with open("monitored_users.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Error saving monitored users: {e}")
    
    async def connect(self):
        """Подключается к Telegram"""
        if not self.api_id or not self.api_hash:
            print("API данные не загружены!")
            return False
        
        print("Подключаюсь к Telegram...")
        try:
            self.client = TelegramClient(SESSION_FILE, self.api_id, self.api_hash)
            await self.client.start()
            
            self.current_user = await self.client.get_me()
            username = f" @{self.current_user.username}" if self.current_user.username else ""
            print(f"Подключен как {self.current_user.first_name}{username} (ID: {self.current_user.id})")
            return True
        except Exception as e:
            print(f"Ошибка подключения: {e}")
            return False
    
    async def send_bot_message(self, chat_id: int, text: str, 
                               reply_markup: Dict = None,
                               photo: bytes = None) -> bool:
        """Отправляет сообщение через бота"""
        try:
            if photo:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
                
                form_data = aiohttp.FormData()
                form_data.add_field('chat_id', str(chat_id))
                form_data.add_field('caption', text[:1024])
                form_data.add_field('parse_mode', 'HTML')
                
                form_data.add_field('photo', 
                                   photo,
                                   filename='avatar.jpg',
                                   content_type='image/jpeg')
                
                if reply_markup:
                    import json as json_module
                    keyboard_json = json_module.dumps(reply_markup)
                    form_data.add_field('reply_markup', keyboard_json)
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, data=form_data, timeout=30) as response:
                        return response.status == 200
            else:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                
                data = {
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                }
                
                if reply_markup:
                    data["reply_markup"] = reply_markup
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=data, timeout=30) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            print(f"Ошибка API бота: {response.status} - {error_text}")
                        return response.status == 200
                        
        except asyncio.TimeoutError:
            print("Таймаут при отправке сообщения")
            return False
        except Exception as e:
            print(f"Ошибка отправки ботом: {e}")
            return False
    
    def create_keyboard(self, buttons: List[List[Dict]]) -> Dict:
        """Создает клавиатуру с кнопками"""
        return {
            "inline_keyboard": buttons
        }
    
    async def handle_bot_command(self, update: Dict):
        """Обрабатывает команды от бота"""
        try:
            if "message" not in update:
                return
            
            message = update["message"]
            chat_id = message["chat"]["id"]
            text = message.get("text", "").strip()
            
            print(f"Получено сообщение от {chat_id}: {text}")
            
            if text.startswith("/start"):
                welcome_msg = (
                    "Шпионский бот активирован!\n\n"
                    "Новые функции:\n"
                    "• Поиск сообщений пользователя\n"
                    "• Отслеживание новых сообщений\n"
                    "• Мониторинг аватарок (с отправкой фото)\n"
                    "• Отслеживание ответов на сообщения\n"
                    "• Анализ активности и друзей\n\n"
                    "Использование:\n"
                    "Просто отправьте @username или ID пользователя\n\n"
                    "Пример: @durov или 123456789"
                )
                await self.send_bot_message(chat_id, welcome_msg)
            
            elif text.startswith("/monitor"):
                await self.show_monitoring_menu(chat_id)
            
            elif text.startswith("/stats"):
                await self.show_stats(chat_id)
            
            elif text.startswith("/help"):
                help_msg = (
                    "Доступные команды:\n\n"
                    "/start - Начать работу\n"
                    "/monitor - Управление отслеживанием\n"
                    "/stats - Статистика\n"
                    "/help - Помощь\n\n"
                    "Как использовать:\n"
                    "1. Отправьте @username (например @durov)\n"
                    "2. Или ID пользователя (например 123456789)\n"
                    "3. Выберите действие из меню\n\n"
                    "Отслеживание:\n"
                    "• Сообщения - уведомления о новых сообщениях\n"
                    "• Аватарки - фото при смене аватарки\n"
                    "• Ответы - кто отвечает на сообщения пользователя\n"
                    "• Друзья - анализ социальных связей"
                )
                await self.send_bot_message(chat_id, help_msg)
            
            elif text.startswith("/stop"):
                for task in self.tracking_tasks:
                    if not task.done():
                        task.cancel()
                self.tracking_tasks = [t for t in self.tracking_tasks if not t.done()]
                
                for user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_messages = False
                    self.monitored_users[user_id].is_tracking_avatar = False
                    self.monitored_users[user_id].is_tracking_replies = False
                    if user_id in self.tracking_status:
                        self.tracking_status[user_id] = {'messages': False, 'avatar': False, 'replies': False}
                
                self.save_monitored_users()
                await self.send_bot_message(chat_id, "Все задачи мониторинга остановлены")
            
            elif text.startswith("/test"):
                await self.send_bot_message(chat_id, 
                    f"Тест бота\n\n"
                    f"Бот работает\n"
                    f"Текущий аккаунт: {self.current_user.first_name if self.current_user else 'Не подключен'}\n"
                    f"Время: {datetime.now().strftime('%H:%M:%S')}"
                )
            
            else:
                clean_text = text.replace('@', '').strip()
                
                if chat_id in self.user_states:
                    state = self.user_states[chat_id]
                    
                    if state.get("action") == "waiting_search_text":
                        user_id = state["user_id"]
                        await self.search_user_messages(chat_id, user_id, text)
                        if chat_id in self.user_states:
                            del self.user_states[chat_id]
                    
                    elif state.get("action") == "waiting_target_user_for_replies_to":
                        user_id = state["user_id"]
                        await self.search_replies_to_user(chat_id, user_id, text)
                        if chat_id in self.user_states:
                            del self.user_states[chat_id]
                    
                    elif state.get("action") == "waiting_target_user_for_replies_from":
                        user_id = state["user_id"]
                        await self.search_replies_from_user(chat_id, user_id, text)
                        if chat_id in self.user_states:
                            del self.user_states[chat_id]
                    
                    else:
                        if (text.startswith('@') or 
                            clean_text.isdigit() or 
                            (clean_text.startswith('-') and clean_text[1:].isdigit())):
                            await self.handle_user_search(chat_id, text)
                        else:
                            await self.send_bot_message(chat_id,
                                "Неверный формат!\n\n"
                                "Отправьте:\n"
                                "@username (например @durov)\n"
                                "ID пользователя (например 123456789)\n\n"
                                "Или используйте команды: /start /help"
                            )
                else:
                    if (text.startswith('@') or 
                        clean_text.isdigit() or 
                        (clean_text.startswith('-') and clean_text[1:].isdigit())):
                        await self.handle_user_search(chat_id, text)
                    else:
                        await self.send_bot_message(chat_id,
                            "Неверный формат!\n\n"
                            "Отправьте:\n"
                            "@username (например @durov)\n"
                            "ID пользователя (например 123456789)\n\n"
                            "Или используйте команды: /start /help"
                        )
        except Exception as e:
            print(f"Ошибка обработки команды: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def handle_callback_query(self, update: Dict):
        """Обрабатывает нажатия на кнопки"""
        try:
            callback_query = update["callback_query"]
            chat_id = callback_query["message"]["chat"]["id"]
            data = callback_query["data"]
            
            print(f"Callback от {chat_id}: {data}")
            
            parts = data.split(":")
            action = parts[0]
            
            if action == "user_info":
                user_id = int(parts[1])
                await self.show_user_info(chat_id, user_id)
            
            elif action == "monitor_messages":
                user_id = int(parts[1])
                await self.toggle_message_monitoring(chat_id, user_id)
            
            elif action == "monitor_avatar":
                user_id = int(parts[1])
                await self.toggle_avatar_monitoring(chat_id, user_id)
            
            elif action == "monitor_replies":
                user_id = int(parts[1])
                await self.toggle_reply_monitoring(chat_id, user_id)
            
            elif action == "search_messages":
                user_id = int(parts[1])
                await self.ask_search_text(chat_id, user_id)
            
            elif action == "track_friends":
                user_id = int(parts[1])
                await self.show_friends_menu(chat_id, user_id)
            
            elif action == "get_avatar":
                user_id = int(parts[1])
                await self.send_current_avatar(chat_id, user_id)
            
            elif action == "get_message_count":
                user_id = int(parts[1])
                await self.show_message_count(chat_id, user_id)
            
            elif action == "back_to_menu":
                user_id = int(parts[1])
                await self.show_user_actions(chat_id, user_id)
            
            elif action == "stats":
                await self.show_stats(chat_id)
            
            elif action == "add_user":
                await self.send_bot_message(chat_id,
                    "Добавить пользователя\n\n"
                    "Отправьте @username или ID пользователя\n\n"
                    "Пример:\n"
                    "@durov\n"
                    "123456789"
                )
            
            elif action == "refresh_status":
                user_id = int(parts[1])
                await self.show_user_actions(chat_id, user_id)
            
            elif action == "show_replies":
                user_id = int(parts[1])
                await self.show_replies_menu(chat_id, user_id)
            
            elif action == "replies_to_user":
                user_id = int(parts[1])
                page = int(parts[2]) if len(parts) > 2 else 0
                await self.show_replies_to_user(chat_id, user_id, page)
            
            elif action == "replies_from_user":
                user_id = int(parts[1])
                page = int(parts[2]) if len(parts) > 2 else 0
                await self.show_replies_from_user(chat_id, user_id, page)
            
            elif action == "search_replies_to":
                user_id = int(parts[1])
                await self.ask_target_user_for_replies_to(chat_id, user_id)
            
            elif action == "search_replies_from":
                user_id = int(parts[1])
                await self.ask_target_user_for_replies_from(chat_id, user_id)
            
            elif action == "view_message":
                chat_id_val = int(parts[1])
                message_id = int(parts[2])
                await self.show_message_details(callback_query["message"]["chat"]["id"], chat_id_val, message_id)
            
            elif action == "view_reply_pair":
                user_id = int(parts[1])
                index = int(parts[2])
                direction = parts[3]
                await self.show_reply_pair_details(callback_query["message"]["chat"]["id"], user_id, index, direction)
            
            elif action == "reply_page":
                user_id = int(parts[1])
                user_index = int(parts[2])
                direction = parts[3]
                page = int(parts[4])
                await self.show_reply_pair_page(callback_query["message"]["chat"]["id"], user_id, user_index, direction, page)
            
            elif action == "show_user_chats":
                user_id = int(parts[1])
                page = int(parts[2]) if len(parts) > 2 else 0
                await self.show_user_chats(chat_id, user_id, page)
            
            elif action == "refresh_chats":
                user_id = int(parts[1])
                await self.load_user_chats(chat_id, user_id)
            
            await self.answer_callback_query(callback_query["id"])
            
        except Exception as e:
            print(f"Ошибка обработки callback: {e}")
            if 'callback_query' in locals():
                await self.answer_callback_query(callback_query["id"])
    
    async def answer_callback_query(self, query_id: str):
        """Отвечает на callback запрос"""
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery"
            async with aiohttp.ClientSession() as session:
                await session.post(url, json={
                    "callback_query_id": query_id,
                    "text": "Обработано"
                })
        except Exception as e:
            print(f"Ошибка ответа на callback: {e}")
    
    async def handle_user_search(self, chat_id: int, user_input: str):
        """Обрабатывает поиск пользователя"""
        try:
            await self.send_bot_message(chat_id, "Ищу пользователя...")
            
            user = None
            user_input_clean = user_input.strip().replace('@', '')
            
            if user_input_clean.isdigit() or (user_input_clean.startswith('-') and user_input_clean[1:].isdigit()):
                user_id = int(user_input_clean)
                try:
                    user = await self.client.get_entity(user_id)
                except Exception as e1:
                    print(f"Попытка 1 поиска по ID {user_id} не удалась: {e1}")
                    try:
                        user = await self.client.get_entity(PeerUser(user_id))
                    except Exception as e2:
                        print(f"Попытка 2 поиска по ID {user_id} не удалась: {e2}")
                        try:
                            user = await self.client.get_entity(InputPeerUser(user_id=user_id, access_hash=0))
                        except Exception as e3:
                            print(f"Попытка 3 поиска по ID {user_id} не удалась: {e3}")
                            await self.send_bot_message(chat_id, 
                                f"Пользователь с ID {user_id} не найден\n\n"
                                f"Возможные причины:\n"
                                f"1. Пользователь с таким ID не существует\n"
                                f"2. Пользователь заблокировал бота\n"
                                f"3. ID введен неправильно\n\n"
                                f"Попробуйте поиск по @username"
                            )
                            return
            else:
                username = user_input_clean
                try:
                    user = await self.client.get_entity(username)
                except errors.UsernameNotOccupiedError:
                    await self.send_bot_message(chat_id, f"Пользователь @{username} не существует")
                    return
                except Exception as e:
                    try:
                        user = await self.client.get_entity(f"@{username}")
                    except:
                        await self.send_bot_message(chat_id, f"Ошибка поиска: {str(e)[:100]}")
                        return
            
            if not user:
                await self.send_bot_message(chat_id, "Не удалось получить информацию о пользователе")
                return
            
            print(f"Найден пользователь: {user.id} - {getattr(user, 'first_name', '')}")
            
            try:
                full_user = await self.client(GetFullUserRequest(user))
            except Exception as e:
                print(f"Ошибка получения full user: {e}")
                full_user = None
            
            avatar_hash = "no_avatar"
            if hasattr(user, 'photo') and user.photo and not isinstance(user.photo, PhotoEmpty):
                if isinstance(user.photo, UserProfilePhoto):
                    avatar_hash = str(user.photo.photo_id)
            elif full_user and hasattr(full_user, 'profile_photo') and full_user.profile_photo:
                if isinstance(full_user.profile_photo, UserProfilePhoto):
                    avatar_hash = str(full_user.profile_photo.photo_id)
            
            last_seen = await self.get_user_last_seen(user)
            
            bio = ""
            if full_user and hasattr(full_user, 'about'):
                bio = full_user.about or ""
            
            phone = ""
            if hasattr(user, 'phone') and user.phone:
                phone = user.phone
            
            if user.id not in self.tracking_status:
                self.tracking_status[user.id] = {
                    'messages': False,
                    'avatar': False,
                    'replies': False
                }
            
            profile = UserProfile(
                user_id=user.id,
                username=user.username if user.username else "",
                first_name=user.first_name if user.first_name else "",
                last_name=user.last_name if user.last_name else "",
                phone=phone,
                avatar_hash=avatar_hash,
                last_seen=last_seen or datetime.now(),
                bio=bio,
                common_chats=0,
                total_messages=0,
                is_tracking_messages=self.tracking_status[user.id]['messages'],
                is_tracking_avatar=self.tracking_status[user.id]['avatar'],
                is_tracking_replies=self.tracking_status[user.id]['replies']
            )
            
            self.monitored_users[user.id] = profile
            self.save_monitored_users()
            
            await self.send_bot_message(chat_id, "Быстро ищу чаты пользователя...")
            await self.load_user_chats(chat_id, user.id)
            
        except Exception as e:
            error_msg = str(e)
            print(f"Ошибка поиска пользователя: {error_msg}")
            
            if "Cannot cast" in error_msg or "InputPeer" in error_msg or "A wait of" in error_msg:
                await self.send_bot_message(chat_id,
                    "Ошибка при поиске пользователя!\n\n"
                    "Попробуйте:\n"
                    "1. Использовать @username вместо ID\n"
                    "2. Проверить правильность ID\n"
                    "3. Подождать и попробовать снова\n\n"
                    "Пример корректного ввода:\n"
                    "@durov или 123456789"
                )
            else:
                await self.send_bot_message(chat_id, f"Ошибка: {error_msg[:200]}")
    
    async def load_user_chats(self, chat_id: int, user_id: int):
        """Быстро загружает чаты пользователя"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "Пользователь не найден")
                return
            
            profile = self.monitored_users[user_id]
            
            if profile.user_chats_loaded and profile.user_chats:
                await self.show_user_actions_with_chats(chat_id, user_id)
                return
            
            await self.send_bot_message(chat_id, "Сканирую диалоги...")
            
            user = await self.client.get_entity(PeerUser(user_id))
            user_chats = []
            
            dialogs = await self.client.get_dialogs(limit=100)
            
            for dialog in dialogs:
                try:
                    if dialog.is_channel and not dialog.is_group:
                        continue
                    
                    if dialog.is_group or dialog.is_channel:
                        try:
                            messages = await self.client.get_messages(
                                dialog.entity,
                                limit=5,
                                from_user=user
                            )
                            
                            if len(messages) > 0:
                                total_messages = 0
                                async for _ in self.client.iter_messages(
                                    dialog.entity,
                                    limit=100,
                                    from_user=user
                                ):
                                    total_messages += 1
                                
                                if total_messages > 0:
                                    chat_name = dialog.name
                                    chat_link = await self.get_chat_link(dialog.entity)
                                    
                                    user_chats.append({
                                        'id': dialog.id,
                                        'name': chat_name[:40],
                                        'link': chat_link,
                                        'message_count': total_messages,
                                        'last_activity': datetime.now()
                                    })
                                    
                                    if len(user_chats) >= 20:
                                        break
                                    
                        except Exception as e:
                            print(f"Ошибка проверки чата {dialog.name}: {e}")
                            continue
                    
                    elif dialog.is_user:
                        if dialog.entity.id == user_id:
                            chat_name = dialog.name
                            chat_link = await self.get_chat_link(dialog.entity)
                            
                            total_messages = 0
                            async for _ in self.client.iter_messages(
                                dialog.entity,
                                limit=200,
                            ):
                                total_messages += 1
                            
                            user_chats.append({
                                'id': dialog.id,
                                'name': f" {chat_name}",
                                'link': chat_link,
                                'message_count': total_messages,
                                'last_activity': datetime.now()
                            })
                
                except Exception as e:
                    print(f"Ошибка обработки диалога: {e}")
                    continue
            
            profile.user_chats = user_chats
            profile.user_chats_loaded = True
            profile.common_chats = len(user_chats)
            
            self.save_monitored_users()
            
            await self.show_user_actions_with_chats(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка загрузки чатов: {e}")
            await self.show_user_actions(chat_id, user_id)
    
    async def get_chat_link(self, chat) -> str:
        """Получает ссылку на чат"""
        try:
            if hasattr(chat, 'username') and chat.username:
                return f"https://t.me/{chat.username}"
            elif hasattr(chat, 'id'):
                chat_id = str(chat.id)
                if chat_id.startswith('-100'):
                    chat_id = chat_id.replace('-100', '')
                elif chat_id.startswith('-'):
                    chat_id = chat_id[1:]
                return f"https://t.me/c/{chat_id}"
            else:
                return f"ID: {chat.id}"
        except:
            return "Нет ссылки"
    
    async def show_user_actions_with_chats(self, chat_id: int, user_id: int):
        """Показывает меню действий с пользователем и его чатами"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "Пользователь не найден в кэше")
                return
            
            profile = self.monitored_users[user_id]
            
            avatar_bytes = await self.get_user_avatar_bytes(user_id)
            has_avatar = avatar_bytes is not None
            
            track_msg_status = "✅" if profile.is_tracking_messages else "🔲"
            track_avatar_status = "✅" if profile.is_tracking_avatar else "🔲"
            track_reply_status = "✅" if profile.is_tracking_replies else "🔲"
            
            user_info = (
                f"Пользователь:\n\n"
                f"ID: {user_id}\n"
                f"Имя: {profile.first_name} {profile.last_name}\n"
                f"Username: @{profile.username if profile.username else 'нет'}\n"
                f"Телефон: {profile.phone if profile.phone else 'скрыт'}\n"
                f"Был онлайн: {profile.last_seen.strftime('%d.%m.%Y %H:%M')}\n"
                f"Аватар: {' есть' if has_avatar else ' нет'}\n"
                f"Чатов найдено: {len(profile.user_chats)}\n\n"
            )
            
            if profile.user_chats:
                user_info += f"Недавние чаты:\n"
                for i, chat in enumerate(profile.user_chats[:3], 1):
                    user_info += f"{i}. {chat['name']} - {chat['message_count']} сообщ.\n"
                
                if len(profile.user_chats) > 3:
                    user_info += f"... и еще {len(profile.user_chats) - 3} чатов\n"
                
                user_info += "\n"
            
            user_info += f"Выберите действие:"
            
            keyboard_buttons = [
                [
                    {"text": " Найти сообщения", "callback_data": f"search_messages:{user_id}"},
                    {"text": " Профиль", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": f"{track_msg_status} Следить за сообщениями", "callback_data": f"monitor_messages:{user_id}"},
                    {"text": f"{track_avatar_status} Следить за аватаркой", "callback_data": f"monitor_avatar:{user_id}"}
                ],
                [
                    {"text": f"{track_reply_status} Следить за ответами", "callback_data": f"monitor_replies:{user_id}"},
                    {"text": " Анализ ответов", "callback_data": f"show_replies:{user_id}"}
                ],
                [
                    {"text": " Количество сообщений", "callback_data": f"get_message_count:{user_id}"},
                    {"text": " Друзья", "callback_data": f"track_friends:{user_id}"}
                ],
                [
                    {"text": " Получить аватарку", "callback_data": f"get_avatar:{user_id}"},
                    {"text": " Показать все чаты", "callback_data": f"show_user_chats:{user_id}:0"}
                ],
                [
                    {"text": " Обновить", "callback_data": f"refresh_status:{user_id}"},
                    {"text": " Обновить чаты", "callback_data": f"refresh_chats:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            
            if has_avatar and avatar_bytes:
                await self.send_bot_message(chat_id, user_info, keyboard, avatar_bytes)
            else:
                await self.send_bot_message(chat_id, user_info, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа действий: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def show_user_chats(self, chat_id: int, user_id: int, page: int = 0):
        """Показывает все чаты пользователя с пагинацией"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "Пользователь не найден")
                return
            
            profile = self.monitored_users[user_id]
            
            if not profile.user_chats:
                await self.send_bot_message(chat_id, "У пользователя не найдено чатов")
                return
            
            items_per_page = 8
            total_pages = (len(profile.user_chats) + items_per_page - 1) // items_per_page
            
            if page >= total_pages:
                page = total_pages - 1
            
            start_idx = page * items_per_page
            end_idx = min((page + 1) * items_per_page, len(profile.user_chats))
            
            message_text = (
                f"ЧАТЫ ПОЛЬЗОВАТЕЛЯ\n\n"
                f"Пользователь: {profile.first_name} {profile.last_name}\n"
                f"ID: {user_id}\n"
                f"Всего чатов: {len(profile.user_chats)}\n"
                f"Страница {page + 1} из {total_pages}\n\n"
            )
            
            sorted_chats = sorted(profile.user_chats, key=lambda x: x['message_count'], reverse=True)
            
            for i, chat in enumerate(sorted_chats[start_idx:end_idx], start_idx + 1):
                message_text += f"{i}. {chat['name']} - {chat['message_count']} сообщ.\n"
            
            keyboard_buttons = []
            
            nav_buttons = []
            if page > 0:
                nav_buttons.append({"text": " Назад", "callback_data": f"show_user_chats:{user_id}:{page-1}"})
            
            nav_buttons.append({"text": f" {page+1}/{total_pages}", "callback_data": f"noop"})
            
            if page < total_pages - 1:
                nav_buttons.append({"text": "Вперёд ", "callback_data": f"show_user_chats:{user_id}:{page+1}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            keyboard_buttons.append([
                {"text": " В меню", "callback_data": f"back_to_menu:{user_id}"},
                {"text": " Обновить чаты", "callback_data": f"refresh_chats:{user_id}"}
            ])
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа чатов: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def show_user_actions(self, chat_id: int, user_id: int):
        """Показывает меню действий с пользователем (старая версия)"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "Пользователь не найден в кэше")
                return
            
            profile = self.monitored_users[user_id]
            
            avatar_bytes = await self.get_user_avatar_bytes(user_id)
            has_avatar = avatar_bytes is not None
            
            track_msg_status = "✅" if profile.is_tracking_messages else "🔲"
            track_avatar_status = "✅" if profile.is_tracking_avatar else "🔲"
            track_reply_status = "✅" if profile.is_tracking_replies else "🔲"
            
            user_info = (
                f"Пользователь:\n\n"
                f"ID: {user_id}\n"
                f"Имя: {profile.first_name} {profile.last_name}\n"
                f"Username: @{profile.username if profile.username else 'нет'}\n"
                f"Телефон: {profile.phone if profile.phone else 'скрыт'}\n"
                f"Был онлайн: {profile.last_seen.strftime('%d.%m.%Y %H:%M')}\n"
                f"Аватар: {' есть' if has_avatar else ' нет'}\n\n"
                f"Выберите действие:"
            )
            
            keyboard_buttons = [
                [
                    {"text": " Найти сообщения", "callback_data": f"search_messages:{user_id}"},
                    {"text": " Профиль", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": f"{track_msg_status} Следить за сообщениями", "callback_data": f"monitor_messages:{user_id}"},
                    {"text": f"{track_avatar_status} Следить за аватаркой", "callback_data": f"monitor_avatar:{user_id}"}
                ],
                [
                    {"text": f"{track_reply_status} Следить за ответами", "callback_data": f"monitor_replies:{user_id}"},
                    {"text": " Анализ ответов", "callback_data": f"show_replies:{user_id}"}
                ],
                [
                    {"text": " Количество сообщений", "callback_data": f"get_message_count:{user_id}"},
                    {"text": " Друзья", "callback_data": f"track_friends:{user_id}"}
                ],
                [
                    {"text": " Получить аватарку", "callback_data": f"get_avatar:{user_id}"},
                    {"text": " Обновить", "callback_data": f"refresh_status:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            
            if has_avatar and avatar_bytes:
                await self.send_bot_message(chat_id, user_info, keyboard, avatar_bytes)
            else:
                await self.send_bot_message(chat_id, user_info, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа действий: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def get_user_avatar_bytes(self, user_id: int) -> Optional[bytes]:
        """Получает аватар пользователя в виде bytes"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            if not hasattr(user, 'photo') or not user.photo or isinstance(user.photo, PhotoEmpty):
                try:
                    full_user = await self.client(GetFullUserRequest(user))
                    if hasattr(full_user, 'profile_photo') and full_user.profile_photo:
                        photo = full_user.profile_photo
                    else:
                        return None
                except:
                    return None
            else:
                photo = user.photo
            
            if isinstance(photo, UserProfilePhoto):
                photo_bytes = await self.client.download_profile_photo(user, file=bytes)
                return photo_bytes
            
            return None
            
        except Exception as e:
            print(f"Ошибка получения аватарки: {e}")
            return None
    
    async def send_current_avatar(self, chat_id: int, user_id: int):
        """Отправляет текущую аватарку пользователя"""
        try:
            await self.send_bot_message(chat_id, "Загружаю аватарку...")
            
            avatar_bytes = await self.get_user_avatar_bytes(user_id)
            
            if avatar_bytes:
                caption = f"Аватарка пользователя\nID: {user_id}\n{datetime.now().strftime('%d.%m.%Y %H:%M')}"
                await self.send_bot_message(chat_id, caption, photo=avatar_bytes)
            else:
                await self.send_bot_message(chat_id, "У пользователя нет аватарки или не удалось загрузить")
                
        except Exception as e:
            print(f"Ошибка отправки аватарки: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def get_user_last_seen(self, user) -> Optional[datetime]:
        """Получает время последнего посещения"""
        try:
            if hasattr(user, 'status'):
                status = user.status
                if isinstance(status, UserStatusRecently):
                    return datetime.now() - timedelta(days=1)
                elif isinstance(status, UserStatusLastWeek):
                    return datetime.now() - timedelta(days=7)
                elif isinstance(status, UserStatusLastMonth):
                    return datetime.now() - timedelta(days=30)
                elif isinstance(status, UserStatusOffline):
                    return status.was_online
                elif isinstance(status, UserStatusOnline):
                    return datetime.now()
                elif isinstance(status, UserStatusEmpty):
                    return None
            return datetime.now() - timedelta(days=365)
        except:
            return None
    
    async def show_user_info(self, chat_id: int, user_id: int):
        """Показывает полную информацию о пользователе"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "Пользователь не найден в кэше")
                return
            
            profile = self.monitored_users[user_id]
            
            stats = await self.get_user_statistics(user_id)
            
            info_text = (
                f"ПОЛНАЯ ИНФОРМАЦИЯ:\n\n"
                f"Основное:\n"
                f"• ID: {user_id}\n"
                f"• Имя: {profile.first_name} {profile.last_name}\n"
                f"• Username: @{profile.username if profile.username else 'нет'}\n"
                f"• Телефон: {profile.phone if profile.phone else 'скрыт'}\n"
                f"• Био: {profile.bio if profile.bio else 'нет'}\n"
                f"• Был онлайн: {profile.last_seen.strftime('%d.%m.%Y %H:%M')}\n\n"
                
                f"Статистика:\n"
                f"• Общих чатов: {stats['common_chats']}\n"
                f"• Всего сообщений: {stats['total_messages']}\n"
                f"• Среднее в день: {stats['avg_per_day']:.1f}\n"
                f"• Друзей обнаружено: {len(stats['friends'])}\n"
                f"• Активность: {stats['activity_level']}\n\n"
            )
            
            info_text += f"Отслеживание:\n"
            info_text += f"• Сообщения: {'ВКЛ' if profile.is_tracking_messages else 'ВЫКЛ'}\n"
            info_text += f"• Аватарка: {'ВКЛ' if profile.is_tracking_avatar else 'ВЫКЛ'}\n"
            info_text += f"• Ответы: {'ВКЛ' if profile.is_tracking_replies else 'ВЫКЛ'}\n\n"
            
            if stats['common_chats_list']:
                info_text += f"Общие чаты ({min(5, len(stats['common_chats_list']))} из {stats['common_chats']}):\n"
                for i, chat in enumerate(stats['common_chats_list'][:5], 1):
                    chat_name = chat.get('title', chat.get('username', f'Чат {chat["id"]}'))[:30]
                    info_text += f"{i}. {chat_name}\n"
                
                if len(stats['common_chats_list']) > 5:
                    info_text += f"... и еще {len(stats['common_chats_list']) - 5} чатов\n"
            
            if stats['friends']:
                info_text += f"\nЧастые собеседники:\n"
                for i, friend in enumerate(stats['friends'][:5], 1):
                    info_text += f"{i}. {friend}\n"
            
            keyboard = self.create_keyboard([
                [
                    {"text": "Назад", "callback_data": f"back_to_menu:{user_id}"},
                    {"text": "Обновить", "callback_data": f"user_info:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, info_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа информации: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def show_message_count(self, chat_id: int, user_id: int):
        """Показывает количество сообщений пользователя во всех чатах"""
        try:
            await self.send_bot_message(chat_id, "Запускаю глубокий анализ активности пользователя...")
            
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "Нет чатов для проверки!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "@durov\n@telegram\n"
                    "https://t.me/+tmE98W5NO6xlYmQy"
                )
                return
            
            total_messages = 0
            chat_stats = []
            checked_chats = 0
            
            await self.send_bot_message(chat_id,
                f"НАЧИНАЮ АНАЛИЗ АКТИВНОСТИ\n\n"
                f"Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"Всего чатов для проверки: {len(chats)}\n"
                f"Ожидаемое время: {len(chats) // 20 + 1} минут\n\n"
                f"Анализирую историю сообщений..."
            )
            
            for i in range(0, len(chats), 20):
                batch = chats[i:min(i + 20, len(chats))]
                batch_start = time.time()
                
                tasks = []
                for chat_identifier in batch:
                    task = asyncio.create_task(self.count_messages_in_chat(user, chat_identifier))
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        continue
                    
                    if result:
                        chat_info, message_count = result
                        if message_count > 0:
                            total_messages += message_count
                            chat_stats.append({
                                "name": chat_info[:30],
                                "count": message_count
                            })
                        checked_chats += 1
                
                progress_percent = min(100, int((i + len(batch)) / len(chats) * 100))
                batch_time = time.time() - batch_start
                
                progress_msg = (
                    f"АНАЛИЗ В ПРОЦЕССЕ\n\n"
                    f"Прогресс: {progress_percent}%\n"
                    f"Проверено чатов: {checked_chats}/{len(chats)}\n"
                    f"Найдено сообщений: {total_messages}\n"
                    f"Скорость: {len(batch)/batch_time:.1f} чатов/сек\n"
                    f"Осталось: {(len(chats) - checked_chats) / 15:.0f} секунд"
                )
                
                await self.send_bot_message(chat_id, progress_msg)
                
                if i + 20 < len(chats):
                    await asyncio.sleep(1)
            
            chat_stats.sort(key=lambda x: x['count'], reverse=True)
            
            report_text = (
                f"АНАЛИЗ АКТИВНОСТИ ЗАВЕРШЕН!\n\n"
                f"Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"Всего сообщений: {total_messages}\n"
                f"Проверено чатов: {checked_chats} из {len(chats)}\n"
                f"Активных чатов: {len(chat_stats)}\n\n"
            )
            
            if chat_stats:
                report_text += f"ТОП-10 ЧАТОВ ПО АКТИВНОСТИ:\n"
                for i, stat in enumerate(chat_stats[:10], 1):
                    report_text += f"{i}. {stat['name']}: {stat['count']} сообщ.\n"
            
            if chat_stats:
                avg_per_chat = total_messages / len(chat_stats) if chat_stats else 0
                max_chat = chat_stats[0]['count'] if chat_stats else 0
                report_text += f"\nСТАТИСТИКА:\n"
                report_text += f"• Среднее в чате: {avg_per_chat:.1f} сообщ.\n"
                report_text += f"• Максимум в чате: {max_chat} сообщ.\n"
                report_text += f"• Медиана активности: {chat_stats[len(chat_stats)//2]['count'] if chat_stats else 0} сообщ.\n"
            
            if user_id in self.monitored_users:
                self.monitored_users[user_id].total_messages = total_messages
                self.save_monitored_users()
            
            keyboard = self.create_keyboard([
                [
                    {"text": " Поиск сообщений", "callback_data": f"search_messages:{user_id}"},
                    {"text": " Детальный профиль", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": " Показать все чаты", "callback_data": f"show_user_chats:{user_id}:0"},
                    {"text": " В меню", "callback_data": f"back_to_menu:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, report_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка подсчета сообщений: {e}")
            await self.send_bot_message(chat_id, f"Ошибка анализа: {str(e)[:100]}")
    
    async def count_messages_in_chat(self, user, chat_identifier):
        """Подсчитывает сообщения пользователя в конкретном чате"""
        try:
            chat = await self.get_chat_by_identifier(chat_identifier)
            if not chat:
                return None
            
            message_count = 0
            try:
                async for message in self.client.iter_messages(
                    chat,
                    limit=500,
                    from_user=user
                ):
                    if message:
                        message_count += 1
            except:
                message_count = 0
            
            chat_name = getattr(chat, 'title', 
                              getattr(chat, 'username', 
                                     f'Чат {chat.id}'))
            
            return (chat_name, message_count)
            
        except Exception as e:
            print(f"Ошибка подсчета в чате {chat_identifier}: {e}")
            return None
    
    async def get_user_statistics(self, user_id: int) -> Dict:
        """Получает статистику пользователя"""
        stats = {
            "common_chats": 0,
            "total_messages": 0,
            "friends": [],
            "activity_level": "низкая",
            "common_chats_list": [],
            "avg_per_day": 0.0
        }
        
        try:
            if user_id in self.monitored_users:
                stats["total_messages"] = self.monitored_users[user_id].total_messages
            
            dialogs = await self.client.get_dialogs(limit=50)
            common_chats = []
            
            for dialog in dialogs:
                if dialog.is_group:
                    try:
                        participants = await self.client.get_participants(dialog.entity, limit=100)
                        user_ids = [p.id for p in participants if hasattr(p, 'id')]
                        if user_id in user_ids:
                            common_chats.append({
                                "id": dialog.id,
                                "title": dialog.name[:50],
                                "type": "group"
                            })
                    except:
                        continue
                elif dialog.is_channel:
                    pass
                elif dialog.is_user:
                    if dialog.entity.id == user_id:
                        common_chats.append({
                            "id": dialog.id,
                            "title": dialog.name[:50],
                            "type": "private"
                        })
            
            stats["common_chats"] = len(common_chats)
            stats["common_chats_list"] = common_chats
            
            friends = []
            for dialog in dialogs:
                if dialog.is_user and dialog.entity.id != user_id:
                    try:
                        messages = await self.client.get_messages(
                            dialog.entity,
                            limit=10,
                            from_user=user_id
                        )
                        
                        if len(messages) > 0:
                            user = dialog.entity
                            name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                            if not name:
                                name = f"User {user.id}"
                            if user.username:
                                name += f" (@{user.username})"
                            
                            friends.append(name)
                    except:
                        continue
            
            stats["friends"] = friends[:10]
            
            if stats["total_messages"] > 0:
                stats["avg_per_day"] = stats["total_messages"] / 30.0
            
            if stats["common_chats"] > 15 or stats["total_messages"] > 500:
                stats["activity_level"] = "очень высокая"
            elif stats["common_chats"] > 8 or stats["total_messages"] > 200:
                stats["activity_level"] = "высокая"
            elif stats["common_chats"] > 3 or stats["total_messages"] > 50:
                stats["activity_level"] = "средняя"
            else:
                stats["activity_level"] = "низкая"
            
        except Exception as e:
            print(f"Error getting stats: {e}")
        
        return stats
    
    async def ask_search_text(self, chat_id: int, user_id: int):
        """Запрашивает текст для поиска"""
        await self.send_bot_message(chat_id,
            f"Поиск сообщений пользователя\n\n"
            f"Введите текст для поиска в сообщениях пользователя:\n\n"
            f"Пример: 'привет' или 'как дела'\n\n"
            f"Пользователь: {user_id}\n"
            f"Я найду все сообщения с этим текстом и отправлю ссылки на них."
        )
        
        self.user_states[chat_id] = {
            "action": "waiting_search_text",
            "user_id": user_id
        }
    
    async def search_user_messages(self, chat_id: int, user_id: int, search_text: str):
        """Ищет сообщения пользователя"""
        try:
            await self.send_bot_message(chat_id, 
                f"ЗАПУСКАЮ ПОИСК СООБЩЕНИЙ\n\n"
                f"Искомый текст: '{search_text}'\n"
                f"Пользователь ID: {user_id}\n"
                f"Начинаю сканирование чатов..."
            )
            
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "Нет чатов для поиска!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "@durov\n@telegram\n"
                    "https://t.me/+tmE98W5NO6xlYmQy"
                )
                return
            
            found_messages = []
            checked_chats = 0
            
            await self.send_bot_message(chat_id,
                f"СКАНИРУЮ ИСТОРИЮ СООБЩЕНИЙ\n\n"
                f"Всего чатов для сканирования: {len(chats)}\n"
                f"Искомый текст: '{search_text}'\n"
                f"Ожидаемое время: {len(chats) // 15 + 1} минут\n\n"
                f"Начинаю глубокий поиск..."
            )
            
            for i in range(0, len(chats), 15):
                batch = chats[i:min(i + 15, len(chats))]
                batch_start = time.time()
                
                tasks = []
                for chat_identifier in batch:
                    task = asyncio.create_task(self.search_in_chat(user, chat_identifier, search_text))
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        continue
                    
                    if result:
                        chat_found_messages = result
                        if chat_found_messages:
                            found_messages.extend(chat_found_messages)
                        checked_chats += 1
                
                progress_percent = min(100, int((i + len(batch)) / len(chats) * 100))
                batch_time = time.time() - batch_start
                
                progress_msg = (
                    f"ПОИСК В ПРОЦЕССЕ\n\n"
                    f"Прогресс: {progress_percent}%\n"
                    f"Проверено чатов: {checked_chats}/{len(chats)}\n"
                    f"Найдено сообщений: {len(found_messages)}\n"
                    f"Скорость: {len(batch)/batch_time:.1f} чатов/сек\n"
                    f"Осталось: {(len(chats) - checked_chats) / 12:.0f} секунд\n\n"
                    f"Продолжаю сканирование..."
                )
                
                await self.send_bot_message(chat_id, progress_msg)
                
                if len(found_messages) <= 10 and found_messages:
                    for msg in found_messages[-min(3, len(found_messages)):]:
                        msg_text = (
                            f"НАЙДЕНО:\n\n"
                            f"Чат: {msg['chat']}\n"
                            f"Дата: {msg['date']}\n"
                            f"Текст: {msg['text']}\n"
                            f"Ссылка: {msg['link']}"
                        )
                        await self.send_bot_message(chat_id, msg_text)
                
                if i + 15 < len(chats):
                    await asyncio.sleep(1)
            
            if found_messages:
                total_text = (
                    f"ПОИСК ЗАВЕРШЕН УСПЕШНО!\n\n"
                    f"Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"Искомый текст: '{search_text}'\n"
                    f"Найдено сообщений: {len(found_messages)}\n"
                    f"Проверено чатов: {checked_chats} из {len(chats)}\n"
                    f"Время поиска: {time.time() - batch_start:.0f} секунд\n\n"
                )
                
                chat_stats = {}
                for msg in found_messages:
                    chat_name = msg['chat']
                    chat_stats[chat_name] = chat_stats.get(chat_name, 0) + 1
                
                if chat_stats:
                    total_text += f"РАСПРЕДЕЛЕНИЕ ПО ЧАТАМ:\n"
                    sorted_chats = sorted(chat_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                    for chat_name, count in sorted_chats:
                        total_text += f"• {chat_name[:25]}: {count} сообщ.\n"
                
                if len(found_messages) > 10:
                    total_text += f"\nПОКАЗАНО: 10 из {len(found_messages)}\n"
                    if len(found_messages) > 20:
                        total_text += f"Остальные результаты можно просмотреть в истории чата"
            else:
                total_text = (
                    f"СООБЩЕНИЙ НЕ НАЙДЕНО\n\n"
                    f"Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"Искомый текст: '{search_text}'\n"
                    f"Проверено чатов: {checked_chats} из {len(chats)}\n"
                    f"Время поиска: {time.time() - batch_start:.0f} секунд\n\n"
                    f"Попробуйте изменить поисковый запрос или проверьте другие чаты"
                )
            
            keyboard = self.create_keyboard([
                [
                    {"text": " Новый поиск", "callback_data": f"search_messages:{user_id}"},
                    {"text": " Полная статистика", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": " Чаты пользователя", "callback_data": f"show_user_chats:{user_id}:0"},
                    {"text": " В меню", "callback_data": f"back_to_menu:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, total_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка поиска сообщений: {e}")
            await self.send_bot_message(chat_id, f"Ошибка поиска: {str(e)[:100]}")
    
    async def search_in_chat(self, user, chat_identifier, search_text):
        """Ищет сообщения в конкретном чате"""
        try:
            chat = await self.get_chat_by_identifier(chat_identifier)
            if not chat:
                return []
            
            found_messages = []
            
            async for message in self.client.iter_messages(
                chat,
                limit=300,
                from_user=user
            ):
                if message and message.text and search_text.lower() in message.text.lower():
                    link = await self.get_message_link(chat, message.id)
                    
                    chat_name = getattr(chat, 'title', 
                                      getattr(chat, 'username', 
                                             f'Чат {chat.id}'))
                    
                    found_messages.append({
                        "chat": chat_name,
                        "text": message.text[:150] + "..." if len(message.text) > 150 else message.text,
                        "date": message.date.strftime("%d.%m.%Y %H:%M"),
                        "link": link,
                        "chat_id": chat.id,
                        "message_id": message.id
                    })
            
            return found_messages
            
        except Exception as e:
            print(f"Ошибка поиска в чате {chat_identifier}: {e}")
            return []
    
    async def toggle_message_monitoring(self, chat_id: int, user_id: int):
        """Включает/выключает отслеживание сообщений"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            if user_id not in self.tracking_status:
                self.tracking_status[user_id] = {'messages': False, 'avatar': False, 'replies': False}
            
            current_state = self.tracking_status[user_id]['messages']
            
            if current_state:
                await self.stop_message_monitoring(user_id)
                self.tracking_status[user_id]['messages'] = False
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_messages = False
                
                await self.send_bot_message(chat_id,
                    f"Отслеживание сообщений остановлено\n\n"
                    f"Пользователь: {user.first_name}\n"
                    f"ID: {user_id}\n\n"
                    f"Вы больше не будете получать уведомления о новых сообщениях."
                )
            else:
                task = asyncio.create_task(
                    self.monitor_user_messages(chat_id, user_id),
                    name=f"msg_monitor_{user_id}"
                )
                self.tracking_tasks.append(task)
                
                self.tracking_status[user_id]['messages'] = True
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_messages = True
                
                await self.send_bot_message(chat_id,
                    f"Отслеживание сообщений включено\n\n"
                    f"Пользователь: {user.first_name}\n"
                    f"ID: {user_id}\n"
                    f"Username: @{user.username if user.username else 'нет'}\n\n"
                    f"Теперь вы будете получать уведомления о новых сообщениях.\n"
                    f"Проверка каждые 2 минуты."
                )
            
            self.save_monitored_users()
            
            await self.show_user_actions(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка переключения отслеживания сообщений: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def stop_message_monitoring(self, user_id: int):
        """Останавливает отслеживание сообщений"""
        for task in self.tracking_tasks:
            if not task.done() and task.get_name() == f"msg_monitor_{user_id}":
                task.cancel()
                print(f"Остановлено отслеживание сообщений для {user_id}")
                break
    
    async def monitor_user_messages(self, chat_id: int, user_id: int):
        """Мониторит сообщения пользователя"""
        print(f"Запущен мониторинг сообщений для пользователя {user_id}")
        
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            last_check = datetime.now() - timedelta(minutes=5)
            
            while self.tracking_status.get(user_id, {}).get('messages', False):
                try:
                    dialogs = await self.client.get_dialogs(limit=30)
                    
                    new_messages_found = False
                    
                    for dialog in dialogs:
                        if dialog.is_group or dialog.is_channel:
                            try:
                                messages = await self.client.get_messages(
                                    dialog.entity,
                                    limit=20,
                                    offset_date=last_check
                                )
                                
                                for message in messages:
                                    if (message and message.date > last_check and
                                        hasattr(message, 'from_id') and message.from_id):
                                        
                                        sender_id = None
                                        if hasattr(message.from_id, 'user_id'):
                                            sender_id = message.from_id.user_id
                                        elif hasattr(message, 'sender_id') and hasattr(message.sender_id, 'user_id'):
                                            sender_id = message.sender_id.user_id
                                        
                                        if sender_id == user_id and message.text:
                                            link = await self.get_message_link(dialog.entity, message.id)
                                            
                                            notification = (
                                                f"НОВОЕ СООБЩЕНИЕ!\n\n"
                                                f"От: {user.first_name}\n"
                                                f"Чат: {dialog.name[:50]}\n"
                                                f"Время: {message.date.strftime('%H:%M:%S')}\n"
                                                f"Текст: {message.text[:200]}\n"
                                                f"Ссылка: {link}"
                                            )
                                            
                                            await self.send_bot_message(chat_id, notification)
                                            new_messages_found = True
                                            
                                            if message.date > last_check:
                                                last_check = message.date
                                            
                            except Exception as e:
                                print(f"Ошибка проверки чата {dialog.name}: {e}")
                                continue
                    
                    if new_messages_found:
                        print(f"Найдены новые сообщения от {user_id}")
                    
                    await asyncio.sleep(120)
                    last_check = datetime.now() - timedelta(minutes=2)
                    
                except Exception as e:
                    print(f"Ошибка в цикле мониторинга: {e}")
                    await asyncio.sleep(300)
                    
        except Exception as e:
            print(f"Мониторинг сообщений остановлен для {user_id}: {e}")
    
    async def toggle_avatar_monitoring(self, chat_id: int, user_id: int):
        """Включает/выключает отслеживание аватарки"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            if user_id not in self.tracking_status:
                self.tracking_status[user_id] = {'messages': False, 'avatar': False, 'replies': False}
            
            current_state = self.tracking_status[user_id]['avatar']
            
            if current_state:
                await self.stop_avatar_monitoring(user_id)
                self.tracking_status[user_id]['avatar'] = False
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_avatar = False
                
                await self.send_bot_message(chat_id,
                    f"Отслеживание аватарки остановлено\n\n"
                    f"Пользователь: {user.first_name}\n"
                    f"ID: {user_id}\n\n"
                    f"Вы больше не будете получать уведомления о смене аватарки."
                )
            else:
                current_avatar = await self.get_user_avatar_bytes(user_id)
                current_hash = hashlib.md5(current_avatar).hexdigest() if current_avatar else "no_avatar"
                self.avatar_cache[user_id] = current_hash
                
                task = asyncio.create_task(
                    self.monitor_user_avatar(chat_id, user_id),
                    name=f"avatar_monitor_{user_id}"
                )
                self.tracking_tasks.append(task)
                
                self.tracking_status[user_id]['avatar'] = True
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_avatar = True
                
                await self.send_bot_message(chat_id,
                    f"Отслеживание аватарки включено\n\n"
                    f"Пользователь: {user.first_name}\n"
                    f"ID: {user_id}\n"
                    f"Username: @{user.username if user.username else 'нет'}\n\n"
                    f"Теперь вы будете получать новую аватарку при ее смене.\n"
                    f"Проверка каждые 30 минут."
                )
                
                if current_avatar:
                    caption = f"Текущая аватарка\n {user.first_name}\nID: {user_id}"
                    await self.send_bot_message(chat_id, caption, photo=current_avatar)
            
            self.save_monitored_users()
            
            await self.show_user_actions(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка переключения отслеживания аватарки: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def stop_avatar_monitoring(self, user_id: int):
        """Останавливает отслеживание аватарки"""
        for task in self.tracking_tasks:
            if not task.done() and task.get_name() == f"avatar_monitor_{user_id}":
                task.cancel()
                print(f"Остановлено отслеживание аватарки для {user_id}")
                break
        
        if user_id in self.avatar_cache:
            del self.avatar_cache[user_id]
    
    async def monitor_user_avatar(self, chat_id: int, user_id: int):
        """Мониторит аватарку пользователя"""
        print(f"Запущен мониторинг аватарки для пользователя {user_id}")
        
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            while self.tracking_status.get(user_id, {}).get('avatar', False):
                try:
                    current_avatar = await self.get_user_avatar_bytes(user_id)
                    current_hash = hashlib.md5(current_avatar).hexdigest() if current_avatar else "no_avatar"
                    old_hash = self.avatar_cache.get(user_id, "")
                    
                    if current_hash != old_hash:
                        print(f"Обнаружена смена аватарки у {user_id}")
                        
                        self.avatar_cache[user_id] = current_hash
                        
                        if current_avatar:
                            caption = (
                                f"СМЕНА АВАТАРКИ!\n\n"
                                f"Пользователь: {user.first_name}\n"
                                f"ID: {user_id}\n"
                                f"Username: @{user.username if user.username else 'нет'}\n"
                                f"Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
                            )
                            
                            await self.send_bot_message(chat_id, caption, photo=current_avatar)
                        else:
                            await self.send_bot_message(chat_id,
                                f"АВАТАРКА УДАЛЕНА\n\n"
                                f"Пользователь: {user.first_name}\n"
                                f"ID: {user_id}\n"
                                f"Время: {datetime.now().strftime('%H:%M:%S')}"
                            )
                    
                    await asyncio.sleep(1800)
                    
                except Exception as e:
                    print(f"Ошибка в цикле мониторинга аватарки: {e}")
                    await asyncio.sleep(600)
                    
        except Exception as e:
            print(f"Мониторинг аватарки остановлен для {user_id}: {e}")
    
    async def toggle_reply_monitoring(self, chat_id: int, user_id: int):
        """Включает/выключает отслеживание ответов на сообщения"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            if user_id not in self.tracking_status:
                self.tracking_status[user_id] = {'messages': False, 'avatar': False, 'replies': False}
            
            current_state = self.tracking_status[user_id]['replies']
            
            if current_state:
                await self.stop_reply_monitoring(user_id)
                self.tracking_status[user_id]['replies'] = False
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_replies = False
                
                await self.send_bot_message(chat_id,
                    f"Отслеживание ответов остановлено\n\n"
                    f"Пользователь: {user.first_name}\n"
                    f"ID: {user_id}\n\n"
                    f"Вы больше не будете получать уведомления о ответах на сообщения."
                )
            else:
                task = asyncio.create_task(
                    self.monitor_user_replies(chat_id, user_id),
                    name=f"reply_monitor_{user_id}"
                )
                self.tracking_tasks.append(task)
                
                self.tracking_status[user_id]['replies'] = True
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_replies = True
                
                await self.send_bot_message(chat_id,
                    f"Отслеживание ответов включено\n\n"
                    f"Пользователь: {user.first_name}\n"
                    f"ID: {user_id}\n"
                    f"Username: @{user.username if user.username else 'нет'}\n\n"
                    f"Теперь вы будете получать уведомления о том, кто отвечает на сообщения пользователя.\n"
                    f"Проверка каждые 5 минут."
                )
            
            self.save_monitored_users()
            
            await self.show_user_actions(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка переключения отслеживания ответов: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def stop_reply_monitoring(self, user_id: int):
        """Останавливает отслеживание ответов"""
        for task in self.tracking_tasks:
            if not task.done() and task.get_name() == f"reply_monitor_{user_id}":
                task.cancel()
                print(f"Остановлено отслеживание ответов для {user_id}")
                break
        
        if user_id in self.last_message_ids:
            del self.last_message_ids[user_id]
    
    async def monitor_user_replies(self, chat_id: int, user_id: int):
        """Мониторит ответы на сообщения пользователя"""
        print(f"Запущен мониторинг ответов для пользователя {user_id}")
        
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            last_check = datetime.now() - timedelta(minutes=10)
            
            while self.tracking_status.get(user_id, {}).get('replies', False):
                try:
                    chat_identifiers = await self.load_chats_list()
                    
                    if not chat_identifiers:
                        print(f"Нет чатов для мониторинга ответов у {user_id}")
                        await asyncio.sleep(600)
                        continue
                    
                    if user_id not in self.last_message_ids:
                        self.last_message_ids[user_id] = {}
                    
                    new_replies_found = False
                    
                    for chat_identifier in chat_identifiers:
                        try:
                            chat = await self.get_chat_by_identifier(chat_identifier)
                            if not chat:
                                continue
                            
                            messages = await self.client.get_messages(
                                chat,
                                limit=10,
                                from_user=user,
                                offset_date=last_check
                            )
                            
                            for message in messages:
                                if not message or message.date <= last_check:
                                    continue
                                
                                last_msg_id = self.last_message_ids[user_id].get(chat.id, 0)
                                
                                if message.id > last_msg_id:
                                    self.last_message_ids[user_id][chat.id] = message.id
                                    
                                    await asyncio.sleep(2)
                                    
                                    try:
                                        replies = await self.client.get_messages(
                                            chat,
                                            min_id=message.id,
                                            limit=20
                                        )
                                        
                                        for reply in replies:
                                            if (reply and reply.reply_to and 
                                                reply.reply_to.reply_to_msg_id == message.id and
                                                hasattr(reply, 'from_id') and reply.from_id):
                                                
                                                try:
                                                    reply_sender = await self.client.get_entity(reply.from_id)
                                                    sender_name = getattr(reply_sender, 'first_name', '')
                                                    if hasattr(reply_sender, 'last_name') and reply_sender.last_name:
                                                        sender_name += f" {reply_sender.last_name}"
                                                    if hasattr(reply_sender, 'username') and reply_sender.username:
                                                        sender_name += f" (@{reply_sender.username})"
                                                    
                                                    link = await self.get_message_link(chat, reply.id)
                                                    original_link = await self.get_message_link(chat, message.id)
                                                    
                                                    notification = (
                                                        f"ОТВЕТ НА СООБЩЕНИЕ!\n\n"
                                                        f"На кого ответили: {user.first_name}\n"
                                                        f"Кто ответил: {sender_name or 'Неизвестный'}\n"
                                                        f"Чат: {getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))[:50]}\n"
                                                        f"Время ответа: {reply.date.strftime('%H:%M:%S')}\n"
                                                        f"Ответ: {reply.text[:200] if reply.text else 'нет текста'}\n"
                                                        f"Ответ: {link}\n"
                                                        f"Оригинал: {original_link}"
                                                    )
                                                    
                                                    await self.send_bot_message(chat_id, notification)
                                                    new_replies_found = True
                                                    
                                                except Exception as e:
                                                    print(f"Ошибка получения информации об авторе ответа: {e}")
                                                    continue
                                        
                                    except Exception as e:
                                        print(f"Ошибка проверки ответов в чате {chat_identifier}: {e}")
                                        continue
                        
                        except Exception as e:
                            print(f"Ошибка мониторинга чата {chat_identifier}: {e}")
                            continue
                        
                        await asyncio.sleep(1)
                    
                    if new_replies_found:
                        print(f"Найдены новые ответы на сообщения {user_id}")
                    
                    last_check = datetime.now()
                    
                    await asyncio.sleep(300)
                    
                except Exception as e:
                    print(f"Ошибка в цикле мониторинга ответов: {e}")
                    await asyncio.sleep(300)
                    
        except Exception as e:
            print(f"Мониторинг ответов остановлен для {user_id}: {e}")
    
    async def show_replies_menu(self, chat_id: int, user_id: int):
        """Показывает меню анализа ответов"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            menu_text = (
                f"АНАЛИЗ ОТВЕТОВ\n\n"
                f"Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"ID: {user_id}\n\n"
                f"Выберите тип анализа:"
            )
            
            keyboard_buttons = [
                [
                    {"text": " Кто отвечает пользователю", "callback_data": f"replies_to_user:{user_id}:0"},
                    {"text": " Кому отвечает пользователь", "callback_data": f"replies_from_user:{user_id}:0"}
                ],
                [
                    {"text": " Поиск по конкретному юзеру", "callback_data": f"search_replies_to:{user_id}"},
                    {"text": " Поиск кому отвечает юзеру", "callback_data": f"search_replies_from:{user_id}"}
                ],
                [
                    {"text": " Назад", "callback_data": f"back_to_menu:{user_id}"},
                    {"text": " Обновить", "callback_data": f"show_replies:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, menu_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа меню ответов: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def ask_target_user_for_replies_to(self, chat_id: int, user_id: int):
        """Запрашивает пользователя для поиска реплаев КОМУ"""
        await self.send_bot_message(chat_id,
            f"Поиск: кто отвечает пользователю\n\n"
            f"Введите @username или ID пользователя, чтобы проверить:\n"
            f"• Отвечает ли этот пользователь нашему пользователю\n"
            f"• Сколько раз он отвечал\n"
            f"• Ссылки на все ответы\n\n"
            f"Наш пользователь: {user_id}\n\n"
            f"Введите @username или ID целевого пользователя:"
        )
        
        self.user_states[chat_id] = {
            "action": "waiting_target_user_for_replies_to",
            "user_id": user_id
        }
    
    async def ask_target_user_for_replies_from(self, chat_id: int, user_id: int):
        """Запрашивает пользователя для поиска реплаев ОТ КОГО"""
        await self.send_bot_message(chat_id,
            f"Поиск: кому отвечает пользователь\n\n"
            f"Введите @username или ID пользователя, чтобы проверить:\n"
            f"• Отвечает ли наш пользователь этому пользователю\n"
            f"• Сколько раз он отвечал\n"
            f"• Ссылки на все ответы\n\n"
            f"Наш пользователь: {user_id}\n\n"
            f"Введите @username или ID целевого пользователя:"
        )
        
        self.user_states[chat_id] = {
            "action": "waiting_target_user_for_replies_from",
            "user_id": user_id
        }
    
    async def search_replies_to_user(self, chat_id: int, user_id: int, target_user_input: str):
        """Ищет ответы от конкретного пользователя нашему пользователю"""
        try:
            await self.send_bot_message(chat_id, 
                f"ЗАПУСКАЮ ПОИСК ОТВЕТОВ\n\n"
                f"Ищу кто отвечает: '{target_user_input}'\n"
                f"Наш пользователь ID: {user_id}\n"
                f"Начинаю сканирование чатов..."
            )
            
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            target_user = None
            target_user_input_clean = target_user_input.strip().replace('@', '')
            
            if target_user_input_clean.isdigit() or (target_user_input_clean.startswith('-') and target_user_input_clean[1:].isdigit()):
                target_user_id = int(target_user_input_clean)
                try:
                    target_user = await self.client.get_entity(target_user_id)
                except Exception as e1:
                    print(f"Попытка 1 поиска по ID {target_user_id} не удалась: {e1}")
                    try:
                        target_user = await self.client.get_entity(PeerUser(target_user_id))
                    except Exception as e2:
                        print(f"Попытка 2 поиска по ID {target_user_id} не удалась: {e2}")
                        await self.send_bot_message(chat_id, f"Пользователь с ID {target_user_id} не найден")
                        return
            else:
                username = target_user_input_clean
                try:
                    target_user = await self.client.get_entity(username)
                except errors.UsernameNotOccupiedError:
                    await self.send_bot_message(chat_id, f"Пользователь @{username} не существует")
                    return
                except Exception as e:
                    try:
                        target_user = await self.client.get_entity(f"@{username}")
                    except:
                        await self.send_bot_message(chat_id, f"Ошибка поиска целевого пользователя: {str(e)[:100]}")
                        return
            
            if not target_user:
                await self.send_bot_message(chat_id, "Не удалось получить информацию о целевом пользователе")
                return
            
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "Нет чатов для поиска!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "@durov\n@telegram\n"
                    "https://t.me/+tmE98W5NO6xlYmQy"
                )
                return
            
            found_replies = []
            checked_chats = 0
            start_date = datetime.now() - timedelta(days=30)
            
            await self.send_bot_message(chat_id,
                f"СКАНИРУЮ ИСТОРИЮ ОТВЕТОВ\n\n"
                f"Ищу ответы от: {target_user.first_name if hasattr(target_user, 'first_name') else 'ID: ' + str(target_user.id)}\n"
                f"Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"Всего чатов для сканирования: {len(chats)}\n"
                f"Ожидаемое время: {len(chats) // 10 + 1} минут\n\n"
                f"Начинаю глубокий поиск ответов..."
            )
            
            for i in range(0, len(chats), 10):
                batch = chats[i:min(i + 10, len(chats))]
                batch_start = time.time()
                
                tasks = []
                for chat_identifier in batch:
                    task = asyncio.create_task(self.search_replies_in_chat(user, target_user, chat_identifier, start_date, "to"))
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        continue
                    
                    if result:
                        chat_found_replies = result
                        if chat_found_replies:
                            found_replies.extend(chat_found_replies)
                        checked_chats += 1
                
                progress_percent = min(100, int((i + len(batch)) / len(chats) * 100))
                batch_time = time.time() - batch_start
                
                progress_msg = (
                    f"ПОИСК ОТВЕТОВ В ПРОЦЕССЕ\n\n"
                    f"Прогресс: {progress_percent}%\n"
                    f"Проверено чатов: {checked_chats}/{len(chats)}\n"
                    f"Найдено ответов: {len(found_replies)}\n"
                    f"Скорость: {len(batch)/batch_time:.1f} чатов/сек\n"
                    f"Осталось: {(len(chats) - checked_chats) / 8:.0f} секунд\n\n"
                    f"Анализирую взаимодействия..."
                )
                
                await self.send_bot_message(chat_id, progress_msg)
                
                if len(found_replies) <= 5 and found_replies:
                    for reply in found_replies[-min(2, len(found_replies)):]:
                        reply_info = (
                            f"НАЙДЕН ОТВЕТ:\n\n"
                            f"От: {reply['replier']}\n"
                            f"Чат: {reply['chat'][:30]}\n"
                            f"Время: {reply['reply_time']}\n"
                            f"Ответ: {reply['reply_text']}\n"
                            f"Ответ: {reply['reply_link']}"
                        )
                        await self.send_bot_message(chat_id, reply_info)
                
                if i + 10 < len(chats):
                    await asyncio.sleep(1)
            
            if found_replies:
                target_name = getattr(target_user, 'first_name', '')
                if hasattr(target_user, 'last_name') and target_user.last_name:
                    target_name += f" {target_user.last_name}"
                if hasattr(target_user, 'username') and target_user.username:
                    target_name += f" (@{target_user.username})"
                
                total_text = (
                    f"ПОИСК ОТВЕТОВ ЗАВЕРШЕН!\n\n"
                    f"Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"Целевой пользователь: {target_name}\n"
                    f"ID целевого: {target_user.id}\n"
                    f"Найдено ответов: {len(found_replies)}\n"
                    f"Проверено чатов: {checked_chats} из {len(chats)}\n"
                    f"Период: последние 30 дней\n"
                    f"Время поиска: {time.time() - batch_start:.0f} секунд\n\n"
                )
                
                chat_stats = {}
                for reply in found_replies:
                    chat_name = reply['chat']
                    chat_stats[chat_name] = chat_stats.get(chat_name, 0) + 1
                
                if chat_stats:
                    total_text += f"РАСПРЕДЕЛЕНИЕ ПО ЧАТАМ:\n"
                    sorted_chats = sorted(chat_stats.items(), key=lambda x: x[1], reverse=True)[:3]
                    for chat_name, count in sorted_chats:
                        total_text += f"• {chat_name[:20]}: {count} ответов\n"
                
                if len(found_replies) > 5:
                    total_text += f"\nПОКАЗАНО: 5 из {len(found_replies)}\n"
                    if len(found_replies) > 10:
                        total_text += f"Остальные результаты можно просмотреть в истории чата"
            else:
                target_name = getattr(target_user, 'first_name', '')
                if hasattr(target_user, 'last_name') and target_user.last_name:
                    target_name += f" {target_user.last_name}"
                if hasattr(target_user, 'username') and target_user.username:
                    target_name += f" (@{target_user.username})"
                
                total_text = (
                    f"ОТВЕТЫ НЕ НАЙДЕНЫ\n\n"
                    f"Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"Целевой пользователь: {target_name}\n"
                    f"ID целевого: {target_user.id}\n"
                    f"Проверено чатов: {checked_chats} из {len(chats)}\n"
                    f"Период: последние 30 дней\n"
                    f"Время поиска: {time.time() - batch_start:.0f} секунд\n\n"
                    f"Пользователь {target_name} не отвечал на сообщения нашего пользователя за последние 30 дней"
                )
            
            keyboard = self.create_keyboard([
                [
                    {"text": " Новый поиск", "callback_data": f"search_replies_to:{user_id}"},
                    {"text": " Полная статистика", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": " Анализ ответов", "callback_data": f"show_replies:{user_id}"},
                    {"text": " В меню", "callback_data": f"back_to_menu:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, total_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка поиска ответов от пользователя: {e}")
            await self.send_bot_message(chat_id, f"Ошибка поиска: {str(e)[:100]}")
    
    async def search_replies_in_chat(self, user, target_user, chat_identifier, start_date, direction):
        """Ищет реплаи в конкретном чате"""
        try:
            chat = await self.get_chat_by_identifier(chat_identifier)
            if not chat:
                return []
            
            found_replies = []
            
            if direction == "to":
                user_messages = []
                async for message in self.client.iter_messages(
                    chat,
                    limit=100,
                    from_user=user,
                    offset_date=start_date
                ):
                    if message:
                        user_messages.append(message)
                
                for user_msg in user_messages:
                    try:
                        async for reply in self.client.iter_messages(
                            chat,
                            limit=10,
                            min_id=user_msg.id - 1
                        ):
                            if (reply and reply.reply_to and 
                                reply.reply_to.reply_to_msg_id == user_msg.id and
                                hasattr(reply, 'from_id') and reply.from_id):
                                
                                try:
                                    reply_sender = await self.client.get_entity(reply.from_id)
                                    
                                    if reply_sender.id == target_user.id:
                                        sender_name = getattr(reply_sender, 'first_name', '')
                                        if hasattr(reply_sender, 'last_name') and reply_sender.last_name:
                                            sender_name += f" {reply_sender.last_name}"
                                        if hasattr(reply_sender, 'username') and reply_sender.username:
                                            sender_name += f" (@{reply_sender.username})"
                                        
                                        reply_link = await self.get_message_link(chat, reply.id)
                                        original_link = await self.get_message_link(chat, user_msg.id)
                                        chat_name = getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))
                                        
                                        found_replies.append({
                                            "chat": chat_name,
                                            "original_text": user_msg.text[:100] if user_msg.text else "без текста",
                                            "reply_text": reply.text[:100] if reply.text else "без текста",
                                            "replier": sender_name or f"User {target_user.id}",
                                            "reply_time": reply.date.strftime("%d.%m.%Y %H:%M"),
                                            "reply_link": reply_link,
                                            "original_link": original_link,
                                            "chat_id": chat.id,
                                            "message_id": user_msg.id,
                                            "reply_id": reply.id
                                        })
                                        
                                        break
                                        
                                except:
                                    continue
                                    
                    except Exception as e:
                        print(f"Ошибка проверки ответов на сообщение {user_msg.id}: {e}")
                        continue
            else:
                async for message in self.client.iter_messages(
                    chat,
                    limit=100,
                    from_user=user,
                    offset_date=start_date
                ):
                    if message and message.reply_to:
                        try:
                            try:
                                original_msg = await self.client.get_messages(
                                    chat,
                                    ids=message.reply_to.reply_to_msg_id
                                )
                                
                                if original_msg and hasattr(original_msg, 'from_id') and original_msg.from_id:
                                    original_sender = await self.client.get_entity(original_msg.from_id)
                                    
                                    if original_sender.id == target_user.id:
                                        sender_name = getattr(original_sender, 'first_name', '')
                                        if hasattr(original_sender, 'last_name') and original_sender.last_name:
                                            sender_name += f" {original_sender.last_name}"
                                        if hasattr(original_sender, 'username') and original_sender.username:
                                            sender_name += f" (@{original_sender.username})"
                                        
                                        reply_link = await self.get_message_link(chat, message.id)
                                        original_link = await self.get_message_link(chat, original_msg.id)
                                        chat_name = getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))
                                        
                                        found_replies.append({
                                            "chat": chat_name,
                                            "original_text": original_msg.text[:100] if original_msg.text else "без текста",
                                            "reply_text": message.text[:100] if message.text else "без текста",
                                            "replied_to": sender_name or f"User {target_user.id}",
                                            "reply_time": message.date.strftime("%d.%m.%Y %H:%M"),
                                            "reply_link": reply_link,
                                            "original_link": original_link,
                                            "chat_id": chat.id,
                                            "message_id": original_msg.id,
                                            "reply_id": message.id
                                        })
                                        
                            except:
                                continue
                                
                        except:
                            continue
            
            return found_replies
            
        except Exception as e:
            print(f"Ошибка поиска реплаев в чате {chat_identifier}: {e}")
            return []
    
    async def search_replies_from_user(self, chat_id: int, user_id: int, target_user_input: str):
        """Ищет ответы нашего пользователя конкретному пользователю"""
        try:
            await self.send_bot_message(chat_id, 
                f"ЗАПУСКАЮ ПОИСК ОТВЕТОВ\n\n"
                f"Ищу кому отвечает: '{target_user_input}'\n"
                f"Наш пользователь ID: {user_id}\n"
                f"Начинаю сканирование чатов..."
            )
            
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            target_user = None
            target_user_input_clean = target_user_input.strip().replace('@', '')
            
            if target_user_input_clean.isdigit() or (target_user_input_clean.startswith('-') and target_user_input_clean[1:].isdigit()):
                target_user_id = int(target_user_input_clean)
                try:
                    target_user = await self.client.get_entity(target_user_id)
                except Exception as e1:
                    print(f"Попытка 1 поиска по ID {target_user_id} не удалась: {e1}")
                    try:
                        target_user = await self.client.get_entity(PeerUser(target_user_id))
                    except Exception as e2:
                        print(f"Попытка 2 поиска по ID {target_user_id} не удалась: {e2}")
                        await self.send_bot_message(chat_id, f"Пользователь с ID {target_user_id} не найден")
                        return
            else:
                username = target_user_input_clean
                try:
                    target_user = await self.client.get_entity(username)
                except errors.UsernameNotOccupiedError:
                    await self.send_bot_message(chat_id, f"Пользователь @{username} не существует")
                    return
                except Exception as e:
                    try:
                        target_user = await self.client.get_entity(f"@{username}")
                    except:
                        await self.send_bot_message(chat_id, f"Ошибка поиска целевого пользователя: {str(e)[:100]}")
                        return
            
            if not target_user:
                await self.send_bot_message(chat_id, "Не удалось получить информацию о целевом пользователе")
                return
            
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "Нет чатов для поиска!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "@durov\n@telegram\n"
                    "https://t.me/+tmE98W5NO6xlYmQy"
                )
                return
            
            found_replies = []
            checked_chats = 0
            start_date = datetime.now() - timedelta(days=30)
            
            await self.send_bot_message(chat_id,
                f"СКАНИРУЮ ИСТОРИЮ ОТВЕТОВ\n\n"
                f"Ищу ответы нашему пользователя: {target_user.first_name if hasattr(target_user, 'first_name') else 'ID: ' + str(target_user.id)}\n"
                f"Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"Всего чатов для сканирования: {len(chats)}\n"
                f"Ожидаемое время: {len(chats) // 10 + 1} минут\n\n"
                f"Начинаю глубокий поиск ответов..."
            )
            
            for i in range(0, len(chats), 10):
                batch = chats[i:min(i + 10, len(chats))]
                batch_start = time.time()
                
                tasks = []
                for chat_identifier in batch:
                    task = asyncio.create_task(self.search_replies_in_chat(user, target_user, chat_identifier, start_date, "from"))
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        continue
                    
                    if result:
                        chat_found_replies = result
                        if chat_found_replies:
                            found_replies.extend(chat_found_replies)
                        checked_chats += 1
                
                progress_percent = min(100, int((i + len(batch)) / len(chats) * 100))
                batch_time = time.time() - batch_start
                
                progress_msg = (
                    f"ПОИСК ОТВЕТОВ В ПРОЦЕССЕ\n\n"
                    f"Прогресс: {progress_percent}%\n"
                    f"Проверено чатов: {checked_chats}/{len(chats)}\n"
                    f"Найдено ответов: {len(found_replies)}\n"
                    f"Скорость: {len(batch)/batch_time:.1f} чатов/сек\n"
                    f"Осталось: {(len(chats) - checked_chats) / 8:.0f} секунд\n\n"
                    f"Анализирую взаимодействия..."
                )
                
                await self.send_bot_message(chat_id, progress_msg)
                
                if len(found_replies) <= 5 and found_replies:
                    for reply in found_replies[-min(2, len(found_replies)):]:
                        reply_info = (
                            f"НАЙДЕН ОТВЕТ:\n\n"
                            f"Кому: {reply['replied_to']}\n"
                            f"Чат: {reply['chat'][:30]}\n"
                            f"Время: {reply['reply_time']}\n"
                            f"Ответ: {reply['reply_text']}\n"
                            f"Ответ: {reply['reply_link']}"
                        )
                        await self.send_bot_message(chat_id, reply_info)
                
                if i + 10 < len(chats):
                    await asyncio.sleep(1)
            
            if found_replies:
                target_name = getattr(target_user, 'first_name', '')
                if hasattr(target_user, 'last_name') and target_user.last_name:
                    target_name += f" {target_user.last_name}"
                if hasattr(target_user, 'username') and target_user.username:
                    target_name += f" (@{target_user.username})"
                
                total_text = (
                    f"ПОИСК ОТВЕТОВ ЗАВЕРШЕН!\n\n"
                    f"Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"Целевой пользователь: {target_name}\n"
                    f"ID целевого: {target_user.id}\n"
                    f"Найдено ответов: {len(found_replies)}\n"
                    f"Проверено чатов: {checked_chats} из {len(chats)}\n"
                    f"Период: последние 30 дней\n"
                    f"Время поиска: {time.time() - batch_start:.0f} секунд\n\n"
                )
                
                chat_stats = {}
                for reply in found_replies:
                    chat_name = reply['chat']
                    chat_stats[chat_name] = chat_stats.get(chat_name, 0) + 1
                
                if chat_stats:
                    total_text += f"РАСПРЕДЕЛЕНИЕ ПО ЧАТАМ:\n"
                    sorted_chats = sorted(chat_stats.items(), key=lambda x: x[1], reverse=True)[:3]
                    for chat_name, count in sorted_chats:
                        total_text += f"• {chat_name[:20]}: {count} ответов\n"
                
                if len(found_replies) > 5:
                    total_text += f"\nПОКАЗАНО: 5 из {len(found_replies)}\n"
                    if len(found_replies) > 10:
                        total_text += f"Остальные результаты можно просмотреть в истории чата"
            else:
                target_name = getattr(target_user, 'first_name', '')
                if hasattr(target_user, 'last_name') and target_user.last_name:
                    target_name += f" {target_user.last_name}"
                if hasattr(target_user, 'username') and target_user.username:
                    target_name += f" (@{target_user.username})"
                
                total_text = (
                    f"ОТВЕТЫ НЕ НАЙДЕНЫ\n\n"
                    f"Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"Целевой пользователь: {target_name}\n"
                    f"ID целевого: {target_user.id}\n"
                    f"Проверено чатов: {checked_chats} из {len(chats)}\n"
                    f"Период: последние 30 дней\n"
                    f"Время поиска: {time.time() - batch_start:.0f} секунд\n\n"
                    f"Наш пользователь не отвечал на сообщения пользователя {target_name} за последние 30 дней"
                )
            
            keyboard = self.create_keyboard([
                [
                    {"text": " Новый поиск", "callback_data": f"search_replies_from:{user_id}"},
                    {"text": " Полная статистика", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": " Анализ ответов", "callback_data": f"show_replies:{user_id}"},
                    {"text": " В меню", "callback_data": f"back_to_menu:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, total_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка поиска ответов пользователя: {e}")
            await self.send_bot_message(chat_id, f"Ошибка поиска: {str(e)[:100]}")
    
    async def collect_replies_data(self, user_id: int):
        """Собирает данные о реплаях пользователя"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            chat_identifiers = await self.load_chats_list()
            
            if not chat_identifiers:
                return {"to_user": [], "from_user": []}
            
            replies_to_user = []
            replies_from_user = []
            
            start_date = datetime.now() - timedelta(days=7)
            
            for i, chat_identifier in enumerate(chat_identifiers, 1):
                try:
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    user_messages = []
                    async for message in self.client.iter_messages(
                        chat,
                        limit=100,
                        from_user=user,
                        offset_date=start_date
                    ):
                        if message:
                            user_messages.append(message)
                    
                    for user_msg in user_messages:
                        try:
                            async for reply in self.client.iter_messages(
                                chat,
                                limit=10,
                                min_id=user_msg.id - 1
                            ):
                                if (reply and reply.reply_to and 
                                    reply.reply_to.reply_to_msg_id == user_msg.id and
                                    hasattr(reply, 'from_id') and reply.from_id):
                                    
                                    try:
                                        reply_sender = await self.client.get_entity(reply.from_id)
                                        sender_name = getattr(reply_sender, 'first_name', '')
                                        if hasattr(reply_sender, 'last_name') and reply_sender.last_name:
                                            sender_name += f" {reply_sender.last_name}"
                                        if hasattr(reply_sender, 'username') and reply_sender.username:
                                            sender_name += f" (@{reply_sender.username})"
                                        
                                        reply_link = await self.get_message_link(chat, reply.id)
                                        original_link = await self.get_message_link(chat, user_msg.id)
                                        chat_name = getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))
                                        
                                        replies_to_user.append({
                                            "replier_id": reply_sender.id,
                                            "replier_name": sender_name or "Неизвестный",
                                            "chat_name": chat_name,
                                            "original_text": user_msg.text[:100] if user_msg.text else "без текста",
                                            "reply_text": reply.text[:100] if reply.text else "без текста",
                                            "reply_time": reply.date.strftime("%d.%m.%Y %H:%M"),
                                            "reply_link": reply_link,
                                            "original_link": original_link,
                                            "chat_id": chat.id,
                                            "message_id": user_msg.id,
                                            "reply_id": reply.id
                                        })
                                    except:
                                        continue
                                    break
                                    
                        except:
                            continue
                    
                    async for message in self.client.iter_messages(
                        chat,
                        limit=100,
                        from_user=user,
                        offset_date=start_date
                    ):
                        if message and message.reply_to:
                            try:
                                try:
                                    original_msg = await self.client.get_messages(
                                        chat,
                                        ids=message.reply_to.reply_to_msg_id
                                    )
                                    
                                    if original_msg and hasattr(original_msg, 'from_id') and original_msg.from_id:
                                        original_sender = await self.client.get_entity(original_msg.from_id)
                                        sender_name = getattr(original_sender, 'first_name', '')
                                        if hasattr(original_sender, 'last_name') and original_sender.last_name:
                                            sender_name += f" {original_sender.last_name}"
                                        if hasattr(original_sender, 'username') and original_sender.username:
                                            sender_name += f" (@{original_sender.username})"
                                        
                                        reply_link = await self.get_message_link(chat, message.id)
                                        original_link = await self.get_message_link(chat, original_msg.id)
                                        chat_name = getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))
                                        
                                        replies_from_user.append({
                                            "replied_to_id": original_sender.id,
                                            "replied_to_name": sender_name or "Неизвестный",
                                            "chat_name": chat_name,
                                            "original_text": original_msg.text[:100] if original_msg.text else "без текста",
                                            "reply_text": message.text[:100] if message.text else "без текста",
                                            "reply_time": message.date.strftime("%d.%m.%Y %H:%M"),
                                            "reply_link": reply_link,
                                            "original_link": original_link,
                                            "chat_id": chat.id,
                                            "message_id": original_msg.id,
                                            "reply_id": message.id
                                        })
                                except:
                                    continue
                                    
                            except:
                                continue
                
                except Exception as e:
                    print(f"Ошибка сбора данных в чате {chat_identifier}: {e}")
                    continue
                
                await asyncio.sleep(0.5)
            
            self.reply_data_cache[user_id] = {
                "to_user": replies_to_user,
                "from_user": replies_from_user
            }
            
            return self.reply_data_cache[user_id]
            
        except Exception as e:
            print(f"Ошибка сбора данных о реплаях: {e}")
            return {"to_user": [], "from_user": []}
    
    async def show_replies_to_user(self, chat_id: int, user_id: int, page: int = 0):
        """Показывает кто отвечает пользователю"""
        try:
            await self.send_bot_message(chat_id, "Собираю данные о том, кто отвечает пользователю...")
            
            reply_data = await self.collect_replies_data(user_id)
            replies_to_user = reply_data["to_user"]
            
            if not replies_to_user:
                await self.send_bot_message(chat_id,
                    f"Ответов не найдено\n\n"
                    f"Пользователь: {user_id}\n"
                    f"Период: последние 7 дней\n\n"
                    f"За последнюю неделю никто не отвечал на сообщения пользователя."
                )
                return
            
            user_stats = {}
            for reply in replies_to_user:
                replier_id = reply["replier_id"]
                if replier_id not in user_stats:
                    user_stats[replier_id] = {
                        "name": reply["replier_name"],
                        "count": 0,
                        "replies": []
                    }
                user_stats[replier_id]["count"] += 1
                user_stats[replier_id]["replies"].append(reply)
            
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
            
            items_per_page = 5
            total_pages = (len(sorted_users) + items_per_page - 1) // items_per_page
            
            if page >= total_pages:
                page = total_pages - 1
            
            start_idx = page * items_per_page
            end_idx = min((page + 1) * items_per_page, len(sorted_users))
            
            message_text = (
                f"КТО ОТВЕЧАЕТ ПОЛЬЗОВАТЕЛЮ\n\n"
                f"Пользователь: {user_id}\n"
                f"Всего отвечавших: {len(sorted_users)}\n"
                f"Всего ответов: {len(replies_to_user)}\n"
                f"Период: последние 7 дней\n\n"
                f"Страница {page + 1} из {total_pages}\n\n"
            )
            
            for i, (replier_id, stats) in enumerate(sorted_users[start_idx:end_idx], start_idx + 1):
                message_text += f"{i}. {stats['name']} - {stats['count']} ответов\n"
            
            keyboard_buttons = []
            
            nav_buttons = []
            if page > 0:
                nav_buttons.append({"text": " Назад", "callback_data": f"replies_to_user:{user_id}:{page-1}"})
            
            if page < total_pages - 1:
                nav_buttons.append({"text": "Вперёд ", "callback_data": f"replies_to_user:{user_id}:{page+1}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            for i, (replier_id, stats) in enumerate(sorted_users[start_idx:end_idx], start_idx):
                keyboard_buttons.append([
                    {"text": f" {stats['name'][:20]}", "callback_data": f"view_reply_pair:{user_id}:{i}:to"}
                ])
            
            keyboard_buttons.append([
                {"text": " Поиск по юзеру", "callback_data": f"search_replies_to:{user_id}"},
                {"text": " Назад к меню", "callback_data": f"show_replies:{user_id}"}
            ])
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа ответов пользователю: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def show_replies_from_user(self, chat_id: int, user_id: int, page: int = 0):
        """Показывает кому отвечает пользователь"""
        try:
            await self.send_bot_message(chat_id, "Собираю данные о том, кому отвечает пользователь...")
            
            reply_data = await self.collect_replies_data(user_id)
            replies_from_user = reply_data["from_user"]
            
            if not replies_from_user:
                await self.send_bot_message(chat_id,
                    f"Ответов не найдено\n\n"
                    f"Пользователь: {user_id}\n"
                    f"Период: последние 7 дней\n\n"
                    f"За последнюю неделю пользователь никому не отвечал."
                )
                return
            
            user_stats = {}
            for reply in replies_from_user:
                replied_to_id = reply["replied_to_id"]
                if replied_to_id not in user_stats:
                    user_stats[replied_to_id] = {
                        "name": reply["replied_to_name"],
                        "count": 0,
                        "replies": []
                    }
                user_stats[replied_to_id]["count"] += 1
                user_stats[replied_to_id]["replies"].append(reply)
            
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
            
            items_per_page = 5
            total_pages = (len(sorted_users) + items_per_page - 1) // items_per_page
            
            if page >= total_pages:
                page = total_pages - 1
            
            start_idx = page * items_per_page
            end_idx = min((page + 1) * items_per_page, len(sorted_users))
            
            message_text = (
                f"КОМУ ОТВЕЧАЕТ ПОЛЬЗОВАТЕЛЬ\n\n"
                f"Пользователь: {user_id}\n"
                f"Всего собеседников: {len(sorted_users)}\n"
                f"Всего ответов: {len(replies_from_user)}\n"
                f"Период: последние 7 дней\n\n"
                f"Страница {page + 1} из {total_pages}\n\n"
            )
            
            for i, (replied_to_id, stats) in enumerate(sorted_users[start_idx:end_idx], start_idx + 1):
                message_text += f"{i}. {stats['name']} - {stats['count']} ответов\n"
            
            keyboard_buttons = []
            
            nav_buttons = []
            if page > 0:
                nav_buttons.append({"text": " Назад", "callback_data": f"replies_from_user:{user_id}:{page-1}"})
            
            if page < total_pages - 1:
                nav_buttons.append({"text": "Вперёд ", "callback_data": f"replies_from_user:{user_id}:{page+1}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            for i, (replied_to_id, stats) in enumerate(sorted_users[start_idx:end_idx], start_idx):
                keyboard_buttons.append([
                    {"text": f" {stats['name'][:20]}", "callback_data": f"view_reply_pair:{user_id}:{i}:from"}
                ])
            
            keyboard_buttons.append([
                {"text": " Поиск по юзеру", "callback_data": f"search_replies_from:{user_id}"},
                {"text": " Назад к меню", "callback_data": f"show_replies:{user_id}"}
            ])
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа ответов от пользователя: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def show_reply_pair_details(self, chat_id: int, user_id: int, index: int, direction: str):
        """Показывает детали реплаев с конкретным пользователем"""
        try:
            reply_data = await self.collect_replies_data(user_id)
            
            if direction == "to":
                replies = reply_data["to_user"]
                user_stats = {}
                for reply in replies:
                    replier_id = reply["replier_id"]
                    if replier_id not in user_stats:
                        user_stats[replier_id] = {
                            "name": reply["replier_name"],
                            "count": 0,
                            "replies": []
                        }
                    user_stats[replier_id]["count"] += 1
                    user_stats[replier_id]["replies"].append(reply)
                
                sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
                
                if index < 0 or index >= len(sorted_users):
                    await self.send_bot_message(chat_id, "Неверный индекс пользователя")
                    return
                
                target_user_id, target_user_stats = sorted_users[index]
                target_replies = target_user_stats["replies"]
                title = f" {target_user_stats['name']} отвечает пользователю"
                
            else:
                replies = reply_data["from_user"]
                user_stats = {}
                for reply in replies:
                    replied_to_id = reply["replied_to_id"]
                    if replied_to_id not in user_stats:
                        user_stats[replied_to_id] = {
                            "name": reply["replied_to_name"],
                            "count": 0,
                            "replies": []
                        }
                    user_stats[replied_to_id]["count"] += 1
                    user_stats[replied_to_id]["replies"].append(reply)
                
                sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
                
                if index < 0 or index >= len(sorted_users):
                    await self.send_bot_message(chat_id, "Неверный индекс пользователя")
                    return
                
                target_user_id, target_user_stats = sorted_users[index]
                target_replies = target_user_stats["replies"]
                title = f"Пользователь отвечает {target_user_stats['name']}"
            
            await self.show_reply_pair_page(chat_id, user_id, index, direction, 0)
            
        except Exception as e:
            print(f"Ошибка показа деталей реплаев: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def show_reply_pair_page(self, chat_id: int, user_id: int, user_index: int, direction: str, page: int):
        """Показывает страницу с реплаями для конкретного пользователя"""
        try:
            reply_data = await self.collect_replies_data(user_id)
            
            if direction == "to":
                replies = reply_data["to_user"]
                user_stats = {}
                for reply in replies:
                    replier_id = reply["replier_id"]
                    if replier_id not in user_stats:
                        user_stats[replier_id] = {
                            "name": reply["replier_name"],
                            "count": 0,
                            "replies": []
                        }
                    user_stats[replier_id]["count"] += 1
                    user_stats[replier_id]["replies"].append(reply)
                
                sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
                
                if user_index < 0 or user_index >= len(sorted_users):
                    await self.send_bot_message(chat_id, "Неверный индекс пользователя")
                    return
                
                target_user_id, target_user_stats = sorted_users[user_index]
                target_replies = target_user_stats["replies"]
                title = f" {target_user_stats['name']} отвечает пользователю"
                
            else:
                replies = reply_data["from_user"]
                user_stats = {}
                for reply in replies:
                    replied_to_id = reply["replied_to_id"]
                    if replied_to_id not in user_stats:
                        user_stats[replied_to_id] = {
                            "name": reply["replied_to_name"],
                            "count": 0,
                            "replies": []
                        }
                    user_stats[replied_to_id]["count"] += 1
                    user_stats[replied_to_id]["replies"].append(reply)
                
                sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
                
                if user_index < 0 or user_index >= len(sorted_users):
                    await self.send_bot_message(chat_id, "Неверный индекс пользователя")
                    return
                
                target_user_id, target_user_stats = sorted_users[user_index]
                target_replies = target_user_stats["replies"]
                title = f"Пользователь отвечает {target_user_stats['name']}"
            
            items_per_page = 3
            total_pages = (len(target_replies) + items_per_page - 1) // items_per_page
            
            if page >= total_pages:
                page = total_pages - 1
            
            start_idx = page * items_per_page
            end_idx = min((page + 1) * items_per_page, len(target_replies))
            
            message_text = f"{title}\n\n"
            message_text += f"Всего ответов: {len(target_replies)}\n"
            message_text += f"Страница {page + 1} из {total_pages}\n\n"
            
            for i in range(start_idx, end_idx):
                reply = target_replies[i]
                message_text += f"Ответ {i+1}:\n"
                message_text += f"Чат: {reply['chat_name'][:30]}\n"
                message_text += f"Дата: {reply['reply_time']}\n"
                message_text += f"Оригинал: {reply['original_text']}\n"
                message_text += f"Ответ: {reply['reply_text']}\n"
                message_text += f"Ответ: {reply['reply_link']}\n"
                message_text += f"Оригинал: {reply['original_link']}\n\n"
            
            keyboard_buttons = []
            
            nav_buttons = []
            if page > 0:
                nav_buttons.append({"text": " Назад", "callback_data": f"reply_page:{user_id}:{user_index}:{direction}:{page-1}"})
            
            nav_buttons.append({"text": f" {page+1}/{total_pages}", "callback_data": "noop"})
            
            if page < total_pages - 1:
                nav_buttons.append({"text": "Вперёд ", "callback_data": f"reply_page:{user_id}:{user_index}:{direction}:{page+1}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            back_action = "replies_to_user" if direction == "to" else "replies_from_user"
            keyboard_buttons.append([
                {"text": " К списку", "callback_data": f"{back_action}:{user_id}:0"},
                {"text": " Обновить", "callback_data": f"view_reply_pair:{user_id}:{user_index}:{direction}"}
            ])
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка отправки страницы реплаев: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def show_friends_menu(self, chat_id: int, user_id: int):
        """Показывает меню друзей"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            menu_text = (
                f"АНАЛИЗ ДРУЗЕЙ И КОНТАКТОВ\n\n"
                f"Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"ID: {user_id}\n\n"
                f"Выберите тип анализа:"
            )
            
            keyboard_buttons = [
                [
                    {"text": " Кто отвечает пользователю", "callback_data": f"replies_to_user:{user_id}:0"},
                    {"text": " Кому отвечает пользователь", "callback_data": f"replies_from_user:{user_id}:0"}
                ],
                [
                    {"text": " Поиск кто отвечает юзеру", "callback_data": f"search_replies_to:{user_id}"},
                    {"text": " Поиск кому отвечает юзер", "callback_data": f"search_replies_from:{user_id}"}
                ],
                [
                    {"text": " Частые собеседники", "callback_data": f"track_friends_old:{user_id}"},
                    {"text": " Найти сообщения", "callback_data": f"search_messages:{user_id}"}
                ],
                [
                    {"text": " Назад", "callback_data": f"back_to_menu:{user_id}"},
                    {"text": " Обновить", "callback_data": f"track_friends:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, menu_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа меню друзей: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def show_message_details(self, chat_id: int, message_chat_id: int, message_id: int):
        """Показывает детали сообщения"""
        try:
            try:
                chat = await self.client.get_entity(message_chat_id)
            except:
                await self.send_bot_message(chat_id, "Не удалось получить чат")
                return
            
            try:
                message = await self.client.get_messages(chat, ids=message_id)
            except:
                await self.send_bot_message(chat_id, "Не удалось получить сообщение")
                return
            
            if not message:
                await self.send_bot_message(chat_id, "Сообщение не найдено")
                return
            
            link = await self.get_message_link(chat, message_id)
            
            sender_name = "Неизвестный"
            if hasattr(message, 'sender_id'):
                try:
                    sender = await self.client.get_entity(message.sender_id)
                    sender_name = getattr(sender, 'first_name', '')
                    if hasattr(sender, 'last_name') and sender.last_name:
                        sender_name += f" {sender.last_name}"
                    if hasattr(sender, 'username') and sender.username:
                        sender_name += f" (@{sender.username})"
                except:
                    pass
            
            message_text = (
                f"ДЕТАЛИ СООБЩЕНИЯ\n\n"
                f"Отправитель: {sender_name}\n"
                f"Чат: {getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))}\n"
                f"Дата: {message.date.strftime('%d.%m.%Y %H:%M:%S')}\n"
                f"Ссылка: {link}\n\n"
                f"Текст сообщения:\n"
                f"{message.text[:1000] if message.text else 'Сообщение без текста'}\n\n"
            )
            
            if message.reply_to:
                message_text += f"↪️ Ответ на сообщение: ID {message.reply_to.reply_to_msg_id}\n"
            
            keyboard = self.create_keyboard([
                [
                    {"text": " Открыть в Telegram", "url": link},
                    {"text": " Закрыть", "callback_data": "close"}
                ]
            ])
            
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа деталей сообщения: {e}")
            await self.send_bot_message(chat_id, f"Ошибка: {str(e)[:100]}")
    
    async def show_monitoring_menu(self, chat_id: int):
        """Показывает меню управления отслеживанием"""
        if not self.monitored_users:
            await self.send_bot_message(chat_id,
                "Нет отслеживаемых пользователей\n\n"
                "Отправьте @username чтобы добавить пользователя.\n\n"
                "Пример: @durov"
            )
            return
        
        active_tasks = sum(1 for t in self.tracking_tasks if not t.done())
        
        menu_text = (
            f"ОТСЛЕЖИВАЕМЫЕ ПОЛЬЗОВАТЕЛИ:\n\n"
            f"Всего в кэше: {len(self.monitored_users)}\n"
            f"Активных задач: {active_tasks}\n\n"
            f"Выберите пользователя:"
        )
        
        buttons = []
        users_list = list(self.monitored_users.items())[:8]
        
        for user_id, profile in users_list:
            name = profile.first_name[:15] or f"User {user_id}"
            status_msg = "📨" if profile.is_tracking_messages else ""
            status_ava = "🖼" if profile.is_tracking_avatar else ""
            status_rep = "💬" if profile.is_tracking_replies else ""
            status = f"{status_msg}{status_ava}{status_rep}"
            
            buttons.append([
                {"text": f" {name} {status}", "callback_data": f"user_info:{user_id}"}
            ])
        
        if len(self.monitored_users) > 8:
            buttons.append([
                {"text": f" И еще {len(self.monitored_users) - 8}...", "callback_data": "show_more_users"}
            ])
        
        buttons.append([
            {"text": " Статистика", "callback_data": "stats"},
            {"text": " Добавить", "callback_data": "add_user"}
        ])
        
        keyboard = self.create_keyboard(buttons)
        
        await self.send_bot_message(chat_id, menu_text, keyboard)
    
    async def show_stats(self, chat_id: int):
        """Показывает статистику бота"""
        active_tasks = sum(1 for t in self.tracking_tasks if not t.done())
        
        tracking_msg = sum(1 for u in self.monitored_users.values() if u.is_tracking_messages)
        tracking_ava = sum(1 for u in self.monitored_users.values() if u.is_tracking_avatar)
        tracking_rep = sum(1 for u in self.monitored_users.values() if u.is_tracking_replies)
        
        stats_text = (
            f"СТАТИСТИКА БОТА:\n\n"
            f"Пользователей в кэше: {len(self.monitored_users)}\n"
            f"Отслеживается сообщений: {tracking_msg}\n"
            f"Отслеживается аватарок: {tracking_ava}\n"
            f"Отслеживается ответов: {tracking_rep}\n"
            f"Активных задач мониторинга: {active_tasks}\n"
            f"Аватарок в кэше: {len(self.avatar_cache)}\n"
            f"Сохранено профилей: {len(self.monitored_users)}\n"
            f"Время работы сервера: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        )
        
        if active_tasks > 0:
            stats_text += f"Текущие задачи:\n"
            for task in self.tracking_tasks[:5]:
                if not task.done():
                    task_name = task.get_name() or "Unknown"
                    stats_text += f"• {task_name}\n"
            
            if active_tasks > 5:
                stats_text += f"... и еще {active_tasks - 5} задач\n"
        
        stats_text += f"\nБот работает стабильно!"
        
        await self.send_bot_message(chat_id, stats_text)
    
    async def load_chats_list(self) -> List[str]:
        """Загружает список чатов из файла"""
        if not os.path.exists(CHATS_FILE):
            return []
        
        chats = []
        try:
            with open(CHATS_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        identifier = self.extract_chat_identifier(line)
                        if identifier:
                            chats.append(identifier)
        except Exception as e:
            print(f"Ошибка загрузки чатов: {e}")
        
        return chats
    
    def extract_chat_identifier(self, line: str) -> str:
        """Извлекает идентификатор чата из строки"""
        line = line.strip()
        
        if not line:
            return None
        
        if line.startswith('https://t.me/+'):
            parts = line.split('+')
            if len(parts) > 1:
                invite_code = parts[1]
                return f"https://t.me/+{invite_code}"
        
        elif line.startswith('https://t.me/'):
            username = line.replace('https://t.me/', '').split('/')[0]
            if username:
                return username
        
        elif line.startswith('@'):
            return line.replace('@', '')
        
        elif line.isdigit() or (line.startswith('-') and line[1:].isdigit()):
            return line
        
        elif '/' not in line and ':' not in line:
            return line
        
        return line
    
    async def get_chat_by_identifier(self, identifier: str):
        """Получает чат по идентификатору"""
        try:
            identifier = identifier.strip()
            
            if identifier.startswith('https://t.me/+'):
                try:
                    message = await self.client.get_messages(identifier, limit=1)
                    if message and hasattr(message, 'chat'):
                        return message.chat
                except Exception as e:
                    print(f"Не удалось получить чат по приватной ссылке {identifier}: {e}")
                    
                try:
                    invite_hash = identifier.replace('https://t.me/+', '')
                    result = await self.client(functions.messages.ImportChatInviteRequest(
                        hash=invite_hash
                    ))
                    if hasattr(result, 'chats') and result.chats:
                        return result.chats[0]
                except Exception as e:
                    print(f"Не удалось импортировать чат по приватной ссылке {identifier}: {e}")
            
            identifier_clean = identifier.replace('@', '')
            
            if identifier_clean.startswith('-100') and identifier_clean[4:].isdigit():
                chat_id = int(identifier_clean)
                return await self.client.get_entity(PeerChannel(chat_id))
            elif identifier_clean.isdigit() or (identifier_clean.startswith('-') and identifier_clean[1:].isdigit()):
                chat_id = int(identifier_clean)
                try:
                    return await self.client.get_entity(chat_id)
                except:
                    if chat_id < 0:
                        try:
                            return await self.client.get_entity(PeerChannel(chat_id))
                        except:
                            return None
                    return None
            else:
                try:
                    return await self.client.get_entity(identifier_clean)
                except:
                    try:
                        return await self.client.get_entity(f"@{identifier_clean}")
                    except:
                        return None
        except Exception as e:
            print(f"Ошибка получения чата {identifier}: {e}")
            return None
    
    async def get_message_link(self, chat, message_id: int) -> str:
        """Формирует ссылку на сообщение"""
        try:
            if hasattr(chat, 'username') and chat.username:
                return f"https://t.me/{chat.username}/{message_id}"
            else:
                chat_id = str(getattr(chat, 'id', ''))
                if chat_id.startswith('-100'):
                    chat_id = chat_id.replace('-100', '')
                elif chat_id.startswith('-'):
                    chat_id = chat_id[1:]
                
                return f"https://t.me/c/{chat_id}/{message_id}"
        except:
            return f"Message ID: {message_id}"
    
    async def run_bot_polling(self):
        """Запускает polling бота"""
        print("Запускаю polling бота...")
        
        offset = 0
        max_retries = 5
        retry_count = 0
        
        while True:
            try:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
                params = {
                    "offset": offset,
                    "timeout": 25,
                    "allowed_updates": ["message", "callback_query"]
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            retry_count = 0
                            
                            if data.get("ok") and data.get("result"):
                                for update in data["result"]:
                                    offset = update["update_id"] + 1
                                    
                                    if "message" in update:
                                        await self.handle_bot_command(update)
                                    elif "callback_query" in update:
                                        await self.handle_callback_query(update)
                        else:
                            print(f"Ошибка API: {response.status}")
                            retry_count += 1
                
                if retry_count >= max_retries:
                    print(f"Много ошибок, увеличиваю задержку...")
                    await asyncio.sleep(30)
                    retry_count = 0
                
            except asyncio.TimeoutError:
                continue
            except aiohttp.ClientError as e:
                print(f"Ошибка сети: {e}")
                retry_count += 1
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Ошибка polling: {e}")
                retry_count += 1
                await asyncio.sleep(5)
    
    async def run(self):
        """Основной метод запуска"""
        print("="*60)
        print("TELEGRAM SPY BOT v3.4")
        print("="*60)
        print("Новые функции:")
        print("• Поддержка приватных ссылок https://t.me/+XXXXXXXXXXX")
        print("• Улучшенный поиск по ID пользователя")
        print("• Автопарсинг разных форматов чатов")
        print("="*60)
        
        if not await self.connect():
            print("Не удалось подключиться к Telegram")
            return
        
        print("Тестирую подключение к боту...")
        test_msg = (
            f"Шпионский бот запущен!\n\n"
            f"Подключение установлено\n"
            f"Аккаунт: {self.current_user.first_name if self.current_user else 'Неизвестно'}\n"
            f"ID: {self.current_user.id if self.current_user else 'Неизвестно'}\n"
            f"{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
            f"Новые функции v3.4:\n"
            f"• Поддержка приватных ссылок https://t.me/+XXXXXXXXXXX\n"
            f"• Улучшенный поиск пользователей по ID\n"
            f"• Поддержка разных форматов чатов в chats.txt\n"
            f"• Автопарсинг ссылок и username\n\n"
            f"Отправьте /start для начала работы"
        )
        
        if await self.send_bot_message(ADMIN_ID, test_msg):
            print("Бот подключен и готов к работе!")
        else:
            print("Бот не отвечает, но поиск будет работать")
        
        bot_task = asyncio.create_task(self.run_bot_polling())
        
        print("\n" + "="*60)
        print("Бот запущен! Отправьте команду /start в Telegram")
        print(f"ID вашего бота можно найти по токену")
        print("="*60 + "\n")
        
        try:
            await bot_task
        except KeyboardInterrupt:
            print("\n\nПолучен сигнал остановки...")
        except Exception as e:
            print(f"\nКритическая ошибка: {e}")
        finally:
            print("\nСохраняю данные...")
            self.save_monitored_users()
            
            print("Останавливаю задачи мониторинга...")
            for task in self.tracking_tasks:
                if not task.done():
                    task.cancel()
            
            print("Отключаюсь от Telegram...")
            if self.client:
                await self.client.disconnect()
            
            print("Бот завершил работу")

# Запуск бота
if __name__ == "__main__":
    try:
        import telethon
        import aiohttp
    except ImportError as e:
        print(f"Не установлены зависимости: {e}")
        print("Установите: pip install telethon aiohttp")
        sys.exit(1)
    
    if sys.version_info < (3, 7):
        print("Требуется Python 3.7 или выше!")
        sys.exit(1)
    
    if not os.path.exists("api_config.txt"):
        print("Файл api_config.txt не найден!")
        print("Создайте его с содержимым:")
        print("API_ID=ваш_api_id")
        print("API_HASH=ваш_api_hash")
        sys.exit(1)
    
    bot = TelegramSpyBot()
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\nПрограмма завершена пользователем")
    except Exception as e:
        print(f"\nФатальная ошибка: {e}")
        import traceback
        traceback.print_exc()