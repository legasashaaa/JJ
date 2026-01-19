import asyncio
import os
import sys
import json
import time
import hashlib
import io
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
    is_tracking_messages: bool = False  # Флаг отслеживания сообщений
    is_tracking_avatar: bool = False    # Флаг отслеживания аватарки
    is_tracking_replies: bool = False   # Флаг отслеживания ответов на сообщения
    user_chats: List[Dict] = None       # Чаты пользователя
    user_chats_loaded: bool = False     # Флаг загрузки чатов
    
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
        self.current_user = None  # Добавляем атрибут для текущего пользователя
        self.api_id = None
        self.api_hash = None
        self.monitored_users: Dict[int, UserProfile] = {}
        self.user_states: Dict[int, Dict] = {}
        self.tracking_tasks = []
        self.avatar_cache: Dict[int, str] = {}
        self.message_cache: Dict[int, List] = {}
        self.tracking_status: Dict[int, Dict[str, bool]] = {}  # Состояние отслеживания
        self.last_message_ids: Dict[int, Dict[int, int]] = {}  # Кэш последних сообщений по чатам {user_id: {chat_id: message_id}}
        self.reply_data_cache: Dict[int, Dict[str, List]] = {}  # Кэш для данных о реплаях {user_id: {"to_user": [], "from_user": []}}
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
                        
                        # Загружаем чаты пользователя если есть
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
                            user_chats_loaded=bool(user_chats)  # Если есть чаты в сохраненке, считаем загруженными
                        )
                        self.monitored_users[int(user_id)] = profile
                        
                        # Загружаем состояние отслеживания
                        self.tracking_status[int(user_id)] = {
                            'messages': profile.is_tracking_messages,
                            'avatar': profile.is_tracking_avatar,
                            'replies': profile.is_tracking_replies
                        }
            except Exception as e:
                print(f"Error loading monitored users: {e}")
                # Создаем пустой файл если ошибка
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
            print("❌ API данные не загружены!")
            return False
        
        print("🔗 Подключаюсь к Telegram...")
        try:
            self.client = TelegramClient(SESSION_FILE, self.api_id, self.api_hash)
            await self.client.start()
            
            self.current_user = await self.client.get_me()  # Сохраняем текущего пользователя
            username = f" @{self.current_user.username}" if self.current_user.username else ""
            print(f"✅ Подключен как {self.current_user.first_name}{username} (ID: {self.current_user.id})")
            return True
        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")
            return False
    
    async def send_bot_message(self, chat_id: int, text: str, 
                               reply_markup: Dict = None,
                               photo: bytes = None) -> bool:
        """Отправляет сообщение через бота"""
        try:
            if photo:
                # Отправляем фото с текстом как подпись
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
                
                # Используем FormData для отправки фото
                form_data = aiohttp.FormData()
                form_data.add_field('chat_id', str(chat_id))
                form_data.add_field('caption', text[:1024])
                form_data.add_field('parse_mode', 'HTML')
                
                # Добавляем фото
                form_data.add_field('photo', 
                                   photo,
                                   filename='avatar.jpg',
                                   content_type='image/jpeg')
                
                # Добавляем клавиатуру если есть
                if reply_markup:
                    # Преобразуем клавиатуру в JSON
                    import json as json_module
                    keyboard_json = json_module.dumps(reply_markup)
                    form_data.add_field('reply_markup', keyboard_json)
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, data=form_data, timeout=30) as response:
                        return response.status == 200
            else:
                # Отправляем обычное сообщение
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
                            print(f"❌ Ошибка API бота: {response.status} - {error_text}")
                        return response.status == 200
                        
        except asyncio.TimeoutError:
            print("❌ Таймаут при отправке сообщения")
            return False
        except Exception as e:
            print(f"❌ Ошибка отправки ботом: {e}")
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
            
            print(f"📨 Получено сообщение от {chat_id}: {text}")
            
            if text.startswith("/start"):
                welcome_msg = (
                    "👋 <b>Шпионский бот активирован!</b>\n\n"
                    "🔍 <b>Новые функции:</b>\n"
                    "• Поиск сообщений пользователя\n"
                    "• Отслеживание новых сообщений\n"
                    "• Мониторинг аватарок (с отправкой фото)\n"
                    "• Отслеживание ответов на сообщения\n"
                    "• Анализ активности и друзей\n\n"
                    "📝 <b>Использование:</b>\n"
                    "Просто отправьте @username или ID пользователя\n\n"
                    "Пример: <code>@durov</code> или <code>123456789</code>"
                )
                await self.send_bot_message(chat_id, welcome_msg)
            
            elif text.startswith("/monitor"):
                await self.show_monitoring_menu(chat_id)
            
            elif text.startswith("/stats"):
                await self.show_stats(chat_id)
            
            elif text.startswith("/help"):
                help_msg = (
                    "📋 <b>Доступные команды:</b>\n\n"
                    "/start - Начать работу\n"
                    "/monitor - Управление отслеживанием\n"
                    "/stats - Статистика\n"
                    "/help - Помощь\n\n"
                    "📝 <b>Как использовать:</b>\n"
                    "1. Отправьте @username (например @durov)\n"
                    "2. Или ID пользователя (например 123456789)\n"
                    "3. Выберите действие из меню\n\n"
                    "👁 <b>Отслеживание:</b>\n"
                    "• Сообщения - уведомления о новых сообщениях\n"
                    "• Аватарки - фото при смене аватарки\n"
                    "• Ответы - кто отвечает на сообщения пользователя\n"
                    "• Друзья - анализ социальных связей"
                )
                await self.send_bot_message(chat_id, help_msg)
            
            elif text.startswith("/stop"):
                # Останавливаем все задачи мониторинга
                for task in self.tracking_tasks:
                    if not task.done():
                        task.cancel()
                self.tracking_tasks = [t for t in self.tracking_tasks if not t.done()]
                
                # Сбрасываем флаги
                for user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_messages = False
                    self.monitored_users[user_id].is_tracking_avatar = False
                    self.monitored_users[user_id].is_tracking_replies = False
                    if user_id in self.tracking_status:
                        self.tracking_status[user_id] = {'messages': False, 'avatar': False, 'replies': False}
                
                self.save_monitored_users()
                await self.send_bot_message(chat_id, "🛑 Все задачи мониторинга остановлены")
            
            elif text.startswith("/test"):
                # Тестовая команда
                await self.send_bot_message(chat_id, 
                    f"🤖 <b>Тест бота</b>\n\n"
                    f"✅ Бот работает\n"
                    f"👤 Текущий аккаунт: {self.current_user.first_name if self.current_user else 'Не подключен'}\n"
                    f"🕐 Время: {datetime.now().strftime('%H:%M:%S')}"
                )
            
            else:
                # Проверяем, это username/id или текст
                clean_text = text.replace('@', '').strip()
                
                # Проверяем состояние пользователя
                if chat_id in self.user_states:
                    state = self.user_states[chat_id]
                    
                    if state.get("action") == "waiting_search_text":
                        user_id = state["user_id"]
                        await self.search_user_messages(chat_id, user_id, text)
                        # Удаляем состояние
                        if chat_id in self.user_states:
                            del self.user_states[chat_id]
                    
                    elif state.get("action") == "waiting_target_user_for_replies_to":
                        user_id = state["user_id"]
                        await self.search_replies_to_user(chat_id, user_id, text)
                        # Удаляем состояние
                        if chat_id in self.user_states:
                            del self.user_states[chat_id]
                    
                    elif state.get("action") == "waiting_target_user_for_replies_from":
                        user_id = state["user_id"]
                        await self.search_replies_from_user(chat_id, user_id, text)
                        # Удаляем состояние
                        if chat_id in self.user_states:
                            del self.user_states[chat_id]
                    
                    else:
                        if (text.startswith('@') or 
                            clean_text.isdigit() or 
                            (clean_text.startswith('-') and clean_text[1:].isdigit())):
                            await self.handle_user_search(chat_id, text)
                        else:
                            await self.send_bot_message(chat_id,
                                "❌ <b>Неверный формат!</b>\n\n"
                                "Отправьте:\n"
                                "• @username (например @durov)\n"
                                "• ID пользователя (например 123456789)\n\n"
                                "Или используйте команды: /start /help"
                            )
                else:
                    if (text.startswith('@') or 
                        clean_text.isdigit() or 
                        (clean_text.startswith('-') and clean_text[1:].isdigit())):
                        await self.handle_user_search(chat_id, text)
                    else:
                        await self.send_bot_message(chat_id,
                            "❌ <b>Неверный формат!</b>\n\n"
                            "Отправьте:\n"
                                "• @username (например @durov)\n"
                                "• ID пользователя (например 123456789)\n\n"
                                "Или используйте команды: /start /help"
                        )
        except Exception as e:
            print(f"Ошибка обработки команды: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def handle_callback_query(self, update: Dict):
        """Обрабатывает нажатия на кнопки"""
        try:
            callback_query = update["callback_query"]
            chat_id = callback_query["message"]["chat"]["id"]
            data = callback_query["data"]
            
            print(f"🔘 Callback от {chat_id}: {data}")
            
            # Парсим данные callback
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
                    "👤 <b>Добавить пользователя</b>\n\n"
                    "Отправьте @username или ID пользователя\n\n"
                    "Пример:\n"
                    "<code>@durov</code>\n"
                    "<code>123456789</code>"
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
                direction = parts[3]  # "to" или "from"
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
            
            # Подтверждаем нажатие кнопки
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
                    "text": "✅ Обработано"
                })
        except Exception as e:
            print(f"Ошибка ответа на callback: {e}")
    
    async def handle_user_search(self, chat_id: int, user_input: str):
        """Обрабатывает поиск пользователя"""
        try:
            await self.send_bot_message(chat_id, "🔍 Ищу пользователя...")
            
            # Получаем информацию о пользователе
            user = None
            user_input_clean = user_input.strip().replace('@', '')
            
            if user_input_clean.isdigit() or (user_input_clean.startswith('-') and user_input_clean[1:].isdigit()):
                # Поиск по ID
                user_id = int(user_input_clean)
                try:
                    user = await self.client.get_entity(user_id)
                except:
                    try:
                        user = await self.client.get_entity(PeerUser(user_id))
                    except:
                        await self.send_bot_message(chat_id, f"❌ Пользователь с ID {user_id} не найден")
                        return
            else:
                # Поиск по username
                username = user_input_clean
                try:
                    user = await self.client.get_entity(username)
                except errors.UsernameNotOccupiedError:
                    await self.send_bot_message(chat_id, f"❌ Пользователь @{username} не существует")
                    return
                except Exception as e:
                    try:
                        user = await self.client.get_entity(f"@{username}")
                    except:
                        await self.send_bot_message(chat_id, f"❌ Ошибка поиска: {str(e)[:100]}")
                        return
            
            if not user:
                await self.send_bot_message(chat_id, "❌ Не удалось получить информацию о пользователе")
                return
            
            # Получаем полную информацию
            try:
                full_user = await self.client(GetFullUserRequest(user))
            except Exception as e:
                print(f"Ошибка получения full user: {e}")
                full_user = None
            
            # Получаем информацию об аватарке
            avatar_hash = "no_avatar"
            if hasattr(user, 'photo') and user.photo and not isinstance(user.photo, PhotoEmpty):
                if isinstance(user.photo, UserProfilePhoto):
                    avatar_hash = str(user.photo.photo_id)
            elif full_user and hasattr(full_user, 'profile_photo') and full_user.profile_photo:
                if isinstance(full_user.profile_photo, UserProfilePhoto):
                    avatar_hash = str(full_user.profile_photo.photo_id)
            
            # Получаем последний онлайн
            last_seen = await self.get_user_last_seen(user)
            
            # Получаем био
            bio = ""
            if full_user and hasattr(full_user, 'about'):
                bio = full_user.about or ""
            
            # Получаем телефон
            phone = ""
            if hasattr(user, 'phone') and user.phone:
                phone = user.phone
            
            # Инициализируем статус отслеживания
            if user.id not in self.tracking_status:
                self.tracking_status[user.id] = {
                    'messages': False,
                    'avatar': False,
                    'replies': False
                }
            
            # Создаем профиль
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
            
            # Сохраняем в кэш
            self.monitored_users[user.id] = profile
            self.save_monitored_users()
            
            # Быстрая загрузка чатов пользователя
            await self.send_bot_message(chat_id, "📁 Быстро ищу чаты пользователя...")
            await self.load_user_chats(chat_id, user.id)
            
        except Exception as e:
            error_msg = str(e)
            print(f"Ошибка поиска пользователя: {error_msg}")
            
            if "Cannot cast" in error_msg or "InputPeer" in error_msg:
                await self.send_bot_message(chat_id,
                    "❌ <b>Ошибка приведения типа!</b>\n\n"
                    "Попробуйте другой формат:\n"
                    "• @username (например @durov)\n"
                    "• ID пользователя (только цифры)\n\n"
                    "Или проверьте правильность ввода."
                )
            else:
                await self.send_bot_message(chat_id, f"❌ Ошибка: {error_msg[:100]}")
    
    async def load_user_chats(self, chat_id: int, user_id: int):
        """Быстро загружает чаты пользователя"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "❌ Пользователь не найден")
                return
            
            profile = self.monitored_users[user_id]
            
            # Если уже загружены, просто показываем
            if profile.user_chats_loaded and profile.user_chats:
                await self.show_user_actions_with_chats(chat_id, user_id)
                return
            
            await self.send_bot_message(chat_id, "🔍 Сканирую диалоги...")
            
            user = await self.client.get_entity(PeerUser(user_id))
            user_chats = []
            
            # Получаем все диалоги быстро
            dialogs = await self.client.get_dialogs(limit=100)  # Увеличил лимит
            
            for dialog in dialogs:
                try:
                    # Пропускаем каналы (только channels)
                    if dialog.is_channel and not dialog.is_group:
                        continue
                    
                    # Проверяем есть ли пользователь в чате/группе
                    if dialog.is_group or dialog.is_channel:
                        # Для групп и супергрупп
                        try:
                            # Быстрая проверка через поиск сообщений (ограниченное количество)
                            messages = await self.client.get_messages(
                                dialog.entity,
                                limit=5,  # Только 5 сообщений для быстрой проверки
                                from_user=user
                            )
                            
                            if len(messages) > 0:
                                # Получаем точное количество сообщений
                                total_messages = 0
                                async for _ in self.client.iter_messages(
                                    dialog.entity,
                                    limit=100,  # Ограничим 100 для скорости
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
                                    
                                    # Если набрали 20 чатов, останавливаемся для скорости
                                    if len(user_chats) >= 20:
                                        break
                                    
                        except Exception as e:
                            print(f"Ошибка проверки чата {dialog.name}: {e}")
                            continue
                    
                    elif dialog.is_user:
                        # Личный чат
                        if dialog.entity.id == user_id:
                            chat_name = dialog.name
                            chat_link = await self.get_chat_link(dialog.entity)
                            
                            # Подсчет сообщений в личном чате
                            total_messages = 0
                            async for _ in self.client.iter_messages(
                                dialog.entity,
                                limit=200,  # Ограничим для скорости
                            ):
                                total_messages += 1
                            
                            user_chats.append({
                                'id': dialog.id,
                                'name': f"👤 {chat_name}",
                                'link': chat_link,
                                'message_count': total_messages,
                                'last_activity': datetime.now()
                            })
                
                except Exception as e:
                    print(f"Ошибка обработки диалога: {e}")
                    continue
            
            # Сохраняем чаты
            profile.user_chats = user_chats
            profile.user_chats_loaded = True
            profile.common_chats = len(user_chats)
            
            # Сохраняем
            self.save_monitored_users()
            
            # Показываем действия с чатами
            await self.show_user_actions_with_chats(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка загрузки чатов: {e}")
            # Все равно показываем меню, но без чатов
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
                await self.send_bot_message(chat_id, "❌ Пользователь не найден в кэше")
                return
            
            profile = self.monitored_users[user_id]
            
            # Получаем аватар для показа
            avatar_bytes = await self.get_user_avatar_bytes(user_id)
            has_avatar = avatar_bytes is not None
            
            # Определяем статусы отслеживания
            track_msg_status = "✅" if profile.is_tracking_messages else "🔲"
            track_avatar_status = "✅" if profile.is_tracking_avatar else "🔲"
            track_reply_status = "✅" if profile.is_tracking_replies else "🔲"
            
            user_info = (
                f"👤 <b>Пользователь:</b>\n\n"
                f"🆔 ID: <code>{user_id}</code>\n"
                f"👤 Имя: {profile.first_name} {profile.last_name}\n"
                f"📱 Username: @{profile.username if profile.username else 'нет'}\n"
                f"📞 Телефон: {profile.phone if profile.phone else 'скрыт'}\n"
                f"👀 Был онлайн: {profile.last_seen.strftime('%d.%m.%Y %H:%M')}\n"
                f"🖼 Аватар: {'✅ есть' if has_avatar else '❌ нет'}\n"
                f"📁 Чатов найдено: {len(profile.user_chats)}\n\n"
            )
            
            # Добавляем информацию о чатах (первые 3)
            if profile.user_chats:
                user_info += f"<b>Недавние чаты:</b>\n"
                for i, chat in enumerate(profile.user_chats[:3], 1):
                    user_info += f"{i}. <a href='{chat['link']}'>{chat['name']}</a> - {chat['message_count']} сообщ.\n"
                
                if len(profile.user_chats) > 3:
                    user_info += f"... и еще {len(profile.user_chats) - 3} чатов\n"
                
                user_info += "\n"
            
            user_info += f"<i>Выберите действие:</i>"
            
            # Создаем кнопки со статусами отслеживания
            keyboard_buttons = [
                [
                    {"text": "🔍 Найти сообщения", "callback_data": f"search_messages:{user_id}"},
                    {"text": "📊 Профиль", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": f"{track_msg_status} Следить за сообщениями", "callback_data": f"monitor_messages:{user_id}"},
                    {"text": f"{track_avatar_status} Следить за аватаркой", "callback_data": f"monitor_avatar:{user_id}"}
                ],
                [
                    {"text": f"{track_reply_status} Следить за ответами", "callback_data": f"monitor_replies:{user_id}"},
                    {"text": "💬 Анализ ответов", "callback_data": f"show_replies:{user_id}"}
                ],
                [
                    {"text": "📊 Количество сообщений", "callback_data": f"get_message_count:{user_id}"},
                    {"text": "👥 Друзья", "callback_data": f"track_friends:{user_id}"}
                ],
                [
                    {"text": "📸 Получить аватарку", "callback_data": f"get_avatar:{user_id}"},
                    {"text": "📁 Показать все чаты", "callback_data": f"show_user_chats:{user_id}:0"}
                ],
                [
                    {"text": "🔄 Обновить", "callback_data": f"refresh_status:{user_id}"},
                    {"text": "🔄 Обновить чаты", "callback_data": f"refresh_chats:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            
            # Отправляем с фото если есть аватар
            if has_avatar and avatar_bytes:
                await self.send_bot_message(chat_id, user_info, keyboard, avatar_bytes)
            else:
                await self.send_bot_message(chat_id, user_info, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа действий: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_user_chats(self, chat_id: int, user_id: int, page: int = 0):
        """Показывает все чаты пользователя с пагинацией"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "❌ Пользователь не найден")
                return
            
            profile = self.monitored_users[user_id]
            
            if not profile.user_chats:
                await self.send_bot_message(chat_id, "📭 У пользователя не найдено чатов")
                return
            
            # Разбиваем на страницы
            items_per_page = 8
            total_pages = (len(profile.user_chats) + items_per_page - 1) // items_per_page
            
            if page >= total_pages:
                page = total_pages - 1
            
            start_idx = page * items_per_page
            end_idx = min((page + 1) * items_per_page, len(profile.user_chats))
            
            # Формируем сообщение
            message_text = (
                f"📁 <b>ЧАТЫ ПОЛЬЗОВАТЕЛЯ</b>\n\n"
                f"👤 Пользователь: {profile.first_name} {profile.last_name}\n"
                f"🆔 ID: <code>{user_id}</code>\n"
                f"📊 Всего чатов: {len(profile.user_chats)}\n"
                f"📄 Страница {page + 1} из {total_pages}\n\n"
            )
            
            # Сортируем по количеству сообщений
            sorted_chats = sorted(profile.user_chats, key=lambda x: x['message_count'], reverse=True)
            
            # Добавляем чаты текущей страницы
            for i, stat in enumerate(sorted_chats[start_idx:end_idx], start_idx + 1):
                message_text += f"{i}. <a href='{stat['link']}'>{stat['name']}</a> - {stat['message_count']} сообщ.\n"
            
            # Создаем клавиатуру с пагинацией
            keyboard_buttons = []
            
            # Кнопки навигации
            nav_buttons = []
            if page > 0:
                nav_buttons.append({"text": "⬅️ Назад", "callback_data": f"show_user_chats:{user_id}:{page-1}"})
            
            nav_buttons.append({"text": f"📄 {page+1}/{total_pages}", "callback_data": f"noop"})
            
            if page < total_pages - 1:
                nav_buttons.append({"text": "Вперёд ➡️", "callback_data": f"show_user_chats:{user_id}:{page+1}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            # Кнопки управления
            keyboard_buttons.append([
                {"text": "🔙 В меню", "callback_data": f"back_to_menu:{user_id}"},
                {"text": "🔄 Обновить чаты", "callback_data": f"refresh_chats:{user_id}"}
            ])
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа чатов: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_user_actions(self, chat_id: int, user_id: int):
        """Показывает меню действий с пользователем (старая версия)"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "❌ Пользователь не найден в кэше")
                return
            
            profile = self.monitored_users[user_id]
            
            # Получаем аватар для показа
            avatar_bytes = await self.get_user_avatar_bytes(user_id)
            has_avatar = avatar_bytes is not None
            
            # Определяем статусы отслеживания
            track_msg_status = "✅" if profile.is_tracking_messages else "🔲"
            track_avatar_status = "✅" if profile.is_tracking_avatar else "🔲"
            track_reply_status = "✅" if profile.is_tracking_replies else "🔲"
            
            user_info = (
                f"👤 <b>Пользователь:</b>\n\n"
                f"🆔 ID: <code>{user_id}</code>\n"
                f"👤 Имя: {profile.first_name} {profile.last_name}\n"
                f"📱 Username: @{profile.username if profile.username else 'нет'}\n"
                f"📞 Телефон: {profile.phone if profile.phone else 'скрыт'}\n"
                f"👀 Был онлайн: {profile.last_seen.strftime('%d.%m.%Y %H:%M')}\n"
                f"🖼 Аватар: {'✅ есть' if has_avatar else '❌ нет'}\n\n"
                f"<i>Выберите действие:</i>"
            )
            
            # Создаем кнопки со статусами отслеживания
            keyboard_buttons = [
                [
                    {"text": "🔍 Найти сообщения", "callback_data": f"search_messages:{user_id}"},
                    {"text": "📊 Профиль", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": f"{track_msg_status} Следить за сообщениями", "callback_data": f"monitor_messages:{user_id}"},
                    {"text": f"{track_avatar_status} Следить за аватаркой", "callback_data": f"monitor_avatar:{user_id}"}
                ],
                [
                    {"text": f"{track_reply_status} Следить за ответами", "callback_data": f"monitor_replies:{user_id}"},
                    {"text": "💬 Анализ ответов", "callback_data": f"show_replies:{user_id}"}
                ],
                [
                    {"text": "📊 Количество сообщений", "callback_data": f"get_message_count:{user_id}"},
                    {"text": "👥 Друзья", "callback_data": f"track_friends:{user_id}"}
                ],
                [
                    {"text": "📸 Получить аватарку", "callback_data": f"get_avatar:{user_id}"},
                    {"text": "🔄 Обновить", "callback_data": f"refresh_status:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            
            # Отправляем с фото если есть аватар
            if has_avatar and avatar_bytes:
                await self.send_bot_message(chat_id, user_info, keyboard, avatar_bytes)
            else:
                await self.send_bot_message(chat_id, user_info, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа действий: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def get_user_avatar_bytes(self, user_id: int) -> Optional[bytes]:
        """Получает аватар пользователя в виде bytes"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            # Проверяем есть ли фото
            if not hasattr(user, 'photo') or not user.photo or isinstance(user.photo, PhotoEmpty):
                # Пробуем через GetFullUser
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
            
            # Скачиваем фото
            if isinstance(photo, UserProfilePhoto):
                # Получаем фото профиля
                photo_bytes = await self.client.download_profile_photo(user, file=bytes)
                return photo_bytes
            
            return None
            
        except Exception as e:
            print(f"Ошибка получения аватарки: {e}")
            return None
    
    async def send_current_avatar(self, chat_id: int, user_id: int):
        """Отправляет текущую аватарку пользователя"""
        try:
            await self.send_bot_message(chat_id, "📸 Загружаю аватарку...")
            
            avatar_bytes = await self.get_user_avatar_bytes(user_id)
            
            if avatar_bytes:
                # Отправляем фото
                caption = f"🖼 <b>Аватарка пользователя</b>\n🆔 ID: <code>{user_id}</code>\n📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
                await self.send_bot_message(chat_id, caption, photo=avatar_bytes)
            else:
                await self.send_bot_message(chat_id, "❌ У пользователя нет аватарки или не удалось загрузить")
                
        except Exception as e:
            print(f"Ошибка отправки аватарки: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
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
            return datetime.now() - timedelta(days=365)  # Очень давно
        except:
            return None
    
    async def show_user_info(self, chat_id: int, user_id: int):
        """Показывает полную информацию о пользователе"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "❌ Пользователь не найден в кэше")
                return
            
            profile = self.monitored_users[user_id]
            
            # Получаем дополнительную статистику
            stats = await self.get_user_statistics(user_id)
            
            info_text = (
                f"📊 <b>ПОЛНАЯ ИНФОРМАЦИЯ:</b>\n\n"
                f"👤 <b>Основное:</b>\n"
                f"• ID: <code>{user_id}</code>\n"
                f"• Имя: {profile.first_name} {profile.last_name}\n"
                f"• Username: @{profile.username if profile.username else 'нет'}\n"
                f"• Телефон: {profile.phone if profile.phone else 'скрыт'}\n"
                f"• Био: {profile.bio if profile.bio else 'нет'}\n"
                f"• Был онлайн: {profile.last_seen.strftime('%d.%m.%Y %H:%M')}\n\n"
                
                f"📈 <b>Статистика:</b>\n"
                f"• Общих чатов: {stats['common_chats']}\n"
                f"• Всего сообщений: {stats['total_messages']}\n"
                f"• Среднее в день: {stats['avg_per_day']:.1f}\n"
                f"• Друзей обнаружено: {len(stats['friends'])}\n"
                f"• Активность: {stats['activity_level']}\n\n"
            )
            
            # Добавляем статус отслеживания
            info_text += f"👁 <b>Отслеживание:</b>\n"
            info_text += f"• Сообщения: {'✅ ВКЛ' if profile.is_tracking_messages else '❌ ВЫКЛ'}\n"
            info_text += f"• Аватарка: {'✅ ВКЛ' if profile.is_tracking_avatar else '❌ ВЫКЛ'}\n"
            info_text += f"• Ответы: {'✅ ВКЛ' if profile.is_tracking_replies else '❌ ВЫКЛ'}\n\n"
            
            if stats['common_chats_list']:
                info_text += f"👥 <b>Общие чаты ({min(5, len(stats['common_chats_list']))} из {stats['common_chats']}):</b>\n"
                for i, chat in enumerate(stats['common_chats_list'][:5], 1):
                    chat_name = chat.get('title', chat.get('username', f'Чат {chat["id"]}'))[:30]
                    info_text += f"{i}. {chat_name}\n"
                
                if len(stats['common_chats_list']) > 5:
                    info_text += f"... и еще {len(stats['common_chats_list']) - 5} чатов\n"
            
            if stats['friends']:
                info_text += f"\n👫 <b>Частые собеседники:</b>\n"
                for i, friend in enumerate(stats['friends'][:5], 1):
                    info_text += f"{i}. {friend}\n"
            
            keyboard = self.create_keyboard([
                [
                    {"text": "🔙 Назад", "callback_data": f"back_to_menu:{user_id}"},
                    {"text": "🔄 Обновить", "callback_data": f"user_info:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, info_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа информации: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_message_count(self, chat_id: int, user_id: int):
        """Показывает количество сообщений пользователя во всех чатах"""
        try:
            await self.send_bot_message(chat_id, "📊 Подсчитываю количество сообщений...")
            
            # Получаем пользователя
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            # Загружаем список чатов
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "❌ Нет чатов для проверки!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "<code>@durov\n@telegram</code>"
                )
                return
            
            total_messages = 0
            chat_stats = []
            checked_chats = 0
            
            # Проверяем каждый чат
            for i, chat_identifier in enumerate(chats, 1):
                try:
                    # УБРАНО ПРОГРЕСС-СООБЩЕНИЕ
                    
                    # Получаем чат
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    checked_chats += 1
                    
                    # Получаем сообщения пользователя (ограничим 200 для скорости)
                    message_count = 0
                    try:
                        async for message in self.client.iter_messages(
                            chat,
                            limit=200,
                            from_user=user
                        ):
                            if message:
                                message_count += 1
                    except:
                        # Если не удалось получить сообщения, пропускаем
                        continue
                    
                    if message_count > 0:
                        total_messages += message_count
                        chat_name = getattr(chat, 'title', 
                                          getattr(chat, 'username', 
                                                 f'Чат {chat.id}'))
                        
                        chat_stats.append({
                            "name": chat_name[:30],
                            "count": message_count
                        })
                    
                except Exception as e:
                    print(f"Ошибка проверки чата {chat_identifier}: {e}")
                    continue
                
                # Небольшая пауза
                await asyncio.sleep(0.3)
            
            # Сортируем по количеству сообщений
            chat_stats.sort(key=lambda x: x['count'], reverse=True)
            
            # Формируем отчет
            report_text = (
                f"📊 <b>СТАТИСТИКА СООБЩЕНИЙ:</b>\n\n"
                f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"📈 Всего сообщений: {total_messages}\n"
                f"📁 Проверено чатов: {checked_chats} из {len(chats)}\n"
                f"💬 Чатов с сообщениями: {len(chat_stats)}\n\n"
            )
            
            # Добавляем топ чатов
            if chat_stats:
                report_text += f"🏆 <b>Топ чатов по активности:</b>\n"
                for i, stat in enumerate(chat_stats[:10], 1):
                    report_text += f"{i}. {stat['name']}: {stat['count']} сообщ.\n"
            
            # Обновляем профиль
            if user_id in self.monitored_users:
                self.monitored_users[user_id].total_messages = total_messages
                self.save_monitored_users()
            
            keyboard = self.create_keyboard([
                [
                    {"text": "🔍 Найти сообщения", "callback_data": f"search_messages:{user_id}"},
                    {"text": "📊 Профиль", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": "🔙 В меню", "callback_data": f"back_to_menu:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, report_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка подсчета сообщений: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
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
            # Используем сохраненные данные
            if user_id in self.monitored_users:
                stats["total_messages"] = self.monitored_users[user_id].total_messages
            
            # Получаем список диалогов
            dialogs = await self.client.get_dialogs(limit=50)
            common_chats = []
            
            for dialog in dialogs:
                if dialog.is_group:
                    try:
                        # Для групп проверяем участников
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
                    # Для каналов сложнее, пропускаем или обрабатываем по-другому
                    pass
                elif dialog.is_user:
                    # Для личных чатов
                    if dialog.entity.id == user_id:
                        common_chats.append({
                            "id": dialog.id,
                            "title": dialog.name[:50],
                            "type": "private"
                        })
            
            stats["common_chats"] = len(common_chats)
            stats["common_chats_list"] = common_chats
            
            # Получаем друзей (из личных чатов с историей)
            friends = []
            for dialog in dialogs:
                if dialog.is_user and dialog.entity.id != user_id:
                    try:
                        # Проверяем есть ли сообщения от этого пользователя
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
            
            # Рассчитываем среднее количество сообщений в день (за 30 дней)
            if stats["total_messages"] > 0:
                stats["avg_per_day"] = stats["total_messages"] / 30.0
            
            # Определяем уровень активности
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
            f"🔍 <b>Поиск сообщений пользователя</b>\n\n"
            f"Введите текст для поиска в сообщениях пользователя:\n\n"
            f"<i>Пример: 'привет' или 'как дела'</i>\n\n"
            f"👤 Пользователь: <code>{user_id}</code>\n"
            f"🔎 Я найду все сообщения с этим текстом и отправлю ссылки на них."
        )
        
        # Сохраняем состояние
        self.user_states[chat_id] = {
            "action": "waiting_search_text",
            "user_id": user_id
        }
    
    async def search_user_messages(self, chat_id: int, user_id: int, search_text: str):
        """Ищет сообщения пользователя"""
        try:
            await self.send_bot_message(chat_id, 
                f"🔎 Ищу сообщения с текстом: '{search_text}'\n"
                f"👤 Пользователь ID: <code>{user_id}</code>"
            )
            
            # Получаем пользователя
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            # Загружаем список чатов
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "❌ Нет чатов для поиска!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "<code>@durov\n@telegram</code>"
                )
                return
            
            found_messages = []
            checked_chats = 0
            
            # Ищем в каждом чате
            for i, chat_identifier in enumerate(chats, 1):
                try:
                    # УБРАНО ПРОГРЕСС-СООБЩЕНИЕ
                    
                    # Получаем чат
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    checked_chats += 1
                    
                    # Ищем сообщения (ограничим 100 сообщений на чат)
                    async for message in self.client.iter_messages(
                        chat,
                        limit=100,
                        from_user=user
                    ):
                        if message and message.text and search_text.lower() in message.text.lower():
                            # Формируем ссылку
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
                            
                            # Отправляем сразу если нашли (первые 5 сразу)
                            if len(found_messages) <= 5:
                                msg_text = (
                                    f"💬 <b>Найдено сообщение:</b>\n\n"
                                    f"📌 Чат: {found_messages[-1]['chat']}\n"
                                    f"📅 Дата: {found_messages[-1]['date']}\n"
                                    f"📝 Текст: {found_messages[-1]['text']}\n"
                                    f"🔗 Ссылка: {found_messages[-1]['link']}"
                                )
                                await self.send_bot_message(chat_id, msg_text)
                
                except Exception as e:
                    print(f"Ошибка поиска в чате {chat_identifier}: {e}")
                    continue
                
                # Небольшая пауза
                await asyncio.sleep(0.5)
            
            # Итоговый отчет
            if found_messages:
                total_text = (
                    f"✅ <b>Поиск завершен!</b>\n\n"
                    f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"🔍 Текст: '{search_text}'\n"
                    f"📊 Найдено сообщений: {len(found_messages)}\n"
                    f"📁 Проверено чатов: {checked_chats} из {len(chats)}\n\n"
                    f"<i>Первые результаты отправлены выше ↑</i>"
                )
                
                # Если нашли больше 5, отправляем еще результаты
                if len(found_messages) > 5:
                    remaining = found_messages[5:min(15, len(found_messages))]
                    for msg in remaining:
                        msg_text = (
                            f"💬 <b>Еще найдено:</b>\n\n"
                            f"📌 Чат: {msg['chat']}\n"
                            f"📅 Дата: {msg['date']}\n"
                            f"📝 Текст: {msg['text']}\n"
                            f"🔗 Ссылка: {msg['link']}"
                        )
                        await self.send_bot_message(chat_id, msg_text)
                    
                    if len(found_messages) > 15:
                        await self.send_bot_message(chat_id,
                            f"📄 <b>И еще {len(found_messages) - 15} сообщений...</b>\n"
                            f"Всего найдено: {len(found_messages)}"
                        )
            else:
                total_text = (
                    f"❌ <b>Сообщений не найдено</b>\n\n"
                    f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"🔍 Текст: '{search_text}'\n"
                    f"📁 Проверено чатов: {checked_chats} из {len(chats)}"
                )
            
            keyboard = self.create_keyboard([
                [
                    {"text": "🔍 Новый поиск", "callback_data": f"search_messages:{user_id}"},
                    {"text": "📊 Информация", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": "🔙 В меню", "callback_data": f"back_to_menu:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, total_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка поиска сообщений: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка поиска: {str(e)[:100]}")
    
    async def toggle_message_monitoring(self, chat_id: int, user_id: int):
        """Включает/выключает отслеживание сообщений"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            # Проверяем текущее состояние
            if user_id not in self.tracking_status:
                self.tracking_status[user_id] = {'messages': False, 'avatar': False, 'replies': False}
            
            current_state = self.tracking_status[user_id]['messages']
            
            if current_state:
                # Выключаем отслеживание
                await self.stop_message_monitoring(user_id)
                self.tracking_status[user_id]['messages'] = False
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_messages = False
                
                await self.send_bot_message(chat_id,
                    f"🛑 <b>Отслеживание сообщений остановлено</b>\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: <code>{user_id}</code>\n\n"
                    f"❌ Вы больше не будете получать уведомления о новых сообщениях."
                )
            else:
                # Включаем отслеживание
                task = asyncio.create_task(
                    self.monitor_user_messages(chat_id, user_id),
                    name=f"msg_monitor_{user_id}"
                )
                self.tracking_tasks.append(task)
                
                self.tracking_status[user_id]['messages'] = True
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_messages = True
                
                await self.send_bot_message(chat_id,
                    f"👁 <b>Отслеживание сообщений включено</b>\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"📱 Username: @{user.username if user.username else 'нет'}\n\n"
                    f"📨 Теперь вы будете получать уведомления о новых сообщениях.\n"
                    f"🔔 Проверка каждые 2 минуты."
                )
            
            # Сохраняем изменения
            self.save_monitored_users()
            
            # Показываем обновленное меню
            await self.show_user_actions(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка переключения отслеживания сообщений: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def stop_message_monitoring(self, user_id: int):
        """Останавливает отслеживание сообщений"""
        # Ищем и останавливаем задачу
        for task in self.tracking_tasks:
            if not task.done() and task.get_name() == f"msg_monitor_{user_id}":
                task.cancel()
                print(f"🛑 Остановлено отслеживание сообщений для {user_id}")
                break
    
    async def monitor_user_messages(self, chat_id: int, user_id: int):
        """Мониторит сообщения пользователя"""
        print(f"🚀 Запущен мониторинг сообщений для пользователя {user_id}")
        
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            last_check = datetime.now() - timedelta(minutes=5)  # Проверять последние 5 минут
            
            while self.tracking_status.get(user_id, {}).get('messages', False):
                try:
                    # Получаем список диалогов
                    dialogs = await self.client.get_dialogs(limit=30)
                    
                    new_messages_found = False
                    
                    for dialog in dialogs:
                        if dialog.is_group or dialog.is_channel:
                            try:
                                # Получаем последние сообщения
                                messages = await self.client.get_messages(
                                    dialog.entity,
                                    limit=20,
                                    offset_date=last_check
                                )
                                
                                for message in messages:
                                    if (message and message.date > last_check and
                                        hasattr(message, 'from_id') and message.from_id):
                                        
                                        # Проверяем отправителя
                                        sender_id = None
                                        if hasattr(message.from_id, 'user_id'):
                                            sender_id = message.from_id.user_id
                                        elif hasattr(message, 'sender_id') and hasattr(message.sender_id, 'user_id'):
                                            sender_id = message.sender_id.user_id
                                        
                                        if sender_id == user_id and message.text:
                                            # Формируем ссылку
                                            link = await self.get_message_link(dialog.entity, message.id)
                                            
                                            # Отправляем уведомление
                                            notification = (
                                                f"🔔 <b>НОВОЕ СООБЩЕНИЕ!</b>\n\n"
                                                f"👤 От: {user.first_name}\n"
                                                f"💬 Чат: {dialog.name[:50]}\n"
                                                f"📅 Время: {message.date.strftime('%H:%M:%S')}\n"
                                                f"📝 Текст: {message.text[:200]}\n"
                                                f"🔗 Ссылка: {link}"
                                            )
                                            
                                            await self.send_bot_message(chat_id, notification)
                                            new_messages_found = True
                                            
                                            # Обновляем время последней проверки
                                            if message.date > last_check:
                                                last_check = message.date
                                            
                            except Exception as e:
                                print(f"Ошибка проверки чата {dialog.name}: {e}")
                                continue
                    
                    if new_messages_found:
                        print(f"📨 Найдены новые сообщения от {user_id}")
                    
                    # Ждем перед следующей проверкой
                    await asyncio.sleep(120)  # 2 минуты
                    last_check = datetime.now() - timedelta(minutes=2)  # Сдвигаем время проверки
                    
                except Exception as e:
                    print(f"Ошибка в цикле мониторинга: {e}")
                    await asyncio.sleep(300)  # 5 минут при ошибке
                    
        except Exception as e:
            print(f"Мониторинг сообщений остановлен для {user_id}: {e}")
    
    async def toggle_avatar_monitoring(self, chat_id: int, user_id: int):
        """Включает/выключает отслеживание аватарки"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            # Проверяем текущее состояние
            if user_id not in self.tracking_status:
                self.tracking_status[user_id] = {'messages': False, 'avatar': False, 'replies': False}
            
            current_state = self.tracking_status[user_id]['avatar']
            
            if current_state:
                # Выключаем отслеживание
                await self.stop_avatar_monitoring(user_id)
                self.tracking_status[user_id]['avatar'] = False
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_avatar = False
                
                await self.send_bot_message(chat_id,
                    f"🛑 <b>Отслеживание аватарки остановлено</b>\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: <code>{user_id}</code>\n\n"
                    f"❌ Вы больше не будете получать уведомления о смене аватарки."
                )
            else:
                # Включаем отслеживание
                # Получаем текущую аватарку
                current_avatar = await self.get_user_avatar_bytes(user_id)
                current_hash = hashlib.md5(current_avatar).hexdigest() if current_avatar else "no_avatar"
                self.avatar_cache[user_id] = current_hash
                
                # Запускаем задачу мониторинга
                task = asyncio.create_task(
                    self.monitor_user_avatar(chat_id, user_id),
                    name=f"avatar_monitor_{user_id}"
                )
                self.tracking_tasks.append(task)
                
                self.tracking_status[user_id]['avatar'] = True
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_avatar = True
                
                await self.send_bot_message(chat_id,
                    f"🖼 <b>Отслеживание аватарки включено</b>\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"📱 Username: @{user.username if user.username else 'нет'}\n\n"
                    f"📸 Теперь вы будете получать новую аватарку при ее смене.\n"
                    f"🔔 Проверка каждые 30 минут."
                )
                
                # Отправляем текущую аватарку если есть
                if current_avatar:
                    caption = f"📸 <b>Текущая аватарка</b>\n👤 {user.first_name}\n🆔 <code>{user_id}</code>"
                    await self.send_bot_message(chat_id, caption, photo=current_avatar)
            
            # Сохраняем изменения
            self.save_monitored_users()
            
            # Показываем обновленное меню
            await self.show_user_actions(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка переключения отслеживания аватарки: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def stop_avatar_monitoring(self, user_id: int):
        """Останавливает отслеживание аватарки"""
        # Ищем и останавливаем задачу
        for task in self.tracking_tasks:
            if not task.done() and task.get_name() == f"avatar_monitor_{user_id}":
                task.cancel()
                print(f"🛑 Остановлено отслеживание аватарки для {user_id}")
                break
        
        # Удаляем из кэша
        if user_id in self.avatar_cache:
            del self.avatar_cache[user_id]
    
    async def monitor_user_avatar(self, chat_id: int, user_id: int):
        """Мониторит аватарку пользователя"""
        print(f"🚀 Запущен мониторинг аватарки для пользователя {user_id}")
        
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            while self.tracking_status.get(user_id, {}).get('avatar', False):
                try:
                    # Получаем текущую аватарку
                    current_avatar = await self.get_user_avatar_bytes(user_id)
                    current_hash = hashlib.md5(current_avatar).hexdigest() if current_avatar else "no_avatar"
                    old_hash = self.avatar_cache.get(user_id, "")
                    
                    if current_hash != old_hash:
                        print(f"🔄 Обнаружена смена аватарки у {user_id}")
                        
                        # Обновляем кэш
                        self.avatar_cache[user_id] = current_hash
                        
                        if current_avatar:
                            # Отправляем новую аватарку
                            caption = (
                                f"🔄 <b>СМЕНА АВАТАРКИ!</b>\n\n"
                                f"👤 Пользователь: {user.first_name}\n"
                                f"🆔 ID: <code>{user_id}</code>\n"
                                f"📱 Username: @{user.username if user.username else 'нет'}\n"
                                f"📅 Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
                            )
                            
                            await self.send_bot_message(chat_id, caption, photo=current_avatar)
                        else:
                            # Аватарка удалена
                            await self.send_bot_message(chat_id,
                                f"🗑 <b>АВАТАРКА УДАЛЕНА</b>\n\n"
                                f"👤 Пользователь: {user.first_name}\n"
                                f"🆔 ID: <code>{user_id}</code>\n"
                                f"📅 Время: {datetime.now().strftime('%H:%M:%S')}"
                            )
                    
                    # Ждем перед следующей проверкой
                    await asyncio.sleep(1800)  # 30 минут
                    
                except Exception as e:
                    print(f"Ошибка в цикле мониторинга аватарки: {e}")
                    await asyncio.sleep(600)  # 10 минут при ошибке
                    
        except Exception as e:
            print(f"Мониторинг аватарки остановлен для {user_id}: {e}")
    
    async def toggle_reply_monitoring(self, chat_id: int, user_id: int):
        """Включает/выключает отслеживание ответов на сообщения"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            # Проверяем текущее состояние
            if user_id not in self.tracking_status:
                self.tracking_status[user_id] = {'messages': False, 'avatar': False, 'replies': False}
            
            current_state = self.tracking_status[user_id]['replies']
            
            if current_state:
                # Выключаем отслеживание
                await self.stop_reply_monitoring(user_id)
                self.tracking_status[user_id]['replies'] = False
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_replies = False
                
                await self.send_bot_message(chat_id,
                    f"🛑 <b>Отслеживание ответов остановлено</b>\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: <code>{user_id}</code>\n\n"
                    f"❌ Вы больше не будете получать уведомления о ответах на сообщения."
                )
            else:
                # Включаем отслеживание
                task = asyncio.create_task(
                    self.monitor_user_replies(chat_id, user_id),
                    name=f"reply_monitor_{user_id}"
                )
                self.tracking_tasks.append(task)
                
                self.tracking_status[user_id]['replies'] = True
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_replies = True
                
                await self.send_bot_message(chat_id,
                    f"💬 <b>Отслеживание ответов включено</b>\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"📱 Username: @{user.username if user.username else 'нет'}\n\n"
                    f"📨 Теперь вы будете получать уведомления о том, кто отвечает на сообщения пользователя.\n"
                    f"🔔 Проверка каждые 5 минут."
                )
            
            # Сохраняем изменения
            self.save_monitored_users()
            
            # Показываем обновленное меню
            await self.show_user_actions(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка переключения отслеживания ответов: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def stop_reply_monitoring(self, user_id: int):
        """Останавливает отслеживание ответов"""
        # Ищем и останавливаем задачу
        for task in self.tracking_tasks:
            if not task.done() and task.get_name() == f"reply_monitor_{user_id}":
                task.cancel()
                print(f"🛑 Остановлено отслеживание ответов для {user_id}")
                break
        
        # Очищаем кэш
        if user_id in self.last_message_ids:
            del self.last_message_ids[user_id]
    
    async def monitor_user_replies(self, chat_id: int, user_id: int):
        """Мониторит ответы на сообщения пользователя"""
        print(f"🚀 Запущен мониторинг ответов для пользователя {user_id}")
        
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            last_check = datetime.now() - timedelta(minutes=10)
            
            while self.tracking_status.get(user_id, {}).get('replies', False):
                try:
                    # Получаем список чатов из файла
                    chat_identifiers = await self.load_chats_list()
                    
                    if not chat_identifiers:
                        print(f"⚠️ Нет чатов для мониторинга ответов у {user_id}")
                        await asyncio.sleep(600)  # 10 минут
                        continue
                    
                    # Инициализируем кэш для этого пользователя если нужно
                    if user_id not in self.last_message_ids:
                        self.last_message_ids[user_id] = {}
                    
                    new_replies_found = False
                    
                    # Проверяем каждый чат
                    for chat_identifier in chat_identifiers:
                        try:
                            chat = await self.get_chat_by_identifier(chat_identifier)
                            if not chat:
                                continue
                            
                            # Получаем последние сообщения пользователя в этом чате
                            messages = await self.client.get_messages(
                                chat,
                                limit=10,
                                from_user=user,
                                offset_date=last_check
                            )
                            
                            for message in messages:
                                if not message or message.date <= last_check:
                                    continue
                                
                                # Получаем последний известный ID сообщения для этого чата
                                last_msg_id = self.last_message_ids[user_id].get(chat.id, 0)
                                
                                # Если это новое сообщение
                                if message.id > last_msg_id:
                                    # Обновляем кэш
                                    self.last_message_ids[user_id][chat.id] = message.id
                                    
                                    # Ждем немного чтобы могли появиться ответы
                                    await asyncio.sleep(2)
                                    
                                    # Проверяем есть ли ответы на это сообщение
                                    try:
                                        # Получаем историю вокруг сообщения
                                        replies = await self.client.get_messages(
                                            chat,
                                            min_id=message.id,
                                            limit=20
                                        )
                                        
                                        for reply in replies:
                                            # Проверяем что это ответ на наше сообщение
                                            if (reply and reply.reply_to and 
                                                reply.reply_to.reply_to_msg_id == message.id and
                                                hasattr(reply, 'from_id') and reply.from_id):
                                                
                                                # Получаем информацию об авторе ответа
                                                try:
                                                    reply_sender = await self.client.get_entity(reply.from_id)
                                                    sender_name = getattr(reply_sender, 'first_name', '')
                                                    if hasattr(reply_sender, 'last_name') and reply_sender.last_name:
                                                        sender_name += f" {reply_sender.last_name}"
                                                    if hasattr(reply_sender, 'username') and reply_sender.username:
                                                        sender_name += f" (@{reply_sender.username})"
                                                    
                                                    # Формируем ссылку
                                                    link = await self.get_message_link(chat, reply.id)
                                                    original_link = await self.get_message_link(chat, message.id)
                                                    
                                                    # Отправляем уведомление
                                                    notification = (
                                                        f"💬 <b>ОТВЕТ НА СООБЩЕНИЕ!</b>\n\n"
                                                        f"👤 На кого ответили: {user.first_name}\n"
                                                        f"👥 Кто ответил: {sender_name or 'Неизвестный'}\n"
                                                        f"💬 Чат: {getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))[:50]}\n"
                                                        f"📅 Время ответа: {reply.date.strftime('%H:%M:%S')}\n"
                                                        f"📝 Ответ: {reply.text[:200] if reply.text else 'нет текста'}\n"
                                                        f"🔗 Ответ: {link}\n"
                                                        f"🔗 Оригинал: {original_link}"
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
                        
                        # Небольшая пауза между чатами
                        await asyncio.sleep(1)
                    
                    if new_replies_found:
                        print(f"💬 Найдены новые ответы на сообщения {user_id}")
                    
                    # Обновляем время последней проверки
                    last_check = datetime.now()
                    
                    # Ждем перед следующей проверкой
                    await asyncio.sleep(300)  # 5 минут
                    
                except Exception as e:
                    print(f"Ошибка в цикле мониторинга ответов: {e}")
                    await asyncio.sleep(300)  # 5 минут при ошибке
                    
        except Exception as e:
            print(f"Мониторинг ответов остановлен для {user_id}: {e}")
    
    async def show_replies_menu(self, chat_id: int, user_id: int):
        """Показывает меню анализа ответов"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            menu_text = (
                f"💬 <b>АНАЛИЗ ОТВЕТОВ</b>\n\n"
                f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"🆔 ID: <code>{user_id}</code>\n\n"
                f"<i>Выберите тип анализа:</i>"
            )
            
            keyboard_buttons = [
                [
                    {"text": "👥 Кто отвечает пользователю", "callback_data": f"replies_to_user:{user_id}:0"},
                    {"text": "👤 Кому отвечает пользователь", "callback_data": f"replies_from_user:{user_id}:0"}
                ],
                [
                    {"text": "🔍 Поиск по конкретному юзеру", "callback_data": f"search_replies_to:{user_id}"},
                    {"text": "🔍 Поиск кому отвечает юзеру", "callback_data": f"search_replies_from:{user_id}"}
                ],
                [
                    {"text": "🔙 Назад", "callback_data": f"back_to_menu:{user_id}"},
                    {"text": "🔄 Обновить", "callback_data": f"show_replies:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, menu_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа меню ответов: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def ask_target_user_for_replies_to(self, chat_id: int, user_id: int):
        """Запрашивает пользователя для поиска реплаев КОМУ"""
        await self.send_bot_message(chat_id,
            f"🔍 <b>Поиск: кто отвечает пользователю</b>\n\n"
            f"Введите @username или ID пользователя, чтобы проверить:\n"
            f"• Отвечает ли этот пользователь нашему пользователю\n"
            f"• Сколько раз он отвечал\n"
            f"• Ссылки на все ответы\n\n"
            f"👤 Наш пользователь: <code>{user_id}</code>\n\n"
            f"<i>Введите @username или ID целевого пользователя:</i>"
        )
        
        # Сохраняем состояние
        self.user_states[chat_id] = {
            "action": "waiting_target_user_for_replies_to",
            "user_id": user_id
        }
    
    async def ask_target_user_for_replies_from(self, chat_id: int, user_id: int):
        """Запрашивает пользователя для поиска реплаев ОТ КОГО"""
        await self.send_bot_message(chat_id,
            f"🔍 <b>Поиск: кому отвечает пользователь</b>\n\n"
            f"Введите @username или ID пользователя, чтобы проверить:\n"
            f"• Отвечает ли наш пользователь этому пользователю\n"
            f"• Сколько раз он отвечал\n"
            f"• Ссылки на все ответы\n\n"
            f"👤 Наш пользователь: <code>{user_id}</code>\n\n"
            f"<i>Введите @username или ID целевого пользователя:</i>"
        )
        
        # Сохраняем состояние
        self.user_states[chat_id] = {
            "action": "waiting_target_user_for_replies_from",
            "user_id": user_id
        }
    
    async def search_replies_to_user(self, chat_id: int, user_id: int, target_user_input: str):
        """Ищет ответы от конкретного пользователя на сообщения нашего пользователя"""
        try:
            await self.send_bot_message(chat_id, 
                f"🔍 Ищу ответы от пользователя '{target_user_input}' на сообщения нашего пользователя...\n"
                f"👤 Наш пользователь ID: <code>{user_id}</code>"
            )
            
            # Получаем нашего пользователя
            try:
                user = await self.client.get_entity(PeerUser(user_id))
                user_name = user.first_name or f"User {user_id}"
            except:
                user_name = f"User {user_id}"
            
            # Получаем целевого пользователя
            target_user = None
            target_user_input_clean = target_user_input.strip().replace('@', '')
            
            if target_user_input_clean.isdigit() or (target_user_input_clean.startswith('-') and target_user_input_clean[1:].isdigit()):
                # Поиск по ID
                target_user_id = int(target_user_input_clean)
                try:
                    target_user = await self.client.get_entity(PeerUser(target_user_id))
                except:
                    await self.send_bot_message(chat_id, f"❌ Пользователь с ID {target_user_id} не найден")
                    return
            else:
                # Поиск по username
                username = target_user_input_clean
                try:
                    target_user = await self.client.get_entity(f"@{username}")
                except:
                    await self.send_bot_message(chat_id, f"❌ Пользователь @{username} не найден")
                    return
            
            if not target_user:
                await self.send_bot_message(chat_id, "❌ Не удалось получить информацию о целевом пользователе")
                return
            
            # Информация о целевом пользователе
            target_name = getattr(target_user, 'first_name', '')
            if hasattr(target_user, 'last_name') and target_user.last_name:
                target_name += f" {target_user.last_name}"
            if hasattr(target_user, 'username') and target_user.username:
                target_name += f" (@{target_user.username})"
            
            # Загружаем список чатов
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "❌ Нет чатов для поиска!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "<code>@durov\n@telegram</code>"
                )
                return
            
            found_replies = []
            checked_chats = 0
            start_date = datetime.now() - timedelta(days=30)  # Последние 30 дней
            
            # Ищем в каждом чате сообщения нашего пользователя
            for i, chat_identifier in enumerate(chats, 1):
                try:
                    # Получаем чат
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    checked_chats += 1
                    
                    # Получаем сообщения нашего пользователя за последние 30 дней
                    async for user_message in self.client.iter_messages(
                        chat,
                        limit=50,  # Ограничиваем для скорости
                        from_user=user_id,
                        offset_date=start_date
                    ):
                        if not user_message:
                            continue
                        
                        # Пропускаем служебные сообщения
                        if isinstance(user_message, MessageService):
                            continue
                        
                        # Получаем ID нашего сообщения
                        user_message_id = user_message.id
                        
                        try:
                            # Ищем сообщения-ответы на наше сообщение
                            # Используем search с параметром reply_to
                            async for reply_message in self.client.iter_messages(
                                chat,
                                limit=20,  # Проверяем 20 сообщений после нашего
                                offset_id=user_message_id - 1,  # Начинаем с ID меньше нашего
                                reverse=True  # В прямом порядке (вперед во времени)
                            ):
                                # Пропускаем служебные сообщения
                                if isinstance(reply_message, MessageService):
                                    continue
                                
                                # Проверяем, является ли это сообщение ответом
                                if (hasattr(reply_message, 'reply_to') and 
                                    reply_message.reply_to and
                                    hasattr(reply_message.reply_to, 'reply_to_msg_id') and
                                    reply_message.reply_to.reply_to_msg_id == user_message_id):
                                    
                                    # Получаем ID отправителя ответа
                                    reply_sender_id = None
                                    if hasattr(reply_message, 'from_id') and reply_message.from_id:
                                        if hasattr(reply_message.from_id, 'user_id'):
                                            reply_sender_id = reply_message.from_id.user_id
                                    elif hasattr(reply_message, 'sender_id') and reply_message.sender_id:
                                        if hasattr(reply_message.sender_id, 'user_id'):
                                            reply_sender_id = reply_message.sender_id.user_id
                                    
                                    # Если отправитель ответа - наш целевой пользователь
                                    if reply_sender_id == target_user.id:
                                        # Получаем текст сообщений
                                        user_msg_text = user_message.text or user_message.message or "без текста"
                                        reply_msg_text = reply_message.text or reply_message.message or "без текста"
                                        
                                        # Формируем ссылки
                                        reply_link = await self.get_message_link(chat, reply_message.id)
                                        original_link = await self.get_message_link(chat, user_message_id)
                                        chat_name = getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))
                                        
                                        found_replies.append({
                                            "chat": chat_name,
                                            "original_text": user_msg_text[:100],
                                            "reply_text": reply_msg_text[:100],
                                            "replier": target_name,
                                            "reply_time": reply_message.date.strftime("%d.%m.%Y %H:%M"),
                                            "reply_link": reply_link,
                                            "original_link": original_link,
                                            "chat_id": chat.id,
                                            "message_id": user_message_id,
                                            "reply_id": reply_message.id
                                        })
                                        
                                        # Отправляем уведомление если нашли
                                        if len(found_replies) <= 3:
                                            reply_info = (
                                                f"💬 <b>Найден ответ от {target_name}:</b>\n\n"
                                                f"👤 На чье сообщение: {user_name}\n"
                                                f"👥 Кто ответил: {target_name}\n"
                                                f"💬 Чат: {chat_name[:50]}\n"
                                                f"📅 Время ответа: {reply_message.date.strftime('%d.%m.%Y %H:%M')}\n"
                                                f"📝 Оригинал: {user_msg_text[:150]}\n"
                                                f"📝 Ответ: {reply_msg_text[:150]}\n"
                                                f"🔗 Ответ: {reply_link}\n"
                                                f"🔗 Оригинал: {original_link}"
                                            )
                                            await self.send_bot_message(chat_id, reply_info)
                                        
                                        # Прерываем поиск ответов для этого сообщения
                                        break
                        
                        except Exception as e:
                            print(f"Ошибка поиска ответов на сообщение {user_message_id}: {e}")
                            continue
                    
                except Exception as e:
                    print(f"Ошибка поиска в чате {chat_identifier}: {e}")
                    continue
                
                # Пауза между чатами
                await asyncio.sleep(0.3)
            
            # Итоговый отчет
            if found_replies:
                total_text = (
                    f"✅ <b>Поиск завершен!</b>\n\n"
                    f"👤 Наш пользователь: {user_name}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"👥 Целевой пользователь: {target_name}\n"
                    f"🆔 ID целевого: <code>{target_user.id}</code>\n"
                    f"📊 Найдено ответов: {len(found_replies)}\n"
                    f"📁 Проверено чатов: {checked_chats} из {len(chats)}\n"
                    f"⏳ Период: последние 30 дней\n\n"
                    f"<i>Первые результаты отправлены выше ↑</i>"
                )
                
                # Если нашли больше 3, отправляем еще результаты
                if len(found_replies) > 3:
                    remaining = found_replies[3:min(8, len(found_replies))]
                    for reply in remaining:
                        reply_info = (
                            f"💬 <b>Еще ответ от {target_name}:</b>\n\n"
                            f"💬 Чат: {reply['chat'][:50]}\n"
                            f"📅 Время: {reply['reply_time']}\n"
                            f"📝 Ответ: {reply['reply_text']}\n"
                            f"🔗 Ответ: {reply['reply_link']}"
                        )
                        await self.send_bot_message(chat_id, reply_info)
                    
                    if len(found_replies) > 8:
                        await self.send_bot_message(chat_id,
                            f"📄 <b>И еще {len(found_replies) - 8} ответов...</b>\n"
                            f"Всего найдено: {len(found_replies)}"
                        )
            else:
                total_text = (
                    f"❌ <b>Ответы не найдены</b>\n\n"
                    f"👤 Наш пользователь: {user_name}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"👥 Целевой пользователь: {target_name}\n"
                    f"🆔 ID целевого: <code>{target_user.id}</code>\n"
                    f"📁 Проверено чатов: {checked_chats} из {len(chats)}\n"
                    f"⏳ Период: последние 30 дней\n\n"
                    f"<i>Пользователь {target_name} не отвечал на сообщения нашего пользователя за последние 30 дней</i>"
                )
            
            keyboard = self.create_keyboard([
                [
                    {"text": "🔍 Новый поиск", "callback_data": f"search_replies_to:{user_id}"},
                    {"text": "📊 Профиль", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": "🔙 В меню", "callback_data": f"back_to_menu:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, total_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка поиска ответов от пользователя: {e}")
            import traceback
            traceback.print_exc()
            await self.send_bot_message(chat_id, f"❌ Ошибка поиска: {str(e)[:100]}")
    
    async def search_replies_from_user(self, chat_id: int, user_id: int, target_user_input: str):
        """Ищет ответы нашего пользователя на сообщения целевого пользователя"""
        try:
            await self.send_bot_message(chat_id, 
                f"🔍 Ищу ответы нашего пользователя на сообщения '{target_user_input}'...\n"
                f"👤 Наш пользователь ID: <code>{user_id}</code>"
            )
            
            # Получаем нашего пользователя
            try:
                user = await self.client.get_entity(PeerUser(user_id))
                user_name = user.first_name or f"User {user_id}"
            except:
                user_name = f"User {user_id}"
            
            # Получаем целевого пользователя
            target_user = None
            target_user_input_clean = target_user_input.strip().replace('@', '')
            
            if target_user_input_clean.isdigit() or (target_user_input_clean.startswith('-') and target_user_input_clean[1:].isdigit()):
                # Поиск по ID
                target_user_id = int(target_user_input_clean)
                try:
                    target_user = await self.client.get_entity(PeerUser(target_user_id))
                except:
                    await self.send_bot_message(chat_id, f"❌ Пользователь с ID {target_user_id} не найден")
                    return
            else:
                # Поиск по username
                username = target_user_input_clean
                try:
                    target_user = await self.client.get_entity(f"@{username}")
                except:
                    await self.send_bot_message(chat_id, f"❌ Пользователь @{username} не найден")
                    return
            
            if not target_user:
                await self.send_bot_message(chat_id, "❌ Не удалось получить информацию о целевом пользователе")
                return
            
            # Информация о целевом пользователе
            target_name = getattr(target_user, 'first_name', '')
            if hasattr(target_user, 'last_name') and target_user.last_name:
                target_name += f" {target_user.last_name}"
            if hasattr(target_user, 'username') and target_user.username:
                target_name += f" (@{target_user.username})"
            
            # Загружаем список чатов
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "❌ Нет чатов для поиска!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "<code>@durov\n@telegram</code>"
                )
                return
            
            found_replies = []
            checked_chats = 0
            start_date = datetime.now() - timedelta(days=30)  # Последние 30 дней
            
            # Ищем в каждом чате ответы нашего пользователя
            for i, chat_identifier in enumerate(chats, 1):
                try:
                    # Получаем чат
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    checked_chats += 1
                    
                    # Получаем ответы нашего пользователя за последние 30 дней
                    async for user_reply in self.client.iter_messages(
                        chat,
                        limit=50,  # Ограничиваем для скорости
                        from_user=user_id,
                        offset_date=start_date
                    ):
                        if not user_reply or not hasattr(user_reply, 'reply_to') or not user_reply.reply_to:
                            continue
                        
                        # Пропускаем служебные сообщения
                        if isinstance(user_reply, MessageService):
                            continue
                        
                        # Получаем ID сообщения, на которое отвечают
                        reply_to_id = user_reply.reply_to.reply_to_msg_id
                        
                        try:
                            # Получаем оригинальное сообщение
                            original_msg = await self.client.get_messages(
                                chat,
                                ids=reply_to_id
                            )
                            
                            if not original_msg:
                                continue
                            
                            # Получаем отправителя оригинального сообщения
                            original_sender_id = None
                            if hasattr(original_msg, 'from_id') and original_msg.from_id:
                                if hasattr(original_msg.from_id, 'user_id'):
                                    original_sender_id = original_msg.from_id.user_id
                            elif hasattr(original_msg, 'sender_id') and original_msg.sender_id:
                                if hasattr(original_msg.sender_id, 'user_id'):
                                    original_sender_id = original_msg.sender_id.user_id
                            
                            # Если отправитель оригинального сообщения - наш целевой пользователь
                            if original_sender_id == target_user.id:
                                # Получаем текст сообщений
                                original_text = original_msg.text or original_msg.message or "без текста"
                                reply_text = user_reply.text or user_reply.message or "без текста"
                                
                                # Формируем ссылки
                                reply_link = await self.get_message_link(chat, user_reply.id)
                                original_link = await self.get_message_link(chat, reply_to_id)
                                chat_name = getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))
                                
                                found_replies.append({
                                    "chat": chat_name,
                                    "original_text": original_text[:100],
                                    "reply_text": reply_text[:100],
                                    "replied_to": target_name,
                                    "reply_time": user_reply.date.strftime("%d.%m.%Y %H:%M"),
                                    "reply_link": reply_link,
                                    "original_link": original_link,
                                    "chat_id": chat.id,
                                    "message_id": reply_to_id,
                                    "reply_id": user_reply.id
                                })
                                
                                # Отправляем уведомление если нашли
                                if len(found_replies) <= 3:
                                    reply_info = (
                                        f"💬 <b>Найден ответ нашего пользователя {user_name}:</b>\n\n"
                                        f"👤 Кому ответил: {target_name}\n"
                                        f"👥 Кто ответил: {user_name}\n"
                                        f"💬 Чат: {chat_name[:50]}\n"
                                        f"📅 Время ответа: {user_reply.date.strftime('%d.%m.%Y %H:%M')}\n"
                                        f"📝 Оригинал: {original_text[:150]}\n"
                                        f"📝 Ответ: {reply_text[:150]}\n"
                                        f"🔗 Ответ: {reply_link}\n"
                                        f"🔗 Оригинал: {original_link}"
                                    )
                                    await self.send_bot_message(chat_id, reply_info)
                        
                        except Exception as e:
                            print(f"Ошибка получения оригинального сообщения: {e}")
                            continue
                    
                except Exception as e:
                    print(f"Ошибка поиска в чате {chat_identifier}: {e}")
                    continue
                
                # Пауза между чатами
                await asyncio.sleep(0.3)
            
            # Итоговый отчет
            if found_replies:
                total_text = (
                    f"✅ <b>Поиск завершен!</b>\n\n"
                    f"👤 Наш пользователь: {user_name}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"👥 Целевой пользователь: {target_name}\n"
                    f"🆔 ID целевого: <code>{target_user.id}</code>\n"
                    f"📊 Найдено ответов: {len(found_replies)}\n"
                    f"📁 Проверено чатов: {checked_chats} из {len(chats)}\n"
                    f"⏳ Период: последние 30 дней\n\n"
                    f"<i>Первые результаты отправлены выше ↑</i>"
                )
                
                # Если нашли больше 3, отправляем еще результаты
                if len(found_replies) > 3:
                    remaining = found_replies[3:min(8, len(found_replies))]
                    for reply in remaining:
                        reply_info = (
                            f"💬 <b>Еще ответ нашего пользователя {target_name}:</b>\n\n"
                            f"💬 Чат: {reply['chat'][:50]}\n"
                            f"📅 Время: {reply['reply_time']}\n"
                            f"📝 Ответ: {reply['reply_text']}\n"
                            f"🔗 Ответ: {reply['reply_link']}"
                        )
                        await self.send_bot_message(chat_id, reply_info)
                    
                    if len(found_replies) > 8:
                        await self.send_bot_message(chat_id,
                            f"📄 <b>И еще {len(found_replies) - 8} ответов...</b>\n"
                            f"Всего найдено: {len(found_replies)}"
                        )
            else:
                total_text = (
                    f"❌ <b>Ответы не найдены</b>\n\n"
                    f"👤 Наш пользователь: {user_name}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"👥 Целевой пользователь: {target_name}\n"
                    f"🆔 ID целевого: <code>{target_user.id}</code>\n"
                    f"📁 Проверено чатов: {checked_chats} из {len(chats)}\n"
                    f"⏳ Период: последние 30 дней\n\n"
                    f"<i>Наш пользователь не отвечал на сообщения пользователя {target_name} за последние 30 дней</i>"
                )
            
            keyboard = self.create_keyboard([
                [
                    {"text": "🔍 Новый поиск", "callback_data": f"search_replies_from:{user_id}"},
                    {"text": "📊 Профиль", "callback_data": f"user_info:{user_id}"}
                ],
                [
                    {"text": "🔙 В меню", "callback_data": f"back_to_menu:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, total_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка поиска ответов пользователя: {e}")
            import traceback
            traceback.print_exc()
            await self.send_bot_message(chat_id, f"❌ Ошибка поиска: {str(e)[:100]}")
    
    async def collect_replies_data(self, user_id: int):
        """Собирает данные о реплаях пользователя"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            # Загружаем список чатов
            chat_identifiers = await self.load_chats_list()
            
            if not chat_identifiers:
                return {"to_user": [], "from_user": []}
            
            replies_to_user = []  # Кто отвечает пользователю
            replies_from_user = []  # Кому отвечает пользователь
            
            start_date = datetime.now() - timedelta(days=7)  # Последние 7 дней
            
            for i, chat_identifier in enumerate(chat_identifiers, 1):
                try:
                    # Получаем чат
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    # Получаем сообщения пользователя за последние 7 дней
                    user_messages = []
                    async for message in self.client.iter_messages(
                        chat,
                        limit=100,
                        from_user=user,
                        offset_date=start_date
                    ):
                        if message:
                            user_messages.append(message)
                    
                    # Проверяем кто отвечает на сообщения пользователя
                    for user_msg in user_messages:
                        try:
                            # Получаем ответы на это сообщение
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
                                        
                                        # Формируем ссылки
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
                                    break  # Нашел один ответ, можно перейти к следующему сообщению
                                    
                        except:
                            continue
                    
                    # Проверяем кому отвечает пользователь
                    async for message in self.client.iter_messages(
                        chat,
                        limit=100,
                        from_user=user,
                        offset_date=start_date
                    ):
                        if message and message.reply_to:
                            try:
                                # Получаем оригинальное сообщение
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
                                        
                                        # Формируем ссылки
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
                
                # Пауза между чатами
                await asyncio.sleep(0.5)
            
            # Сохраняем в кэш
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
            await self.send_bot_message(chat_id, "🔍 Собираю данные о том, кто отвечает пользователю...")
            
            # Получаем данные о реплаях
            reply_data = await self.collect_replies_data(user_id)
            replies_to_user = reply_data["to_user"]
            
            if not replies_to_user:
                await self.send_bot_message(chat_id,
                    f"❌ <b>Ответов не найдено</b>\n\n"
                    f"👤 Пользователь: <code>{user_id}</code>\n"
                    f"⏳ Период: последние 7 дней\n\n"
                    f"За последнюю неделю никто не отвечал на сообщения пользователя."
                )
                return
            
            # Группируем по пользователям
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
            
            # Сортируем по количеству ответов
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
            
            # Разбиваем на страницы
            items_per_page = 5
            total_pages = (len(sorted_users) + items_per_page - 1) // items_per_page
            
            if page >= total_pages:
                page = total_pages - 1
            
            start_idx = page * items_per_page
            end_idx = min((page + 1) * items_per_page, len(sorted_users))
            
            # Формируем сообщение
            message_text = (
                f"👥 <b>КТО ОТВЕЧАЕТ ПОЛЬЗОВАТЕЛЮ</b>\n\n"
                f"👤 Пользователь: <code>{user_id}</code>\n"
                f"📊 Всего отвечавших: {len(sorted_users)}\n"
                f"💬 Всего ответов: {len(replies_to_user)}\n"
                f"⏳ Период: последние 7 дней\n\n"
                f"<i>Страница {page + 1} из {total_pages}</i>\n\n"
            )
            
            # Добавляем список пользователей
            for i, (replier_id, stats) in enumerate(sorted_users[start_idx:end_idx], start_idx + 1):
                message_text += f"{i}. {stats['name']} - {stats['count']} ответов\n"
            
            # Создаем клавиатуру
            keyboard_buttons = []
            
            # Кнопки навигации
            nav_buttons = []
            if page > 0:
                nav_buttons.append({"text": "⬅️ Назад", "callback_data": f"replies_to_user:{user_id}:{page-1}"})
            
            if page < total_pages - 1:
                nav_buttons.append({"text": "Вперёд ➡️", "callback_data": f"replies_to_user:{user_id}:{page+1}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            # Кнопки для детального просмотра
            for i, (replier_id, stats) in enumerate(sorted_users[start_idx:end_idx], start_idx):
                keyboard_buttons.append([
                    {"text": f"👁️‍🗨️ {stats['name'][:20]}", "callback_data": f"view_reply_pair:{user_id}:{i}:to"}
                ])
            
            # Кнопки управления
            keyboard_buttons.append([
                {"text": "🔍 Поиск по юзеру", "callback_data": f"search_replies_to:{user_id}"},
                {"text": "🔙 Назад к меню", "callback_data": f"show_replies:{user_id}"}
            ])
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа ответов пользователю: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_replies_from_user(self, chat_id: int, user_id: int, page: int = 0):
        """Показывает кому отвечает пользователь"""
        try:
            await self.send_bot_message(chat_id, "🔍 Собираю данные о том, кому отвечает пользователь...")
            
            # Получаем данные о реплаях
            reply_data = await self.collect_replies_data(user_id)
            replies_from_user = reply_data["from_user"]
            
            if not replies_from_user:
                await self.send_bot_message(chat_id,
                    f"❌ <b>Ответов не найдено</b>\n\n"
                    f"👤 Пользователь: <code>{user_id}</code>\n"
                    f"⏳ Период: последние 7 дней\n\n"
                    f"За последнюю неделю пользователь никому не отвечал."
                )
                return
            
            # Группируем по пользователям
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
            
            # Сортируем по количеству ответов
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
            
            # Разбиваем на страницы
            items_per_page = 5
            total_pages = (len(sorted_users) + items_per_page - 1) // items_per_page
            
            if page >= total_pages:
                page = total_pages - 1
            
            start_idx = page * items_per_page
            end_idx = min((page + 1) * items_per_page, len(sorted_users))
            
            # Формируем сообщение
            message_text = (
                f"👤 <b>КОМУ ОТВЕЧАЕТ ПОЛЬЗОВАТЕЛЬ</b>\n\n"
                f"👤 Пользователь: <code>{user_id}</code>\n"
                f"📊 Всего собеседников: {len(sorted_users)}\n"
                f"💬 Всего ответов: {len(replies_from_user)}\n"
                f"⏳ Период: последние 7 дней\n\n"
                f"<i>Страница {page + 1} из {total_pages}</i>\n\n"
            )
            
            # Добавляем список пользователей
            for i, (replied_to_id, stats) in enumerate(sorted_users[start_idx:end_idx], start_idx + 1):
                message_text += f"{i}. {stats['name']} - {stats['count']} ответов\n"
            
            # Создаем клавиатуру
            keyboard_buttons = []
            
            # Кнопки навигации
            nav_buttons = []
            if page > 0:
                nav_buttons.append({"text": "⬅️ Назад", "callback_data": f"replies_from_user:{user_id}:{page-1}"})
            
            if page < total_pages - 1:
                nav_buttons.append({"text": "Вперёд ➡️", "callback_data": f"replies_from_user:{user_id}:{page+1}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            # Кнопки для детального просмотра
            for i, (replied_to_id, stats) in enumerate(sorted_users[start_idx:end_idx], start_idx):
                keyboard_buttons.append([
                    {"text": f"👁️‍🗨️ {stats['name'][:20]}", "callback_data": f"view_reply_pair:{user_id}:{i}:from"}
                ])
            
            # Кнопки управления
            keyboard_buttons.append([
                {"text": "🔍 Поиск по юзеру", "callback_data": f"search_replies_from:{user_id}"},
                {"text": "🔙 Назад к меню", "callback_data": f"show_replies:{user_id}"}
            ])
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа ответов от пользователя: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_reply_pair_details(self, chat_id: int, user_id: int, index: int, direction: str):
        """Показывает детали реплаев с конкретным пользователем"""
        try:
            # Получаем данные о реплаях
            reply_data = await self.collect_replies_data(user_id)
            
            if direction == "to":
                replies = reply_data["to_user"]
                # Группируем по пользователям
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
                    await self.send_bot_message(chat_id, "❌ Неверный индекс пользователя")
                    return
                
                target_user_id, target_user_stats = sorted_users[index]
                target_replies = target_user_stats["replies"]
                title = f"👥 {target_user_stats['name']} отвечает пользователю"
                
            else:  # direction == "from"
                replies = reply_data["from_user"]
                # Группируем по пользователям
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
                    await self.send_bot_message(chat_id, "❌ Неверный индекс пользователя")
                    return
                
                target_user_id, target_user_stats = sorted_users[index]
                target_replies = target_user_stats["replies"]
                title = f"👤 Пользователь отвечает {target_user_stats['name']}"
            
            # Отправляем первую страницу
            await self.show_reply_pair_page(chat_id, user_id, index, direction, 0)
            
        except Exception as e:
            print(f"Ошибка показа деталей реплаев: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_reply_pair_page(self, chat_id: int, user_id: int, user_index: int, direction: str, page: int):
        """Показывает страницу с реплаями для конкретного пользователя"""
        try:
            # Получаем данные о реплаях
            reply_data = await self.collect_replies_data(user_id)
            
            if direction == "to":
                replies = reply_data["to_user"]
                # Группируем по пользователям
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
                    await self.send_bot_message(chat_id, "❌ Неверный индекс пользователя")
                    return
                
                target_user_id, target_user_stats = sorted_users[user_index]
                target_replies = target_user_stats["replies"]
                title = f"👥 {target_user_stats['name']} отвечает пользователю"
                
            else:  # direction == "from"
                replies = reply_data["from_user"]
                # Группируем по пользователям
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
                    await self.send_bot_message(chat_id, "❌ Неверный индекс пользователя")
                    return
                
                target_user_id, target_user_stats = sorted_users[user_index]
                target_replies = target_user_stats["replies"]
                title = f"👤 Пользователь отвечает {target_user_stats['name']}"
            
            # Разбиваем на страницы
            items_per_page = 3
            total_pages = (len(target_replies) + items_per_page - 1) // items_per_page
            
            if page >= total_pages:
                page = total_pages - 1
            
            start_idx = page * items_per_page
            end_idx = min((page + 1) * items_per_page, len(target_replies))
            
            # Формируем сообщение
            message_text = f"💬 <b>{title}</b>\n\n"
            message_text += f"📊 Всего ответов: {len(target_replies)}\n"
            message_text += f"📄 Страница {page + 1} из {total_pages}\n\n"
            
            # Добавляем реплаи
            for i in range(start_idx, end_idx):
                reply = target_replies[i]
                message_text += f"<b>Ответ {i+1}:</b>\n"
                message_text += f"💬 Чат: {reply['chat_name'][:30]}\n"
                message_text += f"📅 Дата: {reply['reply_time']}\n"
                message_text += f"📝 Оригинал: {reply['original_text']}\n"
                message_text += f"📝 Ответ: {reply['reply_text']}\n"
                message_text += f"🔗 Ответ: {reply['reply_link']}\n"
                message_text += f"🔗 Оригинал: {reply['original_link']}\n\n"
            
            # Создаем клавиатуру
            keyboard_buttons = []
            
            # Кнопки навигации по страницам
            nav_buttons = []
            if page > 0:
                nav_buttons.append({"text": "⬅️ Назад", "callback_data": f"reply_page:{user_id}:{user_index}:{direction}:{page-1}"})
            
            nav_buttons.append({"text": f"📄 {page+1}/{total_pages}", "callback_data": "noop"})
            
            if page < total_pages - 1:
                nav_buttons.append({"text": "Вперёд ➡️", "callback_data": f"reply_page:{user_id}:{user_index}:{direction}:{page+1}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            # Кнопки управления
            back_action = "replies_to_user" if direction == "to" else "replies_from_user"
            keyboard_buttons.append([
                {"text": "🔙 К списку", "callback_data": f"{back_action}:{user_id}:0"},
                {"text": "🔄 Обновить", "callback_data": f"view_reply_pair:{user_id}:{user_index}:{direction}"}
            ])
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка отправки страницы реплаев: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_friends_menu(self, chat_id: int, user_id: int):
        """Показывает меню друзей"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            menu_text = (
                f"👥 <b>АНАЛИЗ ДРУЗЕЙ И КОНТАКТОВ</b>\n\n"
                f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"🆔 ID: <code>{user_id}</code>\n\n"
                f"<i>Выберите тип анализа:</i>"
            )
            
            keyboard_buttons = [
                [
                    {"text": "👥 Кто отвечает пользователю", "callback_data": f"replies_to_user:{user_id}:0"},
                    {"text": "👤 Кому отвечает пользователь", "callback_data": f"replies_from_user:{user_id}:0"}
                ],
                [
                    {"text": "🔍 Поиск кто отвечает юзеру", "callback_data": f"search_replies_to:{user_id}"},
                    {"text": "🔍 Поиск кому отвечает юзер", "callback_data": f"search_replies_from:{user_id}"}
                ],
                [
                    {"text": "📊 Частые собеседники", "callback_data": f"track_friends_old:{user_id}"},
                    {"text": "🔍 Найти сообщения", "callback_data": f"search_messages:{user_id}"}
                ],
                [
                    {"text": "🔙 Назад", "callback_data": f"back_to_menu:{user_id}"},
                    {"text": "🔄 Обновить", "callback_data": f"track_friends:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, menu_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа меню друзей: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_message_details(self, chat_id: int, message_chat_id: int, message_id: int):
        """Показывает детали сообщения"""
        try:
            # Получаем чат
            try:
                chat = await self.client.get_entity(message_chat_id)
            except:
                await self.send_bot_message(chat_id, "❌ Не удалось получить чат")
                return
            
            # Получаем сообщение
            try:
                message = await self.client.get_messages(chat, ids=message_id)
            except:
                await self.send_bot_message(chat_id, "❌ Не удалось получить сообщение")
                return
            
            if not message:
                await self.send_bot_message(chat_id, "❌ Сообщение не найдено")
                return
            
            # Формируем ссылку
            link = await self.get_message_link(chat, message_id)
            
            # Получаем информацию об отправителе
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
            
            # Формируем сообщение
            message_text = (
                f"💬 <b>ДЕТАЛИ СООБЩЕНИЯ</b>\n\n"
                f"👤 Отправитель: {sender_name}\n"
                f"💬 Чат: {getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))}\n"
                f"📅 Дата: {message.date.strftime('%d.%m.%Y %H:%M:%S')}\n"
                f"🔗 Ссылка: {link}\n\n"
                f"<b>Текст сообщения:</b>\n"
                f"{message.text[:1000] if message.text else 'Сообщение без текста'}\n\n"
            )
            
            # Если есть реплай
            if message.reply_to:
                message_text += f"<b>↪️ Ответ на сообщение:</b> ID {message.reply_to.reply_to_msg_id}\n"
            
            keyboard = self.create_keyboard([
                [
                    {"text": "🔗 Открыть в Telegram", "url": link},
                    {"text": "🔙 Закрыть", "callback_data": "close"}
                ]
            ])
            
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа деталей сообщения: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_monitoring_menu(self, chat_id: int):
        """Показывает меню управления отслеживанием"""
        if not self.monitored_users:
            await self.send_bot_message(chat_id,
                "📭 <b>Нет отслеживаемых пользователей</b>\n\n"
                "Отправьте @username чтобы добавить пользователя.\n\n"
                "Пример: <code>@durov</code>"
            )
            return
        
        # Считаем активные задачи
        active_tasks = sum(1 for t in self.tracking_tasks if not t.done())
        
        menu_text = (
            f"👁 <b>ОТСЛЕЖИВАЕМЫЕ ПОЛЬЗОВАТЕЛИ:</b>\n\n"
            f"📊 Всего в кэше: {len(self.monitored_users)}\n"
            f"🔄 Активных задач: {active_tasks}\n\n"
            f"<i>Выберите пользователя:</i>"
        )
        
        # Создаем кнопки для каждого пользователя (первые 8)
        buttons = []
        users_list = list(self.monitored_users.items())[:8]
        
        for user_id, profile in users_list:
            name = profile.first_name[:15] or f"User {user_id}"
            status_msg = "📨" if profile.is_tracking_messages else ""
            status_ava = "🖼" if profile.is_tracking_avatar else ""
            status_rep = "💬" if profile.is_tracking_replies else ""
            status = f"{status_msg}{status_ava}{status_rep}"
            
            buttons.append([
                {"text": f"👤 {name} {status}", "callback_data": f"user_info:{user_id}"}
            ])
        
        if len(self.monitored_users) > 8:
            buttons.append([
                {"text": f"📄 И еще {len(self.monitored_users) - 8}...", "callback_data": "show_more_users"}
            ])
        
        buttons.append([
            {"text": "📊 Статистика", "callback_data": "stats"},
            {"text": "➕ Добавить", "callback_data": "add_user"}
        ])
        
        keyboard = self.create_keyboard(buttons)
        
        await self.send_bot_message(chat_id, menu_text, keyboard)
    
    async def show_stats(self, chat_id: int):
        """Показывает статистику бота"""
        active_tasks = sum(1 for t in self.tracking_tasks if not t.done())
        
        # Считаем пользователей с отслеживанием
        tracking_msg = sum(1 for u in self.monitored_users.values() if u.is_tracking_messages)
        tracking_ava = sum(1 for u in self.monitored_users.values() if u.is_tracking_avatar)
        tracking_rep = sum(1 for u in self.monitored_users.values() if u.is_tracking_replies)
        
        stats_text = (
            f"📊 <b>СТАТИСТИКА БОТА:</b>\n\n"
            f"👥 Пользователей в кэше: {len(self.monitored_users)}\n"
            f"📨 Отслеживается сообщений: {tracking_msg}\n"
            f"🖼 Отслеживается аватарок: {tracking_ava}\n"
            f"💬 Отслеживается ответов: {tracking_rep}\n"
            f"🔄 Активных задач мониторинга: {active_tasks}\n"
            f"📸 Аватарок в кэше: {len(self.avatar_cache)}\n"
            f"💾 Сохранено профилей: {len(self.monitored_users)}\n"
            f"⏰ Время работы сервера: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        )
        
        # Добавляем информацию о задачах
        if active_tasks > 0:
            stats_text += f"⚙️ <b>Текущие задачи:</b>\n"
            for task in self.tracking_tasks[:5]:  # Первые 5 задач
                if not task.done():
                    task_name = task.get_name() or "Unknown"
                    stats_text += f"• {task_name}\n"
            
            if active_tasks > 5:
                stats_text += f"... и еще {active_tasks - 5} задач\n"
        
        stats_text += f"\n🤖 <b>Бот работает стабильно!</b>"
        
        await self.send_bot_message(chat_id, stats_text)
    
    # Вспомогательные методы
    
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
                        chats.append(line)
        except:
            pass
        
        return chats
    
    async def get_chat_by_identifier(self, identifier: str):
        """Получает чат по идентификатору"""
        try:
            identifier = identifier.strip().replace('@', '')
            
            if identifier.startswith('-100') and identifier[4:].isdigit():
                # ID канала/супергруппы
                chat_id = int(identifier)
                return await self.client.get_entity(PeerChannel(chat_id))
            elif identifier.isdigit() or (identifier.startswith('-') and identifier[1:].isdigit()):
                # Просто ID
                chat_id = int(identifier)
                try:
                    return await self.client.get_entity(chat_id)
                except:
                    return None
            else:
                # Username
                try:
                    return await self.client.get_entity(identifier)
                except:
                    try:
                        return await self.client.get_entity(f"@{identifier}")
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
        print("🤖 Запускаю polling бота...")
        
        offset = 0
        max_retries = 5
        retry_count = 0
        
        while True:
            try:
                # Получаем обновления
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
                            retry_count = 0  # Сброс счетчика при успехе
                            
                            if data.get("ok") and data.get("result"):
                                for update in data["result"]:
                                    offset = update["update_id"] + 1
                                    
                                    if "message" in update:
                                        await self.handle_bot_command(update)
                                    elif "callback_query" in update:
                                        await self.handle_callback_query(update)
                        else:
                            print(f"❌ Ошибка API: {response.status}")
                            retry_count += 1
                
                # Если много ошибок подряд, ждем подольше
                if retry_count >= max_retries:
                    print(f"⚠️ Много ошибок, увеличиваю задержку...")
                    await asyncio.sleep(30)
                    retry_count = 0
                
            except asyncio.TimeoutError:
                # Таймаут - это нормально для long polling
                continue
            except aiohttp.ClientError as e:
                print(f"❌ Ошибка сети: {e}")
                retry_count += 1
                await asyncio.sleep(5)
            except Exception as e:
                print(f"❌ Ошибка polling: {e}")
                retry_count += 1
                await asyncio.sleep(5)
    
    async def run(self):
        """Основной метод запуска"""
        print("="*60)
        print("🤖 TELEGRAM SPY BOT v3.3")
        print("="*60)
        print("✨ Новая функция: Поиск реплаев по конкретному пользователю")
        print("="*60)
        
        # Подключаемся к Telegram
        if not await self.connect():
            print("❌ Не удалось подключиться к Telegram")
            return
        
        # Тестируем бота
        print("🔍 Тестирую подключение к боту...")
        test_msg = (
            f"🤖 <b>Шпионский бот запущен!</b>\n\n"
            f"✅ Подключение установлено\n"
            f"👤 Аккаунт: {self.current_user.first_name if self.current_user else 'Неизвестно'}\n"
            f"🆔 ID: {self.current_user.id if self.current_user else 'Неизвестно'}\n"
            f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
            f"✨ <b>Новые функции:</b>\n"
            f"• Поиск кто отвечает конкретному пользователю\n"
            f"• Поиск кому отвечает конкретному пользователю\n"
            f"• Все сообщения со ссылками и текстом\n"
            f"• Пагинация для длинных списков\n"
            f"• Улучшенный поиск сообщений\n\n"
            f"📝 Отправьте /start для начала работы"
        )
        
        if await self.send_bot_message(ADMIN_ID, test_msg):
            print("✅ Бот подключен и готов к работе!")
        else:
            print("⚠️ Бот не отвечает, но поиск будет работать")
        
        # Запускаем polling бота в фоне
        bot_task = asyncio.create_task(self.run_bot_polling())
        
        print("\n" + "="*60)
        print("✅ Бот запущен! Отправьте команду /start в Telegram")
        print(f"📱 ID вашего бота можно найти по токену")
        print("="*60 + "\n")
        
        try:
            # Ждем завершения
            await bot_task
        except KeyboardInterrupt:
            print("\n\n🛑 Получен сигнал остановки...")
        except Exception as e:
            print(f"\n❌ Критическая ошибка: {e}")
        finally:
            print("\n💾 Сохраняю данные...")
            self.save_monitored_users()
            
            # Отменяем все задачи
            print("🛑 Останавливаю задачи мониторинга...")
            for task in self.tracking_tasks:
                if not task.done():
                    task.cancel()
            
            # Отключаемся
            print("🔒 Отключаюсь от Telegram...")
            if self.client:
                await self.client.disconnect()
            
            print("👋 Бот завершил работу")

# Запуск бота
if __name__ == "__main__":
    # Проверяем зависимости
    try:
        import telethon
        import aiohttp
    except ImportError as e:
        print(f"❌ Не установлены зависимости: {e}")
        print("📦 Установите: pip install telethon aiohttp")
        sys.exit(1)
    
    # Проверяем Python версию
    if sys.version_info < (3, 7):
        print("❌ Требуется Python 3.7 или выше!")
        sys.exit(1)
    
    # Проверяем API данные
    if not os.path.exists("api_config.txt"):
        print("❌ Файл api_config.txt не найден!")
        print("📝 Создайте его с содержимым:")
        print("API_ID=ваш_api_id")
        print("API_HASH=ваш_api_hash")
        sys.exit(1)
    
    # Создаем и запускаем бота
    bot = TelegramSpyBot()
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n👋 Программа завершена пользователем")
    except Exception as e:
        print(f"\n❌ Фатальная ошибка: {e}")
        import traceback
        traceback.print_exc()