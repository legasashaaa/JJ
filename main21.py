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
        self.user_stats_cache: Dict[int, Dict] = {}  # Кэш статистики по реплаям для каждого пользователя
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
    
    async def send_bot_message_with_id(self, chat_id: int, text: str) -> Optional[int]:
        """Отправляет сообщение через бота и возвращает его ID"""
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            
            data = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get("result", {}).get("message_id")
                    return None
                        
        except Exception as e:
            print(f"Ошибка отправки сообщения с ID: {e}")
            return None
    
    async def edit_bot_message(self, chat_id: int, message_id: int, text: str) -> bool:
        """Редактирует сообщение бота"""
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
            
            data = {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, timeout=30) as response:
                    return response.status == 200
                        
        except Exception as e:
            print(f"Ошибка редактирования сообщения: {e}")
            return False
    
    async def delete_bot_message(self, chat_id: int, message_id: int) -> bool:
        """Удаляет сообщение бота"""
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage"
            
            data = {
                "chat_id": chat_id,
                "message_id": message_id
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, timeout=30) as response:
                    return response.status == 200
                        
        except Exception as e:
            print(f"Ошибка удаления сообщения: {e}")
            return False
    
    async def send_document(self, chat_id: int, filename: str, content: bytes, caption: str = None) -> bool:
        """Отправляет документ через бота"""
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
            
            form_data = aiohttp.FormData()
            form_data.add_field('chat_id', str(chat_id))
            form_data.add_field('document', 
                               content,
                               filename=filename,
                               content_type='text/plain')
            
            if caption:
                form_data.add_field('caption', caption[:1024])
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=form_data, timeout=60) as response:
                    return response.status == 200
                        
        except Exception as e:
            print(f"Ошибка отправки документа: {e}")
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
                    "👋 Шпионский бот активирован!\n\n"
                    "🔍 Новые функции:\n"
                    "• Поиск сообщений пользователя\n"
                    "• Отслеживание новых сообщений\n"
                    "• Мониторинг аватарок (с отправкой фото)\n"
                    "• Отслеживание ответов на сообщения\n"
                    "• Анализ активности и друзей\n\n"
                    "📝 Использование:\n"
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
                    "📋 Доступные команды:\n\n"
                    "/start - Начать работу\n"
                    "/monitor - Управление отслеживанием\n"
                    "/stats - Статистика\n"
                    "/help - Помощь\n\n"
                    "📝 Как использовать:\n"
                    "1. Отправьте @username (например @durov)\n"
                    "2. Или ID пользователя (например 123456789)\n"
                    "3. Выберите действие из меню\n\n"
                    "👁 Отслеживание:\n"
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
                    f"🤖 Тест бота\n\n"
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
                    
                    elif state.get("action") == "waiting_target_user_for_replies":
                        user_id = state["user_id"]
                        await self.search_replies_to_specific_user(chat_id, user_id, text)
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
                                "❌ Неверный формат!\n\n"
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
                            "❌ Неверный формат!\n\n"
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
                    "👤 Добавить пользователя\n\n"
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
            
            elif action == "all_user_replies":
                user_id = int(parts[1])
                await self.show_all_user_replies(chat_id, user_id)
            
            elif action == "search_replies_to_specific":
                user_id = int(parts[1])
                await self.ask_target_user_for_replies(chat_id, user_id)
            
            elif action == "view_reply_details":
                user_id = int(parts[1])
                target_user_id = int(parts[2])
                await self.show_reply_details_for_user(chat_id, user_id, target_user_id)
            
            elif action == "view_message":
                chat_id_val = int(parts[1])
                message_id = int(parts[2])
                await self.show_message_details(callback_query["message"]["chat"]["id"], chat_id_val, message_id)
            
            elif action == "show_user_chats":
                user_id = int(parts[1])
                page = int(parts[2]) if len(parts) > 2 else 0
                await self.show_user_chats(chat_id, user_id, page)
            
            elif action == "refresh_chats":
                user_id = int(parts[1])
                await self.load_user_chats(chat_id, user_id)
            
            elif action == "show_more_reply_users":
                user_id = int(parts[1])
                start_idx = int(parts[2]) if len(parts) > 2 else 6
                await self.show_more_reply_users(chat_id, user_id, start_idx)
            
            elif action == "export_reply_users":
                user_id = int(parts[1])
                await self.export_reply_users_to_file(chat_id, user_id)
            
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
            
            # Создаем профиль (БЕЗ загрузки чатов!)
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
            
            # НЕ загружаем чаты пользователя автоматически
            # Показываем меню действий без чатов
            await self.show_user_actions(chat_id, user.id)
            
        except Exception as e:
            error_msg = str(e)
            print(f"Ошибка поиска пользователя: {error_msg}")
            
            if "Cannot cast" in error_msg or "InputPeer" in error_msg:
                await self.send_bot_message(chat_id,
                    "❌ Ошибка приведения типа!\n\n"
                    "Попробуйте другой формат:\n"
                    "• @username (например @durov)\n"
                    "• ID пользователя (только цифры)\n\n"
                    "Или проверьте правильность ввода."
                )
            else:
                await self.send_bot_message(chat_id, f"❌ Ошибка: {error_msg[:100]}")
    
    async def load_user_chats(self, chat_id: int, user_id: int):
        """Быстро загружает чаты пользователя (по запросу)"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "❌ Пользователь не найден")
                return
            
            profile = self.monitored_users[user_id]
            
            # Если уже загружены, просто показываем
            if profile.user_chats_loaded and profile.user_chats:
                await self.show_user_actions_with_chats(chat_id, user_id)
                return
            
            # Отправляем красивое уведомление о прогрессе
            progress_msg = await self.send_bot_message(chat_id, "🔍 Сканирую диалоги... 📊 0%")
            last_update = time.time()
            
            user = await self.client.get_entity(PeerUser(user_id))
            user_chats = []
            
            # Получаем все диалоги
            dialogs = await self.client.get_dialogs()
            total_dialogs = len(dialogs)
            
            for dialog_idx, dialog in enumerate(dialogs):
                try:
                    # Пропускаем каналы (только channels)
                    if dialog.is_channel and not dialog.is_group:
                        continue
                    
                    # Обновляем прогресс каждые 5 секунд или каждые 10 диалогов
                    current_time = time.time()
                    if current_time - last_update > 5 or dialog_idx % 10 == 0:
                        progress = (dialog_idx / total_dialogs) * 100
                        emoji_progress = self.get_progress_emoji(progress)
                        await self.send_bot_message(chat_id, 
                            f"🔍 Сканирую диалоги... {emoji_progress} {progress:.1f}%\n"
                            f"📊 Обработано: {dialog_idx}/{total_dialogs} диалогов\n"
                            f"✅ Найдено чатов: {len(user_chats)}")
                        last_update = current_time
                    
                    # Проверяем есть ли пользователь в чате/группе
                    if dialog.is_group or dialog.is_channel:
                        # Для групп и супергрупп
                        try:
                            # Проверяем сообщения пользователя без лимита
                            message_count = 0
                            try:
                                async for message in self.client.iter_messages(
                                    dialog.entity,
                                    from_user=user
                                ):
                                    message_count += 1
                            except:
                                continue
                            
                            if message_count > 0:
                                chat_name = dialog.name
                                chat_link = await self.get_chat_link(dialog.entity)
                                
                                user_chats.append({
                                    'id': dialog.id,
                                    'name': chat_name[:40],
                                    'link': chat_link,
                                    'message_count': message_count,
                                    'last_activity': datetime.now()
                                })
                                
                        except Exception as e:
                            continue
                    
                    elif dialog.is_user:
                        # Личный чат
                        if dialog.entity.id == user_id:
                            chat_name = dialog.name
                            chat_link = await self.get_chat_link(dialog.entity)
                            
                            # Подсчет сообщений в личном чате без лимита
                            total_messages = 0
                            async for _ in self.client.iter_messages(dialog.entity):
                                total_messages += 1
                            
                            user_chats.append({
                                'id': dialog.id,
                                'name': f"👤 {chat_name}",
                                'link': chat_link,
                                'message_count': total_messages,
                                'last_activity': datetime.now()
                            })
                
                except Exception as e:
                    continue
            
            # Сохраняем чаты
            profile.user_chats = user_chats
            profile.user_chats_loaded = True
            profile.common_chats = len(user_chats)
            
            # Сохраняем
            self.save_monitored_users()
            
            # Финальное уведомление
            await self.send_bot_message(chat_id, 
                f"✅ Сканирование завершено!\n\n"
                f"📊 Всего диалогов: {total_dialogs}\n"
                f"💬 Найдено чатов: {len(user_chats)}\n"
                f"⏱ Время выполнения: {time.time() - start_time:.1f} сек"
            )
            
            # Показываем действия с чатами
            await self.show_user_actions_with_chats(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка загрузки чатов: {e}")
            # Все равно показываем меню, но без чатов
            await self.show_user_actions(chat_id, user_id)
    
    def get_progress_emoji(self, percentage: float) -> str:
        """Возвращает эмодзи для индикатора прогресса"""
        if percentage < 20:
            return "🟥🟥🟥🟥🟥"
        elif percentage < 40:
            return "🟧🟧🟥🟥🟥"
        elif percentage < 60:
            return "🟨🟨🟨🟥🟥"
        elif percentage < 80:
            return "🟩🟩🟩🟩🟥"
        else:
            return "🟩🟩🟩🟩🟩"
    
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
                f"👤 Пользователь:\n\n"
                f"🆔 ID: {user_id}\n"
                f"👤 Имя: {profile.first_name} {profile.last_name}\n"
                f"📱 Username: @{profile.username if profile.username else 'нет'}\n"
                f"📞 Телефон: {profile.phone if profile.phone else 'скрыт'}\n"
                f"👀 Был онлайн: {profile.last_seen.strftime('%d.%m.%Y %H:%M')}\n"
                f"🖼 Аватар: {'✅ есть' if has_avatar else '❌ нет'}\n"
                f"📁 Чатов найдено: {len(profile.user_chats)}\n\n"
            )
            
            # Добавляем информацию о чатах (первые 3)
            if profile.user_chats:
                user_info += f"Недавние чаты:\n"
                for i, chat in enumerate(profile.user_chats[:3], 1):
                    user_info += f"{i}. {chat['name']} - {chat['message_count']} сообщ.\n"
                
                if len(profile.user_chats) > 3:
                    user_info += f"... и еще {len(profile.user_chats) - 3} чатов\n"
                
                user_info += "\n"
            
            user_info += f"Выберите действие:"
            
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
                    {"text": "💬 Анализ реплаев", "callback_data": f"show_replies:{user_id}"}
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
                f"📁 ЧАТЫ ПОЛЬЗОВАТЕЛЯ\n\n"
                f"👤 Пользователь: {profile.first_name} {profile.last_name}\n"
                f"🆔 ID: {user_id}\n"
                f"📊 Всего чатов: {len(profile.user_chats)}\n"
                f"📄 Страница {page + 1} из {total_pages}\n\n"
            )
            
            # Сортируем по количеству сообщений
            sorted_chats = sorted(profile.user_chats, key=lambda x: x['message_count'], reverse=True)
            
            # Добавляем чаты текущей страницы
            for i, chat in enumerate(sorted_chats[start_idx:end_idx], start_idx + 1):
                message_text += f"{i}. {chat['name']} - {chat['message_count']} сообщ.\n"
            
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
                f"👤 Пользователь:\n\n"
                f"🆔 ID: {user_id}\n"
                f"👤 Имя: {profile.first_name} {profile.last_name}\n"
                f"📱 Username: @{profile.username if profile.username else 'нет'}\n"
                f"📞 Телефон: {profile.phone if profile.phone else 'скрыт'}\n"
                f"👀 Был онлайн: {profile.last_seen.strftime('%d.%m.%Y %H:%M')}\n"
                f"🖼 Аватар: {'✅ есть' if has_avatar else '❌ нет'}\n\n"
                f"Выберите действие:"
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
                    {"text": "💬 Анализ реплаев", "callback_data": f"show_replies:{user_id}"}
                ],
                [
                    {"text": "📊 Количество сообщений", "callback_data": f"get_message_count:{user_id}"},
                    {"text": "👥 Друзья", "callback_data": f"track_friends:{user_id}"}
                ],
                [
                    {"text": "📸 Получить аватарку", "callback_data": f"get_avatar:{user_id}"},
                    {"text": "🔄 Обновить", "callback_data": f"refresh_status:{user_id}"}
                ],
                [
                    {"text": "📁 Загрузить чаты пользователя", "callback_data": f"refresh_chats:{user_id}"}
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
                caption = f"🖼 Аватарка пользователя\n🆔 ID: {user_id}\n📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
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
                f"📊 ПОЛНАЯ ИНФОРМАЦИЯ:\n\n"
                f"👤 Основное:\n"
                f"• ID: {user_id}\n"
                f"• Имя: {profile.first_name} {profile.last_name}\n"
                f"• Username: @{profile.username if profile.username else 'нет'}\n"
                f"• Телефон: {profile.phone if profile.phone else 'скрыт'}\n"
                f"• Био: {profile.bio if profile.bio else 'нет'}\n"
                f"• Был онлайн: {profile.last_seen.strftime('%d.%m.%Y %H:%M')}\n\n"
                
                f"📈 Статистика:\n"
                f"• Общих чатов: {stats['common_chats']}\n"
                f"• Всего сообщений: {stats['total_messages']}\n"
                f"• Среднее в день: {stats['avg_per_day']:.1f}\n"
                f"• Активность: {stats['activity_level']}\n\n"
            )
            
            # Добавляем статус отслеживания
            info_text += f"👁 Отслеживание:\n"
            info_text += f"• Сообщения: {'✅ ВКЛ' if profile.is_tracking_messages else '❌ ВЫКЛ'}\n"
            info_text += f"• Аватарка: {'✅ ВКЛ' if profile.is_tracking_avatar else '❌ ВЫКЛ'}\n"
            info_text += f"• Ответы: {'✅ ВКЛ' if profile.is_tracking_replies else '❌ ВЫКЛ'}\n\n"
            
            if stats['common_chats_list']:
                info_text += f"👥 Общие чаты ({min(5, len(stats['common_chats_list']))} из {stats['common_chats']}):\n"
                for i, chat in enumerate(stats['common_chats_list'][:5], 1):
                    chat_name = chat.get('title', chat.get('username', f'Чат {chat["id"]}'))[:30]
                    info_text += f"{i}. {chat_name}\n"
                
                if len(stats['common_chats_list']) > 5:
                    info_text += f"... и еще {len(stats['common_chats_list']) - 5} чатов\n"
            
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
            # Создаем прогресс сообщение
            progress_msg = await self.send_bot_message(chat_id, "📊 Начинаю подсчёт сообщений... 📊 0%")
            last_update = time.time()
            start_time = time.time()
            
            # Получаем пользователя
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            # Загружаем список чатов (все из файла)
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "❌ Нет чатов для проверки!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "@durov\n@telegram\n"
                    "https://t.me/+tmE98W5NO6xlYmQy"
                )
                return
            
            total_messages = 0
            chat_stats = []
            checked_chats = 0
            
            # Проверяем каждый чат
            for i, chat_identifier in enumerate(chats, 1):
                try:
                    # Обновляем прогресс каждые 5 секунд или каждые 5 чатов
                    current_time = time.time()
                    if current_time - last_update > 5 or i % 5 == 0:
                        progress = (i / len(chats)) * 100
                        emoji_progress = self.get_progress_emoji(progress)
                        await self.send_bot_message(chat_id, 
                            f"📊 Считаю сообщения... {emoji_progress} {progress:.1f}%\n"
                            f"📁 Обработано: {i}/{len(chats)} чатов\n"
                            f"💬 Найдено сообщений: {total_messages}\n"
                            f"✅ Чатов с сообщениями: {len(chat_stats)}")
                        last_update = current_time
                    
                    # Получаем чат
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    checked_chats += 1
                    
                    # Получаем сообщения пользователя (БЕЗ ЛИМИТА)
                    message_count = 0
                    try:
                        async for message in self.client.iter_messages(
                            chat,
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
                    continue
                
                # Небольшая пауза чтобы не спамить
                await asyncio.sleep(0.1)
            
            # Сортируем по количеству сообщений
            chat_stats.sort(key=lambda x: x['count'], reverse=True)
            
            # Формируем отчет
            report_text = (
                f"✅ ПОДСЧЁТ ЗАВЕРШЁН!\n\n"
                f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"📈 Всего сообщений: {total_messages:,}\n"
                f"📁 Всего чатов в списке: {len(chats)}\n"
                f"✅ Проверено чатов: {checked_chats}\n"
                f"💬 Чатов с сообщениями: {len(chat_stats)}\n"
                f"⏱ Время выполнения: {time.time() - start_time:.1f} сек\n\n"
            )
            
            # Добавляем топ чатов
            if chat_stats:
                report_text += f"🏆 Топ чатов по активности:\n"
                for i, stat in enumerate(chat_stats[:10], 1):
                    report_text += f"{i}. {stat['name']}: {stat['count']:,} сообщ.\n"
            
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
            
            # Получаем список диалогов (без лимита)
            dialogs = await self.client.get_dialogs()
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
            f"🔍 Поиск сообщений пользователя\n\n"
            f"Введите текст для поиска в сообщениях пользователя:\n\n"
            f"Пример: 'привет' или 'как дела'\n\n"
            f"👤 Пользователь: {user_id}\n"
            f"🔎 Я найду все сообщения с этим текстом и отправлю файл со всеми ссылками."
        )
        
        # Сохраняем состояние
        self.user_states[chat_id] = {
            "action": "waiting_search_text",
            "user_id": user_id
        }
    
    async def search_user_messages(self, chat_id: int, user_id: int, search_text: str):
        """Ищет сообщения пользователя и отправляет файл с результатами"""
        try:
            await self.send_bot_message(chat_id, 
                f"🔎 Начинаю поиск сообщений с текстом: '{search_text}'\n"
                f"👤 Пользователь ID: {user_id}\n\n"
                f"⏳ Собираю все сообщения... Это может занять несколько минут."
            )
            
            start_time = time.time()
            
            # Получаем пользователя
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            # Загружаем список чатов (все из файла)
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "❌ Нет чатов для поиска!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "@durov\n@telegram\n"
                    "https://t.me/+tmE98W5NO6xlYmQy"
                )
                return
            
            found_messages = []
            checked_chats = 0
            
            # Ищем в каждом чате
            for i, chat_identifier in enumerate(chats, 1):
                try:
                    # Обновляем прогресс каждые 10 чатов
                    if i % 10 == 0:
                        progress = (i / len(chats)) * 100
                        await self.send_bot_message(chat_id, 
                            f"🔎 Ищу сообщения... {progress:.1f}%\n"
                            f"📁 Обработано: {i}/{len(chats)} чатов\n"
                            f"💬 Найдено сообщений: {len(found_messages)}")
                    
                    # Получаем чат
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    checked_chats += 1
                    
                    # Ищем сообщения (БЕЗ ЛИМИТА!)
                    async for message in self.client.iter_messages(
                        chat,
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
                                "text": message.text[:200],
                                "date": message.date.strftime("%d.%m.%Y %H:%M"),
                                "link": link,
                                "chat_id": chat.id,
                                "message_id": message.id
                            })
                            
                except Exception as e:
                    continue
                
                # Небольшая пауза
                await asyncio.sleep(0.05)
            
            # Если нашли сообщения, создаем файл
            if found_messages:
                # Создаем содержимое файла
                file_content = "=" * 60 + "\n"
                file_content += f"РЕЗУЛЬТАТЫ ПОИСКА СООБЩЕНИЙ\n"
                file_content += f"Пользователь: {user_id}\n"
                file_content += f"Текст для поиска: '{search_text}'\n"
                file_content += f"Дата поиска: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                file_content += f"Всего найдено: {len(found_messages)} сообщений\n"
                file_content += f"Чатов проверено: {checked_chats}/{len(chats)}\n"
                file_content += f"Время выполнения: {time.time() - start_time:.1f} сек\n"
                file_content += "=" * 60 + "\n\n"
                
                # Добавляем все сообщения
                for i, msg in enumerate(found_messages, 1):
                    file_content += f"{i}. ЧАТ: {msg['chat']}\n"
                    file_content += f"   ДАТА: {msg['date']}\n"
                    file_content += f"   ТЕКСТ: {msg['text']}\n"
                    file_content += f"   ССЫЛКА: {msg['link']}\n"
                    file_content += "-" * 40 + "\n"
                
                # Сохраняем файл
                filename = f"search_results_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                
                # Отправляем файл
                caption = (
                    f"📄 Результаты поиска\n\n"
                    f"👤 Пользователь: ID {user_id}\n"
                    f"🔍 Текст: '{search_text}'\n"
                    f"📊 Найдено сообщений: {len(found_messages):,}\n"
                    f"📁 Чатов проверено: {checked_chats}/{len(chats)}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек"
                )
                
                await self.send_document(chat_id, filename, file_content.encode('utf-8'), caption)
                
                # Отправляем итоговое сообщение
                total_text = (
                    f"✅ ПОИСК ЗАВЕРШЁН!\n\n"
                    f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"🔍 Текст: '{search_text}'\n"
                    f"📊 Найдено сообщений: {len(found_messages):,}\n"
                    f"📁 Всего чатов в списке: {len(chats)}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек\n\n"
                    f"📄 Файл с результатами отправлен выше."
                )
            else:
                total_text = (
                    f"❌ Сообщений не найдено\n\n"
                    f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"🔍 Текст: '{search_text}'\n"
                    f"📁 Всего чатов в списке: {len(chats)}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек"
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
                    f"🛑 Отслеживание сообщений остановлено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}\n\n"
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
                    f"👁 Отслеживание сообщений включено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}\n"
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
                                # Получаем последние сообщения (без лимита для новых)
                                messages = await self.client.get_messages(
                                    dialog.entity,
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
                                                f"🔔 НОВОЕ СООБЩЕНИЕ!\n\n"
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
                    f"🛑 Отслеживание аватарки остановлено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}\n\n"
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
                    f"🖼 Отслеживание аватарки включено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}\n"
                    f"📱 Username: @{user.username if user.username else 'нет'}\n\n"
                    f"📸 Теперь вы будете получать новую аватарку при ее смене.\n"
                    f"🔔 Проверка каждые 30 минут."
                )
                
                # Отправляем текущую аватарку если есть
                if current_avatar:
                    caption = f"📸 Текущая аватарка\n👤 {user.first_name}\n🆔 {user_id}"
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
                                f"🔄 СМЕНА АВАТАРКИ!\n\n"
                                f"👤 Пользователь: {user.first_name}\n"
                                f"🆔 ID: {user_id}\n"
                                f"📱 Username: @{user.username if user.username else 'нет'}\n"
                                f"📅 Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
                            )
                            
                            await self.send_bot_message(chat_id, caption, photo=current_avatar)
                        else:
                            # Аватарка удалена
                            await self.send_bot_message(chat_id,
                                f"🗑 АВАТАРКА УДАЛЕНА\n\n"
                                f"👤 Пользователь: {user.first_name}\n"
                                f"🆔 ID: {user_id}\n"
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
                    f"🛑 Отслеживание ответов остановлено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}\n\n"
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
                    f"💬 Отслеживание ответов включено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}\n"
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
                    # Получаем список чатов из файла (все чаты)
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
                            
                            # Получаем последние сообщения пользователя в этом чате (без лимита)
                            messages = await self.client.get_messages(
                                chat,
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
                                        # Получаем историю вокруг сообщения (без лимита)
                                        replies = await self.client.get_messages(
                                            chat,
                                            min_id=message.id
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
                                                        f"💬 ОТВЕТ НА СООБЩЕНИЕ!\n\n"
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
                                                    continue
                                        
                                    except Exception as e:
                                        continue
                        
                        except Exception as e:
                            continue
                        
                        # Небольшая пауза между чатами
                        await asyncio.sleep(0.5)
                    
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
        """Показывает меню анализа реплаев"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            menu_text = (
                f"💬 АНАЛИЗ РЕПЛАЕВ ПОЛЬЗОВАТЕЛЯ\n\n"
                f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"🆔 ID: {user_id}\n\n"
                f"Выберите тип анализа:"
            )
            
            keyboard_buttons = [
                [
                    {"text": "📊 Все реплаи пользователя", "callback_data": f"all_user_replies:{user_id}"},
                ],
                [
                    {"text": "🔍 Поиск по конкретному юзеру", "callback_data": f"search_replies_to_specific:{user_id}"},
                ],
                [
                    {"text": "🔙 Назад", "callback_data": f"back_to_menu:{user_id}"},
                    {"text": "🔄 Обновить", "callback_data": f"show_replies:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, menu_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа меню реплаев: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def ask_target_user_for_replies(self, chat_id: int, user_id: int):
        """Запрашивает пользователя для поиска реплаев КОМУ"""
        await self.send_bot_message(chat_id,
            f"🔍 Поиск: кому реплаит пользователь\n\n"
            f"Введите @username или ID пользователя, чтобы проверить:\n"
            f"• Реплаил ли наш пользователь этому пользователю\n"
            f"• Сколько раз он реплаил\n"
            f"• Ссылки на все реплаи\n\n"
            f"👤 Наш пользователь: {user_id}\n\n"
            f"Введите @username или ID целевого пользователя:"
        )
        
        # Сохраняем состояние
        self.user_states[chat_id] = {
            "action": "waiting_target_user_for_replies",
            "user_id": user_id
        }
    
    async def search_replies_to_specific_user(self, chat_id: int, user_id: int, target_user_input: str):
        """Ищет реплаи нашего пользователя конкретному пользователю и отправляет файл с результатами"""
        try:
            await self.send_bot_message(chat_id, 
                f"🔍 Начинаю поиск реплаев нашего пользователя пользователю '{target_user_input}'...\n"
                f"👤 Наш пользователь ID: {user_id}\n\n"
                f"⏳ Собираю все реплаи... Это может занять несколько минут."
            )
            
            start_time = time.time()
            
            # Получаем нашего пользователя
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            # Получаем целевого пользователя
            target_user = None
            target_user_input_clean = target_user_input.strip().replace('@', '')
            
            if target_user_input_clean.isdigit() or (target_user_input_clean.startswith('-') and target_user_input_clean[1:].isdigit()):
                # Поиск по ID
                target_user_id = int(target_user_input_clean)
                try:
                    target_user = await self.client.get_entity(target_user_id)
                except:
                    try:
                        target_user = await self.client.get_entity(PeerUser(target_user_id))
                    except:
                        await self.send_bot_message(chat_id, f"❌ Пользователь с ID {target_user_id} не найден")
                        return
            else:
                # Поиск по username
                username = target_user_input_clean
                try:
                    target_user = await self.client.get_entity(username)
                except errors.UsernameNotOccupiedError:
                    await self.send_bot_message(chat_id, f"❌ Пользователь @{username} не существует")
                    return
                except Exception as e:
                    try:
                        target_user = await self.client.get_entity(f"@{username}")
                    except:
                        await self.send_bot_message(chat_id, f"❌ Ошибка поиска целевого пользователя: {str(e)[:100]}")
                        return
            
            if not target_user:
                await self.send_bot_message(chat_id, "❌ Не удалось получить информацию о целевом пользователе")
                return
            
            # Получаем информацию о целевом пользователе
            target_name = getattr(target_user, 'first_name', '')
            if hasattr(target_user, 'last_name') and target_user.last_name:
                target_name += f" {target_user.last_name}"
            if hasattr(target_user, 'username') and target_user.username:
                target_name += f" (@{target_user.username})"
            
            # Загружаем список чатов (все из файла)
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "❌ Нет чатов для поиска!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "@durov\n@telegram\n"
                    "https://t.me/+tmE98W5NO6xlYmQy"
                )
                return
            
            found_replies = []
            checked_chats = 0
            
            # Ищем в каждом чате
            for i, chat_identifier in enumerate(chats, 1):
                try:
                    # Обновляем прогресс каждые 10 чатов
                    if i % 10 == 0:
                        progress = (i / len(chats)) * 100
                        await self.send_bot_message(chat_id, 
                            f"🔍 Ищу реплаи... {progress:.1f}%\n"
                            f"📁 Обработано: {i}/{len(chats)} чатов\n"
                            f"💬 Найдено реплаев: {len(found_replies)}")
                    
                    # Получаем чат
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    checked_chats += 1
                    
                    # Получаем сообщения нашего пользователя (БЕЗ ЛИМИТА)
                    async for message in self.client.iter_messages(
                        chat,
                        from_user=user
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
                                        
                                        # Если это наш целевой пользователь
                                        if original_sender.id == target_user.id:
                                            # Получаем информацию об авторе
                                            sender_name = getattr(original_sender, 'first_name', '')
                                            if hasattr(original_sender, 'last_name') and original_sender.last_name:
                                                sender_name += f" {original_sender.last_name}"
                                            if hasattr(original_sender, 'username') and original_sender.username:
                                                sender_name += f" (@{original_sender.username})"
                                            
                                            # Формируем ссылки
                                            reply_link = await self.get_message_link(chat, message.id)
                                            original_link = await self.get_message_link(chat, original_msg.id)
                                            chat_name = getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))
                                            
                                            found_replies.append({
                                                "chat": chat_name,
                                                "original_text": original_msg.text[:150] if original_msg.text else "без текста",
                                                "reply_text": message.text[:150] if message.text else "без текста",
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
                    
                except Exception as e:
                    continue
            
            # Сортируем по дате (новые сверху)
            found_replies.sort(key=lambda x: x['reply_time'], reverse=True)
            
            # Если нашли реплаи, создаем файл
            if found_replies:
                # Создаем содержимое файла
                file_content = "=" * 60 + "\n"
                file_content += f"РЕЗУЛЬТАТЫ ПОИСКА РЕПЛАЕВ\n"
                file_content += f"Наш пользователь: {user_id}\n"
                file_content += f"Целевой пользователь: {target_name}\n"
                file_content += f"ID целевого: {target_user_id}\n"
                file_content += f"Дата поиска: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                file_content += f"Всего найдено: {len(found_replies)} реплаев\n"
                file_content += f"Чатов проверено: {checked_chats}/{len(chats)}\n"
                file_content += f"Время выполнения: {time.time() - start_time:.1f} сек\n"
                file_content += "=" * 60 + "\n\n"
                
                # Добавляем все реплаи
                for i, reply in enumerate(found_replies, 1):
                    file_content += f"{i}. ЧАТ: {reply['chat']}\n"
                    file_content += f"   КОМУ: {reply['replied_to']}\n"
                    file_content += f"   ДАТА: {reply['reply_time']}\n"
                    file_content += f"   ОРИГИНАЛ: {reply['original_text']}\n"
                    file_content += f"   РЕПЛАЙ: {reply['reply_text']}\n"
                    file_content += f"   ССЫЛКА НА РЕПЛАЙ: {reply['reply_link']}\n"
                    file_content += f"   ССЫЛКА НА ОРИГИНАЛ: {reply['original_link']}\n"
                    file_content += "-" * 40 + "\n"
                
                # Сохраняем файл
                filename = f"replies_to_{target_user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                
                # Отправляем файл
                caption = (
                    f"📄 Результаты поиска реплаев\n\n"
                    f"👤 Наш пользователь: ID {user_id}\n"
                    f"👥 Кому реплаил: {target_name}\n"
                    f"🆔 ID целевого: {target_user_id}\n"
                    f"📊 Найдено реплаев: {len(found_replies):,}\n"
                    f"📁 Чатов проверено: {checked_chats}/{len(chats)}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек"
                )
                
                await self.send_document(chat_id, filename, file_content.encode('utf-8'), caption)
                
                # Отправляем итоговое сообщение
                total_text = (
                    f"✅ ПОИСК ЗАВЕРШЁН!\n\n"
                    f"👤 Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"👥 Целевой пользователь: {target_name}\n"
                    f"🆔 ID целевого: {target_user_id}\n"
                    f"📊 Найдено реплаев: {len(found_replies):,}\n"
                    f"📁 Всего чатов в списке: {len(chats)}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек\n\n"
                    f"📄 Файл со всеми реплаями отправлен выше."
                )
            else:
                total_text = (
                    f"❌ Реплаев не найдено\n\n"
                    f"👤 Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"👥 Целевой пользователь: {target_name}\n"
                    f"🆔 ID целевого: {target_user_id}\n"
                    f"📁 Всего чатов в списке: {len(chats)}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек\n\n"
                    f"Пользователь не реплаил этому пользователю."
                )
            
            keyboard = self.create_keyboard([
                [
                    {"text": "🔍 Новый поиск", "callback_data": f"search_replies_to_specific:{user_id}"},
                    {"text": "📊 Все реплаи", "callback_data": f"all_user_replies:{user_id}"}
                ],
                [
                    {"text": "🔙 В меню", "callback_data": f"back_to_menu:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, total_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка поиска реплаев пользователя: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка поиска: {str(e)[:100]}")
    
    async def show_all_user_replies(self, chat_id: int, user_id: int):
        """Показывает все реплаи пользователя и отправляет файл"""
        try:
            await self.send_bot_message(chat_id, 
                f"🔍 Собираю данные о всех реплаях пользователя...\n"
                f"👤 Пользователь: ID {user_id}\n\n"
                f"⏳ Это может занять несколько минут. По завершению отправлю файл со всеми реплаями."
            )
            
            start_time = time.time()
            
            # Получаем нашего пользователя
            try:
                user = await self.client.get_entity(PeerUser(user_id))
                user_name = user.first_name if hasattr(user, 'first_name') else f"User {user_id}"
            except:
                user_name = f"User {user_id}"
                user = None
            
            # Загружаем список чатов (все из файла)
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "❌ Нет чатов для поиска!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "@durov\n@telegram\n"
                    "https://t.me/+tmE98W5NO6xlYmQy"
                )
                return
            
            # Словарь для хранения статистики по пользователям
            user_stats = {}
            total_replies = 0
            checked_chats = 0
            
            # Кэш для ускорения получения информации о пользователях
            user_info_cache = {}
            
            # Ищем в каждом чате
            for i, chat_identifier in enumerate(chats, 1):
                try:
                    # Обновляем прогресс каждые 10 чатов
                    if i % 10 == 0:
                        progress = (i / len(chats)) * 100
                        await self.send_bot_message(chat_id, 
                            f"🔍 Собираю реплаи... {progress:.1f}%\n"
                            f"📁 Обработано: {i}/{len(chats)} чатов\n"
                            f"💬 Найдено реплаев: {total_replies}\n"
                            f"👥 Уникальных пользователей: {len(user_stats)}")
                    
                    # Получаем чат
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    checked_chats += 1
                    
                    # Получаем ВСЕ сообщения нашего пользователя (БЕЗ ЛИМИТА)
                    message_batch = []
                    async for message in self.client.iter_messages(
                        chat,
                        from_user=user
                    ):
                        if message and message.reply_to:
                            message_batch.append(message)
                    
                    # Обрабатываем найденные сообщения с реплаями
                    for message in message_batch:
                        if message and message.reply_to:
                            try:
                                # Получаем оригинальное сообщение
                                try:
                                    original_msg = await self.client.get_messages(
                                        chat,
                                        ids=message.reply_to.reply_to_msg_id
                                    )
                                    
                                    if original_msg and hasattr(original_msg, 'from_id') and original_msg.from_id:
                                        # Получаем ID отправителя оригинального сообщения
                                        original_sender_id = None
                                        if hasattr(original_msg.from_id, 'user_id'):
                                            original_sender_id = original_msg.from_id.user_id
                                        elif hasattr(original_msg, 'sender_id') and hasattr(original_msg.sender_id, 'user_id'):
                                            original_sender_id = original_msg.sender_id.user_id
                                        
                                        if not original_sender_id:
                                            continue
                                        
                                        # Получаем информацию об авторе (используем кэш)
                                        if original_sender_id not in user_info_cache:
                                            try:
                                                original_sender = await self.client.get_entity(PeerUser(original_sender_id))
                                                sender_name = getattr(original_sender, 'first_name', '')
                                                if hasattr(original_sender, 'last_name') and original_sender.last_name:
                                                    sender_name += f" {original_sender.last_name}"
                                                
                                                username = getattr(original_sender, 'username', '')
                                                
                                                if not sender_name.strip():
                                                    sender_name = f"User {original_sender_id}"
                                                
                                                user_info_cache[original_sender_id] = {
                                                    'name': sender_name,
                                                    'username': username
                                                }
                                            except:
                                                user_info_cache[original_sender_id] = {
                                                    'name': f"User {original_sender_id}",
                                                    'username': ''
                                                }
                                        
                                        sender_info = user_info_cache[original_sender_id]
                                        sender_name = sender_info['name']
                                        username = sender_info['username']
                                        
                                        # Формируем ссылки
                                        reply_link = await self.get_message_link(chat, message.id)
                                        original_link = await self.get_message_link(chat, original_msg.id)
                                        chat_name = getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))
                                        
                                        # Добавляем в статистику
                                        if original_sender_id not in user_stats:
                                            user_stats[original_sender_id] = {
                                                "name": sender_name,
                                                "username": username,
                                                "count": 0,
                                                "replies": [],
                                                "last_reply": message.date
                                            }
                                        
                                        user_stats[original_sender_id]["count"] += 1
                                        user_stats[original_sender_id]["replies"].append({
                                            "chat": chat_name,
                                            "original_text": original_msg.text[:100] if original_msg.text else "без текста",
                                            "reply_text": message.text[:100] if message.text else "без текста",
                                            "reply_time": message.date.strftime("%d.%m.%Y %H:%M"),
                                            "reply_link": reply_link,
                                            "original_link": original_link,
                                            "chat_id": chat.id,
                                            "message_id": original_msg.id,
                                            "reply_id": message.id
                                        })
                                        
                                        total_replies += 1
                                        
                                        # Если накопилось много реплаев, делаем паузу
                                        if total_replies % 100 == 0:
                                            await asyncio.sleep(0.05)
                                            
                                except Exception as e:
                                    continue
                                    
                            except Exception as e:
                                continue
                    
                    # Небольшая пауза между чатами для избежания блокировок
                    if i % 5 == 0:
                        await asyncio.sleep(0.05)
                    
                except Exception as e:
                    continue
            
            # Сортируем пользователей по количеству реплаев
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
            
            # Создаем файл с результатами
            if sorted_users:
                # Создаем содержимое файла
                file_content = "=" * 60 + "\n"
                file_content += f"АНАЛИЗ ВСЕХ РЕПЛАЕВ ПОЛЬЗОВАТЕЛЯ\n"
                file_content += f"Пользователь: {user_name}\n"
                file_content += f"ID пользователя: {user_id}\n"
                file_content += f"Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                file_content += f"Всего реплаев: {total_replies}\n"
                file_content += f"Всего пользователям: {len(sorted_users)}\n"
                file_content += f"Чатов проверено: {checked_chats}/{len(chats)}\n"
                file_content += f"Время выполнения: {time.time() - start_time:.1f} сек\n"
                file_content += "=" * 60 + "\n\n"
                
                # Добавляем статистику по каждому пользователю
                for i, (target_id, stats) in enumerate(sorted_users, 1):
                    percentage = (stats["count"] / total_replies * 100) if total_replies > 0 else 0
                    
                    file_content += f"{i}. {stats['name']}\n"
                    file_content += f"   ID: {target_id}\n"
                    file_content += f"   Username: {stats.get('username', '')}\n"
                    file_content += f"   Реплаев: {stats['count']} ({percentage:.1f}%)\n"
                    file_content += f"   Последний реплай: {stats['last_reply'].strftime('%d.%m.%Y %H:%M')}\n"
                    
                    # Добавляем все реплаи этому пользователю
                    file_content += f"   Все реплаи:\n"
                    for reply_idx, reply in enumerate(stats["replies"][:10], 1):  # Ограничиваем до 10 реплаев на пользователя
                        file_content += f"   {reply_idx}. Чат: {reply['chat']}\n"
                        file_content += f"      Дата: {reply['reply_time']}\n"
                        file_content += f"      Реплай: {reply['reply_text']}\n"
                        file_content += f"      Ссылка: {reply['reply_link']}\n"
                    
                    if len(stats["replies"]) > 10:
                        file_content += f"   ... и еще {len(stats['replies']) - 10} реплаев\n"
                    
                    file_content += "-" * 40 + "\n"
                
                # Сохраняем файл
                filename = f"all_replies_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                
                # Отправляем файл
                caption = (
                    f"📄 Анализ всех реплаев пользователя\n\n"
                    f"👤 Пользователь: {user_name}\n"
                    f"🆔 ID: {user_id}\n"
                    f"📊 Всего реплаев: {total_replies:,}\n"
                    f"👥 Всего пользователям: {len(sorted_users):,}\n"
                    f"📁 Чатов проверено: {checked_chats}/{len(chats)}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек"
                )
                
                await self.send_document(chat_id, filename, file_content.encode('utf-8'), caption)
                
                # Отправляем итоговое сообщение
                total_text = (
                    f"✅ АНАЛИЗ ЗАВЕРШЁН!\n\n"
                    f"👤 Пользователь: {user_name}\n"
                    f"🆔 ID: {user_id}\n"
                    f"📊 Всего реплаев: {total_replies:,}\n"
                    f"👥 Всего пользователям: {len(sorted_users):,}\n"
                    f"📁 Всего чатов в списке: {len(chats)}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Общее время: {time.time() - start_time:.1f} сек\n\n"
                    f"📄 Файл со всеми реплаями отправлен выше."
                )
            else:
                total_text = (
                    f"❌ Реплаев не найдено\n\n"
                    f"👤 Пользователь: {user_name}\n"
                    f"📁 Всего чатов в списке: {len(chats)}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек\n\n"
                    f"Пользователь не реплаил никому в указанных чатах."
                )
            
            keyboard = self.create_keyboard([
                [
                    {"text": "🔍 Поиск по юзеру", "callback_data": f"search_replies_to_specific:{user_id}"},
                    {"text": "🔙 В меню", "callback_data": f"back_to_menu:{user_id}"}
                ]
            ])
            
            await self.send_bot_message(chat_id, total_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка анализа реплаев: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:200]}")
    
    async def export_reply_users_to_file(self, chat_id: int, user_id: int):
        """Выгружает список пользователей с реплаями в файл"""
        try:
            await self.send_bot_message(chat_id, "📁 Формирую файл со списком пользователей...")
            
            # Проверяем есть ли статистика в кэше
            if user_id not in self.user_stats_cache:
                await self.send_bot_message(chat_id, 
                    "❌ Статистика не найдена в кэше!\n\n"
                    "Сначала выполните анализ реплаев пользователя."
                )
                return
            
            cache_data = self.user_stats_cache[user_id]
            user_stats = cache_data["user_stats"]
            total_replies = cache_data["total_replies"]
            
            if not user_stats:
                await self.send_bot_message(chat_id, "❌ Нет данных для выгрузки")
                return
            
            # Сортируем пользователей по количеству реплаев
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
            
            # Формируем содержимое файла
            file_content = "=" * 60 + "\n"
            file_content += f"АНАЛИЗ РЕПЛАЕВ ПОЛЬЗОВАТЕЛЯ\n"
            file_content += f"ID пользователя: {user_id}\n"
            file_content += f"Дата выгрузки: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
            file_content += f"Всего реплаев: {total_replies:,}\n"
            file_content += f"Всего пользователей: {len(sorted_users):,}\n"
            file_content += "=" * 60 + "\n\n"
            
            # Добавляем статистику по каждому пользователю
            for i, (target_id, stats) in enumerate(sorted_users, 1):
                percentage = (stats["count"] / total_replies * 100) if total_replies > 0 else 0
                
                file_content += f"{i}. {stats['name']}\n"
                file_content += f"   ID: {target_id}\n"
                file_content += f"   Username: {stats.get('username', '')}\n"
                file_content += f"   Реплаев: {stats['count']:,} ({percentage:.1f}%)\n"
                file_content += f"   Последний реплай: {stats['last_reply'].strftime('%d.%m.%Y %H:%M')}\n"
                
                # Добавляем информацию о чатах если есть
                if stats["replies"]:
                    unique_chats = set()
                    for reply in stats["replies"]:
                        if "chat" in reply:
                            unique_chats.add(reply["chat"])
                    
                    file_content += f"   Чаты: {', '.join(list(unique_chats)[:3])}"
                    if len(unique_chats) > 3:
                        file_content += f" и еще {len(unique_chats) - 3} чатов"
                    file_content += "\n"
                
                file_content += "-" * 40 + "\n"
            
            # Добавляем общую статистику
            file_content += "\n" + "=" * 60 + "\n"
            file_content += "ОБЩАЯ СТАТИСТИКА:\n"
            file_content += "=" * 60 + "\n"
            
            # Распределение по количеству реплаев
            file_content += "\nРаспределение пользователей по количеству реплаев:\n"
            
            groups = {
                "1 реплай": 0,
                "2-5 реплаев": 0,
                "6-10 реплаев": 0,
                "11-20 реплаев": 0,
                "21-50 реплаев": 0,
                "51-100 реплаев": 0,
                "Более 100 реплаев": 0
            }
            
            for _, stats in sorted_users:
                count = stats["count"]
                if count == 1:
                    groups["1 реплай"] += 1
                elif 2 <= count <= 5:
                    groups["2-5 реплаев"] += 1
                elif 6 <= count <= 10:
                    groups["6-10 реплаев"] += 1
                elif 11 <= count <= 20:
                    groups["11-20 реплаев"] += 1
                elif 21 <= count <= 50:
                    groups["21-50 реплаев"] += 1
                elif 51 <= count <= 100:
                    groups["51-100 реплаев"] += 1
                else:
                    groups["Более 100 реплаев"] += 1
            
            for group_name, count in groups.items():
                if count > 0:
                    percentage = (count / len(sorted_users) * 100) if len(sorted_users) > 0 else 0
                    file_content += f"{group_name}: {count} пользователей ({percentage:.1f}%)\n"
            
            # Топ-10 пользователей
            file_content += "\nТоп-10 пользователей по реплаям:\n"
            for i, (target_id, stats) in enumerate(sorted_users[:10], 1):
                percentage = (stats["count"] / total_replies * 100) if total_replies > 0 else 0
                file_content += f"{i}. {stats['name']} - {stats['count']:,} реплаев ({percentage:.1f}%)\n"
            
            # Сохраняем файл
            filename = f"replies_user_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
            # Отправляем файл
            caption = (
                f"📁 Файл со списком пользователей\n\n"
                f"👤 Пользователь: ID {user_id}\n"
                f"📊 Всего реплаев: {total_replies:,}\n"
                f"👥 Всего пользователей: {len(sorted_users):,}\n"
                f"📅 Дата выгрузки: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            
            await self.send_document(chat_id, filename, file_content.encode('utf-8'), caption)
            
            # Также отправляем краткое сообщение с подтверждением
            await self.send_bot_message(chat_id,
                f"✅ Файл успешно выгружен!\n\n"
                f"📄 Имя файла: {filename}\n"
                f"👤 Пользователь: ID {user_id}\n"
                f"📊 Пользователей в файле: {len(sorted_users):,}\n"
                f"💬 Всего реплаев: {total_replies:,}"
            )
            
        except Exception as e:
            print(f"Ошибка выгрузки пользователей в файл: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка выгрузки: {str(e)[:200]}")
    
    async def show_more_reply_users(self, chat_id: int, user_id: int, start_idx: int = 6):
        """Показывает дополнительных пользователей из топа реплаев"""
        try:
            # Загружаем сохраненные данные из кэша
            if user_id not in self.user_stats_cache:
                await self.send_bot_message(chat_id,
                    f"📄 Дополнительные пользователи\n\n"
                    f"👤 Пользователь: ID {user_id}\n"
                    f"🔢 Показать пользователей с {start_idx}\n\n"
                    f"Сначала выполните анализ реплаев пользователя."
                )
                return
            
            cache_data = self.user_stats_cache[user_id]
            user_stats = cache_data["user_stats"]
            
            if not user_stats:
                await self.send_bot_message(chat_id, "❌ Нет данных о пользователях")
                return
            
            # Сортируем пользователей по количеству реплаев
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
            
            if start_idx >= len(sorted_users):
                await self.send_bot_message(chat_id, "❌ Нет дополнительных пользователей для показа")
                return
            
            # Определяем конец диапазона
            end_idx = min(start_idx + 10, len(sorted_users))
            
            # Формируем сообщение
            message_text = (
                f"📄 Пользователи {start_idx+1}-{end_idx} из {len(sorted_users)}\n\n"
                f"👤 Целевой пользователь: ID {user_id}\n"
                f"👥 Всего пользователей: {len(sorted_users):,}\n\n"
            )
            
            # Добавляем пользователей
            for i in range(start_idx, end_idx):
                if i < len(sorted_users):
                    target_id, stats = sorted_users[i]
                    percentage = (stats["count"] / cache_data["total_replies"] * 100) if cache_data["total_replies"] > 0 else 0
                    name_display = stats['name'][:30] + "..." if len(stats['name']) > 30 else stats['name']
                    
                    # Определяем отображаемый ID
                    display_id = f"@{stats.get('username', '')}" if stats.get('username') else f"ID: {target_id}"
                    
                    message_text += (
                        f"{i+1}. {name_display}\n"
                        f"   {display_id}\n"
                        f"   📊 Реплаев: {stats['count']} ({percentage:.1f}%)\n\n"
                    )
            
            # Создаем кнопки для навигации
            keyboard_buttons = []
            
            # Кнопки навигации
            nav_buttons = []
            if start_idx > 10:
                nav_buttons.append({"text": "⬅️ Предыдущие", "callback_data": f"show_more_reply_users:{user_id}:{max(0, start_idx-10)}"})
            
            nav_buttons.append({"text": f"📄 {start_idx//10+1}/{(len(sorted_users)+9)//10}", "callback_data": f"noop"})
            
            if end_idx < len(sorted_users):
                nav_buttons.append({"text": "Следующие ➡️", "callback_data": f"show_more_reply_users:{user_id}:{end_idx}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            # Кнопки для экспорта
            keyboard_buttons.append([
                {"text": "📁 Выгрузить файлом", "callback_data": f"export_reply_users:{user_id}"},
                {"text": "🔙 К результатам", "callback_data": f"all_user_replies:{user_id}"}
            ])
            
            keyboard = self.create_keyboard(keyboard_buttons)
            
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа дополнительных пользователей: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_reply_details_for_user(self, chat_id: int, user_id: int, target_user_id: int):
        """Показывает детали реплаев нашего пользователя конкретному пользователю"""
        try:
            await self.send_bot_message(chat_id, f"🔍 Собираю детали реплаев пользователю {target_user_id}...")
            
            # Получаем нашего пользователя
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            # Получаем целевого пользователя
            try:
                target_user = await self.client.get_entity(PeerUser(target_user_id))
            except:
                target_user = await self.client.get_entity(target_user_id)
            
            # Получаем информацию о целевом пользователе
            target_name = getattr(target_user, 'first_name', '')
            if hasattr(target_user, 'last_name') and target_user.last_name:
                target_name += f" {target_user.last_name}"
            if hasattr(target_user, 'username') and target_user.username:
                target_name += f" (@{target_user.username})"
            
            # Загружаем список чатов (все из файла)
            chats = await self.load_chats_list()
            
            if not chats:
                await self.send_bot_message(chat_id,
                    "❌ Нет чатов для поиска!\n"
                    "Добавьте чаты в файл chats.txt\n\n"
                    "Пример содержимого файла:\n"
                    "@durov\n@telegram\n"
                    "https://t.me/+tmE98W5NO6xlYmQy"
                )
                return
            
            found_replies = []
            checked_chats = 0
            
            # Ищем в каждом чате
            for chat_identifier in chats:
                try:
                    # Получаем чат
                    chat = await self.get_chat_by_identifier(chat_identifier)
                    if not chat:
                        continue
                    
                    checked_chats += 1
                    
                    # Получаем сообщения нашего пользователя (БЕЗ ЛИМИТА)
                    async for message in self.client.iter_messages(
                        chat,
                        from_user=user
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
                                        
                                        # Если это наш целевой пользователь
                                        if original_sender.id == target_user.id:
                                            # Формируем ссылки
                                            reply_link = await self.get_message_link(chat, message.id)
                                            original_link = await self.get_message_link(chat, original_msg.id)
                                            chat_name = getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))
                                            
                                            found_replies.append({
                                                "chat": chat_name,
                                                "original_text": original_msg.text[:100] if original_msg.text else "без текста",
                                                "reply_text": message.text[:100] if message.text else "без текста",
                                                "replied_to": target_name,
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
                    continue
            
            # Сортируем по дате (новые сверху)
            found_replies.sort(key=lambda x: x['reply_time'], reverse=True)
            
            # Формируем отчет
            if found_replies:
                total_text = (
                    f"💬 РЕПЛАИ НАШЕГО ПОЛЬЗОВАТЕЛЯ\n\n"
                    f"👤 Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"👥 Кому реплаил: {target_name}\n"
                    f"🆔 ID целевого: {target_user_id}\n"
                    f"📊 Всего реплаев: {len(found_replies):,}\n"
                    f"📁 Чатов проверено: {checked_chats}\n\n"
                )
                
                # Показываем первые 5 реплаев
                for i, reply in enumerate(found_replies[:5], 1):
                    total_text += f"Реплай {i}:\n"
                    total_text += f"💬 Чат: {reply['chat'][:30]}\n"
                    total_text += f"📅 Время: {reply['reply_time']}\n"
                    total_text += f"📝 Оригинал: {reply['original_text']}\n"
                    total_text += f"📝 Реплай: {reply['reply_text']}\n"
                    total_text += f"🔗 Реплай: {reply['reply_link']}\n\n"
                
                # Если есть еще реплаи
                if len(found_replies) > 5:
                    total_text += f"... и еще {len(found_replies) - 5} реплаев\n\n"
                
                keyboard_buttons = [
                    [
                        {"text": "🔍 Новый поиск", "callback_data": f"search_replies_to_specific:{user_id}"},
                        {"text": "📊 Все реплаи", "callback_data": f"all_user_replies:{user_id}"}
                    ],
                    [
                        {"text": "🔙 В меню", "callback_data": f"back_to_menu:{user_id}"}
                    ]
                ]
                
            else:
                total_text = (
                    f"❌ Реплаев не найдено\n\n"
                    f"👤 Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"👥 Кому искали: {target_name}\n"
                    f"🆔 ID целевого: {target_user_id}\n"
                    f"📁 Чатов проверено: {checked_chats}\n\n"
                    f"Пользователь не реплаил этому пользователю"
                )
                
                keyboard_buttons = [
                    [
                        {"text": "🔍 Новый поиск", "callback_data": f"search_replies_to_specific:{user_id}"},
                        {"text": "📊 Все реплаи", "callback_data": f"all_user_replies:{user_id}"}
                    ],
                    [
                        {"text": "🔙 В меню", "callback_data": f"back_to_menu:{user_id}"}
                    ]
                ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, total_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа деталей реплаев: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_friends_menu(self, chat_id: int, user_id: int):
        """Показывает меню друзей"""
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            menu_text = (
                f"👥 АНАЛИЗ СОЦИАЛЬНЫХ СВЯЗЕЙ\n\n"
                f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"🆔 ID: {user_id}\n\n"
                f"Выберите тип анализа:"
            )
            
            keyboard_buttons = [
                [
                    {"text": "📊 Все реплаи пользователя", "callback_data": f"all_user_replies:{user_id}"},
                ],
                [
                    {"text": "🔍 Поиск реплаев по юзеру", "callback_data": f"search_replies_to_specific:{user_id}"},
                ],
                [
                    {"text": "🔍 Найти сообщения", "callback_data": f"search_messages:{user_id}"},
                    {"text": "📊 Профиль", "callback_data": f"user_info:{user_id}"}
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
                f"💬 ДЕТАЛИ СООБЩЕНИЯ\n\n"
                f"👤 Отправитель: {sender_name}\n"
                f"💬 Чат: {getattr(chat, 'title', getattr(chat, 'username', f'Чат {chat.id}'))}\n"
                f"📅 Дата: {message.date.strftime('%d.%m.%Y %H:%M:%S')}\n"
                f"🔗 Ссылка: {link}\n\n"
                f"Текст сообщения:\n"
                f"{message.text[:1000] if message.text else 'Сообщение без текста'}\n\n"
            )
            
            # Если есть реплай
            if message.reply_to:
                message_text += f"↪️ Ответ на сообщение: ID {message.reply_to.reply_to_msg_id}\n"
            
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
                "📭 Нет отслеживаемых пользователей\n\n"
                "Отправьте @username чтобы добавить пользователя.\n\n"
                "Пример: @durov"
            )
            return
        
        # Считаем активные задачи
        active_tasks = sum(1 for t in self.tracking_tasks if not t.done())
        
        menu_text = (
            f"👁 ОТСЛЕЖИВАЕМЫЕ ПОЛЬЗОВАТЕЛИ:\n\n"
            f"📊 Всего в кэше: {len(self.monitored_users)}\n"
            f"🔄 Активных задач: {active_tasks}\n\n"
            f"Выберите пользователя:"
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
            f"📊 СТАТИСТИКА БОТА:\n\n"
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
            stats_text += f"⚙️ Текущие задачи:\n"
            for task in self.tracking_tasks[:5]:  # Первые 5 задач
                if not task.done():
                    task_name = task.get_name() or "Unknown"
                    stats_text += f"• {task_name}\n"
            
            if active_tasks > 5:
                stats_text += f"... и еще {active_tasks - 5} задач\n"
        
        stats_text += f"\n🤖 Бот работает стабильно!"
        
        await self.send_bot_message(chat_id, stats_text)
    
    # Вспомогательные методы
    
    async def load_chats_list(self) -> List[str]:
        """Загружает список чатов из файла (поддерживает ссылки)"""
        if not os.path.exists(CHATS_FILE):
            return []
        
        chats = []
        try:
            with open(CHATS_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        # Поддержка ссылок типа https://t.me/+tmE98W5NO6xlYmQy
                        if line.startswith('https://t.me/'):
                            # Извлекаем идентификатор из ссылки
                            if line.startswith('https://t.me/+'):
                                # Приватная ссылка
                                chats.append(line)
                            else:
                                # Обычная ссылка на канал/чат
                                # Извлекаем username или ID
                                parts = line.replace('https://t.me/', '').split('/')
                                if parts and parts[0]:
                                    chats.append(parts[0])
                        else:
                            chats.append(line)
        except:
            pass
        
        return chats
    
    async def get_chat_by_identifier(self, identifier: str):
        """Получает чат по идентификатору (поддерживает ссылки)"""
        try:
            identifier = identifier.strip()
            
            # Проверяем если это ссылка
            if identifier.startswith('https://t.me/+'):
                # Приватная ссылка
                try:
                    # Пробуем получить через клиент
                    result = await self.client(functions.messages.CheckChatInviteRequest(
                        hash=identifier.replace('https://t.me/+', '')
                    ))
                    
                    if hasattr(result, 'chat'):
                        return result.chat
                    elif hasattr(result, 'channel'):
                        return result.channel
                    else:
                        return None
                except:
                    return None
            elif identifier.startswith('https://t.me/'):
                # Обычная ссылка
                username = identifier.replace('https://t.me/', '').split('/')[0]
                if username:
                    try:
                        return await self.client.get_entity(username)
                    except:
                        return None
                else:
                    return None
            elif identifier.startswith('@'):
                # Username
                try:
                    return await self.client.get_entity(identifier[1:])
                except:
                    try:
                        return await self.client.get_entity(identifier)
                    except:
                        return None
            elif identifier.startswith('-100') and identifier[4:].isdigit():
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
                # Username без @
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
        print("🤖 TELEGRAM SPY BOT v4.0")
        print("="*60)
        print("✨ Улучшенная версия:")
        print("• 📊 Анализ всех реплаев пользователя")
        print("• 🔍 Поиск реплаев по конкретному пользователю")
        print("• 📈 Статистика кому чаще всего реплаит")
        print("• 📁 Выгрузка всех результатов в файл")
        print("• ⏳ Сбор всех данных перед отправкой")
        print("="*60)
        
        # Подключаемся к Telegram
        if not await self.connect():
            print("❌ Не удалось подключиться к Telegram")
            return
        
        # Тестируем бота
        print("🔍 Тестирую подключение к боту...")
        test_msg = (
            f"🤖 Шпионский бот запущен!\n\n"
            f"✅ Подключение установлено\n"
            f"👤 Аккаунт: {self.current_user.first_name if self.current_user else 'Неизвестно'}\n"
            f"🆔 ID: {self.current_user.id if self.current_user else 'Неизвестно'}\n"
            f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
            f"✨ Улучшенная версия 4.0:\n"
            f"• 📊 Анализ всех реплаев пользователя\n"
            f"• 🔍 Поиск реплаев по конкретному пользователю\n"
            f"• 📈 Статистика кому чаще всего реплаит\n"
            f"• 📁 Выгрузка всех результатов в файл\n"
            f"• ⏳ Сбор всех данных перед отправкой\n\n"
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