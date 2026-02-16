import asyncio
import os
import sys
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
import aiohttp
from telethon import TelegramClient, errors
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import (
    PeerUser, PeerChat, PeerChannel,
    User, Chat, Channel,
    UserProfilePhoto, PhotoEmpty,
    UserStatusRecently, UserStatusLastWeek,
    UserStatusLastMonth, UserStatusOffline, 
    UserStatusOnline, UserStatusEmpty,
    InputPeerEmpty
)
from telethon.tl import types
from enum import Enum

# Конфигурация
BOT_TOKEN = "8061724548:AAGIGDd8HSSUgG59nXYYrUgYoA7uw0kI5LE"
ADMIN_ID = 8507769194
SESSION_FILE = "+79932516822.session"

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
        self.user_stats_cache: Dict[int, Dict] = {}
        self.available_chats_cache = None
        self.last_chats_update = None
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
            print("❌ API данные не загружены!")
            return False
        
        print("🔗 Подключаюсь к Telegram...")
        try:
            self.client = TelegramClient(SESSION_FILE, self.api_id, self.api_hash)
            await self.client.start()
            
            self.current_user = await self.client.get_me()
            username = f" @{self.current_user.username}" if self.current_user.username else ""
            print(f"✅ Подключен как {self.current_user.first_name}{username} (ID: {self.current_user.id})")
            return True
        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")
            return False
    
    async def get_all_accessible_chats(self, force_refresh: bool = False) -> List:
        """Получает все доступные чаты через API"""
        current_time = time.time()
        
        # Используем кэш если он не старше 5 минут
        if not force_refresh and self.available_chats_cache and self.last_chats_update:
            if current_time - self.last_chats_update < 300:  # 5 минут
                return self.available_chats_cache
        
        print("🔄 Загружаю список доступных чатов через API...")
        try:
            dialogs = await self.client.get_dialogs(limit=None)  # Получаем все диалоги
            available_chats = []
            
            for dialog in dialogs:
                entity = dialog.entity
                # Добавляем все типы чатов, кроме личных с ботом и удаленных
                if dialog.is_user and not dialog.entity.bot:
                    # Личные чаты с пользователями
                    available_chats.append({
                        'entity': entity,
                        'id': entity.id,
                        'title': f"👤 {dialog.name}",
                        'type': 'private'
                    })
                elif dialog.is_group or dialog.is_channel:
                    # Группы и каналы
                    available_chats.append({
                        'entity': entity,
                        'id': entity.id,
                        'title': dialog.name or f"Чат {entity.id}",
                        'type': 'group' if dialog.is_group else 'channel'
                    })
            
            self.available_chats_cache = available_chats
            self.last_chats_update = current_time
            
            print(f"✅ Загружено {len(available_chats)} доступных чатов")
            return available_chats
            
        except errors.FloodWaitError as e:
            wait_time = e.seconds
            print(f"⚠️ Flood wait на получение диалогов: нужно подождать {wait_time} сек")
            return self.available_chats_cache or []
        except Exception as e:
            print(f"❌ Ошибка получения чатов: {e}")
            return self.available_chats_cache or []
    
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
                    "• Поиск сообщений пользователя по всем доступным чатам\n"
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
                await self.send_bot_message(chat_id, "🛑 Все задачи мониторинга остановлены")
            
            elif text.startswith("/test"):
                await self.send_bot_message(chat_id, 
                    f"🤖 Тест бота\n\n"
                    f"✅ Бот работает\n"
                    f"👤 Текущий аккаунт: {self.current_user.first_name if self.current_user else 'Не подключен'}\n"
                    f"🕐 Время: {datetime.now().strftime('%H:%M:%S')}"
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
                    
                    elif state.get("action") == "waiting_target_user_for_replies":
                        user_id = state["user_id"]
                        await self.search_replies_to_specific_user(chat_id, user_id, text)
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
                                "Или используйте команды: /start /help")
                else:
                    if (text.startswith('@') or 
                        clean_text.isdigit() or 
                        (text.startswith('-') and text[1:].isdigit())):
                        await self.handle_user_search(chat_id, text)
                    else:
                        await self.send_bot_message(chat_id,
                            "❌ Неверный формат!\n\n"
                            "Отправьте:\n"
                            "• @username (например @durov)\n"
                            "• ID пользователя (например 123456789)\n\n"
                            "Или используйте команды: /start /help")
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
                    "123456789")
            
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
            
            elif action == "close":
                # Закрываем сообщение
                try:
                    await self.delete_bot_message(chat_id, callback_query["message"]["message_id"])
                except:
                    pass
            
            await self.answer_callback_query(callback_query["id"])
            
        except Exception as e:
            print(f"Ошибка обработки callback: {e}")
            if 'callback_query' in locals():
                await self.answer_callback_query(callback_query["id"])
    
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
            
            user = None
            user_input_clean = user_input.strip().replace('@', '')
            
            if user_input_clean.isdigit() or (user_input_clean.startswith('-') and user_input_clean[1:].isdigit()):
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
                    "Или проверьте правильность ввода.")
            else:
                await self.send_bot_message(chat_id, f"❌ Ошибка: {error_msg[:100]}")
    
    async def load_user_chats(self, chat_id: int, user_id: int):
        """Быстро загружает чаты пользователя через API"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "❌ Пользователь не найден")
                return
            
            profile = self.monitored_users[user_id]
            
            if profile.user_chats_loaded and profile.user_chats:
                await self.show_user_actions_with_chats(chat_id, user_id)
                return
            
            await self.send_bot_message(chat_id, "🔍 Получаю список всех доступных чатов через API...")
            
            # Получаем все доступные чаты
            all_chats = await self.get_all_accessible_chats(force_refresh=True)
            
            if not all_chats:
                await self.send_bot_message(chat_id, "❌ Не удалось получить список чатов")
                return
            
            user = await self.client.get_entity(PeerUser(user_id))
            user_chats = []
            
            total_chats = len(all_chats)
            last_update = time.time()
            
            for idx, chat_info in enumerate(all_chats):
                try:
                    chat_entity = chat_info['entity']
                    
                    # Обновляем прогресс каждые 10 чатов или 5 секунд
                    current_time = time.time()
                    if current_time - last_update > 5 or idx % 10 == 0:
                        progress = (idx / total_chats) * 100
                        await self.send_bot_message(chat_id, 
                            f"🔍 Анализирую чаты... {progress:.1f}%\n"
                            f"📊 Обработано: {idx}/{total_chats} чатов\n"
                            f"✅ Найдено чатов: {len(user_chats)}")
                        last_update = current_time
                    
                    # Для групп и каналов
                    if chat_info['type'] in ['group', 'channel']:
                        try:
                            message_count = 0
                            # Проверяем только последние 50 сообщений для скорости
                            async for message in self.client.iter_messages(
                                chat_entity,
                                from_user=user,
                                limit=50
                            ):
                                if message:
                                    message_count += 1
                            
                            if message_count > 0:
                                chat_link = await self.get_chat_link(chat_entity)
                                
                                user_chats.append({
                                    'id': chat_entity.id,
                                    'name': chat_info['title'][:40],
                                    'link': chat_link,
                                    'message_count': message_count,
                                    'last_activity': datetime.now()
                                })
                        except Exception as e:
                            continue
                    
                    # Для личных чатов
                    elif chat_info['type'] == 'private' and chat_entity.id == user_id:
                        try:
                            total_messages = 0
                            async for message in self.client.iter_messages(chat_entity, limit=50):
                                total_messages += 1
                            
                            if total_messages > 0:
                                chat_link = await self.get_chat_link(chat_entity)
                                
                                user_chats.append({
                                    'id': chat_entity.id,
                                    'name': f"👤 {chat_info['title']}",
                                    'link': chat_link,
                                    'message_count': total_messages,
                                    'last_activity': datetime.now()
                                })
                        except Exception as e:
                            continue
                    
                    # Небольшая пауза для избежания флуда
                    if idx % 20 == 0:
                        await asyncio.sleep(0.1)
                    
                except Exception as e:
                    continue
            
            # Сортируем по количеству сообщений
            user_chats.sort(key=lambda x: x['message_count'], reverse=True)
            
            profile.user_chats = user_chats
            profile.user_chats_loaded = True
            profile.common_chats = len(user_chats)
            
            self.save_monitored_users()
            
            await self.send_bot_message(chat_id, 
                f"✅ Сканирование завершено!\n\n"
                f"📊 Всего проанализировано чатов: {total_chats}\n"
                f"💬 Найдено чатов с пользователем: {len(user_chats)}")
            
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
                await self.send_bot_message(chat_id, "❌ Пользователь не найден в кэше")
                return
            
            profile = self.monitored_users[user_id]
            
            avatar_bytes = await self.get_user_avatar_bytes(user_id)
            has_avatar = avatar_bytes is not None
            
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
                    {"text": "🔄 Обновить чаты (API)", "callback_data": f"refresh_chats:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            
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
            
            items_per_page = 8
            total_pages = (len(profile.user_chats) + items_per_page - 1) // items_per_page
            
            if page >= total_pages:
                page = total_pages - 1
            
            start_idx = page * items_per_page
            end_idx = min((page + 1) * items_per_page, len(profile.user_chats))
            
            message_text = (
                f"📁 ЧАТЫ ПОЛЬЗОВАТЕЛЯ\n\n"
                f"👤 Пользователь: {profile.first_name} {profile.last_name}\n"
                f"🆔 ID: {user_id}\n"
                f"📊 Всего чатов: {len(profile.user_chats)}\n"
                f"📄 Страница {page + 1} из {total_pages}\n\n"
            )
            
            sorted_chats = sorted(profile.user_chats, key=lambda x: x['message_count'], reverse=True)
            
            for i, chat in enumerate(sorted_chats[start_idx:end_idx], start_idx + 1):
                message_text += f"{i}. {chat['name']} - {chat['message_count']} сообщ.\n"
            
            keyboard_buttons = []
            
            nav_buttons = []
            if page > 0:
                nav_buttons.append({"text": "⬅️ Назад", "callback_data": f"show_user_chats:{user_id}:{page-1}"})
            
            nav_buttons.append({"text": f"📄 {page+1}/{total_pages}", "callback_data": "noop"})
            
            if page < total_pages - 1:
                nav_buttons.append({"text": "Вперёд ➡️", "callback_data": f"show_user_chats:{user_id}:{page+1}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
            keyboard_buttons.append([
                {"text": "🔙 В меню", "callback_data": f"back_to_menu:{user_id}"},
                {"text": "🔄 Обновить чаты (API)", "callback_data": f"refresh_chats:{user_id}"}
            ])
            
            keyboard = self.create_keyboard(keyboard_buttons)
            await self.send_bot_message(chat_id, message_text, keyboard)
            
        except Exception as e:
            print(f"Ошибка показа чатов: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_user_actions(self, chat_id: int, user_id: int):
        """Показывает меню действий с пользователем"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "❌ Пользователь не найден в кэше")
                return
            
            profile = self.monitored_users[user_id]
            
            avatar_bytes = await self.get_user_avatar_bytes(user_id)
            has_avatar = avatar_bytes is not None
            
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
                    {"text": "📁 Загрузить чаты пользователя (через API)", "callback_data": f"refresh_chats:{user_id}"}
                ]
            ]
            
            keyboard = self.create_keyboard(keyboard_buttons)
            
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
            await self.send_bot_message(chat_id, "📸 Загружаю аватарку...")
            
            avatar_bytes = await self.get_user_avatar_bytes(user_id)
            
            if avatar_bytes:
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
            return datetime.now() - timedelta(days=365)
        except:
            return None
    
    async def show_user_info(self, chat_id: int, user_id: int):
        """Показывает полную информацию о пользователе"""
        try:
            if user_id not in self.monitored_users:
                await self.send_bot_message(chat_id, "❌ Пользователь не найден в кэше")
                return
            
            profile = self.monitored_users[user_id]
            
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
        """Показывает количество сообщений пользователя во всех доступных чатах"""
        try:
            await self.send_bot_message(chat_id, "📊 Начинаю подсчёт сообщений через API...")
            start_time = time.time()
            
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            # Получаем все доступные чаты
            all_chats = await self.get_all_accessible_chats()
            
            if not all_chats:
                await self.send_bot_message(chat_id, "❌ Нет доступных чатов для проверки!")
                return
            
            total_messages = 0
            chat_stats = []
            checked_chats = 0
            total_chats = len(all_chats)
            
            last_update = time.time()
            
            for idx, chat_info in enumerate(all_chats):
                try:
                    current_time = time.time()
                    if current_time - last_update > 5 or idx % 5 == 0:
                        progress = (idx / total_chats) * 100
                        await self.send_bot_message(chat_id, 
                            f"📊 Считаю сообщения... {progress:.1f}%\n"
                            f"📁 Обработано: {idx}/{total_chats} чатов\n"
                            f"💬 Найдено сообщений: {total_messages}\n"
                            f"✅ Чатов с сообщениями: {len(chat_stats)}")
                        last_update = current_time
                    
                    chat_entity = chat_info['entity']
                    checked_chats += 1
                    
                    message_count = 0
                    try:
                        # Ограничиваем до 100 сообщений для скорости
                        async for message in self.client.iter_messages(
                            chat_entity,
                            from_user=user,
                            limit=100
                        ):
                            if message:
                                message_count += 1
                    except:
                        continue
                    
                    if message_count > 0:
                        total_messages += message_count
                        
                        chat_stats.append({
                            "name": chat_info['title'][:30],
                            "count": message_count
                        })
                    
                    await asyncio.sleep(0.05)
                    
                except Exception as e:
                    continue
            
            chat_stats.sort(key=lambda x: x['count'], reverse=True)
            
            report_text = (
                f"✅ ПОДСЧЁТ ЗАВЕРШЁН!\n\n"
                f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                f"📈 Всего сообщений: {total_messages:,}\n"
                f"📁 Всего чатов в доступе: {total_chats}\n"
                f"✅ Проверено чатов: {checked_chats}\n"
                f"💬 Чатов с сообщениями: {len(chat_stats)}\n"
                f"⏱ Время выполнения: {time.time() - start_time:.1f} сек\n\n"
            )
            
            if chat_stats:
                report_text += f"🏆 Топ чатов по активности:\n"
                for i, stat in enumerate(chat_stats[:10], 1):
                    report_text += f"{i}. {stat['name']}: {stat['count']:,} сообщ.\n"
            
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
            if user_id in self.monitored_users:
                stats["total_messages"] = self.monitored_users[user_id].total_messages
            
            dialogs = await self.client.get_dialogs(limit=100)
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
                elif dialog.is_user:
                    if dialog.entity.id == user_id:
                        common_chats.append({
                            "id": dialog.id,
                            "title": dialog.name[:50],
                            "type": "private"
                        })
            
            stats["common_chats"] = len(common_chats)
            stats["common_chats_list"] = common_chats
            
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
            f"🔍 Поиск сообщений пользователя\n\n"
            f"Введите текст для поиска в сообщениях пользователя:\n\n"
            f"Пример: 'привет' или 'как дела'\n\n"
            f"👤 Пользователь: {user_id}\n"
            f"🔎 Я найду все сообщения с этим текстом во всех доступных чатах и отправлю файл.")
        
        self.user_states[chat_id] = {
            "action": "waiting_search_text",
            "user_id": user_id
        }
    
    async def search_user_messages(self, chat_id: int, user_id: int, search_text: str):
        """Ищет сообщения пользователя через API и отправляет файл"""
        try:
            await self.send_bot_message(chat_id, 
                f"🔎 Начинаю поиск сообщений с текстом: '{search_text}'\n"
                f"👤 Пользователь ID: {user_id}\n\n"
                f"⏳ Собираю все сообщения из доступных чатов...")
            
            start_time = time.time()
            
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            # Получаем все доступные чаты
            all_chats = await self.get_all_accessible_chats()
            
            if not all_chats:
                await self.send_bot_message(chat_id, "❌ Нет доступных чатов для поиска!")
                return
            
            found_messages = []
            checked_chats = 0
            total_chats = len(all_chats)
            
            last_update = time.time()
            
            for idx, chat_info in enumerate(all_chats):
                try:
                    current_time = time.time()
                    if current_time - last_update > 5 or idx % 5 == 0:
                        progress = (idx / total_chats) * 100
                        await self.send_bot_message(chat_id, 
                            f"🔎 Ищу сообщения... {progress:.1f}%\n"
                            f"📁 Обработано: {idx}/{total_chats} чатов\n"
                            f"💬 Найдено сообщений: {len(found_messages)}")
                        last_update = current_time
                    
                    chat_entity = chat_info['entity']
                    checked_chats += 1
                    
                    # Ищем сообщения (без лимита, но с паузами)
                    async for message in self.client.iter_messages(
                        chat_entity,
                        from_user=user
                    ):
                        if message and message.text and search_text.lower() in message.text.lower():
                            link = await self.get_message_link(chat_entity, message.id)
                            
                            chat_name = chat_info['title']
                            
                            found_messages.append({
                                "chat": chat_name,
                                "text": message.text[:200],
                                "date": message.date.strftime("%d.%m.%Y %H:%M"),
                                "link": link,
                                "chat_id": chat_entity.id,
                                "message_id": message.id
                            })
                    
                    await asyncio.sleep(0.05)
                    
                except Exception as e:
                    continue
            
            if found_messages:
                file_content = "=" * 60 + "\n"
                file_content += f"РЕЗУЛЬТАТЫ ПОИСКА СООБЩЕНИЙ\n"
                file_content += f"Пользователь: {user_id}\n"
                file_content += f"Текст для поиска: '{search_text}'\n"
                file_content += f"Дата поиска: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                file_content += f"Всего найдено: {len(found_messages)} сообщений\n"
                file_content += f"Чатов проверено: {checked_chats}/{total_chats}\n"
                file_content += f"Время выполнения: {time.time() - start_time:.1f} сек\n"
                file_content += "=" * 60 + "\n\n"
                
                for i, msg in enumerate(found_messages, 1):
                    file_content += f"{i}. ЧАТ: {msg['chat']}\n"
                    file_content += f"   ДАТА: {msg['date']}\n"
                    file_content += f"   ТЕКСТ: {msg['text']}\n"
                    file_content += f"   ССЫЛКА: {msg['link']}\n"
                    file_content += "-" * 40 + "\n"
                
                filename = f"search_results_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                
                caption = (
                    f"📄 Результаты поиска\n\n"
                    f"👤 Пользователь: ID {user_id}\n"
                    f"🔍 Текст: '{search_text}'\n"
                    f"📊 Найдено сообщений: {len(found_messages):,}\n"
                    f"📁 Чатов проверено: {checked_chats}/{total_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек"
                )
                
                await self.send_document(chat_id, filename, file_content.encode('utf-8'), caption)
                
                total_text = (
                    f"✅ ПОИСК ЗАВЕРШЁН!\n\n"
                    f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"🔍 Текст: '{search_text}'\n"
                    f"📊 Найдено сообщений: {len(found_messages):,}\n"
                    f"📁 Всего чатов в доступе: {total_chats}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек\n\n"
                    f"📄 Файл с результатами отправлен выше."
                )
            else:
                total_text = (
                    f"❌ Сообщений не найдено\n\n"
                    f"👤 Пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"🔍 Текст: '{search_text}'\n"
                    f"📁 Всего чатов в доступе: {total_chats}\n"
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
            
            if user_id not in self.tracking_status:
                self.tracking_status[user_id] = {'messages': False, 'avatar': False, 'replies': False}
            
            current_state = self.tracking_status[user_id]['messages']
            
            if current_state:
                await self.stop_message_monitoring(user_id)
                self.tracking_status[user_id]['messages'] = False
                
                if user_id in self.monitored_users:
                    self.monitored_users[user_id].is_tracking_messages = False
                
                await self.send_bot_message(chat_id,
                    f"🛑 Отслеживание сообщений остановлено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}\n\n"
                    f"❌ Вы больше не будете получать уведомления.")
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
                    f"👁 Отслеживание сообщений включено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}\n\n"
                    f"📨 Проверка каждые 2 минуты.")
            
            self.save_monitored_users()
            await self.show_user_actions(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка переключения отслеживания сообщений: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def stop_message_monitoring(self, user_id: int):
        """Останавливает отслеживание сообщений"""
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
            last_check = datetime.now() - timedelta(minutes=5)
            
            while self.tracking_status.get(user_id, {}).get('messages', False):
                try:
                    all_chats = await self.get_all_accessible_chats()
                    
                    new_messages_found = False
                    
                    for chat_info in all_chats[:30]:
                        try:
                            messages = await self.client.get_messages(
                                chat_info['entity'],
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
                                        link = await self.get_message_link(chat_info['entity'], message.id)
                                        
                                        notification = (
                                            f"🔔 НОВОЕ СООБЩЕНИЕ!\n\n"
                                            f"👤 От: {user.first_name}\n"
                                            f"💬 Чат: {chat_info['title'][:50]}\n"
                                            f"📅 Время: {message.date.strftime('%H:%M:%S')}\n"
                                            f"📝 Текст: {message.text[:200]}\n"
                                            f"🔗 Ссылка: {link}"
                                        )
                                        
                                        await self.send_bot_message(chat_id, notification)
                                        new_messages_found = True
                                        
                                        if message.date > last_check:
                                            last_check = message.date
                                            
                        except Exception as e:
                            continue
                    
                    if new_messages_found:
                        print(f"📨 Найдены новые сообщения от {user_id}")
                    
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
                    f"🛑 Отслеживание аватарки остановлено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}")
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
                    f"🖼 Отслеживание аватарки включено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}\n\n"
                    f"📸 Проверка каждые 30 минут.")
                
                if current_avatar:
                    caption = f"📸 Текущая аватарка\n👤 {user.first_name}\n🆔 {user_id}"
                    await self.send_bot_message(chat_id, caption, photo=current_avatar)
            
            self.save_monitored_users()
            await self.show_user_actions(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка переключения отслеживания аватарки: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def stop_avatar_monitoring(self, user_id: int):
        """Останавливает отслеживание аватарки"""
        for task in self.tracking_tasks:
            if not task.done() and task.get_name() == f"avatar_monitor_{user_id}":
                task.cancel()
                print(f"🛑 Остановлено отслеживание аватарки для {user_id}")
                break
        
        if user_id in self.avatar_cache:
            del self.avatar_cache[user_id]
    
    async def monitor_user_avatar(self, chat_id: int, user_id: int):
        """Мониторит аватарку пользователя"""
        print(f"🚀 Запущен мониторинг аватарки для пользователя {user_id}")
        
        try:
            user = await self.client.get_entity(PeerUser(user_id))
            
            while self.tracking_status.get(user_id, {}).get('avatar', False):
                try:
                    current_avatar = await self.get_user_avatar_bytes(user_id)
                    current_hash = hashlib.md5(current_avatar).hexdigest() if current_avatar else "no_avatar"
                    old_hash = self.avatar_cache.get(user_id, "")
                    
                    if current_hash != old_hash:
                        print(f"🔄 Обнаружена смена аватарки у {user_id}")
                        
                        self.avatar_cache[user_id] = current_hash
                        
                        if current_avatar:
                            caption = (
                                f"🔄 СМЕНА АВАТАРКИ!\n\n"
                                f"👤 Пользователь: {user.first_name}\n"
                                f"🆔 ID: {user_id}\n"
                                f"📅 Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
                            )
                            
                            await self.send_bot_message(chat_id, caption, photo=current_avatar)
                        else:
                            await self.send_bot_message(chat_id,
                                f"🗑 АВАТАРКА УДАЛЕНА\n\n"
                                f"👤 Пользователь: {user.first_name}\n"
                                f"🆔 ID: {user_id}\n"
                                f"📅 Время: {datetime.now().strftime('%H:%M:%S')}")
                    
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
                    f"🛑 Отслеживание ответов остановлено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}")
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
                    f"💬 Отслеживание ответов включено\n\n"
                    f"👤 Пользователь: {user.first_name}\n"
                    f"🆔 ID: {user_id}\n\n"
                    f"📨 Проверка каждые 5 минут.")
            
            self.save_monitored_users()
            await self.show_user_actions(chat_id, user_id)
            
        except Exception as e:
            print(f"Ошибка переключения отслеживания ответов: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def stop_reply_monitoring(self, user_id: int):
        """Останавливает отслеживание ответов"""
        for task in self.tracking_tasks:
            if not task.done() and task.get_name() == f"reply_monitor_{user_id}":
                task.cancel()
                print(f"🛑 Остановлено отслеживание ответов для {user_id}")
                break
        
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
                    all_chats = await self.get_all_accessible_chats()
                    
                    if not all_chats:
                        await asyncio.sleep(600)
                        continue
                    
                    if user_id not in self.last_message_ids:
                        self.last_message_ids[user_id] = {}
                    
                    new_replies_found = False
                    
                    for chat_info in all_chats:
                        try:
                            chat_entity = chat_info['entity']
                            
                            messages = await self.client.get_messages(
                                chat_entity,
                                from_user=user,
                                offset_date=last_check
                            )
                            
                            for message in messages:
                                if not message or message.date <= last_check:
                                    continue
                                
                                last_msg_id = self.last_message_ids[user_id].get(chat_entity.id, 0)
                                
                                if message.id > last_msg_id:
                                    self.last_message_ids[user_id][chat_entity.id] = message.id
                                    
                                    await asyncio.sleep(2)
                                    
                                    try:
                                        replies = await self.client.get_messages(
                                            chat_entity,
                                            min_id=message.id
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
                                                    
                                                    link = await self.get_message_link(chat_entity, reply.id)
                                                    original_link = await self.get_message_link(chat_entity, message.id)
                                                    
                                                    notification = (
                                                        f"💬 ОТВЕТ НА СООБЩЕНИЕ!\n\n"
                                                        f"👤 На кого ответили: {user.first_name}\n"
                                                        f"👥 Кто ответил: {sender_name or 'Неизвестный'}\n"
                                                        f"💬 Чат: {chat_info['title'][:50]}\n"
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
                        
                        await asyncio.sleep(0.5)
                    
                    if new_replies_found:
                        print(f"💬 Найдены новые ответы на сообщения {user_id}")
                    
                    last_check = datetime.now()
                    await asyncio.sleep(300)
                    
                except Exception as e:
                    print(f"Ошибка в цикле мониторинга ответов: {e}")
                    await asyncio.sleep(300)
                    
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
        """Запрашивает пользователя для поиска реплаев"""
        await self.send_bot_message(chat_id,
            f"🔍 Поиск: кому реплаит пользователь\n\n"
            f"Введите @username или ID пользователя:\n\n"
            f"👤 Наш пользователь: {user_id}")
        
        self.user_states[chat_id] = {
            "action": "waiting_target_user_for_replies",
            "user_id": user_id
        }
    
    async def search_replies_to_specific_user(self, chat_id: int, user_id: int, target_user_input: str):
        """Ищет реплаи нашего пользователя конкретному пользователю"""
        try:
            await self.send_bot_message(chat_id, 
                f"🔍 Начинаю поиск реплаев пользователю '{target_user_input}'...\n"
                f"👤 Наш пользователь ID: {user_id}")
            
            start_time = time.time()
            
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
                except:
                    try:
                        target_user = await self.client.get_entity(PeerUser(target_user_id))
                    except:
                        await self.send_bot_message(chat_id, f"❌ Пользователь с ID {target_user_id} не найден")
                        return
            else:
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
                        await self.send_bot_message(chat_id, f"❌ Ошибка поиска: {str(e)[:100]}")
                        return
            
            if not target_user:
                await self.send_bot_message(chat_id, "❌ Не удалось получить информацию о целевом пользователе")
                return
            
            target_name = getattr(target_user, 'first_name', '')
            if hasattr(target_user, 'last_name') and target_user.last_name:
                target_name += f" {target_user.last_name}"
            if hasattr(target_user, 'username') and target_user.username:
                target_name += f" (@{target_user.username})"
            
            all_chats = await self.get_all_accessible_chats()
            
            if not all_chats:
                await self.send_bot_message(chat_id, "❌ Нет доступных чатов для поиска!")
                return
            
            found_replies = []
            checked_chats = 0
            total_chats = len(all_chats)
            
            for idx, chat_info in enumerate(all_chats):
                try:
                    if idx % 10 == 0:
                        progress = (idx / total_chats) * 100
                        await self.send_bot_message(chat_id, 
                            f"🔍 Ищу реплаи... {progress:.1f}%\n"
                            f"📁 Обработано: {idx}/{total_chats} чатов\n"
                            f"💬 Найдено реплаев: {len(found_replies)}")
                    
                    chat_entity = chat_info['entity']
                    checked_chats += 1
                    
                    async for message in self.client.iter_messages(
                        chat_entity,
                        from_user=user
                    ):
                        if message and message.reply_to:
                            try:
                                try:
                                    original_msg = await self.client.get_messages(
                                        chat_entity,
                                        ids=message.reply_to.reply_to_msg_id
                                    )
                                    
                                    if original_msg and hasattr(original_msg, 'from_id') and original_msg.from_id:
                                        original_sender = await self.client.get_entity(original_msg.from_id)
                                        
                                        if original_sender.id == target_user.id:
                                            reply_link = await self.get_message_link(chat_entity, message.id)
                                            original_link = await self.get_message_link(chat_entity, original_msg.id)
                                            chat_name = chat_info['title']
                                            
                                            found_replies.append({
                                                "chat": chat_name,
                                                "original_text": original_msg.text[:150] if original_msg.text else "без текста",
                                                "reply_text": message.text[:150] if message.text else "без текста",
                                                "replied_to": target_name,
                                                "reply_time": message.date.strftime("%d.%m.%Y %H:%M"),
                                                "reply_link": reply_link,
                                                "original_link": original_link,
                                                "chat_id": chat_entity.id,
                                                "message_id": original_msg.id,
                                                "reply_id": message.id
                                            })
                                            
                                except:
                                    continue
                                    
                            except:
                                continue
                    
                except Exception as e:
                    continue
            
            found_replies.sort(key=lambda x: x['reply_time'], reverse=True)
            
            if found_replies:
                file_content = "=" * 60 + "\n"
                file_content += f"РЕЗУЛЬТАТЫ ПОИСКА РЕПЛАЕВ\n"
                file_content += f"Наш пользователь: {user_id}\n"
                file_content += f"Целевой пользователь: {target_name}\n"
                file_content += f"Дата поиска: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                file_content += f"Всего найдено: {len(found_replies)} реплаев\n"
                file_content += f"Чатов проверено: {checked_chats}/{total_chats}\n"
                file_content += f"Время выполнения: {time.time() - start_time:.1f} сек\n"
                file_content += "=" * 60 + "\n\n"
                
                for i, reply in enumerate(found_replies, 1):
                    file_content += f"{i}. ЧАТ: {reply['chat']}\n"
                    file_content += f"   КОМУ: {reply['replied_to']}\n"
                    file_content += f"   ДАТА: {reply['reply_time']}\n"
                    file_content += f"   ОРИГИНАЛ: {reply['original_text']}\n"
                    file_content += f"   РЕПЛАЙ: {reply['reply_text']}\n"
                    file_content += f"   ССЫЛКА НА РЕПЛАЙ: {reply['reply_link']}\n"
                    file_content += f"   ССЫЛКА НА ОРИГИНАЛ: {reply['original_link']}\n"
                    file_content += "-" * 40 + "\n"
                
                filename = f"replies_to_{target_user.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                
                caption = (
                    f"📄 Результаты поиска реплаев\n\n"
                    f"👤 Наш пользователь: ID {user_id}\n"
                    f"👥 Кому реплаил: {target_name}\n"
                    f"🆔 ID целевого: {target_user.id}\n"
                    f"📊 Найдено реплаев: {len(found_replies):,}\n"
                    f"📁 Чатов проверено: {checked_chats}/{total_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек"
                )
                
                await self.send_document(chat_id, filename, file_content.encode('utf-8'), caption)
                
                total_text = (
                    f"✅ ПОИСК ЗАВЕРШЁН!\n\n"
                    f"👤 Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"👥 Целевой пользователь: {target_name}\n"
                    f"🆔 ID целевого: {target_user.id}\n"
                    f"📊 Найдено реплаев: {len(found_replies):,}\n"
                    f"📁 Всего чатов в доступе: {total_chats}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек\n\n"
                    f"📄 Файл со всеми реплаями отправлен выше."
                )
            else:
                total_text = (
                    f"❌ Реплаев не найдено\n\n"
                    f"👤 Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"👥 Целевой пользователь: {target_name}\n"
                    f"📁 Всего чатов в доступе: {total_chats}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек")
            
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
            print(f"Ошибка поиска реплаев: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка: {str(e)[:100]}")
    
    async def show_all_user_replies(self, chat_id: int, user_id: int):
        """Показывает все реплаи пользователя и отправляет файл"""
        try:
            await self.send_bot_message(chat_id, 
                f"🔍 Собираю данные о всех реплаях пользователя...\n"
                f"👤 Пользователь: ID {user_id}\n\n"
                f"⏳ Это может занять несколько минут.")
            
            start_time = time.time()
            
            try:
                user = await self.client.get_entity(PeerUser(user_id))
                user_name = user.first_name if hasattr(user, 'first_name') else f"User {user_id}"
            except:
                user_name = f"User {user_id}"
                user = None
            
            all_chats = await self.get_all_accessible_chats()
            
            if not all_chats:
                await self.send_bot_message(chat_id, "❌ Нет доступных чатов для поиска!")
                return
            
            user_stats = {}
            total_replies = 0
            checked_chats = 0
            total_chats = len(all_chats)
            user_info_cache = {}
            
            for idx, chat_info in enumerate(all_chats):
                try:
                    if idx % 10 == 0:
                        progress = (idx / total_chats) * 100
                        await self.send_bot_message(chat_id, 
                            f"🔍 Собираю реплаи... {progress:.1f}%\n"
                            f"📁 Обработано: {idx}/{total_chats} чатов\n"
                            f"💬 Найдено реплаев: {total_replies}\n"
                            f"👥 Уникальных пользователей: {len(user_stats)}")
                    
                    chat_entity = chat_info['entity']
                    checked_chats += 1
                    
                    message_batch = []
                    async for message in self.client.iter_messages(
                        chat_entity,
                        from_user=user
                    ):
                        if message and message.reply_to:
                            message_batch.append(message)
                    
                    for message in message_batch:
                        if message and message.reply_to:
                            try:
                                try:
                                    original_msg = await self.client.get_messages(
                                        chat_entity,
                                        ids=message.reply_to.reply_to_msg_id
                                    )
                                    
                                    if original_msg and hasattr(original_msg, 'from_id') and original_msg.from_id:
                                        original_sender_id = None
                                        if hasattr(original_msg.from_id, 'user_id'):
                                            original_sender_id = original_msg.from_id.user_id
                                        elif hasattr(original_msg, 'sender_id') and hasattr(original_msg.sender_id, 'user_id'):
                                            original_sender_id = original_msg.sender_id.user_id
                                        
                                        if not original_sender_id:
                                            continue
                                        
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
                                        
                                        reply_link = await self.get_message_link(chat_entity, message.id)
                                        original_link = await self.get_message_link(chat_entity, original_msg.id)
                                        chat_name = chat_info['title']
                                        
                                        if original_sender_id not in user_stats:
                                            user_stats[original_sender_id] = {
                                                "name": sender_info['name'],
                                                "username": sender_info['username'],
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
                                            "chat_id": chat_entity.id,
                                            "message_id": original_msg.id,
                                            "reply_id": message.id
                                        })
                                        
                                        total_replies += 1
                                        
                                        if total_replies % 100 == 0:
                                            await asyncio.sleep(0.05)
                                            
                                except Exception as e:
                                    continue
                                    
                            except Exception as e:
                                continue
                    
                    if idx % 5 == 0:
                        await asyncio.sleep(0.05)
                    
                except Exception as e:
                    continue
            
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
            
            if sorted_users:
                file_content = "=" * 60 + "\n"
                file_content += f"АНАЛИЗ ВСЕХ РЕПЛАЕВ ПОЛЬЗОВАТЕЛЯ\n"
                file_content += f"Пользователь: {user_name}\n"
                file_content += f"ID пользователя: {user_id}\n"
                file_content += f"Дата анализа: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                file_content += f"Всего реплаев: {total_replies}\n"
                file_content += f"Всего пользователям: {len(sorted_users)}\n"
                file_content += f"Чатов проверено: {checked_chats}/{total_chats}\n"
                file_content += f"Время выполнения: {time.time() - start_time:.1f} сек\n"
                file_content += "=" * 60 + "\n\n"
                
                for i, (target_id, stats) in enumerate(sorted_users, 1):
                    percentage = (stats["count"] / total_replies * 100) if total_replies > 0 else 0
                    
                    file_content += f"{i}. {stats['name']}\n"
                    file_content += f"   ID: {target_id}\n"
                    file_content += f"   Username: {stats.get('username', '')}\n"
                    file_content += f"   Реплаев: {stats['count']} ({percentage:.1f}%)\n"
                    file_content += f"   Последний реплай: {stats['last_reply'].strftime('%d.%m.%Y %H:%M')}\n"
                    
                    file_content += f"   Все реплаи:\n"
                    for reply_idx, reply in enumerate(stats["replies"][:5], 1):
                        file_content += f"   {reply_idx}. Чат: {reply['chat']}\n"
                        file_content += f"      Дата: {reply['reply_time']}\n"
                        file_content += f"      Реплай: {reply['reply_text']}\n"
                        file_content += f"      Ссылка: {reply['reply_link']}\n"
                    
                    if len(stats["replies"]) > 5:
                        file_content += f"   ... и еще {len(stats['replies']) - 5} реплаев\n"
                    
                    file_content += "-" * 40 + "\n"
                
                filename = f"all_replies_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                
                caption = (
                    f"📄 Анализ всех реплаев пользователя\n\n"
                    f"👤 Пользователь: {user_name}\n"
                    f"🆔 ID: {user_id}\n"
                    f"📊 Всего реплаев: {total_replies:,}\n"
                    f"👥 Всего пользователям: {len(sorted_users):,}\n"
                    f"📁 Чатов проверено: {checked_chats}/{total_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек"
                )
                
                await self.send_document(chat_id, filename, file_content.encode('utf-8'), caption)
                
                total_text = (
                    f"✅ АНАЛИЗ ЗАВЕРШЁН!\n\n"
                    f"👤 Пользователь: {user_name}\n"
                    f"🆔 ID: {user_id}\n"
                    f"📊 Всего реплаев: {total_replies:,}\n"
                    f"👥 Всего пользователям: {len(sorted_users):,}\n"
                    f"📁 Всего чатов в доступе: {total_chats}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Общее время: {time.time() - start_time:.1f} сек\n\n"
                    f"📄 Файл со всеми реплаями отправлен выше."
                )
            else:
                total_text = (
                    f"❌ Реплаев не найдено\n\n"
                    f"👤 Пользователь: {user_name}\n"
                    f"📁 Всего чатов в доступе: {total_chats}\n"
                    f"✅ Проверено чатов: {checked_chats}\n"
                    f"⏱ Время выполнения: {time.time() - start_time:.1f} сек")
            
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
            
            if user_id not in self.user_stats_cache:
                await self.send_bot_message(chat_id, 
                    "❌ Статистика не найдена в кэше!\n\n"
                    "Сначала выполните анализ реплаев пользователя.")
                return
            
            cache_data = self.user_stats_cache[user_id]
            user_stats = cache_data["user_stats"]
            total_replies = cache_data["total_replies"]
            
            if not user_stats:
                await self.send_bot_message(chat_id, "❌ Нет данных для выгрузки")
                return
            
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
            
            file_content = "=" * 60 + "\n"
            file_content += f"АНАЛИЗ РЕПЛАЕВ ПОЛЬЗОВАТЕЛЯ\n"
            file_content += f"ID пользователя: {user_id}\n"
            file_content += f"Дата выгрузки: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
            file_content += f"Всего реплаев: {total_replies:,}\n"
            file_content += f"Всего пользователей: {len(sorted_users):,}\n"
            file_content += "=" * 60 + "\n\n"
            
            for i, (target_id, stats) in enumerate(sorted_users, 1):
                percentage = (stats["count"] / total_replies * 100) if total_replies > 0 else 0
                
                file_content += f"{i}. {stats['name']}\n"
                file_content += f"   ID: {target_id}\n"
                file_content += f"   Username: {stats.get('username', '')}\n"
                file_content += f"   Реплаев: {stats['count']:,} ({percentage:.1f}%)\n"
                file_content += f"   Последний реплай: {stats['last_reply'].strftime('%d.%m.%Y %H:%M')}\n"
                
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
            
            file_content += "\n" + "=" * 60 + "\n"
            file_content += "ОБЩАЯ СТАТИСТИКА:\n"
            file_content += "=" * 60 + "\n"
            
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
            
            file_content += "\nТоп-10 пользователей по реплаям:\n"
            for i, (target_id, stats) in enumerate(sorted_users[:10], 1):
                percentage = (stats["count"] / total_replies * 100) if total_replies > 0 else 0
                file_content += f"{i}. {stats['name']} - {stats['count']:,} реплаев ({percentage:.1f}%)\n"
            
            filename = f"replies_user_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
            caption = (
                f"📁 Файл со списком пользователей\n\n"
                f"👤 Пользователь: ID {user_id}\n"
                f"📊 Всего реплаев: {total_replies:,}\n"
                f"👥 Всего пользователей: {len(sorted_users):,}\n"
                f"📅 Дата выгрузки: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            
            await self.send_document(chat_id, filename, file_content.encode('utf-8'), caption)
            
            await self.send_bot_message(chat_id,
                f"✅ Файл успешно выгружен!\n\n"
                f"📄 Имя файла: {filename}\n"
                f"👤 Пользователь: ID {user_id}\n"
                f"📊 Пользователей в файле: {len(sorted_users):,}\n"
                f"💬 Всего реплаев: {total_replies:,}")
            
        except Exception as e:
            print(f"Ошибка выгрузки пользователей в файл: {e}")
            await self.send_bot_message(chat_id, f"❌ Ошибка выгрузки: {str(e)[:200]}")
    
    async def show_more_reply_users(self, chat_id: int, user_id: int, start_idx: int = 6):
        """Показывает дополнительных пользователей из топа реплаев"""
        try:
            if user_id not in self.user_stats_cache:
                await self.send_bot_message(chat_id,
                    f"📄 Дополнительные пользователи\n\n"
                    f"👤 Пользователь: ID {user_id}\n"
                    f"Сначала выполните анализ реплаев.")
                return
            
            cache_data = self.user_stats_cache[user_id]
            user_stats = cache_data["user_stats"]
            
            if not user_stats:
                await self.send_bot_message(chat_id, "❌ Нет данных о пользователях")
                return
            
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
            
            if start_idx >= len(sorted_users):
                await self.send_bot_message(chat_id, "❌ Нет дополнительных пользователей для показа")
                return
            
            end_idx = min(start_idx + 10, len(sorted_users))
            
            message_text = (
                f"📄 Пользователи {start_idx+1}-{end_idx} из {len(sorted_users)}\n\n"
                f"👤 Целевой пользователь: ID {user_id}\n"
                f"👥 Всего пользователей: {len(sorted_users):,}\n\n"
            )
            
            for i in range(start_idx, end_idx):
                if i < len(sorted_users):
                    target_id, stats = sorted_users[i]
                    percentage = (stats["count"] / cache_data["total_replies"] * 100) if cache_data["total_replies"] > 0 else 0
                    name_display = stats['name'][:30] + "..." if len(stats['name']) > 30 else stats['name']
                    
                    display_id = f"@{stats.get('username', '')}" if stats.get('username') else f"ID: {target_id}"
                    
                    message_text += (
                        f"{i+1}. {name_display}\n"
                        f"   {display_id}\n"
                        f"   📊 Реплаев: {stats['count']} ({percentage:.1f}%)\n\n"
                    )
            
            keyboard_buttons = []
            
            nav_buttons = []
            if start_idx > 10:
                nav_buttons.append({"text": "⬅️ Предыдущие", "callback_data": f"show_more_reply_users:{user_id}:{max(0, start_idx-10)}"})
            
            nav_buttons.append({"text": f"📄 {start_idx//10+1}/{(len(sorted_users)+9)//10}", "callback_data": "noop"})
            
            if end_idx < len(sorted_users):
                nav_buttons.append({"text": "Следующие ➡️", "callback_data": f"show_more_reply_users:{user_id}:{end_idx}"})
            
            if nav_buttons:
                keyboard_buttons.append(nav_buttons)
            
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
        """Показывает детали реплаев нашему пользователю"""
        try:
            await self.send_bot_message(chat_id, f"🔍 Собираю детали реплаев пользователю {target_user_id}...")
            
            try:
                user = await self.client.get_entity(PeerUser(user_id))
            except:
                user = await self.client.get_entity(user_id)
            
            try:
                target_user = await self.client.get_entity(PeerUser(target_user_id))
            except:
                target_user = await self.client.get_entity(target_user_id)
            
            target_name = getattr(target_user, 'first_name', '')
            if hasattr(target_user, 'last_name') and target_user.last_name:
                target_name += f" {target_user.last_name}"
            if hasattr(target_user, 'username') and target_user.username:
                target_name += f" (@{target_user.username})"
            
            all_chats = await self.get_all_accessible_chats()
            
            if not all_chats:
                await self.send_bot_message(chat_id, "❌ Нет доступных чатов для поиска!")
                return
            
            found_replies = []
            checked_chats = 0
            
            for chat_info in all_chats:
                try:
                    chat_entity = chat_info['entity']
                    checked_chats += 1
                    
                    async for message in self.client.iter_messages(
                        chat_entity,
                        from_user=user
                    ):
                        if message and message.reply_to:
                            try:
                                try:
                                    original_msg = await self.client.get_messages(
                                        chat_entity,
                                        ids=message.reply_to.reply_to_msg_id
                                    )
                                    
                                    if original_msg and hasattr(original_msg, 'from_id') and original_msg.from_id:
                                        original_sender = await self.client.get_entity(original_msg.from_id)
                                        
                                        if original_sender.id == target_user.id:
                                            reply_link = await self.get_message_link(chat_entity, message.id)
                                            original_link = await self.get_message_link(chat_entity, original_msg.id)
                                            chat_name = chat_info['title']
                                            
                                            found_replies.append({
                                                "chat": chat_name,
                                                "original_text": original_msg.text[:100] if original_msg.text else "без текста",
                                                "reply_text": message.text[:100] if message.text else "без текста",
                                                "replied_to": target_name,
                                                "reply_time": message.date.strftime("%d.%m.%Y %H:%M"),
                                                "reply_link": reply_link,
                                                "original_link": original_link,
                                                "chat_id": chat_entity.id,
                                                "message_id": original_msg.id,
                                                "reply_id": message.id
                                            })
                                            
                                except:
                                    continue
                                    
                            except:
                                continue
                    
                except Exception as e:
                    continue
            
            found_replies.sort(key=lambda x: x['reply_time'], reverse=True)
            
            if found_replies:
                total_text = (
                    f"💬 РЕПЛАИ НАШЕГО ПОЛЬЗОВАТЕЛЯ\n\n"
                    f"👤 Наш пользователь: {user.first_name if hasattr(user, 'first_name') else 'ID: ' + str(user_id)}\n"
                    f"👥 Кому реплаил: {target_name}\n"
                    f"🆔 ID целевого: {target_user_id}\n"
                    f"📊 Всего реплаев: {len(found_replies):,}\n"
                    f"📁 Чатов проверено: {checked_chats}\n\n"
                )
                
                for i, reply in enumerate(found_replies[:5], 1):
                    total_text += f"Реплай {i}:\n"
                    total_text += f"💬 Чат: {reply['chat'][:30]}\n"
                    total_text += f"📅 Время: {reply['reply_time']}\n"
                    total_text += f"📝 Оригинал: {reply['original_text']}\n"
                    total_text += f"📝 Реплай: {reply['reply_text']}\n"
                    total_text += f"🔗 Реплай: {reply['reply_link']}\n\n"
                
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
                    f"📁 Чатов проверено: {checked_chats}")
                
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
    
    async def show_monitoring_menu(self, chat_id: int):
        """Показывает меню управления отслеживанием"""
        if not self.monitored_users:
            await self.send_bot_message(chat_id,
                "📭 Нет отслеживаемых пользователей\n\n"
                "Отправьте @username чтобы добавить пользователя.\n\n"
                "Пример: @durov")
            return
        
        active_tasks = sum(1 for t in self.tracking_tasks if not t.done())
        
        menu_text = (
            f"👁 ОТСЛЕЖИВАЕМЫЕ ПОЛЬЗОВАТЕЛИ:\n\n"
            f"📊 Всего в кэше: {len(self.monitored_users)}\n"
            f"🔄 Активных задач: {active_tasks}\n\n"
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
        
        if active_tasks > 0:
            stats_text += f"⚙️ Текущие задачи:\n"
            for task in self.tracking_tasks[:5]:
                if not task.done():
                    task_name = task.get_name() or "Unknown"
                    stats_text += f"• {task_name}\n"
            
            if active_tasks > 5:
                stats_text += f"... и еще {active_tasks - 5} задач\n"
        
        stats_text += f"\n🤖 Бот работает стабильно!"
        
        await self.send_bot_message(chat_id, stats_text)
    
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
                            print(f"❌ Ошибка API: {response.status}")
                            retry_count += 1
                
                if retry_count >= max_retries:
                    print(f"⚠️ Много ошибок, увеличиваю задержку...")
                    await asyncio.sleep(30)
                    retry_count = 0
                
            except asyncio.TimeoutError:
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
        print("🤖 TELEGRAM SPY BOT v5.0")
        print("="*60)
        print("✨ Новая версия с динамическим API поиском:")
        print("• 📊 Анализ всех реплаев пользователя")
        print("• 🔍 Поиск по всем доступным чатам через API")
        print("• 📈 Статистика кому чаще всего реплаит")
        print("• 📁 Выгрузка всех результатов в файл")
        print("• ⏳ Сбор всех данных перед отправкой")
        print("• 🔄 Автообновление списка чатов")
        print("="*60)
        
        if not await self.connect():
            print("❌ Не удалось подключиться к Telegram")
            return
        
        print("🔍 Тестирую подключение к боту...")
        test_msg = (
            f"🤖 Шпионский бот запущен!\n\n"
            f"✅ Подключение установлено\n"
            f"👤 Аккаунт: {self.current_user.first_name if self.current_user else 'Неизвестно'}\n"
            f"🆔 ID: {self.current_user.id if self.current_user else 'Неизвестно'}\n"
            f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
            f"✨ Новая версия 5.0:\n"
            f"• 📊 Анализ всех реплаев пользователя\n"
            f"• 🔍 Поиск по всем доступным чатам через API\n"
            f"• 📈 Статистика кому чаще всего реплаит\n"
            f"• 📁 Выгрузка всех результатов в файл\n"
            f"• 🔄 Автообновление списка чатов\n\n"
            f"📝 Отправьте /start для начала работы"
        )
        
        if await self.send_bot_message(ADMIN_ID, test_msg):
            print("✅ Бот подключен и готов к работе!")
        else:
            print("⚠️ Бот не отвечает, но поиск будет работать")
        
        bot_task = asyncio.create_task(self.run_bot_polling())
        
        print("\n" + "="*60)
        print("✅ Бот запущен! Отправьте команду /start в Telegram")
        print("📱 ID вашего бота можно найти по токену")
        print("="*60 + "\n")
        
        try:
            await bot_task
        except KeyboardInterrupt:
            print("\n\n🛑 Получен сигнал остановки...")
        except Exception as e:
            print(f"\n❌ Критическая ошибка: {e}")
        finally:
            print("\n💾 Сохраняю данные...")
            self.save_monitored_users()
            
            print("🛑 Останавливаю задачи мониторинга...")
            for task in self.tracking_tasks:
                if not task.done():
                    task.cancel()
            
            print("🔒 Отключаюсь от Telegram...")
            if self.client:
                await self.client.disconnect()
            
            print("👋 Бот завершил работу")

if __name__ == "__main__":
    try:
        import telethon
        import aiohttp
    except ImportError as e:
        print(f"❌ Не установлены зависимости: {e}")
        print("📦 Установите: pip install telethon aiohttp")
        sys.exit(1)
    
    if sys.version_info < (3, 7):
        print("❌ Требуется Python 3.7 или выше!")
        sys.exit(1)
    
    if not os.path.exists("api_config.txt"):
        print("❌ Файл api_config.txt не найден!")
        print("📝 Создайте его с содержимым:")
        print("API_ID=ваш_api_id")
        print("API_HASH=ваш_api_hash")
        sys.exit(1)
    
    bot = TelegramSpyBot()
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n👋 Программа завершена пользователем")
    except Exception as e:
        print(f"\n❌ Фатальная ошибка: {e}")
        import traceback
        traceback.print_exc()
