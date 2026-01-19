import asyncio
import re
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
import aiofiles
import aiohttp
from telethon import TelegramClient, events, Button
from telethon.tl.types import Message, User, Chat, Channel
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty
from telethon.errors import FloodWaitError
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
API_ID = 123456  # Замените на ваш API ID
API_HASH = 'ваш_api_hash'  # Замените на ваш API HASH
BOT_TOKEN = 'ваш_бот_токен'  # Замените на токен вашего бота
SESSION_NAME = '+380994588662'  # Имя вашей существующей сессии
CHATS_FILE = 'chat.txt'

# Инициализация клиента - используем существующую сессию
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# Глобальные переменные для хранения данных
user_data_cache = {}
message_cache = {}
avatar_tracker = {}
active_tracking = {}
current_search = {}

class UserSearchBot:
    def __init__(self):
        self.target_user = None
        self.chats = []
        self.user_messages = defaultdict(list)
        self.user_chats = []
        self.message_count = 0
        self.user_info = None
        
    async def load_chats(self):
        """Загрузка чатов из файла"""
        try:
            if os.path.exists(CHATS_FILE):
                async with aiofiles.open(CHATS_FILE, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    lines = content.strip().split('\n')
                    self.chats = []
                    for line in lines:
                        line = line.strip()
                        if line:
                            if 't.me/' in line:
                                if 't.me/+' in line:
                                    self.chats.append(line)
                                else:
                                    username = line.split('t.me/')[-1].replace('@', '')
                                    if username:
                                        self.chats.append(f'@{username}')
                            elif line.startswith('@'):
                                self.chats.append(line)
                            else:
                                self.chats.append(f'@{line}')
                logger.info(f"Загружено {len(self.chats)} чатов из файла")
            else:
                logger.warning(f"Файл {CHATS_FILE} не найден")
                self.chats = []
                
        except Exception as e:
            logger.error(f"Ошибка загрузки чатов: {e}")
            self.chats = []

    async def resolve_username(self, username: str):
        """Преобразование юзернейма в объект пользователя"""
        try:
            username = username.replace('@', '').strip()
            
            # Пробуем разные способы поиска пользователя
            if username.startswith('+'):
                # Номер телефона
                return await client.get_input_entity(username)
            elif username.isdigit():
                # ID пользователя
                return await client.get_entity(int(username))
            else:
                # Юзернейм
                return await client.get_entity(username)
                
        except Exception as e:
            logger.error(f"Ошибка разрешения username {username}: {e}")
            return None

    async def search_user_in_chats(self, user_identifier: str):
        """Поиск пользователя во всех чатах"""
        try:
            logger.info(f"Начинаю поиск пользователя: {user_identifier}")
            
            # Сначала получаем информацию о пользователе
            self.target_user = await self.resolve_username(user_identifier)
            if not self.target_user:
                return "❌ Пользователь не найден. Проверьте правильность ввода."
            
            # Сохраняем информацию
            self.user_info = self.target_user
            
            # Очищаем предыдущие данные
            self.user_messages.clear()
            self.user_chats.clear()
            self.message_count = 0
            
            # Загружаем чаты
            await self.load_chats()
            
            if not self.chats:
                return "⚠️ Не загружены чаты для поиска. Проверьте файл chat.txt"
            
            total_chats = len(self.chats)
            found_in_chats = []
            total_messages = 0
            
            logger.info(f"Начинаю поиск в {total_chats} чатах...")
            
            # Поиск в каждом чате
            for i, chat in enumerate(self.chats, 1):
                try:
                    logger.info(f"Поиск в чате {i}/{total_chats}: {chat}")
                    
                    # Получаем чат
                    chat_entity = None
                    try:
                        if chat.startswith('https://t.me/+'):
                            chat_entity = await client.get_entity(chat)
                        else:
                            chat_entity = await client.get_entity(chat)
                    except Exception as e:
                        logger.warning(f"Не удалось получить чат {chat}: {e}")
                        continue
                    
                    if not chat_entity:
                        continue
                    
                    # Ищем сообщения пользователя
                    message_count_in_chat = 0
                    try:
                        # Используем limit=None для поиска всех сообщений
                        async for message in client.iter_messages(
                            chat_entity,
                            from_user=self.target_user,
                            limit=None
                        ):
                            if message:
                                message_count_in_chat += 1
                                total_messages += 1
                                chat_key = getattr(chat_entity, 'title', str(chat_entity.id))
                                self.user_messages[chat_key].append(message)
                    except Exception as e:
                        logger.error(f"Ошибка при поиске сообщений в {chat}: {e}")
                        continue
                    
                    if message_count_in_chat > 0:
                        found_in_chats.append({
                            'chat': chat,
                            'title': getattr(chat_entity, 'title', chat),
                            'message_count': message_count_in_chat,
                            'entity': chat_entity
                        })
                        self.user_chats.append(chat_entity)
                        
                except Exception as e:
                    logger.error(f"Ошибка при работе с чатом {chat}: {e}")
                    continue
            
            self.message_count = total_messages
            
            # Формируем результат
            result = f"🔍 **Результаты поиска**\n\n"
            
            # Информация о пользователе
            first_name = getattr(self.target_user, 'first_name', '')
            last_name = getattr(self.target_user, 'last_name', '')
            username = getattr(self.target_user, 'username', 'нет')
            user_id = getattr(self.target_user, 'id', '')
            
            result += f"👤 **Пользователь:** {first_name} {last_name}\n"
            result += f"📱 **Username:** @{username}\n"
            result += f"🆔 **ID:** {user_id}\n\n"
            
            # Статистика
            result += f"📊 **Статистика:**\n"
            result += f"• Найден в чатах: **{len(found_in_chats)}/{total_chats}**\n"
            result += f"• Всего сообщений: **{total_messages}**\n\n"
            
            if found_in_chats:
                result += "📋 **Найден в чатах:**\n"
                for chat_info in found_in_chats[:5]:  # Первые 5 чатов
                    result += f"• {chat_info['title']}: {chat_info['message_count']} сообщ.\n"
                
                if len(found_in_chats) > 5:
                    result += f"\n... и еще {len(found_in_chats) - 5} чатов"
            else:
                result += "⚠️ Пользователь не найден ни в одном из чатов"
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при поиске пользователя: {e}")
            return f"❌ Ошибка при поиске: {str(e)}"

    async def get_user_avatar(self):
        """Получение аватарки пользователя"""
        if not self.target_user:
            return None
        
        try:
            photos = await client.get_profile_photos(self.target_user)
            if photos:
                latest_photo = photos[0]
                # Скачиваем фото в память
                photo_bytes = await client.download_media(latest_photo, file=bytes)
                return photo_bytes
        except Exception as e:
            logger.error(f"Ошибка при получении аватарки: {e}")
        
        return None

    async def search_replies_to_user(self, target_username: str):
        """Поиск реплаев пользователя на другого пользователя"""
        try:
            if not self.target_user:
                return "❌ Сначала найдите пользователя!"
            
            target_user = await self.resolve_username(target_username)
            if not target_user:
                return f"❌ Пользователь @{target_username} не найден"
            
            replies = []
            found_count = 0
            
            # Ищем реплы во всех сообщениях
            for chat_name, messages in self.user_messages.items():
                for message in messages:
                    if message.reply_to:
                        try:
                            replied_msg = await client.get_messages(
                                message.peer_id,
                                ids=message.reply_to.reply_to_msg_id
                            )
                            if replied_msg and replied_msg.sender_id == target_user.id:
                                # Формируем ссылку
                                try:
                                    chat = await client.get_entity(message.peer_id)
                                    chat_username = getattr(chat, 'username', None)
                                    
                                    if chat_username:
                                        message_link = f"https://t.me/{chat_username}/{message.id}"
                                    else:
                                        message_link = f"chat_id: {chat.id}, message_id: {message.id}"
                                    
                                    replies.append({
                                        'chat': chat,
                                        'message': message,
                                        'link': message_link,
                                        'text': message.text[:100] if message.text else ""
                                    })
                                    found_count += 1
                                    
                                except Exception as e:
                                    continue
                        except Exception as e:
                            continue
            
            # Формируем результат
            result = f"🔁 **Реплаи на @{target_username}**\n\n"
            
            if replies:
                result += f"✅ Найдено реплаев: **{found_count}**\n\n"
                
                for i, reply in enumerate(replies[:15], 1):
                    chat_title = getattr(reply['chat'], 'title', 'Неизвестный чат')
                    result += f"{i}. [{chat_title}]({reply['link']})\n"
                    if reply['text']:
                        result += f"   📝 {reply['text']}...\n"
                    result += "\n"
                
                if len(replies) > 15:
                    result += f"\n... и еще {len(replies) - 15} реплаев"
            else:
                result += "❌ Реплаев не найдено"
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при поиске реплаев: {e}")
            return f"❌ Ошибка при поиске реплаев: {str(e)}"

    async def get_all_messages_links(self, page: int = 0, per_page: int = 10):
        """Получение всех ссылок на сообщения с пагинацией"""
        all_messages = []
        
        for chat_name, messages in self.user_messages.items():
            for message in messages:
                try:
                    chat = await client.get_entity(message.peer_id)
                    chat_username = getattr(chat, 'username', None)
                    
                    if chat_username:
                        message_link = f"https://t.me/{chat_username}/{message.id}"
                    else:
                        message_link = f"chat_id: {chat.id}, message_id: {message.id}"
                    
                    all_messages.append({
                        'link': message_link,
                        'chat': getattr(chat, 'title', chat_name),
                        'date': message.date,
                        'text': message.text[:100] if message.text else ""
                    })
                except Exception as e:
                    continue
        
        # Сортируем по дате (новые сначала)
        all_messages.sort(key=lambda x: x['date'], reverse=True)
        
        # Пагинация
        total_messages = len(all_messages)
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, total_messages)
        page_messages = all_messages[start_idx:end_idx]
        
        result = f"📨 **Сообщения пользователя**\n\n"
        result += f"📄 Страница {page + 1}\n"
        result += f"📊 Сообщения {start_idx + 1}-{end_idx} из {total_messages}\n\n"
        
        for i, msg in enumerate(page_messages, start_idx + 1):
            result += f"{i}. [{msg['chat']}]({msg['link']})\n"
            if msg['text']:
                result += f"   📝 {msg['text']}...\n"
            result += f"   📅 {msg['date'].strftime('%d.%m.%Y %H:%M')}\n\n"
        
        # Кнопки пагинации
        buttons = []
        if page > 0:
            buttons.append(Button.inline("⬅️ Назад", f"msgs_{page-1}"))
        
        buttons.append(Button.inline(f"📄 {page+1}", "current_page"))
        
        if end_idx < total_messages:
            buttons.append(Button.inline("Вперед ➡️", f"msgs_{page+1}"))
        
        return result, [buttons]

    async def get_all_chats(self, page: int = 0, per_page: int = 10):
        """Получение всех чатов пользователя с пагинацией"""
        unique_chats = []
        seen_chats = set()
        
        for chat_entity in self.user_chats:
            chat_id = chat_entity.id
            if chat_id not in seen_chats:
                seen_chats.add(chat_id)
                unique_chats.append(chat_entity)
        
        # Пагинация
        total_chats = len(unique_chats)
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, total_chats)
        page_chats = unique_chats[start_idx:end_idx]
        
        result = f"👥 **Чаты пользователя**\n\n"
        result += f"📄 Страница {page + 1}\n"
        result += f"📊 Чаты {start_idx + 1}-{end_idx} из {total_chats}\n\n"
        
        for i, chat in enumerate(page_chats, start_idx + 1):
            title = getattr(chat, 'title', 'Без названия')
            members = getattr(chat, 'participants_count', '?')
            username = getattr(chat, 'username', 'нет')
            
            # Считаем сообщения в этом чате
            chat_key = getattr(chat, 'title', str(chat.id))
            msg_count = len(self.user_messages.get(chat_key, []))
            
            result += f"{i}. **{title}**\n"
            result += f"   👤 @{username}\n"
            result += f"   👥 Участников: {members}\n"
            result += f"   💬 Сообщений: {msg_count}\n\n"
        
        # Кнопки пагинации
        buttons = []
        if page > 0:
            buttons.append(Button.inline("⬅️ Назад", f"chats_{page-1}"))
        
        buttons.append(Button.inline(f"📄 {page+1}", "current_page"))
        
        if end_idx < total_chats:
            buttons.append(Button.inline("Вперед ➡️", f"chats_{page+1}"))
        
        return result, [buttons]

# Создаем экземпляр бота
bot = UserSearchBot()

async def start_bot():
    """Запуск бота"""
    try:
        # Подключаемся с использованием существующей сессии
        await client.start(bot_token=BOT_TOKEN)
        logger.info("✅ Бот успешно запущен!")
        
        me = await client.get_me()
        logger.info(f"🤖 Бот авторизован как: @{me.username}")
        
        @client.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            """Обработчик команды /start"""
            buttons = [
                [Button.inline("🔍 Поиск пользователя", "search_user")],
                [Button.inline("ℹ️ Помощь", "help_info")]
            ]
            
            await event.respond(
                "👋 **Добро пожаловать в UserSearchBot!**\n\n"
                "Я могу искать пользователей в чатах и анализировать их активность.\n\n"
                "**Основные функции:**\n"
                "• Поиск пользователя по юзернейму\n"
                "• Просмотр всех сообщений пользователя\n"
                "• Поиск реплаев на других пользователей\n"
                "• Отслеживание аватарки\n\n"
                "Нажмите кнопку ниже для начала:",
                buttons=buttons
            )
        
        @client.on(events.NewMessage(pattern='/search'))
        async def search_handler(event):
            """Обработчик команды /search"""
            await event.respond(
                "🔍 **Поиск пользователя**\n\n"
                "Отправьте мне юзернейм или ID пользователя для поиска:\n\n"
                "Примеры:\n"
                "• @username\n"
                "• +380123456789\n"
                "• 123456789 (ID)"
            )
        
        @client.on(events.NewMessage(pattern='/help'))
        async def help_handler(event):
            """Обработчик команды /help"""
            await event.respond(
                "📚 **Помощь по командам:**\n\n"
                "`/start` - Запустить бота\n"
                "`/search` - Начать поиск пользователя\n"
                "`/help` - Показать это сообщение\n\n"
                "**Как использовать:**\n"
                "1. Нажмите 'Поиск пользователя'\n"
                "2. Отправьте юзернейм пользователя\n"
                "3. Выберите действие из меню\n\n"
                "**Файл чатов:**\n"
                "Добавьте чаты для поиска в файл `chat.txt`"
            )
        
        @client.on(events.NewMessage())
        async def message_handler(event):
            """Обработчик текстовых сообщений"""
            try:
                if event.is_private and not event.message.text.startswith('/'):
                    text = event.message.text.strip()
                    chat_id = event.chat_id
                    
                    # Сохраняем текущий поиск для этого чата
                    current_search[chat_id] = text
                    
                    # Ищем пользователя
                    await event.respond("🔄 Ищу пользователя...")
                    result = await bot.search_user_in_chats(text)
                    
                    if bot.target_user:
                        # Формируем меню действий
                        buttons = [
                            [
                                Button.inline("👥 Группы", "show_groups"),
                                Button.inline("📨 Сообщения", "show_messages_0")
                            ],
                            [
                                Button.inline("🔁 Взаимодействия", "interactions"),
                                Button.inline("🖼️ Аватарка", "get_avatar")
                            ],
                            [
                                Button.inline("🔄 Обновить", "refresh_search"),
                                Button.inline("📊 Статистика", "show_stats")
                            ]
                        ]
                        
                        # Пытаемся получить аватарку
                        avatar = await bot.get_user_avatar()
                        
                        if avatar:
                            await event.delete()
                            await event.respond(
                                file=avatar,
                                caption=result,
                                buttons=buttons
                            )
                        else:
                            await event.respond(result, buttons=buttons)
                    else:
                        await event.respond(result)
                        
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения: {e}")
                await event.respond(f"❌ Ошибка: {str(e)}")
        
        @client.on(events.CallbackQuery())
        async def callback_handler(event):
            """Обработчик inline кнопок"""
            try:
                data = event.data.decode('utf-8')
                chat_id = event.chat_id
                
                if data == "search_user":
                    await event.edit(
                        "🔍 **Поиск пользователя**\n\n"
                        "Отправьте мне юзернейм или ID пользователя:"
                    )
                
                elif data == "help_info":
                    await event.edit(
                        "📚 **Помощь**\n\n"
                        "**Как использовать:**\n"
                        "1. Отправьте юзернейм пользователя\n"
                        "2. Выберите действие из меню\n\n"
                        "**Доступные действия:**\n"
                        "• 👥 Группы - все чаты пользователя\n"
                        "• 📨 Сообщения - все сообщения\n"
                        "• 🔁 Взаимодействия - поиск реплаев\n"
                        "• 🖼️ Аватарка - фото профиля\n"
                        "• 🔄 Обновить - обновить данные\n"
                        "• 📊 Статистика - детальная статистика",
                        buttons=[[Button.inline("🔍 Начать поиск", "search_user")]]
                    )
                
                elif data.startswith("show_messages_"):
                    # Показать сообщения с пагинацией
                    page = int(data.split('_')[-1])
                    result, buttons = await bot.get_all_messages_links(page=page)
                    await event.edit(result, buttons=buttons, link_preview=True)
                
                elif data.startswith("msgs_"):
                    # Пагинация сообщений
                    page = int(data.split('_')[-1])
                    result, buttons = await bot.get_all_messages_links(page=page)
                    await event.edit(result, buttons=buttons, link_preview=True)
                
                elif data == "show_groups":
                    # Показать группы
                    result, buttons = await bot.get_all_chats(page=0)
                    await event.edit(result, buttons=buttons)
                
                elif data.startswith("chats_"):
                    # Пагинация чатов
                    page = int(data.split('_')[-1])
                    result, buttons = await bot.get_all_chats(page=page)
                    await event.edit(result, buttons=buttons)
                
                elif data == "interactions":
                    # Меню взаимодействий
                    buttons = [
                        [Button.inline("🔍 Найти реплаи", "find_replies")],
                        [Button.inline("📊 Статистика ответов", "reply_stats")],
                        [Button.inline("🔙 Назад", "back_to_main")]
                    ]
                    await event.edit(
                        "🔁 **Взаимодействия**\n\n"
                        "Выберите действие:",
                        buttons=buttons
                    )
                
                elif data == "find_replies":
                    await event.edit(
                        "🔍 **Поиск реплаев**\n\n"
                        "Введите юзернейм пользователя, на которого ищем реплаи:\n\n"
                        "Пример: @username"
                    )
                
                elif data == "get_avatar":
                    # Получить аватарку
                    avatar = await bot.get_user_avatar()
                    if avatar:
                        await event.delete()
                        await event.respond(
                            file=avatar,
                            caption="🖼️ **Аватарка пользователя**"
                        )
                    else:
                        await event.answer("❌ Аватарка не найдена", alert=True)
                
                elif data == "refresh_search":
                    # Обновить поиск
                    if chat_id in current_search:
                        await event.edit("🔄 Обновляю данные...")
                        result = await bot.search_user_in_chats(current_search[chat_id])
                        
                        if bot.target_user:
                            buttons = [
                                [
                                    Button.inline("👥 Группы", "show_groups"),
                                    Button.inline("📨 Сообщения", "show_messages_0")
                                ],
                                [
                                    Button.inline("🔁 Взаимодействия", "interactions"),
                                    Button.inline("🖼️ Аватарка", "get_avatar")
                                ]
                            ]
                            await event.edit(result, buttons=buttons)
                        else:
                            await event.edit(result)
                    else:
                        await event.answer("❌ Нет данных для обновления", alert=True)
                
                elif data == "show_stats":
                    # Показать статистику
                    if bot.target_user:
                        result = f"📊 **Статистика пользователя**\n\n"
                        result += f"👤 {getattr(bot.target_user, 'first_name', '')} "
                        result += f"{getattr(bot.target_user, 'last_name', '')}\n"
                        result += f"📱 @{getattr(bot.target_user, 'username', 'нет')}\n\n"
                        result += f"📈 **Общая статистика:**\n"
                        result += f"• Чатов: {len(bot.user_chats)}\n"
                        result += f"• Сообщений: {bot.message_count}\n"
                        result += f"• Уникальных чатов: {len(bot.user_messages)}\n"
                        
                        await event.edit(result)
                    else:
                        await event.answer("❌ Сначала найдите пользователя", alert=True)
                
                elif data == "back_to_main":
                    # Вернуться к главному меню
                    if bot.target_user:
                        result = f"👤 **Пользователь найден**\n\n"
                        result += f"Имя: {getattr(bot.target_user, 'first_name', '')}\n"
                        result += f"Username: @{getattr(bot.target_user, 'username', 'нет')}\n"
                        result += f"Сообщений: {bot.message_count}\n"
                        result += f"Чатов: {len(bot.user_chats)}\n\n"
                        result += "**Выберите действие:**"
                        
                        buttons = [
                            [
                                Button.inline("👥 Группы", "show_groups"),
                                Button.inline("📨 Сообщения", "show_messages_0")
                            ],
                            [
                                Button.inline("🔁 Взаимодействия", "interactions"),
                                Button.inline("🖼️ Аватарка", "get_avatar")
                            ]
                        ]
                        
                        await event.edit(result, buttons=buttons)
                
                elif data == "current_page":
                    # Текущая страница - ничего не делаем
                    await event.answer()
                
                await event.answer()
                
            except Exception as e:
                logger.error(f"Ошибка в callback: {e}")
                await event.answer("❌ Произошла ошибка", alert=True)
        
        logger.info("✅ Бот готов к работе!")
        await client.run_until_disconnected()
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise

async def main():
    """Главная функция"""
    try:
        await start_bot()
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")

if __name__ == '__main__':
    # Запуск бота
    print("🚀 Запуск UserSearchBot...")
    print(f"📁 Используется сессия: {SESSION_NAME}")
    print("📝 Убедитесь, что файл chat.txt с чатами создан")
    print("⏳ Подключение к Telegram...")
    
    # Запускаем бота
    asyncio.run(main())