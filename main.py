import asyncio
import re
import os
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
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
API_ID = 123456  # Замените на ваш API ID
API_HASH = 'ваш_api_hash'  # Замените на ваш API HASH
BOT_TOKEN = 'ваш_бот_токен'  # Замените на токен вашего бота
SESSION_NAME = '+380994588662'
CHATS_FILE = 'chat.txt'

# Инициализация клиента
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
bot = None

# Глобальные переменные для хранения данных
user_data_cache = {}
message_cache = {}
avatar_tracker = {}
active_tracking = {}

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
            async with aiofiles.open(CHATS_FILE, 'r', encoding='utf-8') as f:
                content = await f.read()
                # Извлекаем юзернеймы и ссылки
                lines = content.strip().split('\n')
                self.chats = []
                for line in lines:
                    line = line.strip()
                    if line:
                        # Извлекаем юзернейм из ссылки или используем как есть
                        if 't.me/' in line:
                            # Обрабатываем ссылки типа https://t.me/username или @username
                            if 't.me/+' in line:
                                # Для приватных ссылок оставляем как есть
                                self.chats.append(line)
                            else:
                                # Извлекаем юзернейм после t.me/
                                username = line.split('t.me/')[-1].replace('@', '')
                                if username:
                                    self.chats.append(f'@{username}')
                        elif line.startswith('@'):
                            self.chats.append(line)
                        else:
                            self.chats.append(f'@{line}')
        except Exception as e:
            logger.error(f"Ошибка загрузки чатов: {e}")
            self.chats = []

    async def resolve_username(self, username: str):
        """Преобразование юзернейма в объект пользователя"""
        try:
            # Убираем @ если есть
            username = username.replace('@', '').strip()
            if username.startswith('+'):
                # Если это номер телефона
                return await client.get_input_entity(username)
            else:
                # Если это юзернейм
                return await client.get_entity(username)
        except Exception as e:
            logger.error(f"Ошибка разрешения username {username}: {e}")
            return None

    async def search_user_in_chats(self, user_identifier: str):
        """Поиск пользователя во всех чатах"""
        try:
            # Сначала пытаемся получить информацию о пользователе
            self.target_user = await self.resolve_username(user_identifier)
            if not self.target_user:
                return "Пользователь не найден"
            
            # Сохраняем информацию о пользователе
            self.user_info = self.target_user
            
            # Загружаем чаты
            await self.load_chats()
            
            total_chats = len(self.chats)
            found_in_chats = []
            total_messages = 0
            
            # Поиск в каждом чате
            for i, chat in enumerate(self.chats, 1):
                try:
                    logger.info(f"Поиск в чате {i}/{total_chats}: {chat}")
                    
                    # Получаем чат
                    chat_entity = None
                    if chat.startswith('https://t.me/+'):
                        # Для приватных ссылок
                        chat_entity = await client.get_entity(chat)
                    else:
                        # Для публичных чатов
                        chat_entity = await client.get_entity(chat)
                    
                    if not chat_entity:
                        continue
                    
                    # Получаем сообщения
                    messages_found = False
                    message_count_in_chat = 0
                    
                    try:
                        # Быстрый поиск сообщений пользователя
                        async for message in client.iter_messages(
                            chat_entity,
                            from_user=self.target_user,
                            limit=None  # Без лимита
                        ):
                            if message:
                                message_count_in_chat += 1
                                total_messages += 1
                                self.user_messages[chat].append(message)
                                messages_found = True
                    except Exception as e:
                        logger.error(f"Ошибка при получении сообщений из {chat}: {e}")
                        continue
                    
                    if messages_found:
                        found_in_chats.append({
                            'chat': chat,
                            'title': getattr(chat_entity, 'title', chat),
                            'message_count': message_count_in_chat
                        })
                        self.user_chats.append(chat_entity)
                        
                except Exception as e:
                    logger.error(f"Ошибка при работе с чатом {chat}: {e}")
                    continue
            
            self.message_count = total_messages
            
            # Формируем результат
            result = f"🔍 **Результаты поиска для {user_identifier}**\n\n"
            result += f"👤 **Пользователь:** {getattr(self.target_user, 'first_name', '')} {getattr(self.target_user, 'last_name', '')}\n"
            result += f"📱 **Username:** @{getattr(self.target_user, 'username', 'нет')}\n"
            result += f"🆔 **ID:** {self.target_user.id}\n\n"
            result += f"📊 **Статистика:**\n"
            result += f"• Найден в чатах: {len(found_in_chats)}/{total_chats}\n"
            result += f"• Всего сообщений: {total_messages}\n\n"
            
            if found_in_chats:
                result += "📋 **Чаты:**\n"
                for chat_info in found_in_chats[:10]:  # Первые 10 чатов
                    result += f"• {chat_info['title']}: {chat_info['message_count']} сообщ.\n"
                
                if len(found_in_chats) > 10:
                    result += f"\n... и еще {len(found_in_chats) - 10} чатов"
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при поиске пользователя: {e}")
            return f"Ошибка при поиске: {str(e)}"

    async def get_user_avatar(self):
        """Получение аватарки пользователя"""
        if not self.target_user:
            return None
        
        try:
            # Получаем фото профиля
            photos = await client.get_profile_photos(self.target_user)
            if photos:
                # Берем последнее фото
                latest_photo = photos[0]
                # Скачиваем фото
                photo_path = await client.download_media(latest_photo, file=bytes)
                return photo_path
        except Exception as e:
            logger.error(f"Ошибка при получении аватарки: {e}")
        
        return None

    async def search_replies_to_user(self, target_username: str):
        """Поиск реплаев пользователя на другого пользователя"""
        try:
            target_user = await self.resolve_username(target_username)
            if not target_user:
                return "Целевой пользователь не найден"
            
            if not self.target_user:
                return "Сначала найдите пользователя"
            
            replies = []
            
            # Ищем реплы во всех сохраненных сообщениях
            for chat_name, messages in self.user_messages.items():
                for message in messages:
                    if message.reply_to:
                        # Получаем сообщение, на которое был реплай
                        try:
                            replied_msg = await client.get_messages(
                                message.peer_id,
                                ids=message.reply_to.reply_to_msg_id
                            )
                            if replied_msg and replied_msg.sender_id == target_user.id:
                                # Формируем ссылку на сообщение
                                chat = await client.get_entity(message.peer_id)
                                chat_username = getattr(chat, 'username', None)
                                if chat_username:
                                    message_link = f"https://t.me/{chat_username}/{message.id}"
                                else:
                                    message_link = f"chat: {chat.title}, message: {message.id}"
                                
                                replies.append({
                                    'chat': chat,
                                    'message': message,
                                    'link': message_link,
                                    'text': message.text[:100] if message.text else ""
                                })
                        except Exception as e:
                            continue
            
            # Формируем результат
            result = f"🔁 **Реплаи {getattr(self.target_user, 'username', 'пользователя')} на @{target_username}**\n\n"
            
            if replies:
                result += f"Найдено реплаев: {len(replies)}\n\n"
                for i, reply in enumerate(replies[:20], 1):  # Первые 20 реплаев
                    result += f"{i}. [{reply['chat'].title}]({reply['link']})\n"
                    if reply['text']:
                        result += f"   📝 {reply['text']}...\n"
                    result += "\n"
                
                if len(replies) > 20:
                    result += f"\n... и еще {len(replies) - 20} реплаев"
            else:
                result += "Реплаев не найдено"
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при поиске реплаев: {e}")
            return f"Ошибка при поиске реплаев: {str(e)}"

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
                        'chat': chat.title,
                        'date': message.date,
                        'text': message.text[:50] if message.text else ""
                    })
                except Exception as e:
                    continue
        
        # Сортируем по дате (новые сначала)
        all_messages.sort(key=lambda x: x['date'], reverse=True)
        
        # Пагинация
        start_idx = page * per_page
        end_idx = start_idx + per_page
        page_messages = all_messages[start_idx:end_idx]
        
        result = f"📨 **Все сообщения пользователя**\n\n"
        result += f"Страница {page + 1} (сообщения {start_idx + 1}-{min(end_idx, len(all_messages))} из {len(all_messages)})\n\n"
        
        for i, msg in enumerate(page_messages, start_idx + 1):
            result += f"{i}. [{msg['chat']}]({msg['link']})\n"
            if msg['text']:
                result += f"   {msg['text']}...\n"
            result += f"   📅 {msg['date'].strftime('%Y-%m-%d %H:%M')}\n\n"
        
        # Кнопки пагинации
        buttons = []
        if page > 0:
            buttons.append(Button.inline("⬅️ Назад", f"msgs_page_{page-1}"))
        if end_idx < len(all_messages):
            buttons.append(Button.inline("Вперед ➡️", f"msgs_page_{page+1}"))
        
        return result, buttons

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
        start_idx = page * per_page
        end_idx = start_idx + per_page
        page_chats = unique_chats[start_idx:end_idx]
        
        result = f"👥 **Чаты пользователя**\n\n"
        result += f"Страница {page + 1} (чаты {start_idx + 1}-{min(end_idx, len(unique_chats))} из {len(unique_chats)})\n\n"
        
        for i, chat in enumerate(page_chats, start_idx + 1):
            title = getattr(chat, 'title', 'Без названия')
            members = getattr(chat, 'participants_count', '?')
            result += f"{i}. **{title}**\n"
            result += f"   👥 Участников: {members}\n"
            result += f"   📝 Сообщений: {len(self.user_messages.get(str(chat.id), []))}\n\n"
        
        # Кнопки пагинации
        buttons = []
        if page > 0:
            buttons.append(Button.inline("⬅️ Назад", f"chats_page_{page-1}"))
        if end_idx < len(unique_chats):
            buttons.append(Button.inline("Вперед ➡️", f"chats_page_{page+1}"))
        
        return result, buttons

async def start_bot():
    """Запуск бота"""
    await client.start()
    logger.info("Клиент запущен")
    
    # Создаем экземпляр бота
    global bot
    bot = UserSearchBot()
    
    @client.on(events.NewMessage(pattern='/start'))
    async def start_handler(event):
        """Обработчик команды /start"""
        buttons = [
            [Button.inline("🔍 Поиск пользователя", "search_user")],
            [Button.inline("ℹ️ О боте", "about")]
        ]
        await event.respond(
            "👋 **Добро пожаловать в UserSearchBot!**\n\n"
            "Я могу искать пользователей в чатах, анализировать их активность "
            "и отслеживать изменения.\n\n"
            "Нажмите кнопку ниже для начала поиска:",
            buttons=buttons
        )
    
    @client.on(events.NewMessage(pattern='/search'))
    async def search_handler(event):
        """Обработчик команды /search"""
        await event.respond(
            "🔍 **Поиск пользователя**\n\n"
            "Отправьте мне юзернейм или номер телефона пользователя, "
            "которого нужно найти (например: @username или +380123456789):"
        )
    
    @client.on(events.CallbackQuery())
    async def callback_handler(event):
        """Обработчик inline кнопок"""
        try:
            data = event.data.decode('utf-8')
            chat_id = event.chat_id
            message_id = event.message_id
            
            if data == "search_user":
                await event.edit(
                    "🔍 **Поиск пользователя**\n\n"
                    "Отправьте мне юзернейм или номер телефона пользователя, "
                    "которого нужно найти:"
                )
            
            elif data == "about":
                await event.edit(
                    "🤖 **UserSearchBot**\n\n"
                    "Бот для поиска и анализа пользователей в Telegram чатах.\n\n"
                    "**Функции:**\n"
                    "• Поиск пользователя по юзернейму/номеру\n"
                    "• Статистика по чатам и сообщениям\n"
                    "• Поиск реплаев на других пользователей\n"
                    "• Просмотр всех сообщений пользователя\n"
                    "• Отслеживание изменений аватарки\n\n"
                    "Для начала работы нажмите 'Поиск пользователя'",
                    buttons=[[Button.inline("🔍 Начать поиск", "search_user")]]
                )
            
            elif data.startswith("user_found_"):
                # Кнопки после успешного поиска пользователя
                user_id = data.split("_")[2]
                buttons = [
                    [
                        Button.inline("👥 Группы", "show_groups"),
                        Button.inline("📨 Сообщения", "show_messages")
                    ],
                    [
                        Button.inline("🔁 Взаимодействия", "interactions"),
                        Button.inline("👤 Следить за пользователем", "track_user")
                    ],
                    [
                        Button.inline("🖼️ Следить за аватаркой", "track_avatar"),
                        Button.inline("🔄 Обновить данные", f"refresh_{user_id}")
                    ]
                ]
                
                # Добавляем аватарку если есть
                avatar = await bot.get_user_avatar()
                if avatar:
                    await event.delete()
                    await event.respond(
                        file=avatar,
                        caption=f"👤 **Найден пользователь:** {getattr(bot.target_user, 'first_name', '')}\n"
                               f"📱 @{getattr(bot.target_user, 'username', 'нет')}\n\n"
                               f"**Выберите действие:**",
                        buttons=buttons
                    )
                else:
                    await event.edit(
                        f"👤 **Найден пользователь:** {getattr(bot.target_user, 'first_name', '')}\n"
                        f"📱 @{getattr(bot.target_user, 'username', 'нет')}\n\n"
                        f"**Выберите действие:**",
                        buttons=buttons
                    )
            
            elif data == "show_groups":
                # Показать группы с пагинацией
                result, buttons = await bot.get_all_chats(page=0)
                await event.edit(result, buttons=buttons, link_preview=False)
            
            elif data.startswith("chats_page_"):
                # Пагинация чатов
                page = int(data.split("_")[2])
                result, buttons = await bot.get_all_chats(page=page)
                await event.edit(result, buttons=buttons, link_preview=False)
            
            elif data == "show_messages":
                # Показать сообщения с пагинацией
                result, buttons = await bot.get_all_messages_links(page=0)
                await event.edit(result, buttons=buttons, link_preview=False)
            
            elif data.startswith("msgs_page_"):
                # Пагинация сообщений
                page = int(data.split("_")[2])
                result, buttons = await bot.get_all_messages_links(page=page)
                await event.edit(result, buttons=buttons, link_preview=False)
            
            elif data == "interactions":
                # Меню взаимодействий
                buttons = [
                    [Button.inline("🔍 Найти реплаи", "find_replies")],
                    [Button.inline("📊 Статистика", "interaction_stats")],
                    [Button.inline("🔙 Назад", "back_to_main")]
                ]
                await event.edit(
                    "🔁 **Взаимодействия пользователя**\n\n"
                    "Выберите тип взаимодействий для анализа:",
                    buttons=buttons
                )
            
            elif data == "find_replies":
                await event.edit(
                    "🔍 **Поиск реплаев**\n\n"
                    "Введите юзернейм пользователя, на которого ищем реплаи "
                    "(например: @username):"
                )
            
            elif data == "back_to_main":
                # Возврат к главному меню
                buttons = [
                    [
                        Button.inline("👥 Группы", "show_groups"),
                        Button.inline("📨 Сообщения", "show_messages")
                    ],
                    [
                        Button.inline("🔁 Взаимодействия", "interactions"),
                        Button.inline("👤 Следить за пользователем", "track_user")
                    ],
                    [
                        Button.inline("🖼️ Следить за аватаркой", "track_avatar"),
                        Button.inline("🔄 Обновить данные", f"refresh_{bot.target_user.id}")
                    ]
                ]
                await event.edit(
                    f"👤 **Пользователь:** {getattr(bot.target_user, 'first_name', '')}\n"
                    f"📱 @{getattr(bot.target_user, 'username', 'нет')}\n\n"
                    f"**Выберите действие:**",
                    buttons=buttons
                )
            
            elif data == "track_avatar":
                # Начать отслеживание аватарки
                if bot.target_user:
                    user_id = bot.target_user.id
                    if user_id not in avatar_tracker:
                        avatar_tracker[user_id] = {
                            'last_avatar': None,
                            'last_check': datetime.now(),
                            'chat_id': chat_id
                        }
                        await event.edit(
                            "✅ **Отслеживание аватарки начато**\n\n"
                            "Я буду проверять аватарку каждые 30 минут "
                            "и присылать уведомление, если она изменится."
                        )
                        
                        # Запускаем задачу отслеживания
                        asyncio.create_task(track_avatar_changes(user_id))
                    else:
                        await event.edit("⚠️ Отслеживание аватарки уже активно!")
            
            elif data.startswith("refresh_"):
                # Обновить данные пользователя
                await event.edit("🔄 Обновление данных...")
                result = await bot.search_user_in_chats(
                    getattr(bot.target_user, 'username', f"user{bot.target_user.id}")
                )
                buttons = [[Button.inline("🔍 Показать детали", f"user_found_{bot.target_user.id}")]]
                await event.edit(result, buttons=buttons)
            
            await event.answer()
            
        except Exception as e:
            logger.error(f"Ошибка в callback: {e}")
            await event.answer("Произошла ошибка", alert=True)
    
    @client.on(events.NewMessage())
    async def message_handler(event):
        """Обработчик текстовых сообщений"""
        try:
            text = event.message.text.strip()
            
            if event.is_private and not text.startswith('/'):
                # Если это поиск пользователя
                if not bot.target_user or "поиск" in event.message.text.lower():
                    await event.respond("🔍 Идет поиск пользователя...")
                    result = await bot.search_user_in_chats(text)
                    
                    if "найден" in result.lower() or "результаты" in result.lower():
                        buttons = [[Button.inline("🔍 Показать детали", f"user_found_{bot.target_user.id}")]]
                        await event.respond(result, buttons=buttons)
                    else:
                        await event.respond(result)
                
                # Если это поиск реплаев
                elif "реплаи" in event.message.reply_to_msg.text.lower() if event.message.reply_to else False:
                    await event.respond("🔍 Ищем реплаи...")
                    result = await bot.search_replies_to_user(text)
                    await event.respond(result, link_preview=False)
        
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}")
            await event.respond(f"Ошибка: {str(e)}")

async def track_avatar_changes(user_id):
    """Фоновая задача для отслеживания изменений аватарки"""
    while user_id in avatar_tracker:
        try:
            tracker = avatar_tracker[user_id]
            
            # Проверяем каждые 30 минут
            await asyncio.sleep(1800)  # 30 минут
            
            # Получаем текущую аватарку
            user = await client.get_entity(user_id)
            photos = await client.get_profile_photos(user)
            
            current_avatar = photos[0] if photos else None
            
            if tracker['last_avatar'] is None:
                # Первая проверка
                tracker['last_avatar'] = current_avatar
            elif current_avatar and tracker['last_avatar'].id != current_avatar.id:
                # Аватарка изменилась
                # Скачиваем новую аватарку
                new_avatar = await client.download_media(current_avatar, file=bytes)
                
                # Отправляем уведомление
                await client.send_message(
                    tracker['chat_id'],
                    f"🔄 **Аватарка пользователя изменилась!**\n\n"
                    f"Пользователь: @{getattr(user, 'username', 'без username')}",
                    file=new_avatar
                )
                
                tracker['last_avatar'] = current_avatar
            
            tracker['last_check'] = datetime.now()
            
        except Exception as e:
            logger.error(f"Ошибка отслеживания аватарки: {e}")
            await asyncio.sleep(300)  # Ждем 5 минут при ошибке

async def main():
    """Главная функция"""
    try:
        await start_bot()
        logger.info("Бот запущен и готов к работе!")
        
        # Запускаем бота
        await client.run_until_disconnected()
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")

if __name__ == '__main__':
    # Запуск бота
    asyncio.run(main())