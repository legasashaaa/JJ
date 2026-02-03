import asyncio
import json
import os
import re
import time
from datetime import datetime
from telethon import TelegramClient, events, Button
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty, InputPeerUser, InputPeerChannel
import logging

# Конфигурация - ОБА: и бот, и сессия
API_ID = 2040  # Телеграм API ID
API_HASH = 'b18441a1ff607e10a989891a5462e627'  # Телеграм API Hash
BOT_TOKEN = '8274874473:AAGQTVHI3CkwzotIuqiS6M2Whptcp-EpTnY'  # Ваш токен бота
OWNER_ID = 8524326478  # Ваш ID
SESSION_NAME = '+380962151936'  # Имя сессии вашего аккаунта

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Файлы для хранения данных
CONFIG_FILE = 'bot_config.json'
CACHE_FILE = 'cache.json'
RECORDINGS_FILE = 'recordings.json'

class BotInterface:
    """Класс для работы с ботом (кнопки, меню)"""
    
    def __init__(self, token):
        self.token = token
        self.bot = None
        self.user_client = None  # Клиент для сессии пользователя
        self.config = {}
        self.recordings = {}
        self.active_monitoring = True
        self.is_recording = False  # Флаг записи
        self.current_recording = []  # Текущая запись
        self.current_recording_chat = None  # Чат текущей записи
        self.pending_recording_send = None  # Ожидающая отправка записи
        self.deletion_stats = {
            'total_deleted': 0,
            'deleted_today': 0,
            'by_user': {},
            'by_chat': {}
        }
        
    async def initialize(self):
        """Инициализация бота"""
        logger.info("Инициализация бота...")
        
        # Загружаем конфигурацию и записи
        self.config = self.load_config()
        self.recordings = self.load_recordings()
        
        # Создаем клиент для бота
        self.bot = TelegramClient(
            'bot_session',
            API_ID,
            API_HASH
        )
        
        # Запускаем бота с токеном
        await self.bot.start(bot_token=self.token)
        
        # Получаем информацию о боте
        me = await self.bot.get_me()
        logger.info(f"🤖 Бот запущен как @{me.username}")
        
        # Регистрируем обработчики команд бота
        await self.register_bot_handlers()
        
        return self.bot
    
    async def start_user_session(self):
        """Запуск сессии пользователя для удаления сообщений"""
        logger.info("Запуск сессии пользователя...")
        
        # Создаем клиент для сессии пользователя
        self.user_client = TelegramClient(
            SESSION_NAME,
            API_ID,
            API_HASH
        )
        
        # Запускаем сессию пользователя
        await self.user_client.start()
        
        # Получаем информацию о пользователе
        user_me = await self.user_client.get_me()
        logger.info(f"👤 Сессия пользователя: {user_me.first_name} (ID: {user_me.id})")
        
        # Регистрируем обработчик для удаления сообщений
        await self.register_user_handlers()
        
        return self.user_client
    
    def load_config(self):
        """Загрузка конфигурации"""
        default_config = {
            'blacklist': [],  # Список пользователей
            'enabled_chats': [],  # Список чатов
            'enabled_for_all': True,  # Работать во всех чатах
            'delete_notifications': False,  # Уведомления ВЫКЛЮЧЕНЫ по умолчанию
            'delete_delay': 0  # Задержка удаления
        }
        
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")
        
        return default_config
    
    def load_recordings(self):
        """Загрузка записей"""
        try:
            if os.path.exists(RECORDINGS_FILE):
                with open(RECORDINGS_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки записей: {e}")
        
        return {}
    
    def save_config(self):
        """Сохранение конфигурации"""
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения конфигурации: {e}")
    
    def save_recordings(self):
        """Сохранение записей"""
        try:
            with open(RECORDINGS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.recordings, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения записей: {e}")
    
    async def register_bot_handlers(self):
        """Регистрация обработчиков для бота (меню, команды)"""
        
        @self.bot.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            """Обработчик команды /start"""
            if event.sender_id == OWNER_ID:
                await self.send_main_menu(event)
        
        @self.bot.on(events.NewMessage(pattern='/menu'))
        async def menu_handler(event):
            """Обработчик команды /menu"""
            if event.sender_id == OWNER_ID:
                await self.send_main_menu(event)
        
        @self.bot.on(events.NewMessage(pattern='/add'))
        async def add_handler(event):
            """Обработчик команды /add"""
            if event.sender_id == OWNER_ID:
                await self.handle_add_command(event)
        
        @self.bot.on(events.NewMessage(pattern='/remove'))
        async def remove_handler(event):
            """Обработчик команды /remove"""
            if event.sender_id == OWNER_ID:
                await self.handle_remove_command(event)
        
        @self.bot.on(events.NewMessage(pattern='/list'))
        async def list_handler(event):
            """Обработчик команды /list"""
            if event.sender_id == OWNER_ID:
                await self.show_blacklist(event)
        
        @self.bot.on(events.NewMessage(pattern='/stats'))
        async def stats_handler(event):
            """Обработчик команды /stats"""
            if event.sender_id == OWNER_ID:
                await self.show_stats(event)
        
        @self.bot.on(events.NewMessage(pattern='/toggle'))
        async def toggle_handler(event):
            """Обработчик команды /toggle"""
            if event.sender_id == OWNER_ID:
                self.active_monitoring = not self.active_monitoring
                status = "✅ Включен" if self.active_monitoring else "⏸️ Приостановлен"
                await event.reply(f"**Мониторинг:** {status}")
        
        @self.bot.on(events.NewMessage(pattern='/help'))
        async def help_handler(event):
            """Обработчик команды /help"""
            if event.sender_id == OWNER_ID:
                await self.show_help(event)
        
        @self.bot.on(events.NewMessage(pattern='/chats'))
        async def chats_handler(event):
            """Обработчик команды /chats"""
            if event.sender_id == OWNER_ID:
                await self.show_chat_menu(event)
        
        @self.bot.on(events.NewMessage(pattern='/record'))
        async def record_handler(event):
            """Обработчик команды /record - начать запись"""
            if event.sender_id == OWNER_ID:
                await self.start_recording(event)
        
        @self.bot.on(events.NewMessage(pattern='/stop'))
        async def stop_handler(event):
            """Обработчик команды /stop - остановить запись"""
            if event.sender_id == OWNER_ID:
                await self.stop_recording(event)
        
        @self.bot.on(events.NewMessage(pattern='/recordings'))
        async def recordings_handler(event):
            """Обработчик команды /recordings - показать записи"""
            if event.sender_id == OWNER_ID:
                await self.show_recordings_menu(event)
        
        # Обработчик ввода ID чата для отправки записи
        @self.bot.on(events.NewMessage)
        async def chat_input_handler(event):
            """Обработка ввода ID чата для отправки записи"""
            if event.sender_id == OWNER_ID and self.pending_recording_send:
                await self.handle_chat_input(event)
        
        # Обработчик пересланных сообщений для добавления пользователей
        @self.bot.on(events.NewMessage)
        async def forwarded_handler(event):
            """Обработка пересланных сообщений"""
            if event.sender_id == OWNER_ID and event.message.forward:
                await self.handle_forwarded_message(event)
        
        # Обработчик callback запросов (кнопки)
        @self.bot.on(events.CallbackQuery)
        async def callback_handler(event):
            """Обработчик нажатий на кнопки"""
            await self.handle_callback(event)
    
    async def register_user_handlers(self):
        """Регистрация обработчиков для сессии пользователя (удаление)"""
        
        @self.user_client.on(events.NewMessage())
        async def message_handler(event):
            """Обработчик сообщений для удаления"""
            if not self.active_monitoring:
                return
            
            try:
                # Проверяем, является ли сообщение реплаем
                if event.message.reply_to_msg_id:
                    await self.handle_reply_for_deletion(event)
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения: {e}")
            
            # Если идет запись, сохраняем сообщение
            if self.is_recording and event.sender_id == OWNER_ID:
                await self.save_to_recording(event)
    
    async def save_to_recording(self, event):
        """Сохранение сообщения в текущую запись"""
        try:
            # Пропускаем служебные команды
            if event.message.text in ['/record', '/stop', '/recordings']:
                return
            
            # Получаем время с момента начала записи
            if not self.current_recording:
                time_offset = 0.0
            else:
                time_offset = time.time() - self.current_recording[0]['timestamp']
            
            # Сохраняем данные сообщения
            message_data = {
                'timestamp': time.time(),
                'time_offset': time_offset,
                'text': event.message.text or '',
                'chat_id': event.chat_id,
                'message_id': event.message.id
            }
            
            # Если есть медиа, сохраняем информацию
            if event.message.media:
                message_data['has_media'] = True
                # Здесь можно добавить сохранение медиа
            
            self.current_recording.append(message_data)
            
            # Логируем
            logger.info(f"📝 Запись: сохранено сообщение в {time_offset:.2f}с")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения в запись: {e}")
    
    async def handle_reply_for_deletion(self, event):
        """Обработка реплаев для удаления ВСЕХ сообщений владельца в цепочке"""
        try:
            # Получаем сообщение, на которое сделан реплай
            chat_id = event.chat_id
            replied_msg = await event.get_reply_message()
            
            if not replied_msg:
                return
            
            # Проверяем, что реплай сделан на сообщение владельца
            if replied_msg.sender_id != OWNER_ID:
                return
            
            # Проверяем, включен ли мониторинг для этого чата
            if not self.config['enabled_for_all'] and chat_id not in self.config['enabled_chats']:
                return
            
            # Получаем информацию об отправителе реплая
            sender_id = event.sender_id
            sender = await event.get_sender()
            sender_username = getattr(sender, 'username', None)
            
            # Проверяем, находится ли отправитель в черном списке
            is_blacklisted = self.is_user_in_blacklist(sender_id, sender_username)
            
            if not is_blacklisted:
                return
            
            # Удаляем все сообщения владельца в цепочке
            await self.delete_all_owner_messages(event, replied_msg)
            
        except Exception as e:
            logger.error(f"Ошибка обработки реплая: {e}")
    
    async def delete_all_owner_messages(self, event, start_message):
        """Удаление всех сообщений владельца в цепочке"""
        try:
            chat_id = event.chat_id
            deleted_count = 0
            
            # Собираем все сообщения владельца в этой цепочке
            messages_to_delete = []
            
            # Начинаем с исходного сообщения
            current_msg = start_message
            
            while current_msg and current_msg.sender_id == OWNER_ID:
                messages_to_delete.append(current_msg)
                
                # Ищем следующее сообщение владельца в цепочке
                # (предыдущее по времени, так как обычно это ответы в одном потоке)
                try:
                    # Получаем предыдущие сообщения
                    async for msg in self.user_client.iter_messages(
                        chat_id,
                        min_id=current_msg.id - 50,
                        max_id=current_msg.id - 1,
                        from_user=OWNER_ID
                    ):
                        # Проверяем, является ли это частью той же цепочки
                        # (простая проверка по близости ID и времени)
                        messages_to_delete.append(msg)
                        break  # Берем только одно предыдущее
                        
                except:
                    pass
                
                # Прерываем цикл для предотвращения бесконечного поиска
                if len(messages_to_delete) >= 10:  # Максимум 10 сообщений
                    break
                
                # Для поиска вперед по цепочке
                try:
                    # Пробуем найти ответы на это сообщение от владельца
                    async for msg in self.user_client.iter_messages(
                        chat_id,
                        min_id=current_msg.id + 1,
                        max_id=current_msg.id + 50,
                        from_user=OWNER_ID,
                        reply_to=current_msg.id
                    ):
                        messages_to_delete.append(msg)
                        current_msg = msg
                        break
                    else:
                        # Если ответов нет, прерываем цикл
                        break
                except:
                    break
            
            # Удаляем все собранные сообщения
            for msg in messages_to_delete:
                try:
                    # Небольшая задержка для надежности
                    if self.config['delete_delay'] > 0:
                        await asyncio.sleep(self.config['delete_delay'])
                    
                    await msg.delete()
                    deleted_count += 1
                    
                    # Обновляем статистику
                    self.deletion_stats['total_deleted'] += 1
                    self.deletion_stats['deleted_today'] += 1
                    
                    user_id_str = str(event.sender_id)
                    chat_id_str = str(chat_id)
                    
                    if user_id_str not in self.deletion_stats['by_user']:
                        self.deletion_stats['by_user'][user_id_str] = 0
                    self.deletion_stats['by_user'][user_id_str] += 1
                    
                    if chat_id_str not in self.deletion_stats['by_chat']:
                        self.deletion_stats['by_chat'][chat_id_str] = 0
                    self.deletion_stats['by_chat'][chat_id_str] += 1
                    
                    # Логируем удаление (без отправки уведомлений, как вы просили)
                    logger.info(f"✅ Удалено сообщение {msg.id} в чате {chat_id}")
                    
                    # Небольшая пауза между удалениями
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    error_msg = f"❌ Ошибка при удалении сообщения {msg.id}: {str(e)}"
                    logger.error(error_msg)
            
            logger.info(f"🗑️ Удалено {deleted_count} сообщений от владельца")
            
        except Exception as e:
            logger.error(f"Ошибка при массовом удалении: {e}")
    
    def is_user_in_blacklist(self, user_id, username=None):
        """Проверка, находится ли пользователь в черном списке"""
        for user in self.config['blacklist']:
            # Проверка по ID
            if user['id'] == user_id:
                return True
            
            # Проверка по username
            if username and user.get('username'):
                if user['username'].lower() == username.lower():
                    return True
        
        return False
    
    async def send_main_menu(self, event):
        """Отправка главного меню"""
        menu_text = (
            f"🤖 **Главное меню - Автоудаление сообщений**\n\n"
            f"📊 **Статистика:**\n"
            f"• 👤 Пользователей в черном списке: **{len(self.config['blacklist'])}**\n"
            f"• 💬 Активных чатов: **{len(self.config['enabled_chats'])}**\n"
            f"• 🗑️ Всего удалено: **{self.deletion_stats['total_deleted']}**\n"
            f"• 🗑️ Удалено сегодня: **{self.deletion_stats['deleted_today']}**\n"
            f"• ⚡ Мониторинг: **{'✅ Активен' if self.active_monitoring else '⏸️ Приостановлен'}**\n"
            f"• 📝 Запись: **{'🔴 ВКЛ' if self.is_recording else '⚪ ВЫКЛ'}**\n\n"
            f"🌐 **Режим:** {'Все чаты' if self.config['enabled_for_all'] else 'Только выбранные'}"
        )
        
        buttons = [
            [Button.inline("👤 Управление пользователями", b"user_management"),
             Button.inline("💬 Управление чатами", b"chat_management")],
            [Button.inline("📊 Статистика", b"stats_menu"),
             Button.inline("⚙️ Настройки", b"settings_menu")],
            [Button.inline("🎙️ Записи", b"recordings_menu"),
             Button.inline("📋 Помощь", b"help_menu")]
        ]
        
        if self.is_recording:
            buttons.insert(1, [Button.inline("⏹️ Остановить запись", b"stop_recording")])
        else:
            buttons.insert(1, [Button.inline("🎬 Начать запись", b"start_recording")])
        
        await event.reply(menu_text, buttons=buttons, parse_mode='md')
    
    async def start_recording(self, event):
        """Начать запись сообщений"""
        if self.is_recording:
            await event.reply("⚠️ Запись уже идет!")
            return
        
        self.is_recording = True
        self.current_recording = []
        self.current_recording_chat = event.chat_id
        
        await event.reply(
            "🎬 **Запись начата!**\n\n"
            "Теперь все ваши сообщения будут записываться.\n"
            "Используйте /stop для остановки записи.\n\n"
            "**Что записывается:**\n"
            "• Текст сообщений\n"
            "• Время отправки\n"
            "• Паузы между сообщениями\n"
            "• Порядок сообщений\n\n"
            "⚠️ Не используйте команды /record, /stop, /recordings во время записи!"
        )
        logger.info("Запись сообщений начата")
    
    async def stop_recording(self, event):
        """Остановить запись и сохранить"""
        if not self.is_recording:
            await event.reply("⚠️ Запись не идет!")
            return
        
        if not self.current_recording:
            self.is_recording = False
            await event.reply("❌ Запись пуста!")
            return
        
        # Сохраняем запись
        recording_id = f"recording_{int(time.time())}"
        self.recordings[recording_id] = {
            'id': recording_id,
            'name': f"Запись от {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            'messages': self.current_recording,
            'created_at': time.time(),
            'chat_id': self.current_recording_chat,
            'message_count': len(self.current_recording)
        }
        
        self.save_recordings()
        
        # Сбрасываем состояние записи
        self.is_recording = False
        recording_data = self.current_recording
        self.current_recording = []
        self.current_recording_chat = None
        
        await event.reply(
            f"✅ **Запись сохранена!**\n\n"
            f"📝 ID записи: `{recording_id}`\n"
            f"📊 Сообщений записано: **{len(recording_data)}**\n"
            f"⏱️ Длительность: **{recording_data[-1]['time_offset']:.1f} секунд**\n\n"
            f"Используйте /recordings для управления записями."
        )
        logger.info(f"Запись сохранена: {recording_id} ({len(recording_data)} сообщений)")
    
    async def show_recordings_menu(self, event):
        """Показать меню записей"""
        if not self.recordings:
            await event.reply(
                "📝 **У вас пока нет записей**\n\n"
                "Чтобы создать запись:\n"
                "1. Используйте /record или кнопку 'Начать запись'\n"
                "2. Пишите сообщения как обычно\n"
                "3. Используйте /stop для сохранения\n\n"
                "Запись сохранит все ваши сообщения с оригинальной скоростью и порядком.",
                buttons=[
                    [Button.inline("🎬 Начать запись", b"start_recording")],
                    [Button.inline("↩️ Назад", b"main_menu")]
                ]
            )
            return
        
        text = "📝 **Ваши записи:**\n\n"
        buttons = []
        
        for rec_id, recording in sorted(self.recordings.items(), 
                                        key=lambda x: x[1]['created_at'], 
                                        reverse=True)[:10]:  # Показываем последние 10
            
            rec_name = recording.get('name', f"Запись {rec_id[:8]}")
            msg_count = recording.get('message_count', len(recording.get('messages', [])))
            created_time = datetime.fromtimestamp(recording['created_at']).strftime('%d.%m %H:%M')
            
            text_line = f"• **{rec_name}**\n"
            text_line += f"  📊 {msg_count} сообщ., 📅 {created_time}\n"
            text += text_line
            
            buttons.append([Button.inline(f"▶️ {rec_name[:30]}", f"play_recording_{rec_id}")])
        
        buttons.append([Button.inline("🗑️ Удалить запись", b"delete_recording_menu")])
        buttons.append([Button.inline("↩️ Назад", b"main_menu")])
        
        await event.reply(text, buttons=buttons, parse_mode='md')
    
    async def play_recording(self, event, recording_id):
        """Воспроизвести запись"""
        if recording_id not in self.recordings:
            await event.answer("❌ Запись не найдена!", alert=True)
            return
        
        recording = self.recordings[recording_id]
        
        await event.edit(
            f"▶️ **Воспроизведение записи:** {recording.get('name', 'Без названия')}\n\n"
            f"📊 Сообщений: {recording.get('message_count', 0)}\n"
            f"⏱️ Длительность: {recording['messages'][-1]['time_offset']:.1f}с\n\n"
            "**Шаг 1: Куда отправить запись?**\n"
            "Отправьте ID чата или username:\n"
            "Примеры:\n"
            "• `-1001234567890` (ID группы/канала)\n"
            "• `@username` (юзернейм)\n"
            "• `username` (без @)\n"
            "• `123456789` (ID пользователя)\n\n"
            "Или нажмите кнопку 'Отправить сюда'",
            buttons=[
                [Button.inline("📨 Отправить сюда", f"send_here_{recording_id}")],
                [Button.inline("↩️ Назад", b"recordings_menu")]
            ]
        )
        
        # Сохраняем ожидающую отправку
        self.pending_recording_send = {
            'recording_id': recording_id,
            'step': 'chat_input',
            'event': event
        }
    
    async def handle_chat_input(self, event):
        """Обработка ввода чата для отправки записи"""
        if not self.pending_recording_send:
            return
        
        try:
            recording_id = self.pending_recording_send['recording_id']
            original_event = self.pending_recording_send['event']
            
            # Получаем введенный текст
            chat_input = event.message.text.strip()
            
            # Получаем информацию о чате
            chat_info = await self.get_chat_info(chat_input)
            
            if not chat_info:
                await event.reply("❌ Не удалось найти чат. Попробуйте еще раз.")
                return
            
            # Сохраняем информацию о чате
            self.pending_recording_send['chat_info'] = chat_info
            
            # Переходим к следующему шагу
            await self.ask_target_user(original_event, recording_id, chat_info)
            
            # Удаляем сообщение с вводом
            await event.delete()
            
        except Exception as e:
            logger.error(f"Ошибка обработки ввода чата: {e}")
            await event.reply(f"❌ Ошибка: {str(e)}")
        finally:
            # Не сбрасываем pending_recording_send, переходим к следующему шагу
            self.pending_recording_send['step'] = 'user_input'
    
    async def get_chat_info(self, chat_input):
        """Получение информации о чате"""
        try:
            # Убираем пробелы
            chat_input = chat_input.strip()
            
            # Если это @username или просто username
            if chat_input.startswith('@'):
                chat_input = chat_input[1:]
            
            # Пробуем получить информацию о чате
            try:
                entity = await self.user_client.get_entity(chat_input)
                chat_title = getattr(entity, 'title', getattr(entity, 'first_name', ''))
                return {
                    'id': entity.id,
                    'type': 'channel' if hasattr(entity, 'broadcast') else 
                            'chat' if hasattr(entity, 'megagroup') else 
                            'user',
                    'username': getattr(entity, 'username', None),
                    'title': chat_title,
                    'access_hash': getattr(entity, 'access_hash', None)
                }
            except:
                # Пробуем как числовой ID
                try:
                    chat_id = int(chat_input)
                    # Для ID без @ нужно использовать специальные методы
                    if chat_id < 0:  # Группа/канал
                        return {'id': chat_id, 'type': 'channel', 'title': f'ID: {chat_id}'}
                    else:  # Пользователь
                        return {'id': chat_id, 'type': 'user', 'title': f'ID: {chat_id}'}
                except:
                    return None
            
        except Exception as e:
            logger.error(f"Ошибка получения информации о чате: {e}")
            return None
    
    async def ask_target_user(self, event, recording_id, chat_info):
        """Спросить пользователя, за кем следить"""
        chat_title = chat_info.get('title', f'ID: {chat_info["id"]}')
        
        await event.edit(
            f"✅ **Чат определен:** {chat_title}\n\n"
            f"**Шаг 2: За кем следить?**\n"
            f"Введите username или ID пользователя, за сообщениями которого нужно следить:\n"
            f"Примеры:\n"
            f"• `@username`\n"
            f"• `123456789` (ID пользователя)\n\n"
            f"Бот будет отправлять ваши сообщения, отвечая на последнее сообщение этого пользователя.\n"
            f"Если пользователь удалит сообщение, бот найдет его предыдущее или следующее сообщение.",
            buttons=[
                [Button.inline("🚫 Не следить, отправлять как есть", f"no_tracking_{recording_id}_{chat_info['id']}")],
                [Button.inline("↩️ Назад", b"recordings_menu")]
            ]
        )
    
    async def process_target_user(self, event, user_input):
        """Обработка ввода целевого пользователя"""
        if not self.pending_recording_send:
            return
        
        try:
            recording_id = self.pending_recording_send['recording_id']
            chat_info = self.pending_recording_send.get('chat_info')
            
            if not chat_info:
                await event.reply("❌ Ошибка: информация о чате потеряна.")
                return
            
            # Получаем информацию о пользователе
            user_info = await self.get_user_info(user_input)
            
            if not user_info:
                await event.reply("❌ Не удалось найти пользователя. Попробуйте еще раз.")
                return
            
            # Сохраняем информацию о пользователе
            self.pending_recording_send['target_user'] = user_info
            
            # Переходим к выбору режима отправки
            await self.ask_send_mode(event, recording_id, chat_info, user_info)
            
        except Exception as e:
            logger.error(f"Ошибка обработки целевого пользователя: {e}")
            await event.reply(f"❌ Ошибка: {str(e)}")
    
    async def ask_send_mode(self, event, recording_id, chat_info, user_info):
        """Спросить режим отправки"""
        user_display = self.format_user_display(user_info)
        
        await event.edit(
            f"✅ **Целевой пользователь:** {user_display}\n\n"
            f"**Шаг 3: Режим отправки**\n\n"
            f"**Вариант 1: Отслеживание сообщений**\n"
            f"• Бот будет следить за сообщениями {user_display}\n"
            f"• Отправит ваши сообщения, отвечая на его последнее сообщение\n"
            f"• Если сообщение удалено, найдет другое\n\n"
            f"**Вариант 2: Указать конкретное сообщение**\n"
            f"• Отправьте ссылку на сообщение для ответа\n"
            f"• Пример: `https://t.me/c/1234567890/123`\n\n"
            f"**Вариант 3: Отправить как есть**\n"
            f"• Просто отправит сообщения без ответа",
            buttons=[
                [Button.inline("👁️ Отслеживать сообщения", f"track_messages_{recording_id}_{chat_info['id']}_{user_info['id']}")],
                [Button.inline("🔗 Указать сообщение", f"specify_message_{recording_id}_{chat_info['id']}")],
                [Button.inline("📤 Отправить как есть", f"send_plain_{recording_id}_{chat_info['id']}")],
                [Button.inline("↩️ Назад", b"recordings_menu")]
            ]
        )
    
    async def start_tracking_and_send(self, event, recording_id, chat_id, user_id):
        """Начать отслеживание и отправить запись"""
        recording = self.recordings.get(recording_id)
        if not recording:
            await event.answer("❌ Запись не найдена!", alert=True)
            return
        
        await event.edit("🔍 **Ищу последнее сообщение пользователя...**")
        
        try:
            # Ищем последнее сообщение пользователя в чате
            target_message = await self.find_user_message(chat_id, user_id)
            
            if target_message:
                await self.confirm_send_with_tracking(event, recording_id, chat_id, user_id, target_message.id)
            else:
                await event.edit(
                    "❌ **Сообщение не найдено!**\n\n"
                    "Не удалось найти сообщения пользователя в этом чате.\n"
                    "Выберите другой вариант отправки:",
                    buttons=[
                        [Button.inline("🔗 Указать сообщение вручную", f"specify_message_{recording_id}_{chat_id}")],
                        [Button.inline("📤 Отправить как есть", f"send_plain_{recording_id}_{chat_id}")],
                        [Button.inline("↩️ Отмена", b"recordings_menu")]
                    ]
                )
                
        except Exception as e:
            logger.error(f"Ошибка поиска сообщения: {e}")
            await event.edit(f"❌ Ошибка поиска: {str(e)}")
    
    async def find_user_message(self, chat_id, user_id):
        """Найти сообщение пользователя в чате"""
        try:
            # Ищем последние 50 сообщений
            async for message in self.user_client.iter_messages(chat_id, limit=50):
                if message.sender_id == user_id:
                    return message
            
            # Если не нашли, ищем дальше
            async for message in self.user_client.iter_messages(chat_id, limit=100, offset_id=0):
                if message.sender_id == user_id:
                    return message
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка поиска сообщения пользователя: {e}")
            return None
    
    async def confirm_send_with_tracking(self, event, recording_id, chat_id, user_id, message_id):
        """Подтверждение отправки с отслеживанием"""
        recording = self.recordings[recording_id]
        
        await event.edit(
            f"✅ **Найдено сообщение для ответа!**\n\n"
            f"📝 Запись: {recording.get('name', 'Без названия')}\n"
            f"💬 Чат: `{chat_id}`\n"
            f"👤 Пользователь: `{user_id}`\n"
            f"📎 Ответ на сообщение: `{message_id}`\n"
            f"📊 Сообщений: {recording.get('message_count', 0)}\n"
            f"⏱️ Длительность: {recording['messages'][-1]['time_offset']:.1f}с\n\n"
            f"**Бот будет:**\n"
            f"1. Отправлять сообщения, отвечая на это сообщение\n"
            f"2. Если сообщение удалено, искать другое сообщение пользователя\n"
            f"3. Отправлять все сообщения с оригинальной скоростью",
            buttons=[
                [Button.inline("🚀 Начать отправку", f"execute_tracked_{recording_id}_{chat_id}_{user_id}_{message_id}")],
                [Button.inline("↩️ Отмена", b"recordings_menu")]
            ]
        )
    
    async def execute_tracked_send(self, event, recording_id, chat_id, user_id, initial_message_id):
        """Выполнить отправку с отслеживанием"""
        recording = self.recordings[recording_id]
        messages = recording['messages']
        
        await event.edit("🚀 **Начинаю отправку с отслеживанием...**\n\n0%")
        
        try:
            sent_count = 0
            start_time = time.time()
            last_progress_update = start_time
            
            # Текущее сообщение для ответа
            current_reply_to = initial_message_id
            
            for i, msg_data in enumerate(messages):
                # Рассчитываем задержку для сохранения оригинальной скорости
                if i > 0:
                    time_diff = msg_data['time_offset'] - messages[i-1]['time_offset']
                    if time_diff > 0:
                        await asyncio.sleep(time_diff)
                
                # Пробуем отправить с текущим reply_to
                try:
                    sent_msg = await self.user_client.send_message(
                        chat_id,
                        msg_data['text'],
                        reply_to=current_reply_to
                    )
                    sent_count += 1
                    
                except Exception as e:
                    # Если ошибка из-за удаленного сообщения, ищем новое
                    if "MESSAGE_ID_INVALID" in str(e) or "REPLY_MESSAGE_ID_INVALID" in str(e):
                        logger.info(f"Сообщение {current_reply_to} удалено, ищу новое...")
                        
                        # Ищем новое сообщение пользователя
                        new_message = await self.find_user_message(chat_id, user_id)
                        
                        if new_message:
                            current_reply_to = new_message.id
                            logger.info(f"Найдено новое сообщение: {current_reply_to}")
                            
                            # Пробуем отправить с новым reply_to
                            try:
                                sent_msg = await self.user_client.send_message(
                                    chat_id,
                                    msg_data['text'],
                                    reply_to=current_reply_to
                                )
                                sent_count += 1
                            except:
                                # Если все равно ошибка, отправляем без ответа
                                try:
                                    sent_msg = await self.user_client.send_message(
                                        chat_id,
                                        msg_data['text']
                                    )
                                    sent_count += 1
                                except:
                                    logger.error(f"Не удалось отправить сообщение {i}")
                        else:
                            # Если не нашли сообщение, отправляем без ответа
                            try:
                                sent_msg = await self.user_client.send_message(
                                    chat_id,
                                    msg_data['text']
                                )
                                sent_count += 1
                            except:
                                logger.error(f"Не удалось отправить сообщение {i}")
                    else:
                        # Другая ошибка, пробуем отправить без ответа
                        try:
                            sent_msg = await self.user_client.send_message(
                                chat_id,
                                msg_data['text']
                            )
                            sent_count += 1
                        except:
                            logger.error(f"Не удалось отправить сообщение {i}")
                
                # Обновляем прогресс
                progress = int((i + 1) / len(messages) * 100)
                if progress % 25 == 0 or time.time() - last_progress_update > 2:
                    await event.edit(f"🚀 **Отправка с отслеживанием...**\n\n{progress}%")
                    last_progress_update = time.time()
            
            total_time = time.time() - start_time
            original_time = messages[-1]['time_offset'] if messages else 0
            
            await event.edit(
                f"✅ **Запись успешно отправлена с отслеживанием!**\n\n"
                f"📊 Отправлено сообщений: **{sent_count}/{len(messages)}**\n"
                f"⏱️ Оригинальное время: **{original_time:.1f}с**\n"
                f"⏱️ Фактическое время: **{total_time:.1f}с**\n"
                f"💬 Чат: `{chat_id}`\n"
                f"👤 Отслеживаемый пользователь: `{user_id}`"
            )
            
            logger.info(f"Запись {recording_id} отправлена в чат {chat_id} с отслеживанием пользователя {user_id}")
            
            # Сбрасываем ожидающую отправку
            self.pending_recording_send = None
            
        except Exception as e:
            await event.edit(f"❌ **Ошибка отправки:**\n\n{str(e)}")
            logger.error(f"Ошибка отправки записи с отслеживанием: {e}")
            self.pending_recording_send = None
    
    async def specify_message_link(self, event, recording_id, chat_id):
        """Запрос ссылки на сообщение"""
        await event.edit(
            f"🔗 **Укажите ссылку на сообщение**\n\n"
            f"Отправьте ссылку на сообщение, на которое нужно отвечать:\n"
            f"Пример: `https://t.me/c/1234567890/123`\n\n"
            f"Или нажмите кнопку для отправки как есть.",
            buttons=[
                [Button.inline("📤 Отправить как есть", f"send_plain_{recording_id}_{chat_id}")],
                [Button.inline("↩️ Отмена", b"recordings_menu")]
            ]
        )
        
        # Обновляем ожидающую отправку
        self.pending_recording_send = {
            'recording_id': recording_id,
            'chat_id': chat_id,
            'step': 'message_link',
            'event': event
        }
    
    async def process_message_link(self, event, message_link):
        """Обработка ссылки на сообщение"""
        if not self.pending_recording_send:
            return
        
        try:
            recording_id = self.pending_recording_send['recording_id']
            chat_id = self.pending_recording_send['chat_id']
            
            # Парсим ссылку
            message_id = self.parse_message_link(message_link)
            
            if not message_id:
                await event.reply("❌ Неверный формат ссылки. Пример: https://t.me/c/1234567890/123")
                return
            
            await self.confirm_send_with_message(event, recording_id, chat_id, message_id)
            
        except Exception as e:
            logger.error(f"Ошибка обработки ссылки: {e}")
            await event.reply(f"❌ Ошибка: {str(e)}")
    
    def parse_message_link(self, link):
        """Парсинг ссылки на сообщение"""
        try:
            # Убираем пробелы
            link = link.strip()
            
            # Парсим разные форматы ссылок
            patterns = [
                r't\.me/c/(\d+)/(\d+)',  # t.me/c/1234567890/123
                r't\.me/(\w+)/(\d+)',    # t.me/username/123
                r'tg://openmessage\?chat_id=(-?\d+)&message_id=(\d+)'  # tg://openmessage
            ]
            
            for pattern in patterns:
                match = re.search(pattern, link)
                if match:
                    return int(match.group(2))  # Возвращаем ID сообщения
            
            return None
            
        except:
            return None
    
    async def confirm_send_with_message(self, event, recording_id, chat_id, message_id):
        """Подтверждение отправки с указанным сообщением"""
        recording = self.recordings[recording_id]
        
        await event.edit(
            f"✅ **Сообщение для ответа определено!**\n\n"
            f"📝 Запись: {recording.get('name', 'Без названия')}\n"
            f"💬 Чат: `{chat_id}`\n"
            f"📎 Ответ на сообщение: `{message_id}`\n"
            f"📊 Сообщений: {recording.get('message_count', 0)}\n"
            f"⏱️ Длительность: {recording['messages'][-1]['time_offset']:.1f}с",
            buttons=[
                [Button.inline("🚀 Начать отправку", f"execute_with_message_{recording_id}_{chat_id}_{message_id}")],
                [Button.inline("↩️ Отмена", b"recordings_menu")]
            ]
        )
    
    async def execute_with_message_send(self, event, recording_id, chat_id, message_id):
        """Выполнить отправку с указанным сообщением"""
        recording = self.recordings[recording_id]
        messages = recording['messages']
        
        await event.edit("🚀 **Начинаю отправку...**\n\n0%")
        
        try:
            sent_count = 0
            start_time = time.time()
            last_progress_update = start_time
            
            for i, msg_data in enumerate(messages):
                # Рассчитываем задержку для сохранения оригинальной скорости
                if i > 0:
                    time_diff = msg_data['time_offset'] - messages[i-1]['time_offset']
                    if time_diff > 0:
                        await asyncio.sleep(time_diff)
                
                # Отправляем сообщение
                try:
                    if i == 0:
                        # Первое сообщение как реплай
                        sent_msg = await self.user_client.send_message(
                            chat_id,
                            msg_data['text'],
                            reply_to=message_id
                        )
                    else:
                        # Остальные как обычные сообщения
                        sent_msg = await self.user_client.send_message(
                            chat_id,
                            msg_data['text']
                        )
                    
                    sent_count += 1
                    
                except Exception as e:
                    # Если ошибка с reply_to, отправляем без него
                    if i == 0 and ("MESSAGE_ID_INVALID" in str(e) or "REPLY_MESSAGE_ID_INVALID" in str(e)):
                        try:
                            sent_msg = await self.user_client.send_message(
                                chat_id,
                                msg_data['text']
                            )
                            sent_count += 1
                        except:
                            logger.error(f"Не удалось отправить сообщение {i}")
                    else:
                        logger.error(f"Не удалось отправить сообщение {i}")
                
                # Обновляем прогресс
                progress = int((i + 1) / len(messages) * 100)
                if progress % 25 == 0 or time.time() - last_progress_update > 2:
                    await event.edit(f"🚀 **Отправка записи...**\n\n{progress}%")
                    last_progress_update = time.time()
            
            total_time = time.time() - start_time
            original_time = messages[-1]['time_offset'] if messages else 0
            
            await event.edit(
                f"✅ **Запись успешно отправлена!**\n\n"
                f"📊 Отправлено сообщений: **{sent_count}/{len(messages)}**\n"
                f"⏱️ Оригинальное время: **{original_time:.1f}с**\n"
                f"⏱️ Фактическое время: **{total_time:.1f}с**\n"
                f"💬 Чат: `{chat_id}`\n"
                f"📎 Ответ на сообщение: `{message_id}`"
            )
            
            logger.info(f"Запись {recording_id} отправлена в чат {chat_id} с ответом на {message_id}")
            
            # Сбрасываем ожидающую отправку
            self.pending_recording_send = None
            
        except Exception as e:
            await event.edit(f"❌ **Ошибка отправки:**\n\n{str(e)}")
            logger.error(f"Ошибка отправки записи: {e}")
            self.pending_recording_send = None
    
    async def send_plain_recording(self, event, recording_id, chat_id):
        """Отправить запись без ответа"""
        recording = self.recordings[recording_id]
        
        await event.edit(
            f"✅ **Отправка записи как есть**\n\n"
            f"📝 Запись: {recording.get('name', 'Без названия')}\n"
            f"💬 Чат: `{chat_id}`\n"
            f"📊 Сообщений: {recording.get('message_count', 0)}\n"
            f"⏱️ Длительность: {recording['messages'][-1]['time_offset']:.1f}с\n\n"
            f"Сообщения будут отправлены без ответа на другие сообщения.",
            buttons=[
                [Button.inline("🚀 Начать отправку", f"execute_plain_{recording_id}_{chat_id}")],
                [Button.inline("↩️ Отмена", b"recordings_menu")]
            ]
        )
    
    async def execute_plain_send(self, event, recording_id, chat_id):
        """Выполнить отправку без ответа"""
        recording = self.recordings[recording_id]
        messages = recording['messages']
        
        await event.edit("🚀 **Начинаю отправку...**\n\n0%")
        
        try:
            sent_count = 0
            start_time = time.time()
            last_progress_update = start_time
            
            for i, msg_data in enumerate(messages):
                # Рассчитываем задержку для сохранения оригинальной скорости
                if i > 0:
                    time_diff = msg_data['time_offset'] - messages[i-1]['time_offset']
                    if time_diff > 0:
                        await asyncio.sleep(time_diff)
                
                # Отправляем сообщение
                try:
                    sent_msg = await self.user_client.send_message(
                        chat_id,
                        msg_data['text']
                    )
                    sent_count += 1
                except Exception as e:
                    logger.error(f"Не удалось отправить сообщение {i}: {e}")
                
                # Обновляем прогресс
                progress = int((i + 1) / len(messages) * 100)
                if progress % 25 == 0 or time.time() - last_progress_update > 2:
                    await event.edit(f"🚀 **Отправка записи...**\n\n{progress}%")
                    last_progress_update = time.time()
            
            total_time = time.time() - start_time
            original_time = messages[-1]['time_offset'] if messages else 0
            
            await event.edit(
                f"✅ **Запись успешно отправлена!**\n\n"
                f"📊 Отправлено сообщений: **{sent_count}/{len(messages)}**\n"
                f"⏱️ Оригинальное время: **{original_time:.1f}с**\n"
                f"⏱️ Фактическое время: **{total_time:.1f}с**\n"
                f"💬 Чат: `{chat_id}`"
            )
            
            logger.info(f"Запись {recording_id} отправлена в чат {chat_id} без ответа")
            
            # Сбрасываем ожидающую отправку
            self.pending_recording_send = None
            
        except Exception as e:
            await event.edit(f"❌ **Ошибка отправки:**\n\n{str(e)}")
            logger.error(f"Ошибка отправки записи: {e}")
            self.pending_recording_send = None
    
    async def handle_add_command(self, event):
        """Обработка команды добавления"""
        args = event.message.text.split()
        
        if len(args) < 2:
            # Показываем меню добавления
            await event.reply(
                "👤 **Добавление пользователя**\n\n"
                "Отправьте:\n"
                "• ID пользователя\n"
                "• @username\n"
                "• Или перешлите сообщение от пользователя\n\n"
                "Пример: `/add @username`",
                buttons=[
                    [Button.inline("📋 Способы добавления", b"add_methods")],
                    [Button.inline("↩️ Назад", b"main_menu")]
                ]
            )
        else:
            user_input = ' '.join(args[1:])
            await self.add_user(event, user_input)
    
    async def handle_remove_command(self, event):
        """Обработка команды удаления"""
        args = event.message.text.split()
        
        if len(args) < 2:
            # Показываем черный список для удаления
            await self.show_blacklist_for_removal(event)
        else:
            user_input = ' '.join(args[1:])
            await self.remove_user(event, user_input)
    
    async def add_user(self, event, user_input):
        """Добавление пользователя в черный список"""
        status_msg = await event.reply("🔄 Обработка...")
        
        # Получаем информацию о пользователе
        user_info = await self.get_user_info(user_input)
        
        if not user_info:
            await status_msg.edit("❌ Не удалось найти пользователя.")
            return
        
        # Проверяем, есть ли уже пользователь
        if self.is_user_in_blacklist(user_info['id'], user_info.get('username')):
            await status_msg.edit("⚠️ Пользователь уже в черном списке!")
            return
        
        # Добавляем пользователя
        self.config['blacklist'].append(user_info)
        self.save_config()
        
        user_display = self.format_user_display(user_info)
        
        await status_msg.edit(
            f"✅ **Пользователь добавлен!**\n\n"
            f"{user_display}\n"
            f"🆔 ID: `{user_info['id']}`"
        )
        
        logger.info(f"Добавлен пользователь: {user_display}")
    
    async def remove_user(self, event, user_input):
        """Удаление пользователя из черного списка"""
        status_msg = await event.reply("🔄 Обработка...")
        
        # Получаем информацию о пользователе
        user_info = await self.get_user_info(user_input)
        
        if not user_info:
            await status_msg.edit("❌ Не удалось найти пользователя.")
            return
        
        # Ищем пользователя в черном списке
        for i, user in enumerate(self.config['blacklist']):
            if user['id'] == user_info['id']:
                # Удаляем пользователя
                removed_user = self.config['blacklist'].pop(i)
                self.save_config()
                
                user_display = self.format_user_display(removed_user)
                await status_msg.edit(f"✅ **Пользователь удален:**\n{user_display}")
                return
        
        await status_msg.edit("❌ Пользователь не найден в черном списке.")
    
    async def get_user_info(self, user_input):
        """Получение информации о пользователе"""
        try:
            # Убираем пробелы
            user_input = user_input.strip()
            
            # Если это ID
            if user_input.isdigit():
                user_id = int(user_input)
                try:
                    # Пробуем получить через бота
                    user = await self.bot.get_entity(user_id)
                    return {
                        'id': user.id,
                        'username': getattr(user, 'username', None),
                        'first_name': getattr(user, 'first_name', ''),
                        'last_name': getattr(user, 'last_name', '')
                    }
                except:
                    return {'id': user_id, 'username': None}
            
            # Если это @username
            elif user_input.startswith('@'):
                username = user_input[1:]
                user = await self.bot.get_entity(username)
                return {
                    'id': user.id,
                    'username': username,
                    'first_name': getattr(user, 'first_name', ''),
                    'last_name': getattr(user, 'last_name', '')
                }
            
            # Если это ссылка
            elif 't.me/' in user_input:
                username = user_input.split('t.me/')[-1]
                user = await self.bot.get_entity(username)
                return {
                    'id': user.id,
                    'username': username,
                    'first_name': getattr(user, 'first_name', ''),
                    'last_name': getattr(user, 'last_name', '')
                }
            
        except Exception as e:
            logger.error(f"Ошибка получения информации о пользователе: {e}")
        
        return None
    
    def format_user_display(self, user_info):
        """Форматирование отображения пользователя"""
        parts = []
        if user_info.get('first_name'):
            parts.append(user_info['first_name'])
        if user_info.get('last_name'):
            parts.append(user_info['last_name'])
        
        display = ' '.join(parts) if parts else f"ID: {user_info['id']}"
        
        if user_info.get('username'):
            display += f" (@{user_info['username']})"
        
        return display
    
    async def show_blacklist(self, event):
        """Показать черный список"""
        if not self.config['blacklist']:
            await event.reply("📋 **Черный список пуст.**", parse_mode='md')
            return
        
        text = "📋 **Черный список пользователей:**\n\n"
        
        for i, user in enumerate(self.config['blacklist'], 1):
            user_display = self.format_user_display(user)
            text += f"{i}. {user_display}\n"
            text += f"   🆔 `{user['id']}`\n\n"
        
        buttons = [
            [Button.inline("➖ Удалить пользователя", b"remove_user_menu")],
            [Button.inline("↩️ Назад", b"main_menu")]
        ]
        
        await event.reply(text, buttons=buttons, parse_mode='md')
    
    async def show_blacklist_for_removal(self, event):
        """Показать черный список для удаления"""
        if not self.config['blacklist']:
            await event.reply("📋 **Черный список пуст.**", parse_mode='md')
            return
        
        text = "👤 **Выберите пользователя для удаления:**\n\n"
        buttons = []
        
        for user in self.config['blacklist']:
            user_display = self.format_user_display(user)[:30]
            buttons.append([Button.inline(f"❌ {user_display}", f"remove_{user['id']}")])
        
        buttons.append([Button.inline("↩️ Назад", b"main_menu")])
        
        await event.reply(text, buttons=buttons)
    
    async def show_stats(self, event):
        """Показать статистику"""
        stats_text = (
            f"📊 **Статистика бота**\n\n"
            f"📅 **Дата:** {datetime.now().strftime('%Y-%m-%d')}\n\n"
            f"**Общая статистика:**\n"
            f"• 🗑️ Всего удалено сообщений: **{self.deletion_stats['total_deleted']}**\n"
            f"• 🗑️ Удалено сегодня: **{self.deletion_stats['deleted_today']}**\n"
            f"• 👤 Пользователей в черном списке: **{len(self.config['blacklist'])}**\n"
            f"• 💬 Мониторится чатов: **{'Все' if self.config['enabled_for_all'] else len(self.config['enabled_chats'])}**\n"
            f"• 📝 Записей сохранено: **{len(self.recordings)}**\n"
            f"• ⚡ Статус мониторинга: **{'✅ Активен' if self.active_monitoring else '⏸️ Приостановлен'}**"
        )
        
        buttons = [
            [Button.inline("🔄 Обновить", b"refresh_stats")],
            [Button.inline("↩️ Назад", b"main_menu")]
        ]
        
        await event.reply(stats_text, buttons=buttons, parse_mode='md')
    
    async def show_help(self, event):
        """Показать помощь"""
        help_text = """
        🤖 **Помощь по боту**\n\n
        **📋 Основные команды:**
        `/menu` - Главное меню
        `/add @username` - Добавить пользователя
        `/remove @username` - Удалить пользователя
        `/list` - Показать черный список
        `/stats` - Статистика
        `/toggle` - Вкл/выкл мониторинг
        `/record` - Начать запись сообщений
        `/stop` - Остановить запись
        `/recordings` - Управление записями
        `/help` - Эта справка\n\n
        **⚡ Как это работает:**
        1. Добавьте пользователей в черный список
        2. Бот мониторит все чаты
        3. При реплае от пользователя из черного списка
        4. Все ваши сообщения в цепочке удаляются
        5. **Уведомления отключены**\n\n
        **🎬 Система записей:**
        1. Используйте /record или кнопку
        2. Пишите сообщения как обычно
        3. Бот записывает текст и время
        4. Используйте /stop для сохранения
        5. Воспроизводите записи в любом чате
        6. **Можно следить за сообщениями врага и отвечать на них**
        """
        
        buttons = [
            [Button.inline("📚 Примеры команд", b"examples")],
            [Button.inline("↩️ Назад", b"main_menu")]
        ]
        
        await event.reply(help_text, buttons=buttons, parse_mode='md')
    
    async def show_chat_menu(self, event):
        """Показать меню управления чатами"""
        mode = "🌐 Все чаты" if self.config['enabled_for_all'] else "💬 Только выбранные"
        
        text = (
            f"💬 **Управление чатами**\n\n"
            f"Текущий режим: **{mode}**\n"
            f"Активных чатов: **{len(self.config['enabled_chats'])}**\n\n"
            f"Выберите действие:"
        )
        
        buttons = [
            [Button.inline("🌐 Переключить режим", b"toggle_chat_mode")],
            [Button.inline("➕ Добавить чат", b"add_chat")],
            [Button.inline("➖ Удалить чат", b"remove_chat")],
            [Button.inline("📋 Список чатов", b"list_chats")],
            [Button.inline("↩️ Назад", b"main_menu")]
        ]
        
        await event.reply(text, buttons=buttons, parse_mode='md')
    
    async def handle_forwarded_message(self, event):
        """Обработка пересланных сообщений"""
        try:
            forwarded = event.message.forward
            if forwarded:
                sender_id = forwarded.sender_id
                
                # Получаем информацию о пользователе
                try:
                    user = await self.bot.get_entity(sender_id)
                    user_info = {
                        'id': user.id,
                        'username': getattr(user, 'username', None),
                        'first_name': getattr(user, 'first_name', ''),
                        'last_name': getattr(user, 'last_name', '')
                    }
                    
                    # Проверяем, есть ли уже пользователь
                    if self.is_user_in_blacklist(user_info['id'], user_info.get('username')):
                        await event.reply("⚠️ Пользователь уже в черном списке!")
                        return
                    
                    # Добавляем пользователя
                    self.config['blacklist'].append(user_info)
                    self.save_config()
                    
                    user_display = self.format_user_display(user_info)
                    
                    await event.reply(
                        f"✅ **Пользователь добавлен из пересланного сообщения!**\n\n"
                        f"{user_display}\n"
                        f"🆔 ID: `{user_info['id']}`"
                    )
                    
                except Exception as e:
                    await event.reply(f"❌ Ошибка: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Ошибка обработки пересланного сообщения: {e}")
    
    async def handle_callback(self, event):
        """Обработка нажатий на кнопки"""
        try:
            data = event.data.decode('utf-8')
            
            if data == 'main_menu':
                await self.send_main_menu(event)
            
            elif data == 'user_management':
                await event.edit(
                    "👤 **Управление пользователями**\n\n"
                    "Выберите действие:",
                    buttons=[
                        [Button.inline("➕ Добавить пользователя", b"add_user_menu")],
                        [Button.inline("➖ Удалить пользователя", b"remove_user_menu")],
                        [Button.inline("📋 Показать черный список", b"show_blacklist")],
                        [Button.inline("↩️ Назад", b"main_menu")]
                    ]
                )
            
            elif data == 'chat_management':
                await self.show_chat_menu(event)
            
            elif data == 'stats_menu':
                await self.show_stats(event)
            
            elif data == 'settings_menu':
                await self.show_settings(event)
            
            elif data == 'help_menu':
                await self.show_help(event)
            
            elif data == 'recordings_menu':
                await self.show_recordings_menu(event)
            
            elif data == 'start_recording':
                await self.start_recording(event)
            
            elif data == 'stop_recording':
                await self.stop_recording(event)
            
            elif data.startswith('play_recording_'):
                recording_id = data.replace('play_recording_', '')
                await self.play_recording(event, recording_id)
            
            elif data.startswith('send_here_'):
                # Формат: send_here_{recording_id}
                recording_id = data.replace('send_here_', '')
                chat_id = event.chat_id
                
                # Переходим к выбору пользователя для отслеживания
                await self.ask_target_user(event, recording_id, {'id': chat_id, 'title': 'Текущий чат'})
            
            elif data.startswith('no_tracking_'):
                # Формат: no_tracking_{recording_id}_{chat_id}
                parts = data.split('_')
                recording_id = f"{parts[2]}_{parts[3]}"
                chat_id = int(parts[4])
                await self.send_plain_recording(event, recording_id, chat_id)
            
            elif data.startswith('track_messages_'):
                # Формат: track_messages_{recording_id}_{chat_id}_{user_id}
                parts = data.split('_')
                recording_id = f"{parts[2]}_{parts[3]}"
                chat_id = int(parts[4])
                user_id = int(parts[5])
                await self.start_tracking_and_send(event, recording_id, chat_id, user_id)
            
            elif data.startswith('specify_message_'):
                # Формат: specify_message_{recording_id}_{chat_id}
                parts = data.split('_')
                recording_id = f"{parts[2]}_{parts[3]}"
                chat_id = int(parts[4])
                await self.specify_message_link(event, recording_id, chat_id)
            
            elif data.startswith('send_plain_'):
                # Формат: send_plain_{recording_id}_{chat_id}
                parts = data.split('_')
                recording_id = f"{parts[2]}_{parts[3]}"
                chat_id = int(parts[4])
                await self.send_plain_recording(event, recording_id, chat_id)
            
            elif data.startswith('execute_tracked_'):
                # Формат: execute_tracked_{recording_id}_{chat_id}_{user_id}_{message_id}
                parts = data.split('_')
                recording_id = f"{parts[2]}_{parts[3]}"
                chat_id = int(parts[4])
                user_id = int(parts[5])
                message_id = int(parts[6])
                await self.execute_tracked_send(event, recording_id, chat_id, user_id, message_id)
            
            elif data.startswith('execute_with_message_'):
                # Формат: execute_with_message_{recording_id}_{chat_id}_{message_id}
                parts = data.split('_')
                recording_id = f"{parts[2]}_{parts[3]}"
                chat_id = int(parts[4])
                message_id = int(parts[5])
                await self.execute_with_message_send(event, recording_id, chat_id, message_id)
            
            elif data.startswith('execute_plain_'):
                # Формат: execute_plain_{recording_id}_{chat_id}
                parts = data.split('_')
                recording_id = f"{parts[2]}_{parts[3]}"
                chat_id = int(parts[4])
                await self.execute_plain_send(event, recording_id, chat_id)
            
            elif data == 'add_user_menu':
                await event.edit(
                    "👤 **Добавление пользователя**\n\n"
                    "Отправьте команду:\n"
                    "`/add @username`\n\n"
                    "Или перешлите сообщение от пользователя.",
                    buttons=[[Button.inline("↩️ Назад", b"user_management")]]
                )
            
            elif data == 'remove_user_menu':
                await self.show_blacklist_for_removal(event)
            
            elif data == 'show_blacklist':
                await self.show_blacklist(event)
            
            elif data == 'refresh_stats':
                await self.show_stats(event)
            
            elif data == 'examples':
                await event.edit(
                    "📚 **Примеры команд:**\n\n"
                    "`/add @username`\n"
                    "`/add 123456789`\n"
                    "`/add t.me/username`\n"
                    "`/remove @username`\n"
                    "`/list`\n"
                    "`/stats`\n"
                    "`/toggle`\n"
                    "`/record`\n"
                    "`/stop`\n"
                    "`/recordings`",
                    buttons=[[Button.inline("↩️ Назад", b"help_menu")]]
                )
            
            elif data == 'toggle_chat_mode':
                self.config['enabled_for_all'] = not self.config['enabled_for_all']
                self.save_config()
                
                mode = "🌐 Все чаты" if self.config['enabled_for_all'] else "💬 Только выбранные"
                await event.answer(f"Режим изменен: {mode}", alert=False)
                await self.show_chat_menu(event)
            
            elif data == 'add_chat':
                await event.edit(
                    "➕ **Добавление чата**\n\n"
                    "Перешлите сообщение из чата или отправьте ID чата.",
                    buttons=[[Button.inline("↩️ Назад", b"chat_management")]]
                )
            
            elif data == 'remove_chat':
                await event.edit(
                    "➖ **Удаление чата**\n\n"
                    "Эта функция в разработке.",
                    buttons=[[Button.inline("↩️ Назад", b"chat_management")]]
                )
            
            elif data == 'list_chats':
                await event.edit(
                    "📋 **Список чатов**\n\n"
                    "Активных чатов: 0",
                    buttons=[[Button.inline("↩️ Назад", b"chat_management")]]
                )
            
            elif data.startswith('remove_'):
                user_id = int(data.split('_')[1])
                await self.remove_user_by_id(event, user_id)
            
            elif data == 'add_methods':
                await event.edit(
                    "📋 **Способы добавления:**\n\n"
                    "1. **Командой:** `/add @username`\n"
                    "2. **По ID:** `/add 123456789`\n"
                    "3. **По ссылке:** `/add t.me/username`\n"
                    "4. **Пересылкой:** Просто перешлите сообщение",
                    buttons=[[Button.inline("↩️ Назад", b"add_user_menu")]]
                )
            
            elif data == 'toggle_notifications':
                self.config['delete_notifications'] = not self.config['delete_notifications']
                self.save_config()
                
                status = "✅ Включены" if self.config['delete_notifications'] else "❌ Выключены"
                await event.answer(f"Уведомления: {status}", alert=False)
                await self.show_settings(event)
            
            await event.answer()
            
        except Exception as e:
            logger.error(f"Ошибка обработки callback: {e}")
            await event.answer("❌ Ошибка", alert=True)
    
    async def remove_user_by_id(self, event, user_id):
        """Удаление пользователя по ID"""
        for i, user in enumerate(self.config['blacklist']):
            if user['id'] == user_id:
                # Удаляем пользователя
                removed_user = self.config['blacklist'].pop(i)
                self.save_config()
                
                user_display = self.format_user_display(removed_user)
                await event.edit(f"✅ **Пользователь удален:**\n{user_display}")
                
                # Ждем и возвращаемся в меню
                await asyncio.sleep(2)
                await self.show_blacklist_for_removal(event)
                return
        
        await event.answer("❌ Пользователь не найден", alert=True)
    
    async def show_settings(self, event):
        """Показать настройки"""
        notifications = "✅ Включены" if self.config['delete_notifications'] else "❌ Выключены"
        
        text = (
            f"⚙️ **Настройки**\n\n"
            f"**Текущие настройки:**\n"
            f"• 🔔 Уведомления: {notifications}\n"
            f"• ⏱️ Задержка удаления: {self.config['delete_delay']} сек.\n\n"
            f"Выберите настройку для изменения:"
        )
        
        buttons = [
            [Button.inline("🔔 Уведомления", b"toggle_notifications")],
            [Button.inline("↩️ Назад", b"main_menu")]
        ]
        
        await event.edit(text, buttons=buttons)
    
    async def run(self):
        """Основной метод запуска"""
        try:
            # Инициализируем бота
            await self.initialize()
            
            # Запускаем сессию пользователя
            await self.start_user_session()
            
            # Отправляем приветственное сообщение
            await self.send_welcome_message()
            
            logger.info("✅ Бот полностью запущен и готов к работе!")
            
            # Запускаем оба клиента
            await asyncio.gather(
                self.bot.run_until_disconnected(),
                self.user_client.run_until_disconnected()
            )
            
        except KeyboardInterrupt:
            logger.info("Бот остановлен")
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            raise
    
    async def send_welcome_message(self):
        """Отправить приветственное сообщение"""
        welcome_text = (
            f"🤖 **Бот для автоматического удаления сообщений запущен!**\n\n"
            f"👤 **Владелец:** {OWNER_ID}\n"
            f"👥 **Пользователей в черном списке:** {len(self.config['blacklist'])}\n"
            f"💬 **Мониторинг чатов:** {'🌐 Все чаты' if self.config['enabled_for_all'] else f'💬 {len(self.config['enabled_chats'])} чатов'}\n"
            f"📝 **Сохранено записей:** {len(self.recordings)}\n"
            f"⚡ **Режим:** {'Активный мониторинг' if self.active_monitoring else 'Приостановлен'}\n\n"
            f"⚠️ **Уведомления об удалении:** {'Включены' if self.config['delete_notifications'] else 'Отключены'}\n\n"
            f"🎬 **Новая функция:** Отправка записей с отслеживанием сообщений врага!\n\n"
            f"📋 **Используйте /menu для управления**"
        )
        
        try:
            await self.bot.send_message(OWNER_ID, welcome_text, parse_mode='md')
        except:
            pass


# Запуск бота
async def main():
    print("=" * 60)
    print("🤖 БОТ ДЛЯ АВТОМАТИЧЕСКОГО УДАЛЕНИЯ СООБЩЕНИЙ")
    print("=" * 60)
    print(f"👤 Владелец: {OWNER_ID}")
    print(f"🔑 Токен бота: {BOT_TOKEN[:15]}...")
    print(f"💾 Файл конфигурации: {CONFIG_FILE}")
    print(f"🎬 Файл записей: {RECORDINGS_FILE}")
    print("=" * 60)
    print("⚡ ОСНОВНЫЕ ФУНКЦИИ:")
    print("• 🗑️ Удаление ВСЕХ сообщений в цепочке")
    print("• 🎬 Запись сообщений с оригинальной скоростью")
    print("• 📨 Воспроизведение записей в любом чате")
    print("• 👁️ Отслеживание сообщений врага")
    print("• 🔄 Автопоиск сообщений если удалено")
    print("• 🔕 Уведомления об удалении отключены")
    print("=" * 60)
    print("🚀 Запуск...")
    
    bot = BotInterface(BOT_TOKEN)
    await bot.run()


if __name__ == "__main__":
    # Создаем event loop
    loop = asyncio.get_event_loop()
    
    try:
        # Запускаем бота
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен")
    finally:
        loop.close()