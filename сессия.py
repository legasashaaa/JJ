# create_session.py
from telethon import TelegramClient
from telethon.sessions import StringSession
import asyncio
import json
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Данные из my.telegram.org
API_ID =  29238968 # Замените на ваш
API_HASH = '693fa412a819664c59ec5f1989755842'  # Замените на ваш

# Данные Render API (из переменных окружения или .env)
RENDER_API_KEY = os.getenv('RENDER_API_KEY')
SERVICE_ID = os.getenv('RENDER_SERVICE_ID')  # ID вашего сервиса на Render
RENDER_OWNER_ID = os.getenv('RENDER_OWNER_ID')  # Ваш user ID на Render

async def create_telegram_session():
    """Создаёт сессию Telegram и возвращает строку сессии"""
    print("Создание сессии Telegram...")
    
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.start()
    
    # Получаем строку сессии
    session_string = client.session.save()
    
    # Проверяем подключение
    me = await client.get_me()
    print(f"✅ Подключено как: @{me.username} ({me.first_name})")
    
    await client.disconnect()
    
    return session_string

def send_to_render(session_string):
    """Отправляет сессию в переменные окружения Render"""
    
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {RENDER_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Данные для обновления переменных окружения
    data = {
        "envVars": [
            {
                "key": "SESSION_STRING",
                "value": session_string,
                "sync": False  # Не синхронизировать между сервисами
            }
        ]
    }
    
    # URL API Render для обновления переменных окружения
    url = f"https://api.render.com/v1/services/{SERVICE_ID}/env-vars"
    
    try:
        response = requests.patch(url, headers=headers, json=data)
        
        if response.status_code == 200:
            print("✅ Сессия успешно отправлена в Render!")
            
            # Запускаем деплой
            deploy_url = f"https://api.render.com/v1/services/{SERVICE_ID}/deploys"
            deploy_response = requests.post(deploy_url, headers=headers)
            
            if deploy_response.status_code == 201:
                print("✅ Запущен новый деплой на Render!")
            else:
                print("⚠️ Не удалось запустить деплой")
                
        else:
            print(f"❌ Ошибка: {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"❌ Ошибка при отправке: {e}")

def save_session_file(session_string):
    """Сохраняет сессию в файл для резервной копии"""
    with open('session_backup.txt', 'w', encoding='utf-8') as f:
        f.write(session_string)
    print("📁 Сессия сохранена в файл session_backup.txt")

async def main():
    print("=" * 50)
    print("Генератор сессии Telegram для Render")
    print("=" * 50)
    
    # 1. Создаём сессию
    session_string = await create_telegram_session()
    
    print("\n" + "=" * 50)
    print("ВАША СЕССИЯ:")
    print("=" * 50)
    print(session_string)
    print("=" * 50)
    
    # 2. Сохраняем в файл
    save_session_file(session_string)
    
    # 3. Отправляем в Render (опционально)
    send_to_render_choice = input("\nОтправить сессию в Render? (y/n): ").lower()
    
    if send_to_render_choice == 'y' and RENDER_API_KEY:
        print("\nОтправка сессии в Render...")
        send_to_render(session_string)
    elif not RENDER_API_KEY:
        print("\n⚠️ RENDER_API_KEY не найден. Укажите его в .env файле.")
        print("Инструкция по получению API ключа:")
        print("1. Зайдите на https://dashboard.render.com")
        print("2. Нажмите на Account Settings")
        print("3. В разделе API Keys создайте новый ключ")
    else:
        print("\n✅ Сессия создана. Скопируйте строку выше в переменные окружения Render.")

if __name__ == '__main__':
    asyncio.run(main())
