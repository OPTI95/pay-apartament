# Деплой на Timeweb VPS

## 1. Подключиться к серверу по SSH

```bash
ssh root@ВАШ_IP_АДРЕС
```

## 2. Установить Python и зависимости

```bash
apt update && apt install -y python3 python3-pip python3-venv
```

## 3. Загрузить файлы на сервер

С вашего компьютера (в папке проекта):
```bash
scp -r "c:/telegram bots/" root@ВАШ_IP:/root/apartment_bot/
```

Или через встроенный менеджер файлов Timeweb панели.

## 4. На сервере — создать виртуальное окружение

```bash
cd /root/apartment_bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 5. Запустить бота как systemd-сервис (работает после перезагрузки)

Создать файл сервиса:
```bash
nano /etc/systemd/system/apartment_bot.service
```

Вставить содержимое:
```ini
[Unit]
Description=Apartment Telegram Bot
After=network.target

[Service]
Type=simple
WorkingDirectory=/root/apartment_bot
ExecStart=/root/apartment_bot/venv/bin/python bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Запустить сервис:
```bash
systemctl daemon-reload
systemctl enable apartment_bot
systemctl start apartment_bot
```

Проверить статус:
```bash
systemctl status apartment_bot
```

Посмотреть логи:
```bash
journalctl -u apartment_bot -f
```

## 6. Обновление данных о ЖК

Редактируйте файл `data/apartments.json` и перезапускайте бота:
```bash
systemctl restart apartment_bot
```

---

## Как добавить новый ЖК в data/apartments.json

Скопируйте один блок и замените данные:

```json
{
  "id": "уникальный_id",
  "name": "НАЗВАНИЕ ЖК",
  "aliases": ["АЛЬТЕРНАТИВНОЕ НАЗВАНИЕ", "ОПЕЧАТКА"],
  "address": "Адрес",
  "price_per_sqm": 9000000,
  "description": "Описание (необязательно)",
  "main_photo": "ПРЯМАЯ ССЫЛКА НА ФОТО",
  "photos_url": "https://drive.google.com/...",
  "layouts_url": "https://drive.google.com/...",
  "chess_url": "https://drive.google.com/...",
  "installment_text": "Текст условий рассрочки"
}
```

## Как получить прямую ссылку на фото из Google Drive

1. Загрузите фото в Google Drive
2. ПКМ -> "Открыть доступ" -> "Все у кого есть ссылка"
3. Скопируйте ID файла из ссылки: `https://drive.google.com/file/d/ВОТ_ЭТО_ID/view`
4. Используйте: `https://drive.google.com/uc?export=view&id=ВОТ_ЭТО_ID`


venv\Scripts\activate
python bot.py