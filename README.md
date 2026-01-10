# Legendalf bot (aiogram)

## Структура

- `bot_aiogram.py` — основной бот: доступы, админ‑команды, триггеры, сохранение медиа, polling.
- `schedule_aiogram.py` — расписания и авторассылки (включая праздники по расписанию).
- `features/holidays.py` — `/holydays`, парсер calend.ru и отправка праздничных данных.
- `features/films.py` — парсер Kinopoisk + `/films_day` и `/films_month`.
- `users.db` — база SQLite с пользователями/расписаниями (мигрирует из `users.json` при первом старте).
- `quotes.txt` — база цитат.
- `image/` — папка с медиа (jpg/png/gif/mp4).

## Запуск

Задай токен и запусти:

```bash
set TELEGRAM_BOT_TOKEN=your_token
python bot_aiogram.py
```

## Заметки

- Доступ: пользователи запрашивают доступ через `/mellon`.
- Триггеры и медиа обрабатываются в `bot_aiogram.py`.
- Команда праздников вынесена из расписаний осознанно.
- В `requirements.txt` оставлены только зависимости бота; системные пакеты Debian (например `cloud-init`, `python-apt`, `apt-listchanges`) не ставятся через `pip`.
