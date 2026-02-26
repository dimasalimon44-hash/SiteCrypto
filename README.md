# Arbitrage Dashboard (FastAPI)

Новый проект в отдельной папке для быстрого и красивого веб-дашборда арбитража.

## Что уже оптимизировано
- Асинхронная загрузка MEXC + Bybit параллельно.
- Для BingX используется bulk-режим (1 запрос на endpoint без symbol), а затем fallback на точечные запросы только если нужно.
- Кеш обновляется в фоне, UI читает только `/api/data`.
- Клиентская фильтрация и сортировка выполняются мгновенно в браузере.
- Настройки (`min_vol`, `min_spread`, `enabled`, `refresh_sec`) сохраняются в `arb_dashboard_config.json`.

## Запуск

### Быстрый запуск в Windows (без консоли)
- Просто дважды кликните `run_dashboard.bat` в папке проекта.
- Батник сам:
  - создаст `.venv` (если его нет),
  - установит зависимости (если не установлены),
  - запустит дашборд на `http://127.0.0.1:8000`.

### Linux / macOS
```bash
cd arbitrage_dashboard
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

### Windows (cmd)
```bat
cd arbitrage_dashboard
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python app.py
```

### Windows (PowerShell)
```powershell
cd arbitrage_dashboard
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python app.py
```

Открыть: http://127.0.0.1:8000

## Важно про GitHub и бинарные файлы
- В репозитории намеренно игнорируются бинарные ассеты из `assets/logos` и `assets/sounds` (кроме `README.txt`).
- Это сделано, чтобы избежать ошибки `Binary files are not supported` при обновлении репозитория.
- Если хотите добавить свои логотипы/звуки локально — просто копируйте их в эти папки, в Git они не попадут.

## Доступ к локальному FastAPI в интернете через ngrok

### 1) Установка ngrok в проект (локально)

```bash
cd arbitrage_dashboard
./scripts/install_ngrok.sh
```

Скрипт установит бинарь локально в папку проекта: `.tools/ngrok/ngrok` (в репозиторий не коммитится).

### 2) Запуск FastAPI + ngrok вручную

Сначала запуск API:

```bash
uvicorn app:app --reload --port 8000
```

Потом в другом терминале запуск ngrok:

```bash
.tools/ngrok/ngrok http 8000
```

Или, если `ngrok` уже установлен глобально:

```bash
ngrok http 8000
```

### 3) Быстрый запуск одним скриптом

```bash
./scripts/run_with_ngrok.sh
```

### 4) Пример публичной ссылки от ngrok

После запуска ngrok вы увидите что-то вроде:

```text
Forwarding  https://abc12345.ngrok-free.app -> http://localhost:8000
```

Эту `https://...ngrok-free.app` ссылку можно открыть из интернета.

## Новая структура UI (поддерживаемый фронтенд)

Теперь UI разделён по файлам, чтобы не ломался при будущих правках:

```text
arbitrage_dashboard/
  templates/
    base.html
    index.html
    components/
      header.html
      sidebar.html
  static/
    css/
      main.css
    js/
      api.js
      ui.js
      app.js
```

- `templates/*` — HTML шаблоны и layout.
- `static/css/main.css` — все стили и состояния кнопок/таблиц/cards.
- `static/js/api.js` — только работа с API.
- `static/js/ui.js` — форматирование и UI-хелперы.
- `static/js/app.js` — клиентская логика (polling, фильтры, сортировка, обновление таблицы).

## Как запустить локально

```bash
cd arbitrage_dashboard
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

Откройте: `http://127.0.0.1:8000`

## Local Development

Рекомендуемый запуск для локальной разработки (Mac/Linux):

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload --host 127.0.0.1 --port 8000
```


### Быстрый запуск на macOS (double-click)

Добавлен файл `run_local.command` в корне проекта.

1. В Finder откройте папку `arbitrage_dashboard`.
2. Дважды кликните `run_local.command`.
3. Скрипт автоматически:
   - выберет `python3.11` (или fallback на `python3`),
   - создаст/активирует `venv`,
   - обновит `pip`,
   - установит зависимости,
   - запустит `uvicorn app:app --reload --host 127.0.0.1 --port 8000`.

После запуска откройте: `http://127.0.0.1:8000`
Остановка: `Ctrl+C` в окне терминала.

### Зависимости

- `requirements.txt` — только runtime-зависимости (prod).
- `requirements-dev.txt` — dev-инструменты (`ruff`, `mypy`, `pytest`, `black` и др.) поверх runtime.

### Переключение окружений через ENV

- `ENV=development` — dev-режим (`reload=True`, access logs включены).
- `ENV=production` — prod-режим (`reload=False`, access logs отключены).

Пример:

```bash
ENV=development python app.py
```

или

```bash
ENV=production python app.py
```

### systemd / сервер

Для сервера можно оставить `ENV=production` и задать `HOST`/`PORT` через environment variables.
Пути в проекте относительные (от папки `arbitrage_dashboard`), абсолютные пути вида `/root/...` не используются.

## Как менять дизайн сайта

1. **Layout и блоки страницы**
   - меняйте `templates/base.html` и `templates/index.html`.
2. **Переиспользуемые элементы (header/sidebar)**
   - меняйте файлы в `templates/components/`.
3. **Стили**
   - меняйте `static/css/main.css`.
4. **Логика таблицы/фильтров/автообновления**
   - меняйте `static/js/app.js`.
5. **API слой**
   - меняйте `static/js/api.js`.

## Поведение UI

- Автообновление данных каждые 5 секунд без перезагрузки страницы.
- Обновляются только строки таблицы (через patch), UI не перерисовывается полностью.
- Есть индикатор статуса обновления, время последнего обновления и banner ошибки API.

## Ошибка Codex про обновление PR

Если видите сообщение:

`Codex в настоящее время не поддерживает обновление PR, обновляемых за пределами Codex. Создайте новый PR.`

Это ожидаемое ограничение платформы. Решение:

1. Зафиксируйте изменения новым коммитом.
2. Создайте **новый PR** (не обновляйте старый PR, который изменялся вне Codex).
3. Закройте старый PR, если он больше не нужен.
