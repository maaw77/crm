# CRM
Набор инструментов, обеспечивающих сканирование конечных точек веб-приложения и сохранение данных (ресурсов) в базу данных.
## Особенности
Проект реализован с применением принципов асинхронного программирования на Python.
## Установка и запуск 
1. Установить docker и docker-compose.
Инструкция по установке доступна в официальной документации
2. В папке с проектом выполнить команду
```
docker-compose up
```
## Применение
### Инициализации базы данных 
(```python -m dbase.init_db -h```)
```
python -m dbase.init_db init
```
### Очистка базы данных
```
python -m dbase.init_db delete
```
### Cохранение данных(ресурсов) в базу данных
```
python -m  dbase.loaddata
```
### Запуск бота
```
python crmbot/crmbot.py
```
Бот сканирует конечные точки веб-приложения и сохраняет новые данные (ресурсы) в базу данных.
Автоматически запускает сканирование в 23:00 каждый день.
###  Вывод данных из базы данных в стандартный поток вывода
Пример: 
 ```python -m dbase.dumpdata gsm 2024-05-01 2024-06-01h```

Для более подробной информации введите команду: ```python -m dbase.dumpdata -h```

