# Итоговое ДЗ по модулю 3

## Готово:
1) Развернуты MongoDB и PostgreSQL в Docker Compose
2) В MongoDB есть 5 коллекций с данными:
- user_sessions, event_logs, support_tickets, user_recommendations, moderation_queue
3) Настроена репликация из MongoDB в PostgreSQL
4) Созданы 2 витрины и пайплайн их расчёта:
- mart_dau (DAU по дням)
- mart_support_sla (создано/закрыто тикетов, среднее время обработки)

## Запуск
1) Поднять окружение:
- docker compose up -d
2) Заполнить Mongo данными:
- выполнить mongo_seed.js (mongosh)
3) Запустить DAG-и в Airflow UI:
- ponomareva_olga_final_etl_load — загрузка Mongo -> Postgres
- ponomareva_olga_final_marts — построение витрин

## Репликация (MongoDB -> PostgreSQL)
Источник: MongoDB (db `etl_final`, коллекции `user_sessions`, `event_logs`, `support_tickets`, `user_recommendations`, `moderation_queue`).

ETL выполняется DAG `ponomareva_olga_final_etl_load`:
- Extract: чтение документов из MongoDB через `pymongo`
- Transform: нормализация вложенных структур:
  - `pages_visited`, `actions`, `messages`, `recommended_products`, `flags` разворачиваются в отдельные таблицы
  - объект `device` раскладывается в поля `device_type/device_os/device_browser`
  - поле `details` из `event_logs` раскладывается по колонкам (`user_id/page/product_id/amount/element`)
- Load: запись в PostgreSQL в staging-таблицы. Режим загрузки — полный перезапуск (TRUNCATE + INSERT), поэтому дублей нет.
## Данные в PostgreSQL
Staging-таблицы (основные):
- stg_user_sessions, stg_event_logs, stg_support_tickets, stg_user_recommendations, stg_moderation_queue

Таблицы для массивов:
- stg_session_pages, stg_session_actions, stg_ticket_messages, stg_recommended_products, stg_review_flags

Витрины:
- mart_dau
- mart_support_sla
