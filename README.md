# Warehouse Lab

Экспериментальная реализация быстрого отказоустойчивого виртуального склада.
Проект полностью автономен и не изменяет референс в `C:\Game\HZ\case_6_virt_warehouse`.

Подтверждённые измерения находятся в [docs/RESULTS.md](docs/RESULTS.md).

## Гарантия доставки

Система обеспечивает **at-least-once delivery**:

1. Операция сначала фиксируется в трёхузловом JetStream quorum.
2. Затем она записывается в локальный WAL с `sync_data` и применяется к состоянию.
3. Только после этого клиент получает успешный HTTP-ответ.
4. Durable checkpoint продвигается только после успешного локального применения.
5. При аварии между quorum commit и ответом операция повторно читается из JetStream.

Повторная доставка разрешена и ожидаема. Идемпотентность по `operation_id` гарантирует,
что повтор не изменит баланс второй раз. Одинаковые немедленные публикации дополнительно
схлопываются JetStream по детерминированному `Nats-Msg-Id`.

Гарантия проверяется отдельным аварийным тестом:

```powershell
.\scripts\test-at-least-once-delivery.ps1 -SkipBuild
```

Тест убивает процесс после quorum commit, но до WAL и HTTP-ответа, ждёт автоматическую
доставку после перезапуска и проверяет безопасный повтор запроса.

## Запуск

```powershell
Set-Location C:\Game\Test
docker compose up --build -d
docker compose logs -f warehouse
```

Проверка здоровья:

```powershell
Invoke-RestMethod http://localhost:8080/live
```

Одна операция:

```powershell
$body = @{
  operation_id = 'demo-1'
  owner_id = 'owner-1'
  sku = 'sku-1'
  delta = 10
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/operations `
  -ContentType 'application/json' -Body $body
```

Повторите последний запрос: ожидается `status = duplicate`, а баланс остаётся `10`.

Проверка восстановления:

```powershell
docker compose kill warehouse
docker compose up -d warehouse
Start-Sleep -Seconds 2
Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/operations `
  -ContentType 'application/json' -Body $body
```

После перезапуска ожидаются `status = duplicate` и `balance = 10`.

## Воспроизводимая сборка

Версия Rust, базовый runtime и сторонние контейнеры закреплены точными digest. Rust-зависимости
закреплены в `Cargo.lock`, а сборка всегда использует `--locked` и автоматически запускает
unit-тесты. Полная проверка выполняется отдельным файлом:

```powershell
.\scripts\test-reproducible-build.ps1 -NoCache
```

Без `-NoCache` проверка использует безопасный кеш компилятора и выполняется быстрее.

## Baseline производительности

Команда отправляет 1 000 000 операций пакетами по 1000 через 32 параллельных соединения:

```powershell
docker compose --profile benchmark run --rm loadgen
```

Итог содержит `operations`, `applied`, `duplicates`, полное время и средний throughput.
Для нового `LOAD_RUN_ID` ожидается `applied=1000000` и `duplicates=0`.

## Основные интеграционные тесты

```powershell
.\scripts\test-quorum-failover.ps1
```

```powershell
.\scripts\test-local-volume-loss.ps1
```

```powershell
.\scripts\test-application-failover.ps1
```

```powershell
.\scripts\test-at-least-once-delivery.ps1 -SkipBuild
```

```powershell
.\scripts\test-prepared-read-write-performance.ps1 -SkipBuild
```

Подготовка набора данных для теста производительности:

```powershell
.\scripts\prepare-performance-dataset.ps1 -SkipBuild
```

## Зафиксированные ограничения тестового стенда

- Значения API key, NATS credentials и пароля Grafana пока намеренно остаются тестовыми и
  встроенными в Compose, чтобы стенд можно было передать и запустить без внешнего secret store.
- Все реплики пока запускаются на одном Docker-хосте. Это проверяет программное переключение,
  но не защищает от потери всей машины.
- Усиление аутентификации, управление секретами и multi-host deployment отложены: безопасность
  на текущем этапе не является приоритетом.
- Эти ограничения допустимы только для закрытого тестового окружения и должны быть сняты перед
  подключением реальных данных или публикацией сервиса в интернет.

## Формат WAL

Каждая запись имеет заголовок `payload_length:u32 LE`, `crc32:u32 LE`, затем бинарный payload.
Неполная последняя запись после внезапного отключения удаляется при восстановлении. Ошибка
checksum внутри журнала останавливает запуск, чтобы повреждение не превратилось в молчаливую
потерю данных.
