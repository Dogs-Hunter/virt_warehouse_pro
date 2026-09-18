# Warehouse Lab

Экспериментальная реализация быстрого отказоустойчивого виртуального склада.
Проект полностью автономен и не изменяет референс в `C:\Game\HZ\case_6_virt_warehouse`.

Подтверждённые измерения находятся в [docs/RESULTS.md](docs/RESULTS.md).

## Гарантия текущего этапа

HTTP 200 возвращается только после записи операции в WAL и `sync_data`. Повторный
`operation_id` не изменяет баланс. После штатного или аварийного перезапуска состояние
восстанавливается воспроизведением WAL.

Это пока **одиночный узел**. Он защищает от падения процесса, но не от потери диска или
машины. Репликация и автоматическое лидерство будут следующим отдельным этапом.

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

Автоматизированный тест аварийного восстановления без очистки существующих данных:

```powershell
.\scripts\test-crash-recovery.ps1
```

Он создаёт уникальную контрольную операцию, посылает процессу `SIGKILL`, измеряет RTO,
проверяет восстановленный баланс и дедупликацию после перезапуска. Успешный итог —
`result: PASSED`.

## Baseline производительности

Команда отправляет 1 000 000 операций пакетами по 1000 через 32 параллельных соединения:

```powershell
docker compose --profile benchmark run --rm loadgen
```

Итог содержит `operations`, `applied`, `duplicates`, полное время и средний throughput.
Для нового `LOAD_RUN_ID` ожидается `applied=1000000` и `duplicates=0`.

## Quorum failover под нагрузкой

```powershell
.\scripts\test-quorum-failover.ps1
```

Тест автоматически находит лидера replicated stream, запускает 5 млн операций, аварийно
останавливает лидера, измеряет election, требует успешного завершения всей нагрузки,
проверяет точный баланс и возвращает остановленный узел в кластер.

Проверка сборки, перезапуска и полного quorum catch-up:

```powershell
.\scripts\test-quorum-catchup.ps1
```

Разрушительный тест потери только локального application volume:

```powershell
.\scripts\test-local-volume-loss.ps1
```

Сценарий проверяет Compose-label перед удалением и никогда не удаляет NATS volumes.

Проверка непрерывной прикладной реплики:

```powershell
.\scripts\test-live-replica.ps1
```

Проверка автоматического failover прикладного узла:

```powershell
.\scripts\test-application-failover.ps1
```

Полный тест производительности через HAProxy и quorum:

```powershell
.\scripts\test-performance.ps1
```

Проверка глобальных конфликтующих дублей:

```powershell
.\scripts\test-conflicting-duplicate.ps1
```

Конкурентный конфликт и повтор после failover:

```powershell
.\scripts\test-concurrent-conflict.ps1
```

Атомарный отказ всего пакета при конфликтующем `operation_id`:

```powershell
.\scripts\test-batch-conflict.ps1
```

Потеря quorum без зависания и без локальной «призрачной» записи:

```powershell
.\scripts\test-quorum-loss.ps1
```

Авария primary после quorum commit, но до локального применения:

```powershell
.\scripts\test-post-quorum-crash.ps1
```

Автоматическое восстановление после повреждения локального WAL:

```powershell
.\scripts\test-wal-corruption-recovery.ps1
```

Quorum-backed writer lease и защита от двух активных writers:

```powershell
.\scripts\test-writer-fencing.ps1
```

Изоляция повреждённого replicated event в DLQ:

```powershell
.\scripts\test-poison-dlq.ps1
```

## Формат WAL

Каждая запись имеет заголовок `payload_length:u32 LE`, `crc32:u32 LE`, затем JSON payload.
Неполная последняя запись после внезапного отключения удаляется при восстановлении. Ошибка
checksum внутри журнала останавливает запуск, чтобы повреждение не превратилось в молчаливую
потерю данных.
