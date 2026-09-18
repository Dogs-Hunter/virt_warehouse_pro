# Подтверждённые результаты

Все значения получены на локальном Docker Desktop стенде и не являются гарантией для
другого оборудования.

| Сценарий | Результат |
|---|---:|
| Single-node, 1 000 000 операций | 608 598,1 оп/с |
| Quorum 2/3 с потерей stream leader, 5 000 000 операций | 416 692,1 оп/с |
| Потери при failover | 0 |
| Дубли при failover | 0 |
| Election наблюдаемого stream leader | 7 836 мс |
| WAL recovery, 1 000 000 операций | 650 мс |
| Обычный restart с checkpoint | 2 865 мс |
| Полный rebuild из quorum log до оптимизации | 56 160 мс |
| Полный rebuild из quorum log после chunked WAL | 10 178 мс |
| Полный HA-path, 1 000 000 операций | 439 409,0 оп/с |
| Полный HA-path после конфликтных/quorum-гарантий | 208 580,9 оп/с |
| HA-path batch p50, batch=1000 | 68,070 мс |
| HA-path batch p95, batch=1000 | 102,059 мс |
| HA-path batch p99, batch=1000 | 126,761 мс |
| Live application replica lag, контрольная операция | 35 мс |
| Application failover через HAProxy | 2 091 мс |
| Конкурентный одинаковый ID с разными payload | PASSED: HTTP 200 + 409 |
| Конфликт после application failover | PASSED: HTTP 409 |
| Атомарный отказ batch при внутреннем/сохранённом конфликте | PASSED: HTTP 409, частичных записей 0 |
| Потеря quorum 2/3 | PASSED: HTTP 503 за 5 034 мс |
| Локальные изменения при отклонённой quorum-записи | 0 |
| Повтор после восстановления quorum | PASSED: баланс 53 на обоих приложениях |
| Crash после quorum commit, до локального применения | PASSED |
| Восстановление backup/primary после post-quorum crash | 18 556 мс, баланс 61/61 |
| Повтор неопределённого post-quorum запроса | duplicate, баланс 61 |
| Повреждение локального WAL и rebuild из quorum | PASSED: 847 мс |
| Сохранение повреждённого WAL | 1 quarantined file |
| Дедупликация после WAL rebuild | duplicate, баланс 67 |
| Quorum-backed writer fencing | PASSED |
| Writer lease failover после in-flight guard | 4 577 мс |
| Запись standby и вернувшегося старого primary | HTTP 503 / HTTP 503 |
| Изменение баланса отклонёнными writers | 0 из суммарной delta 1 996 |
| Poison replicated event → DLQ | PASSED: 1 DLQ message |
| Продолжение после poison | 879 мс, итоговый баланс 172 |

Полный rebuild проверен удалением только `warehouse-data` volume. Три NATS volume при этом
сохранялись. После восстановления подтверждены контрольный баланс и дедупликация.
