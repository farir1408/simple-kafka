# simple-kafka

Методический проект для иллюстрации работы с kafka-брокером в golang.

## Запуск kafka + zookeeper:
`docker-compose -f docker-compose.yml up -d`

### Запуск producer:
`go run cmd/main.go sender --brokers="localhost:9092" --topic=test`

### Запуск consumer:
`go run cmd/main.go receiver --brokers="localhost:9092" -g=test_group -t=test`
