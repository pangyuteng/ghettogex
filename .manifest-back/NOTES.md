    redis:
        image: redis:latest
        hostname: redis
        ports:
            - 6379:6379
        restart: always
    rabbitmq:
        image: rabbitmq:management
        hostname: rabbitmq
        ports:
            - 5672:5672
            - 15672:15672
        restart: always