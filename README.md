docker compose setup 

  jobmanager:
    image: flink:latest
    expose:
      - "6123"
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
      - KAFKA_SOURCE_TOPIC=requested-data
      - KAFKA_PROCESSED_TOPIC=missing-data
      - KAFKA_DLQ_TOPIC=log-data
      - KAFKA_BOOTSTRAP_SERVERS=pkc-p11xm.us-east-1.aws.confluent.cloud:9092
      - KAFKA_API_KEY=7KL2EKWVOGWNEBX3
      - KAFKA_API_SECRET=lzcRa1PgnzT9naGh6TJomiW0yti/d6HI91DZbhl3GmG03g8QbzBONuBcp134aOXA
    volumes:
      - ./jars:/opt/flink/jars
   

  taskmanager:
    image: flink:latest
    expose:
      - "6121"
      - "6122"
    depends_on:
      - jobmanager
    command: taskmanager
    links:
      - "jobmanager:jobmanager"
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
      - KAFKA_SOURCE_TOPIC=requested-data
      - KAFKA_PROCESSED_TOPIC=missing-data
      - KAFKA_DLQ_TOPIC=log-data
      - KAFKA_BOOTSTRAP_SERVERS=pkc-p11xm.us-east-1.aws.confluent.cloud:9092
      - KAFKA_API_KEY=7KL2EKWVOGWNEBX3
      - KAFKA_API_SECRET=lzcRa1PgnzT9naGh6TJomiW0yti/d6HI91DZbhl3GmG03g8QbzBONuBcp134aOXA
