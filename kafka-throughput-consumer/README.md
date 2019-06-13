mvn clean package

java -jar target/kafka-throughput-consumer-1.0-SNAPSHOT-jar-with-dependencies.jar 10 consumer-group my-jemeter-topics localhost:9092 earliest true


 int numConsumers = Integer.parseInt(args[0]);
        String groupId = args[1];
        List<String> topics = Arrays.asList(args[2].split(","));
        final String bootstrapServers = args[3];