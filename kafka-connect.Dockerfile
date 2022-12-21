FROM confluentinc/cp-kafka-connect:7.3.1

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:14.0.3 \
    && confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.6.0