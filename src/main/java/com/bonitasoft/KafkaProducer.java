package com.bonitasoft;

import java.util.logging.Logger;

import org.bonitasoft.engine.connector.AbstractConnector;
import org.bonitasoft.engine.connector.ConnectorException;
import org.bonitasoft.engine.connector.ConnectorValidationException;

public class KafkaProducer extends AbstractConnector {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducer.class.getName());

    static final String KAFKA_SERVERS = "kafkaServers";
    static final String KAFKA_USER = "kafkaUser";
    static final String KAFKA_PASSWORD = "kafkaPassword";
    static final String KAFKA_TOPIC = "kafkaTopic";
    static final String KAFKA_ID = "kafkaId";
    static final String KAFKA_MESSAGE = "kafkaMessage";
    static final String KAFKA_RESPONSE = "kafkaResponse";

    private EventProducer event = new EventProducer();

    /**
     * Perform validation on the inputs defined on the connector definition
     * (src/main/resources/connector-kafka-producer.def)
     * You should:
     * - validate that mandatory inputs are presents
     * - validate that the content of the inputs is coherent with your use case
     * (e.g: validate that a date is / isn't in the past ...)
     */
    @Override
    public void validateInputParameters() throws ConnectorValidationException {
        LOGGER.info("Validating parameters...");
        checkMandatoryStringInput(KAFKA_SERVERS);
        checkMandatoryStringInput(KAFKA_USER);
        checkMandatoryStringInput(KAFKA_PASSWORD);
        checkMandatoryStringInput(KAFKA_TOPIC);
        checkMandatoryStringInput(KAFKA_ID);
        checkMandatoryStringInput(KAFKA_MESSAGE);
    }

    protected void checkMandatoryStringInput(String inputName) throws ConnectorValidationException {
        try {
            String value = (String) getInputParameter(inputName);
            if (value == null || value.isEmpty()) {
                throw new ConnectorValidationException(this,
                        String.format("Mandatory parameter '%s' is missing.", inputName));
            }
        } catch (ClassCastException e) {
            throw new ConnectorValidationException(this, String.format("'%s' parameter must be a String", inputName));
        }
    }

    /**
     * Core method:
     * - Execute all the business logic of your connector using the inputs (connect
     * to an external service, compute some values ...).
     * - Set the output of the connector execution. If outputs are not set,
     * connector fails.
     */
    @Override
    protected void executeBusinessLogic() throws ConnectorException {
        LOGGER.info(String.format("KAFKA_SERVER: %s", getInputParameter(KAFKA_SERVERS)));
        LOGGER.info(String.format("KAFKA_USER: %s", getInputParameter(KAFKA_USER)));
        LOGGER.info(String.format("KAFKA_PASSWORD: %s", getInputParameter(KAFKA_PASSWORD)));
        LOGGER.info(String.format("KAFKA_TOPIC: %s", getInputParameter(KAFKA_TOPIC)));
        LOGGER.info(String.format("KAFKA_ID: %s", getInputParameter(KAFKA_ID)));
        LOGGER.info(String.format("KAFKA_MESSAGE: %s", getInputParameter(KAFKA_MESSAGE)));
        LOGGER.info("Sending message...");
        setOutputParameter(KAFKA_RESPONSE, event.send((String) getInputParameter(KAFKA_TOPIC),
                (Long) getInputParameter(KAFKA_ID), (String) getInputParameter(KAFKA_MESSAGE)));
    }

    /**
     * [Optional] Open a connection to remote server
     */
    @Override
    public void connect() throws ConnectorException {
        LOGGER.info("Creating producer...");
        event.createProducer((String) getInputParameter(KAFKA_SERVERS), (String) getInputParameter(KAFKA_USER),
                (String) getInputParameter(KAFKA_PASSWORD));
    }

    /**
     * [Optional] Close connection to remote server
     */
    @Override
    public void disconnect() throws ConnectorException {
    }
}
