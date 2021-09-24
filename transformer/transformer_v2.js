//   .env file should contain:
// AUTH_BROKER: mqtt://{ip address of the auth-checking broker}
// AUTH_BROKER_TOPIC: the topic authenticated data flows into
// NIFI_BROKER: mqtt{hostname of the broker that pipes data into nifi}
// NIFI_BROKER_TOPIC: the topic that nifi is reading from
(require("dotenv")).config();
const config = process.env;
const mqtt = require("mqtt");

const authenticatedBroker = mqtt.connect(config.AUTH_BROKER, {
    host: config.AUTH_BROKER,
    port: 2883
});
const nifiBroker = mqtt.connect(config.NIFI_BROKER, {
    host: config.NIFI_BROKER,
    port: 1883
});

// Runs when the ingest broker is connected
function onConnectAuthBroker(response) {
    console.info("Connected to the auth broker");
    console.debug(response);
    authenticatedBroker.subscribe(config.AUTH_BROKER_TOPIC, () => console.info("Subscribed to auth broker topic: ", config.AUTH_BROKER_TOPIC));
    authenticatedBroker.on("message", onMessageAuthBroker);
}

// Runs when the ingest broker encounters an error
function onErrorAuthBroker(err) {
    console.error("ERROR IN INGEST BROKER");
    console.error(err);
}

function IR_Convert(app_id, ir_message) {
    // Construct the first part of the static section here.
    // This results in us only having to tack on a couple
    // of metric-specific key/values during blob creation
    const static =
        Object.keys(ir_message.metadata)
        .map(field => {
            return {
                name: field,
                value: ir_message.metadata[field]
            };
        });

    // We only need to create this array once since it is
    // the same for each data blob in the output message
    const dynamic = [
        {
            name: "timeStamp",
            value: ir_message.time
        }
    ];

    // Transform the incoming json into suitable data blobs
    const blobs =
        Object.keys(ir_message.payload_fields)
        .map(metric => {
            const payloadField = ir_message.payload_fields[metric];
            return {
                metrics: [{
                    name: `m.${app_id}.${metric}`,
                    value: payloadField.value
                }],
                static: static.concat([
                    {
                        name: "displayName",
                        value: payloadField.displayName
                    },
                    {
                        name: "unit",
                        value: payloadField.unit
                    }
                ]),
                dynamic
            }
        });

    return blobs;
}

// Runs when the ingest broker receives a message
// on the data ingestion topic: data/ingest
function onMessageAuthBroker(topic, message) {
    console.info("Received message");
    message = message.toString();
    console.debug(message);
    try {
        const { app_id, data } = JSON.parse(message);

        // TODO: select different transformers based on the "topic" variable
        // ** Additional topics need to be subscribed to in onConnectAuthBroker()
        const blobs = IR_Convert(app_id, data);

        // Write the finished data transformation to the nifi broker
        console.info("Writing to nifi output stream");
        nifiBroker.publish(config.NIFI_BROKER_TOPIC, JSON.stringify(blobs));
    } catch(err) {
        console.error("ERROR IN INGEST TRANSFORMATION");
        console.error(err);
    }
}

// Runs when the nifi broker is connected
function onConnectNIFI(response) {
    console.info("Connected to NIFI broker");
    console.debug(response);
}

// Runs when the nifi broker encounters an error
function onErrorNIFI(err) {
    console.error("ERROR IN NIFI BROKER")
    console.error(err);
}

// Assign functions to their respective events
authenticatedBroker.on("connect", onConnectAuthBroker);
authenticatedBroker.on("error", onErrorAuthBroker);
nifiBroker.on("connect", onConnectNIFI);
nifiBroker.on("error", onErrorNIFI);
