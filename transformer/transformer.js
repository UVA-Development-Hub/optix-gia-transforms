// Transforms data from the input format, suitable for human
// reading and general purpose data transfer, into the output
// format, whose target audience is NiFi and ultimately OpenTSDB.

// --------------------------------------------------
// ------------------ INPUT FORMAT ------------------
// --------------------------------------------------

// {
//     app_id: 'test_device_1',
//     time: luxon.utc().toISO(),
//     metadata: {
//         all: 'metadata',
//         fields: '(i.e. location, etc) ...'
//     },
//     payload_fields: {
//         power: {
//             displayName: 'power',
//             unit: 'kW',
//             value: 4.17
//         }
//     }
// }

// --------------------------------------------------
// ----------------- OUTPUT FORMAT ------------------
// --------------------------------------------------

// [
//     {
//         metrics: [
//             {
//                 name: 'test_device_1.power',
//                 value: 4.17
//             }
//         ],
//         static: [
//             {
//                 name: 'unit',
//                 value: 'kW'
//             },
//             {
//                 name: 'displayName',
//                 value: 'power'
//             }
//         ],
//         dynamic: [
//             {
//                 name: 'timeStamp',
//                 value: luxon.utc().toISO()
//             }
//         ]
//     }
// ]

(require("dotenv")).config();
const config = process.env;
const mqtt = require("mqtt");

const ingestBroker = mqtt.connect(config.INGEST_BROKER, {
    host: config.INGEST_BROKER,
    port: 2883
});

const nifiBroker = mqtt.connect(config.NIFI_BROKER, {
    host: config.NIFI_BROKER,
    port: 1883
});

// Runs when the ingest broker is connected
function onConnectIngest(response) {
    console.info("Connected to ingest broker");
    console.debug(response);
    ingestBroker.subscribe("data/ingest", () => console.info("Subscribed to ingest broker topic: data/ingest"));
    ingestBroker.on("message", onMessageIngest);
}

// Runs when the ingest broker encounters an error
function onErrorIngest(err) {
    console.error("ERROR IN INGEST BROKER");
    console.error(err);
}

// Runs when the ingest broker receives a message
// on the data ingestion topic: data/ingest
function onMessageIngest(topic, message) {
    try {
        const jsonIn = JSON.parse(message);

        // Construct the first part of the static section here.
        // This results in us only having to tack on a couple
        // of metric-specific key/values during blob creation
        const static =
            Object.keys(jsonIn.metadata)
            .map(field => {
                return {
                    name: field,
                    value: jsonIn.metadata[field]
                };
            });

        // We only need to create this array once since it is
        // the same for each data blob in the output message
        const dynamic = [
            {
                name: "timeStamp",
                value: jsonIn.time
            }
        ];

        // Transform the incoming json into suitable data blobs
        const blobs =
            Object.keys(jsonIn.payload_fields)
            .map(metric => {
                const payloadField = jsonIn.payload_fields[metric];
                return {
                    metrics: [
                        name: `${jsonIn.app_id}.${metric}`,
                        value: payloadField.value
                    ],
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

        // Write the finished data transformation to the nifi broker
        nifiBroker.publish(nifiWriteTopic, JSON.stringify(blobs));
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
ingestBroker.on("connect", onConnectIngest);
ingestBroker.on("error", onErrorIngest);
nifiBroker.on("connect", onConnectNIFI);
nifiBroker.on("error", onErrorNIFI);
