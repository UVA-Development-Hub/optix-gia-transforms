//   .env file should contain:
// PUBLIC_BROKER: mqtt{ip address of the front-facing broker}
// PUBLIC_BROKER_TOPIC: "data/ingest"
// AUTH_BROKER: mqtt://{ip address of the auth-checking broker}
// AUTH_BROKER_TOPIC: the topic authenticated data flows into
(require("dotenv")).config();
const config = process.env;
const CognitoExpress = new (require("cognito-express"))({
    region: config.REGION,
    cognitoUserPoolId: config.USERPOOLID,
    tokenUse: "access",
    tokenExpiration: 3600000
});
const mqtt = require("mqtt");
const IR_Converter = require("./IR_Converter");

const MemoryCache = require("memory-cache").Cache;
const AppCache = new MemoryCache();
const MetricsCache = new MemoryCache();

const publicBroker = mqtt.connect(config.PUBLIC_BROKER, {
    host: config.PUBLIC_BROKER,
    port: 1883
});
const authenticatedBroker = mqtt.connect(config.AUTH_BROKER, {
    host: config.AUTH_BROKER,
    port: 2883
});

const axios = require("axios");
const openTSDBAgent = axios.create({
    baseURL: config.OPENTSDB_URL,
    timeout: 0,
});

const db = require("./db");

//   Checks to see the provided Cognito token is valid
// for the SIF user pool. If it is indeed valid, the
// function returns who the authenticated user is.
async function validateAuthToken(token) {
    const { error, user } = await new Promise((resolve, _) => {
        CognitoExpress.validate(token, (err, user) => {
            if(err) resolve({
                error: err,
                user: null
            });
            else if(!user) resolve({
                error: "Could not parse valid user from token",
                user: null
            });
            else resolve({
                error: null,
                user: user
            })
        });
    });

    return {
        success: !error,
        username: user ? user.username : null,
        groups: user ? user["cognito:groups"] : [],
        error: error
    }
}

//   Queries the database to see if the specified app
// currently exists or not. If it is new, add it to the
// database. Returns the found/added app information.
async function createOrReturnApp(app_name, username) {
    const appKey = `${username}.${app_name}`;
    var cachedId = AppCache.get(appKey);
    if(cachedId === null) {
        // does the app exist?
        const searchResult = await db.query(
            "SELECT id FROM apps WHERE username=$1 AND app_name=$2",
            [
                username,
                app_name
            ]
        );
        if(searchResult.rows.length) {
            // if yes, store the id
            cachedId = searchResult.rows[0].id;
            console.log("Caching existing app with id", cachedId);
        } else {
            // if no, create it and store the id
            const insertion = await db.query(
                "INSERT INTO apps(username, app_name) VALUES($1, $2) RETURNING id",
                [
                    username,
                    app_name
                ]
            );
            console.log("Inserting new app row into the database");
            cachedId = insertion.rows[0].id;
        }
        // cache the id
        AppCache.put(appKey, cachedId, 24*3600*1000); // cache the app id for a day
    } else {
        console.log("App key exists and is cached");
    }
    return cachedId;
}

//   Given a list of metrics, it queries the database to
// see which ones, if any, do not yet exist. If new metrics
// are found, they are added to the datbase and to OpenTSDB.
async function createMetrics(app_id, metrics) {
    for(let i = 0; i < metrics.length; i++) {
        const metric = metrics[i];
        const metricKey = `${app_id}.${metric}`;
        if(MetricsCache.get(metricKey) === null) {
            const searchResult = await db.query(
                "SELECT id FROM metrics WHERE metric=$1 AND app_id=$2"
                [
                    metric,
                    app_id
                ]
            );
            if(searchResult.rows.length) {
                console.log("Caching existing metric");
            } else {
                // metric is not in the database!
                console.log("Creating new metric and adding it to the cache");
                await Promise.all([
                // add metric to the database
                    db.query(
                        "INSERT INTO metrics(metric, app_id) VALUES($1, $2",
                        [
                            metric,
                            app_id
                        ]
                    ),
                    // create metric in optix.time
                    openTSDBAgent.get("assign", {
                        params: {
                            metric: metricKey
                        }
                    })
                ]);
            }
            MetricsCache.put(metricKey, true, 24*3600*1000);
        } else {
            console.log("Metric " + metricKey + " exists and is already cached");
        }
    }
}

//   When the public-facing broker receives a new message,
// is is passed through this function.
async function authenticateBlobs(topic, message) {
    try {
        const jsonIn = JSON.parse(message);
        if(
            !jsonIn.app_name ||
            !jsonIn.token ||
            !jsonIn.data
        ) throw("Missing required property in incoming message");

        const { success, username, groups, error } = await validateAuthToken(jsonIn.token);
        if(!success) throw("Failed to validate token: " + error);

        const app_id = await createOrReturnApp(jsonIn.app_name, username);
        const payload = {
            app_id: app_id,
            data: IR_Converter(topic, jsonIn.data)
        }

        authenticatedBroker.publish(
            config.AUTH_BROKER_TOPIC,
            JSON.stringify(payload)
        );
        await createMetrics(app_id, Object.keys(payload.data.payload_fields));

    } catch(err) {
        console.error("ERROR IN AUTHENICATION BROKER: \n" + err.toString());
    }
}

function main() {
    publicBroker.on("message", authenticateBlobs);
    publicBroker.on("connect", () => {
        console.info("Connected to the public-facing broker");
    });

    authenticatedBroker.on("connect", () => {
        console.info("Connected to the authenticated data broker.");
        publicBroker.subscribe(config.PUBLIC_BROKER_TOPIC, () => {
            console.info(`Subscribed to ${config.PUBLIC_BROKER_TOPIC} on the public-facing broker.`);
        });
    });
}

console.log(IR_Converter);
main();
