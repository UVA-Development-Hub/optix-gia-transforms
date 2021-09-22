const { Pool } = require("pg");

const config = process.env;

const DB_USER = config.DB_USER;
const DB_PASSWORD = config.DB_PASSWORD;
const DB_HOST = config.DB_HOST;
const DB_PORT = config.DB_PORT;
const DB_DATABASE = config.DB_DATABASE;

const pool = new Pool({
    user: DB_USER,
    host: DB_HOST,
    database: DB_DATABASE,
    password: DB_PASSWORD,
    port: DB_PORT,
    // ssl: true,
});

async function query(text, params) {
    // const start = Date.now();
    const res = await pool.query(text, params);
    // const duration = Date.now() - start;
    // console.log("executed query", { text, duration, rows: res.rowCount });
    return res;
}

async function getClient() {
    const client = await pool.connect();
    const query = client.query;
    const release = client.release;
    // set a timeout of 5 seconds, after which we will log this client's last query
    const timeout = setTimeout(() => {
        console.error("A client has been checked out for more than 5 seconds!");
        console.error(`The last executed query on this client was: ${client.lastQuery}`);
    }, 5000);
    // monkey patch the query method to keep track of the last query executed
    client.query = (...args) => {
        client.lastQuery = args;
        return query.apply(client, args);
    };
    client.release = () => {
        // clear our timeout
        clearTimeout(timeout);
        // set the methods back to their old un-monkey-patched version
        client.query = query;
        client.release = release;
        return release.apply(client);
    };
    return client;
}

module.exports = {
    query: query,
    getClient: getClient,
};
