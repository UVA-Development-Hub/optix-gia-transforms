
// A pre-transformation that requires nothing.
function passthrough(data) {
    return data;
}

function passthroughWithSafety(data) {
    var output = {
        app_name: data.app_name,
        time: data.time,
        metadata: data.metadata,
        payload_fields: {}
    };

    Object.keys(data.payload_fields).forEach(field => {
        const safeField = field.replace(/\s+/g, "_");
        output.payload_fields[safeField] = data.payload_fields[field];
    });

    return output;
}

//
//
//
//
//

const conversionMap = {
    "data/ingest/passthrough": passthrough,
    "data/ingest": passthroughWithSafety
}

function Convert(topic, data) {
    return conversionMap[topic](data);
}

module.exports = Convert;
