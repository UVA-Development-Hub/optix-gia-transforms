
// A pre-transformation that requires nothing.
function passthrough(data) {
    return data;
}

//
//
//
//
//

const conversionMap = {
    "data/ingest": passthrough
}

function Convert(topic, data) {
    return conversionMap[topic](data);
}

module.exports = Convert;
