
import optix.time.ingest.transforms.StringTransformer;

import java.util.Iterator;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class SampleTransformer implements StringTransformer {
    private String prefix;

    @Override
    public boolean initialize(String info) {
        if (info !=null && info.length()>0)
            this.prefix = info;
        return true;
    }

    @Override
    public String transform(String sourceProvider, String sourceTopic, String record) {
        System.out.println(sourceProvider, sourceTopic, record);

        JSONParser parser = new JSONParser();

        JSONObject inRecord = (JSONObject) parser.parse(record);

        System.out.println(inRecord);

        JSONObject outRecord = new JSONArray();


        // Input structure:
        // {
        //     topic: 'linklab/nexusedge',
        //     app_id: 'test_device_1',
        //     counter: 0,
        //     metadata: {
        //         time: luxon.utc().toISO(),
        //         handler_id: 'handler_1',
        //         controller_id: 'controller_1',
        //         gateway_address: '8.8.8.8',
        //         location: 'LinkLab',
        //     },
        //     payload_fields: {
        //         device: { displayName: 'device', unit: 'na', value: 2 },
        //         sequence_number: { displayName: 'sequence_number', unit: 'na', value: 28810585 },
        //         rms_voltage: { displayName: 'rms_voltage', unit: 'na', value: 121.2 },
        //         power: { displayName: 'power', unit: 'na', value: 4.17 },
        //         apparent_power: { displayName: 'apparent_power', unit: 'na', value: 99.99 },
        //         energy: { displayName: 'energy', unit: 'na', value: 28327.35 },
        //         power_factor: { displayName: 'power_factor', unit: 'na', value: 0.12 },
        //         flags: { displayName: 'flags', unit: 'na', value: 69 },
        //         deviceType: { displayName: 'device_type', unit: 'na', value: 'powerblade' }
        //     }
        // }

        // Output structure
        // [
        //     {
        //         metrics: [
        //             {
        //                 name: "",
        //                 value: ""
        //             }
        //         ],
        //         dynamic: [
        //             {
        //                 name: "",
        //                 value: ""
        //             }
        //         ],
        //         static: [
        //             {
        //                 name: "",
        //                 value: ""
        //             }
        //         ]
        //     }
        // ]

        // Pull out time and remove it from the list metadata fields
        String time = inRecord.get("metdata").get("time");
        inRecord.get("metadata").remove("time");

        JSONObject payloadFields = (JSONObject) inRecord.get("payload_fields");
        Iterator<?> iter = payloadFields.keys();
        while(iter.hasNext()) {
            String key = (String) iter.next();
            JSONObject payloadField = payloadFields.get(key);
            JSONObject transformedPayload = new JSONObject();

            // Generate metrics section
            JSONArray metrics = new JSONArray();
            JSONObject metric = new JSONObject();
            metricVal.put("name", inRecord.get("app_id") + key);
            metricVal.put("value", payloadField.get("value"));
            metrics.add(metric);
            transformedPayload.put("metrics", metrics);

            // Generate dynamic section
            JSONArray dynamic = new JSONArray();
            JSONObject timestamp = new JSONObject();
            timestamp.put("name": "timeStamp");
            timestamp.put("value": time);
            dynamic.add(timestamp);
            transformedPayload.put("dynamic", dynamic);

            // Generate static section
            JSONArray static_ = new JSONArray();
            Iterator<?> metaIter = inRecord.get("metadata").keys();
            while(metaIter.hasNext()) {
                String metaKey = (String) metaIter.next();
                JSONObject transMeta = new JSONObject();
                transMeta.put(metaKey, inRecord.get("metadata").get(metaKey));
                static_.add(transMeta);
            }
            transformedPayload.put("static", static_);

            outRecord.add(transformedPayload);
        }

        System.out.println(prefix);
        System.out.println(outRecord);
        return outRecord.toString();
    }

    @Override
    public Iterable<String> transform(String sourceProvider, String sourceTopic, Iterable<String> records) {
        if (records!=null){
            return new Wrapper(records);
        }
        return null;
    }

    public final class Wrapper implements Iterable<String>{
        private final Iterable<String> inner;

        @Override
        public Iterator<String> iterator() {
            return new WrapperIterator(inner.iterator());
        }

        public final class WrapperIterator implements Iterator<String>{
            private final Iterator<String> inneriter;

            @Override
            public boolean hasNext() {
                return this.inneriter.hasNext();
            }

            @Override
            public String next() {
                return transform(null, null, this.inneriter.next());
            }

            private WrapperIterator(Iterator<String> inneriter){
                this.inneriter=inneriter;
            }
        }

        private Wrapper(Iterable<String> records){
            this.inner=records;
        }
    }
}
