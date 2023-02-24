

SELECT * FROM json.`${DA.paths.kafka_events}/001.json`



SELECT * FROM json.`${DA.paths.kafka_events}`



CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`;

SELECT * FROM events_temp_view


SELECT * FROM text.`${DA.paths.kafka_events}`



SELECT * FROM binaryFile.`${DA.paths.kafka_events}`


