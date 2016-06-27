# Convergent
* Replicated Data Types *

{"name": "hulc",
"type": "record"
"fields": [
{"name":"l", "type":"timestamp-micros"},
{"name":"c", "type":"int"}
{"name":"node", "type":"string"}
]}

{"name":"op",
 "type":"record"
 "fields":[
 {"name":"id", "type": "string", "logicaltype":"hulc"}
 {"name":"obj_id", "type": "string", "logicaltype":"hulc"}
 {"name":"cmd", "type": "string"}]}

{"name":"reservation_state",
"type":"enum",
"logicaltype":"monotonic-enum",
"symbols":["OPEN","SEATED","CLOSED"]
}

{"name": "Reservation",
 "type":"record",
 "logicaltype":"",
 "fields":[
 {"name":"id", "type":"string","required":true},
 {"name":"rid", "type":"int", "required":true},
 {"name":"partysize", "type":"int", "logicaltype":"lww-elem","required":true},
 {"name":"scheduled", "type": "long", "logicaltype":"timestamp-millis"},
 {"name":"state","type":"reservation_state"}
 {"name":"diner-name", "type":"string","logicaltype","lww-elem"}
 ]}

{
  "namespace": "com.opentable",
  "protocol": "Reservations",
  "doc": "Reservation Protocol",

  "types": [
    {"name": "Reservation", "type": "record",
     "fields": [
     {"name": "message",
     "type": "string"}]},
    {"name": "Error", "type": "error", "fields": [
      {"name": "message", "type": "string"}]}
  ],

  "messages": {
    "op": {
      "doc": "Operation",
      "request": [{"name": "greeting", "type": "Greeting" }],
      "response": "string"
    }
  }
}
