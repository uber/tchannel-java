# TChannel Examples

In TChannel-JAVA, the [SubChannel.send(...)](../tchannel-core/src/main/java/com/uber/tchannel/api/SubChannel.java)
function returns a [future object](../tchannel-core/src/main/java/com/uber/tchannel/api/TFuture.java), which can either
be used to access the response synchronously or asynchronously. They are the basic approaches of TChannel 
protocol communication in JAVA. The synchronous and asynchronous examples are below.
* [SyncRequest](./src/main/java/com/uber/tchannel/basic/SyncRequest.java)
* [AsyncRequest](./src/main/java/com/uber/tchannel/basic/AsyncRequest.java)


## Run Examples

### 1. Requet Examples
#### 1.1 [SyncRequest](./src/main/java/com/uber/tchannel/basic/SyncRequest.java)
**Command**:
```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.basic.SyncRequest
```

**Output**:
```bash
Request received: header: Marco, body: Ping!
Response received: response code: OK, header: Polo, body: Pong!

Request received: header: Marco, body: Ping!
Response received: response code: Error, header: Polo, body: I feel bad ...

Request received: header: Marco, body: Ping!
Got error response: <ErrorResponse id=3 errorType=BadRequest message=Failed to handle the request: I feel very bad!>

Time cost: 157ms
```

#### 1.2 [AsyncRequest](./src/main/java/com/uber/tchannel/basic/AsyncRequest.java)
**Command**:
```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.basic.AsyncRequest
```
**Output**:
```bash
Request received: header: Marco, body: Ping!
Request received: header: Marco, body: Ping!
Request received: header: Marco, body: Ping!
Response received: response code: OK, header: Polo, body: Pong!
Response received: response code: Error, header: Polo, body: I feel bad ...
Got error response: <ErrorResponse id=3 errorType=BadRequest message=Failed to handle the request: I feel very bad!>

Time cost: 110ms
```

### 2. Ping Example
#### 2.1 [PingServer](./src/main/java/com/uber/tchannel/ping/PingServer.java)
**Command**:
```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.ping.PingServer --port 8888
```

#### 2.2 [PingClient](./src/main/java/com/uber/tchannel/ping/PingClient.java)
**Command**:
```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.ping.PingClient --port 8888
```
**Output**:
```bash
<JsonResponse responseCode=OK transportHeaders={as=json, re=c, cn=ping-client} headers=null body=null>
	count:10000
Stopping Client...
```

### 3. JSON Example
#### 3.1. [JsonServer](./src/main/java/com/uber/tchannel/json/JsonServer.java)
**Command**:
```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.json.JsonServer
```

**Output after receiving a client request**:
```bash
<JsonRequest id=1 service=json-server transportHeaders={as=json, re=c, cn=json-server} arg1=json-endpoint arg2={} arg3={"requestId":0,"requestMessage":"hello?"}>
```

#### 3.2. [JsonClient](./src/main/java/com/uber/tchannel/json/JsonClient.java)
**Command**:
```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.json.JsonClient
```
**Output**:
```bash
<JsonResponse responseCode=OK transportHeaders={as=json} headers=null body=null>
```

### 4. Thrift Example
#### 4.1 [KeyValueServer](./src/main/java/com/uber/tchannel/thrift/KeyValueServer.java)
**Command**:
```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.thrift.KeyValueServer
```

#### 4.2 [KeyValueClient](./src/main/java/com/uber/tchannel/thrift/KeyValueClient.java)
**Command**:
```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.thrift.KeyValueClient
```
**Output**:
```bash
setValue succeeded
getValue succeeded
{'foo' => 'bar'}
getValue succeeded
Key 'baz' not found.
Disconnected from KeyValue Server.
```

### 5. [HyperbahnExample](./src/main/java/com/uber/tchannel/hyperbahn/HyperbahnExample.java)
**Command**:
```bash
mvn package
node server.js --port 21300 2>&1 | jq .
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.hyperbahn.HyperbahnExample
tcurl -p 127.0.0.1:21300 javaServer ping -j -2 "{}" -3 '{"request":"hello"}' | jq .
```

**Output of HyperbahnExample**:
```bash
Got response. All set: <AdvertiseResponse connectionCount=1>
```
**Output of tcurl**:
```bash
{
  "ok": true,
  "head": {},
  "body": {
    "response": "pong!"
  },
  "headers": {
    "as": "json",
    "cn": "tcurl",
    "re": "c"
  }
}
```



## MIT Licenced
