## Instructions

`mvn clean verify` currently hangs due to the issue described below. It's probably easiest to run `mvn clean package` then debug `FunctionIT` from an IDE with some breakpoints set.

## Issue description

Function seems to be created, but never called. `pulsar-admin topics stats-internal` shows messages consumed by the function, 
but `pulsar-admin function stats` shows no messages consumed.
 
However, if the function is triggered (via `pulsar-admin functions trigger ...`) before producing messages to the function's input, 
the function is called as expected. See `com.github.hansenc.pulsar.FunctionIT.testWithTrigger` and `com.github.hansenc.pulsar.FunctionIT.testWithoutTrigger`.

Example output (gathered while `FunctionIT` hangs):
```
root@6723490da207:/# /pulsar/bin/pulsar-admin topics stats-internal persistent://public/default/test-input
{
  "entriesAddedCounter" : 3,
  "numberOfEntries" : 3,
  "totalSize" : 200,
  "currentLedgerEntries" : 3,
  "currentLedgerSize" : 200,
  "lastLedgerCreatedTimestamp" : "2020-07-31T23:36:25.319Z",
  "waitingCursorsCount" : 1,
  "pendingAddEntriesCount" : 0,
  "lastConfirmedEntry" : "12:2",
  "state" : "LedgerOpened",
  "ledgers" : [ {
    "ledgerId" : 12,
    "entries" : 0,
    "size" : 0,
    "offloaded" : false
  } ],
  "cursors" : {
    "public%2Fdefault%2FExclamationFunction" : {
      "markDeletePosition" : "12:2",
      "readPosition" : "12:3",
      "waitingReadOp" : true,
      "pendingReadOps" : 0,
      "messagesConsumedCounter" : 3,
      "cursorLedger" : 17,
      "cursorLedgerLastEntry" : 0,
      "individuallyDeletedMessages" : "[]",
      "lastLedgerSwitchTimestamp" : "2020-07-31T23:36:32.386Z",
      "state" : "Open",
      "numberOfEntriesSinceFirstNotAckedMessage" : 1,
      "totalNonContiguousDeletedMessagesRange" : 0,
      "properties" : { }
    }
  }
}
^[broot@6723490da207:/# /pulsar/bin/pulsar-admin functions stats --name ExclamationFunction
{
  "receivedTotal" : 0,
  "processedSuccessfullyTotal" : 0,
  "systemExceptionsTotal" : 0,
  "userExceptionsTotal" : 0,
  "avgProcessLatency" : null,
  "1min" : {
    "receivedTotal" : 0,
    "processedSuccessfullyTotal" : 0,
    "systemExceptionsTotal" : 0,
    "userExceptionsTotal" : 0,
    "avgProcessLatency" : null
  },
  "lastInvocation" : null,
  "instances" : [ {
    "instanceId" : 0,
    "metrics" : {
      "receivedTotal" : 0,
      "processedSuccessfullyTotal" : 0,
      "systemExceptionsTotal" : 0,
      "userExceptionsTotal" : 0,
      "avgProcessLatency" : null,
      "1min" : {
        "receivedTotal" : 0,
        "processedSuccessfullyTotal" : 0,
        "systemExceptionsTotal" : 0,
        "userExceptionsTotal" : 0,
        "avgProcessLatency" : null
      },
      "lastInvocation" : null,
      "userMetrics" : { }
    }
  } ]
}

```
