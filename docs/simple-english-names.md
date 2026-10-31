# Simple English names

This table is for maintainers who read English as a second language.
Left column: the name in the code today. Right column: a plainer name.

How the list was made: a script read all 299 Go files and found 4,246 names
(packages, structs, interfaces, functions, methods, fields, constants, variables).
Only names that are hard to read are listed here. Names that are already
plain (`Broker`, `Config`, `Client`, `Store`, `Segment`) are not listed.

## Status (branch `refactor/simple-names`)

The renames below were applied in 12 batches, with `make` passing after each one.
In the tables, the left column is the OLD name and the right column is the NEW name.

Changed while applying (the code uses these, not the table):

| Table said | Code now has | Why |
|---|---|---|
| `ApiVersionRange` | `APIVersionRange` | The linter wants `API` in capitals. |
| `requestApiVersions`, `stateApiVersions` | `requestAPIVersions`, `stateAPIVersions` | Same linter rule. |
| `pEnd` | `prefixEndOffset` | `prefixEnd` was already used by another function. |
| `TP` -> `TopicPartitionKey` | `NewTopicPartition` | It builds a value, so `New...` is clearer. |
| package `it` | package `integrationtest` | Follows the folder name. |

Not done on purpose:

- `rd` in `cmd/manibench` (meaning is unclear), `timerRole`, `timerCatchUp`.
- Struct fields and methods that share a name with a renamed type (for example `sim` `Lane` field).
- The `CreateTable` method (AWS name), the Makefile target names, string values, and env var names.
- File names such as `checker/veritastest_test.go` and `fakecontrolplane/cpengine.go`. Rename them when you next edit those files.
- Older design docs (`decisions.md`, `lessons.md`, and similar) still use the old names in prose.

Do not rename these, because the outside world defines them:

- Generated code in `internal/controlplane/api/v1` (made from the `.proto` file).
- Kafka words: `Produce`, `Fetch`, `ListOffsets`, `Metadata`, `ApiVersions`, `DescribeTopics`.
- AWS words: `DynamoDB`, `S3`, `IMDS`, `Healthz`.
- Go words: `ctx`, `err`, `mu`, `Reader`, `Writer`.

Rules used for the right column:

1. Write the full word. No cut-short names (`tp`, `ckpt`, `seg`).
2. Use a common word. No slang or idioms (`trip`, `planted`, `swarm`, `tombstone`).
3. The name says what the thing does, not where the idea came from.

---

## 1. Packages (folder names)

| Current | Simple English |
|---|---|
| `veritas` | `speccheck` |
| `veritastest` | `speccheck/checker` |
| `vsys` | `specsystem` |
| `plantedbug` | `injectedbug` |
| `runcontrol` | `runsettings` |
| `sim` | `simulator` |
| `sim/engines` | `sim/fakeservices` |
| `cpengine` | `fakecontrolplane` |
| `logengine` | `fakelogservice` |
| `logstore` | `fakelogstore` |
| `netsim` | `fakenetwork` |
| `objstore` | `fakeobjectstore` |
| `enginetest` | `fakeservicetest` |
| `fakeio` | `fakecompletions` |
| `sys` | `system` |
| `prod` | `production` |
| `awsobs` | `awsmetrics` |
| `httppool` | `httpconnections` |
| `controlplane/exec` | `controlplane/executor` |
| `controlplane/run` | `controlplane/runloop` |
| `controlplane/applier` | `controlplane/stateupdater` |
| `controlplane/machine` | `controlplane/statemachine` |
| `controlplane/cpclient` | `controlplane/controlplaneclient` |
| `controlplane/cpobs` | `controlplane/controlplanemetrics` |
| `controlplane/cptest` | `controlplane/controlplanetest` |
| `controlplane/apiconv` | `controlplane/apiconvert` |
| `controlplane/fakedb` | `controlplane/fakedynamodb` |
| `controlplane/memstore` | `controlplane/memorystore` |
| `controlplane/grpctest` | `controlplane/grpctestserver` |
| `applier/pebblekv` | `stateupdater/pebblestore` |
| `wire/format` | `wire/segmentformat` |
| `wire/kwire` | `wire/kafkawire` |
| `ops/tools/detcheck` | `ops/tools/determinismcheck` |
| `cmd/crapscore` | `cmd/riskscore` |
| `test/it` | `test/integrationtest` |

## 2. Core ideas (`internal/core`)

| Current | Simple English |
|---|---|
| `Buggify` | `FaultInjector` |
| `Site` | `FaultPointID` |
| `Prop` | `PropertyID` |
| `Detail` | `ContextPair` |
| `Hashable` | `HasStateHash` |
| `Incarnation` | `BootNumber` |
| `NodeRef` | `NodeInstance` |
| `Factory` | `MachineFactory` |
| `Env` | `Environment` |
| `IOAction` | `IOCall` |
| `Completion` | `IOResult` |
| `EventBody` | `EventData` |
| `Class` | `InboxKind` |
| `ClassOf` | `InboxKindOf` |
| `Queue` | `Inbox` |
| `InboxPolicy` | `InboxPicker` |
| `ControlFirst` | `ControlFirstPicker` |
| `Framer` | `FrameSplitter` |
| `Codec` | `KafkaCodec` |
| `WallTime` | `RealTime` |
| `Time` | `NodeTime` |
| `LogSlot` | `LogPosition` |
| `ReqID` | `RequestID` |
| `TimerGen` | `TimerVersion` |
| `TP` | `TopicPartition` |
| `RespAs` | `ResponseAs` |
| `FnvU64` | `Fnv64Hash` |
| `fnvP1` … `fnvP8` | `fnvPrimePower1` … `fnvPrimePower8` |
| `U64`, `I64`, `Str` | `Uint64Value`, `Int64Value`, `StringValue` |

## 3. Actions and events (`internal/core`)

| Current | Simple English |
|---|---|
| `Emit` | `RecordFact` |
| `Trip` | `StopForRestart` |
| `Exit` | `ShutdownDone` |
| `Stop` | `StopRequest` |
| `Started` | `NodeStarted` |
| `Dial` | `Connect` |
| `DialDone` | `ConnectDone` |
| `CPCall` | `ControlPlaneCall` |
| `CPDone` | `ControlPlaneDone` |
| `CPOffer` | `ControlPlaneDeliver` |
| `CPReply` | `ControlPlaneReply` |
| `CPOp` | `ControlPlaneOperation` |
| `CPResp` | `ControlPlaneResponse` |
| `Lookup` / `LookupResp` | `FindSegments` / `FindSegmentsResp` |
| `LogCheckTail` | `LogGetEnd` |
| `LogTrim` | `LogDeleteOld` |
| `ObjPut`, `ObjGet`, `ObjList`, `ObjDelete` | `ObjectPut`, `ObjectGet`, `ObjectList`, `ObjectDelete` |
| `*Done` for Obj/Log (`ObjPutDone`) | `ObjectPutDone` (same change) |

## 4. Broker (`internal/broker`)

| Current | Simple English |
|---|---|
| `pbatch` | `partitionBatch` |
| `conn` | `connection` |
| `op`, `opKind`, `opState` | `operation`, `operationKind`, `operationState` |
| `segState` | `segmentState` |
| `kafkaCode` | `kafkaErrorCode` |
| `hasher` | `stateHasher` |
| `Bug`, `BugNone`, `BugEarlyAck` … | `InjectedBug`, `InjectedBugNone`, `InjectedBugEarlyAck` … |
| `sealBatches` / `sealPartitions` / `sealSize` / `sealTimer` / `sealStop` | `closeOnBatches` / `closeOnPartitions` / `closeOnSize` / `closeOnTimer` / `closeOnStop` |
| `segCommit`, `segCommitBackoff` | `segmentCommit`, `segmentCommitRetry` |
| `segPut`, `segPutDone`, `segPutBackoff` | `segmentUpload`, `segmentUploadDone`, `segmentUploadRetry` |
| `opCP`, `opMeta` | `opControlPlane`, `opMetadata` |
| `emit` | `recordFact` |
| `intact` | `isIntact` |
| `*Skeleton` (`fetchSkeleton`) | `*Template` (`fetchTemplate`) |

## 5. Control plane: storage and rules

| Current | Simple English |
|---|---|
| `Tx` | `Transaction` |
| `KV` | `KeyValueStore` |
| `MemKV` | `MemoryKeyValueStore` |
| `Applier` | `StateUpdater` |
| `assignPlan` | `offsetPlan` |
| `AssignHere` / `AssignElsewhere` / `AssignUnknown` | `OffsetOwnedHere` / `OffsetOwnedElsewhere` / `OffsetOwnerUnknown` |
| `idemKey`, `idemRecord`, `parseIdem` | `duplicateCheckKey`, `duplicateCheckRecord`, `parseDuplicateCheck` |
| `digest`, `digestBytes`, `digestLen` | `checksum`, `checksumBytes`, `checksumLength` |
| `growTopic` | `addPartitions` |
| `tpPrefix` | `topicPartitionPrefix` |
| `pEnd`, `pKey`, `pRun`, `pSlot`, `pTopic` | `prefixEnd`, `prefixKey`, `prefixRun`, `prefixSlot`, `prefixTopic` |
| `snapMagic`, `errSnap`, `writeSnap` | `snapshotMarker`, `errSnapshot`, `writeSnapshot` |
| `runBase`, `runKey`, `encodeRun` | `batchRunBase`, `batchRunKey`, `encodeBatchRun` |
| `commitTx`, `lookupTx`, `describeTx` | `commitTransaction`, `lookupTransaction`, `describeTransaction` |
| `readN`, `getUvarint`, `putUvarintVal` | `readBytes`, `readVarint`, `writeVarint` |
| `quiet` | `silentLogger` |
| `decoder`, `encoder` | `entryDecoder`, `entryEncoder` |
| `memEntry`, `memTx` | `memoryEntry`, `memoryTransaction` |

## 6. Control plane: machine, executor, client

| Current | Simple English |
|---|---|
| `flight` | `inFlightAppend` |
| `pendingCmd` | `waitingCommand` |
| `holePlan`, `holePlanArm/Fill/Skip/Wait` | `gapPlan`, `gapPlanArm/Fill/Skip/Wait` |
| `timerHoleFill` | `timerGapFill` |
| `timerCkpt`, `defaultCkptKey` | `timerCheckpoint`, `defaultCheckpointKey` |
| `timerRole`, `timerCatchUp` | `timerRoleCheck`, `timerCatchUp` (keep) |
| `isTomb`, `slotTomb`, `tombstoneB` | `isDeleted`, `slotDeleted`, `deletedMarkerBytes` |
| `envelopeMagic`, `groupMagic` | `envelopeMarker`, `groupMarker` |
| `ReplaySlot` | `ReplayPosition` |
| `Answer` | `ReplyToClient` |
| `ClientOp` | `ClientOperation` |
| `topicSeed` / `TopicSeed` | `initialTopic` |
| `Bug*` (machine, logengine) | `InjectedBug*` |
| `DB` (exec) | `DynamoClient` |
| `TableAdmin` | `TableManager` |
| `CreateTable` | `CreateDynamoTable` |
| `DynLog` | `DynamoLog` |
| `MemLog`, `MemObjects` | `MemoryLog`, `MemoryObjects` |
| `nv`, `sv` | `numberValue`, `stringValue` |
| `logPK` | `logPartitionKey` |
| `slotRec` | `slotRecord` |
| `shouldFailover` | `shouldTryOtherNode` |
| `mapRPCErr` | `convertRPCError` |
| `timeoutOrIO` | `isTimeoutOrIOError` |
| `mutating` | `changesData` |
| `classifyNotSent` | `classifyBeforeSend` |
| `opSum` | `operationSummary` |
| `hist` | `histogram` |
| `ms` | `toMilliseconds` |
| `summaryEvery` | `summaryInterval` |
| `execAs` | `executeAs` |
| `rpcError` | `toRPCError` |
| `prodEnv`, `wallClock` | `productionEnvironment`, `realClock` |

## 7. Fake DynamoDB (`fakedb`)

| Current | Simple English |
|---|---|
| `Fake` | `FakeDynamoDB` |
| `keyCond` | `keyCondition` |
| `evalAtom` | `evalSimpleCondition` |
| `evalCond` | `evalCondition` |
| `compareNum` | `compareNumbers` |
| `addNum` | `addNumbers` |
| `pick` | `chooseItem` |
| `idOf`, `strOf`, `valueOf` | `itemIDOf`, `stringOf`, `valueOf` (keep last) |

## 8. System and production (`sys`, `prod`, `run`, `runinfo`, `httppool`, `awsobs`)

| Current | Simple English |
|---|---|
| `Sink` | `InboxWriter` |
| `Bundle` | `Dependencies` |
| `Conns` | `Connections` |
| `AsyncConns` | `AsyncConnections` |
| `ctrlQ`, `dataQ` | `controlQueue`, `dataQueue` |
| `runner` | `eventLoop` |
| `Classifier` | `ErrorClassifier` |
| `MemClassifier` | `MemoryErrorClassifier` |
| `ErrTripped` | `ErrStoppedForRestart` |
| `ErrKilled` | `ErrKilledByRuntime` |
| `ShutdownOnSignal` | `StopOnSignal` |
| `tapExecutor` | `hookedExecutor` |
| `segInfo` | `segmentInfo` |
| `MemFS` | `MemoryFileSystem` |
| `fi`, `fs` | `fileInfo`, `fileSystem` |
| `max64` | `maxInt64` |
| `AutoBuffered`, `MaxBufferedCap`, `MinBufferedBytes` | keep, but add a comment: "buffered = bytes held before upload" |
| `FetchIMDS` | `FetchInstanceMetadata` |
| `Healthz` | `HealthCheckHandler` |
| `Mux` | `NewRouter` |
| `putS3` | `uploadToS3` |
| `Fatal` | `LogAndExit` |
| `ExpandDest` | `ExpandDestination` |
| `GitSHA` | `GitCommit` |
| `opMW`, `attemptMW` | `operationMiddleware`, `attemptMiddleware` |
| `AWS` (httppool) | `ConfigureAWSClient` |
| `IsThrottle` | `IsTooManyRequests` |

## 9. Wire formats (`format`, `kwire`)

| Current | Simple English |
|---|---|
| `Magic` | `FileMarker` |
| `Checksum` | `CRC32CChecksum` |
| `indexEntry` | `segmentIndexEntry` |
| `seal` | `finishBatch` |
| `crc32c` | `crc32cChecksum` |
| `crcOff`, `crcStart`, `epochOff`, `lengthOff`, `magicOff` | `crcOffset`, `crcStart`, `epochOffset`, `lengthOffset`, `magicOffset` |
| `OneBatch` | `RequireSingleBatch` |
| `PatchBatch` | `ChangeBatchOffset` |
| `prefix` | `readPrefix` |
| `skipTags`, `appendFlexibleTag` | `skipTaggedFields`, `appendTaggedField` |
| `APIRange` | `ApiVersionRange` |

## 10. Simulator (`internal/verify/sim`)

| Current | Simple English |
|---|---|
| `Engine` | `FakeService` |
| `EngineCtx` / `engineCtx` | `FakeServiceContext` |
| `EntryEngineInternal` | `EntryFakeServiceStep` |
| `Invariant` | `SafetyRule` |
| `CPTruth` | `ControlPlaneRealState` |
| `ObjectTruth` | `ObjectStoreRealState` |
| `TwoSpeed` | `FastWithRareSlow` |
| `Swarm` | `RandomSettingsPicker` |
| `swarm` (function) | `pickRandomSettings` |
| `Attrition` | `NodeKills` |
| `Outage` | `ServiceOutage` |
| `ScriptedFault` | `PlannedFault` |
| `PhaseAware` | `ReceivesPhaseChange` |
| `PhaseChaos`, `PhaseSettle`, `PhaseSettling` | `PhaseFaults`, `PhaseRecover`, `PhaseRecovering` |
| `EntryQuiesce` | `EntryRecover` |
| `Lane`, `laneKey`, `laneGroup`, `nodeLane` | `DeliveryLane`, `laneID`, `laneGroup` (keep), `nodeLane` (keep) |
| `laneReserved`, `laneSim` | `laneForSystem`, `laneForSimulator` |
| `sched`, `Scheduler`, `newSched` | `scheduler` (full word) |
| `noCP` | `noControlPlane` |
| `desc` | `description` |
| `wake` | `wakeUp` |
| `heapItem` | `queueItem` |
| `mix64` | `mixBits64` |
| `splitMix` | `SplitMix64` (keep, name of the algorithm) |
| `fnv1a` | `fnv1aHash` |
| `buggify`, `buggifyDefaultP` | `faultInjector`, `faultInjectDefaultChance` |
| `HistOp` | `HistoryOperation` |
| `OpDefinite` / `OpIndeterminate` / `OpAbandoned` | `OpResultKnown` / `OpResultUnknown` / `OpGivenUp` |
| `FailNondeterminism` | `FailNotRepeatable` |
| `FailInconclusive` | `FailUndecided` |
| `kindAlways`, `kindAlwaysOrUn`, `kindSometimes`, `kindReachable`, `kindUnreach` | `kindMustAlwaysHold`, `kindMustHoldIfReached`, `kindMustHappenOnce`, `kindMustBeReached`, `kindMustNotBeReached` |
| `cDeadlineKills`, `cDroppedByCrash`, `cGracefulStops`, `cWakes` … | `countDeadlineKills`, `countDroppedByCrash`, `countGracefulStops`, `countWakeUps` … |
| `nCounters` | `counterCount` |
| `LivenessGrace` | `ProgressGraceTime` |
| `stopGrace` | `stopWaitTime` |
| `wallEpoch` | `wallClockStart` |
| `classifyStats`, `mergeStats` | `groupStats`, `combineStats` |
| `addUnreached` | `addMissedChecks` |
| `noteDeterminism` | `recordRepeatabilityCheck` |
| `hasher`, `newHasher` | `stateHasher`, `newStateHasher` |

## 11. Simulated services (`engines/*`)

| Current | Simple English |
|---|---|
| `healBody` | `networkHealData` |
| `landBody` | `arriveData` |
| `rstBody`, `kRst` | `resetData`, `kindReset` |
| `kFin`, `kData` | `kindClose`, `kindData` |
| `endKey`, `endRef`, `end` | `connectionEndKey`, `connectionEndRef`, `connectionEnd` |
| `pair`, `pairKey`, `hashPair` | `connectionPair`, `connectionPairKey`, `hashConnectionPair` |
| `span` | `timeSpan` |
| `Lost`, `Normal`, `Timeout`, `Unavailable` (`fakeio`) | `ReplyLost`, `ReplyNormal`, `ReplyTimeout`, `ReplyUnavailable` |
| `Hooks`, `Plan`, `Core` (`fakeio`) | `CompletionHooks`, `CompletionPlan`, `CompletionCore` |
| `Delivery`, `asserts` (`enginetest`) | `DeliveredEvent`, `checks` |
| `Backoff`, `Dialing`, `Up` (`session`) | `WaitingToRetry`, `Connecting`, `Connected` |
| `RetryDelay` | `RetryWaitTime` |
| `FaultCoverage` | `FaultCoverageCheck` |
| `Preset` | `ScenarioPreset` |
| `slow` | `isSlowCampaign` |

## 12. Simulated Kafka workloads (`sim/kafka`, `sim/echo`)

| Current | Simple English |
|---|---|
| `oracle` | `expectedLogChecker` |
| `lbat` | `loggedBatch` |
| `plog` | `partitionLog` |
| `liveness` | `progressCheck` |
| `inflight` | `waitingRequests` |
| `sqz`, `unsqz` | `compressValue`, `decompressValue` |
| `ambiguousCode` | `unclearResultCode` |
| `definiteRetriable` | `clearlyRetryable` |
| `codeCoordNA`, `codeLeaderNA` | `codeCoordinatorUnavailable`, `codeLeaderUnavailable` |
| `idemEvery`, `idemPID` | `duplicateCheckEvery`, `duplicateCheckProducerID` |
| `rqAPI`, `rqFetch`, `rqList`, `rqMeta`, `rqProduce` | `requestApiVersions`, `requestFetch`, `requestListOffsets`, `requestMetadata`, `requestProduce` |
| `stAPI`, `stList`, `stMeta`, `stRun` | `stateApiVersions`, `stateListOffsets`, `stateMetadata`, `stateRunning` |
| `nKinds` | `kindCount` |
| `mutAckOnMaybe`, `mutBadRead`, `mutEarlyAck`, `mutNone` | `bugAckOnMaybe`, `bugBadRead`, `bugEarlyAck`, `bugNone` |
| `csBusy`, `csDone`, `csReady` | `clientBusy`, `clientDone`, `clientReady` |
| `stBackoff`, `stCommit`, `stGet`, `stLookup`, `stPut` | `stateRetryWait`, `stateCommit`, `stateGet`, `stateLookup`, `statePut` |
| `ackedIsCommitted`, `readYourAck` | `ackedMeansCommitted`, `readWhatWasAcked` |
| `timerSlack` | `timerExtraTime` |

## 13. Spec checking (`veritas`)

| Current | Simple English |
|---|---|
| `veritas.End` | `PartitionEnd` |
| `Record` | `StoredRecord` |
| `hashRanges`, `hashTP` | `hashBatchRanges`, `hashTopicPartition` |
| `sliceRange` | `cutRange` |
| `extendPrefix` | `nextPrefix` |
| `clone` | `copy` |
| `SpecComparator` | `SpecChecker` |
| `shrinkState` | `minimizeState` |
| `Shrink*` (`ShrinkControlPlaneOp` …) | `Minimize*` (`MinimizeControlPlaneOp` …) |
| `shrinkCommit`, `shrinkLookup`, `shrinkGet` … | `minimizeCommit`, `minimizeLookup`, `minimizeGet` … |
| `shorterStrings`, `simplerBatches` | `shortenStrings`, `simplifyBatches` |
| `suiteShrinkRuns` | `suiteMinimizeRuns` |
| `Catch` | `Detection` |
| `FindControlPlaneCatch`, `findCatch` | `FindControlPlaneDetection`, `findDetection` |
| `RequireCaught`, `requireAllCaught` | `RequireDetected`, `requireAllDetected` |
| `catchRow` | `detectionRow` |
| `BaseOffByOne` … (bug list) | keep, all are clear names |
| `hashBugName`, `withControlPlaneHash` | `stateHashBugName`, `withControlPlaneStateHash` |
| `cpModel`, `cpModelSegment` | `controlPlaneModel`, `controlPlaneModelSegment` |
| `scribble` | `overwriteWithJunk` |
| `Profile`, `Profiles` | `InputProfile`, `InputProfiles` |
| `pickWeighted` | `chooseByWeight` |
| `shapeSeed` | `makeSeedShape` |
| `InvalidHeavy`, `ReplayHeavy` | `MostlyInvalid`, `MostlyRepeats` |
| `SmallKeys` | `FewDistinctKeys` |
| `quoteAll` | `quoteEach` |
| `EnvSeed`, `EnvSeeds`, `EnvCampaign` | `EnvSeedName`, `EnvSeedListName`, `EnvCampaignName` |
| `lookup` (runcontrol) | `readSetting` |
| `Describe` (runcontrol) | `DescribeSettings` |
| `replayFunc` | `rerunFunc` |

## 14. Tools (`cmd/*`, `ops/tools`)

| Current | Simple English |
|---|---|
| `CRAP` | `RiskScore` |
| `Verdict` | `Result` |
| `Block` | `CodeBlock` |
| `applyExclude` | `removeExcluded` |
| `regenHint` | `rebuildHint` |
| `rd` | `readOneBatch` (check usage first) |
| `pct` | `percent` |
| `mib` | `bytesPerMebibyte` |
| `stats` / `newStats` | `latencyStats` / `newLatencyStats` |
| `kindNames` | `failureKindNames` |
| `coverageMin` | `minimumCoverage` |
| `replay` (manisim) | `rerunSeed` |
| `runSharded` | `runInParts` |
| `mergeReports`, `mergeAssertion` | `combineReports`, `combineAssertion` |
| `allowList`, `embeddedAllow`, `allowFile` | `allowedImports`, `builtInAllowedImports`, `allowedImportsFile` |
| `suppressions` | `allowedExceptions` |
| `reporter`, `reportFunc` | `problemReporter`, `reportProblemFunc` |
| `checkAST` | `checkSyntaxTree` |
| `Analyzer` | `DeterminismAnalyzer` |

## 15. Short words used everywhere

These short words appear in many names. Write the full word when you add a new name.

| Short | Full |
|---|---|
| `cp` | `controlPlane` |
| `tp` | `topicPartition` |
| `kv` | `keyValue` |
| `tx` | `transaction` |
| `req`, `resp` | `request`, `response` |
| `cfg` | `config` |
| `ckpt` | `checkpoint` |
| `snap` | `snapshot` |
| `seg` | `segment` |
| `obj` | `object` |
| `idem` | `duplicateCheck` (the word "idempotent" is hard) |
| `dyn` | `dynamo` |
| `mem` | `memory` |
| `pk`, `sk` | `partitionKey`, `sortKey` |
| `ms`, `ns` | `milliseconds`, `nanoseconds` |
| `mw` | `middleware` |
| `ev` | `event` |
| `bg` | `background` |
| `na` | `unavailable` |

## 16. Hard words to avoid in new names

| Hard word | Plain word to use |
|---|---|
| tombstone | deleted marker |
| quiesce, settle | recover, wait until calm |
| trip | stop for restart |
| swarm | random settings |
| oracle | expected-result checker |
| shrink | minimize, make smaller |
| planted, plant | injected, put in on purpose |
| buggify | fault injection |
| sink | writer, target |
| bundle | dependencies, parts |
| envelope | wrapper |
| hole | gap |
| incarnation | boot number |
| liveness | progress |
| invariant | rule that must always be true |
| idempotent | safe to repeat |
| heal | repair, reconnect |
| skeleton | template |
| land | arrive |
