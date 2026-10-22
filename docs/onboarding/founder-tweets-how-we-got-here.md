# Founder tweets. 20 days, one 2-tweet thread per day

Who this is for: people who run Kafka in production. They know brokers, partitions, replicas, producers, consumers, and offsets. They do not know how our code is built inside, so each thread explains its own words.

How to use this file: post one day per day. Each day stands alone. Tweet 1 says the pain and what we built instead. Tweet 2 gives the number, the proof, or the catch.

## Day 1. Who is building this

Tweet 1:
I'm Arun. I'm building Manifester, a system that speaks Kafka but stores records in Amazon S3 instead of on broker disks. Over the next few weeks I'll share what we built, what we measured, and what broke.

Tweet 2:
I'll share the failures with the numbers. If a claim about a system like this cannot be checked, it is just an ad. Follow along if you run Kafka and storage is your pain.

## Day 2. Pain: more storage means more brokers

Tweet 1:
In Kafka, needing more storage usually means adding brokers and rebalancing partitions, then waiting. In Manifester, brokers store nothing long term. Records go to S3, so storage grows in S3 and brokers are only compute.

Tweet 2:
Your producers still use normal Kafka clients. What changes is the workday. Add a broker when you need more compute. Remove one when traffic drops. Lose one and you lose no records.

## Day 3. Pain: an ack you cannot trust

Tweet 1:
An ack should mean your record is safe. In Manifester an ack means this, in this order. The record file is in S3. Our metadata service saved which offsets those records got. Only then does the producer hear yes.

Tweet 2:
That order costs time, hundreds of milliseconds. It buys a promise with no footnote. If you got an ack, the bytes are in S3 and the offsets exist.

## Day 4. Pain: a dead broker becomes a long night

Tweet 1:
A Kafka broker dies and the work starts. Replicas catch up, partitions move, a disk needs replacing. Manifester brokers keep no records, so a dead broker is a reconnect for clients, not a recovery job for you.

Tweet 2:
Any broker can take new records, group them into a file, write the file to S3, and save the offsets. Kill one halfway and another broker does the same job. Nothing moves, because nothing lived there.

## Day 5. Pain: one request per record is too slow

Tweet 1:
Sending every record to S3 on its own would be slow and the request bill would be silly. So we group records from many partitions into one file. We close the file at 16 MB, or after 200 milliseconds, then save it to S3.

Tweet 2:
Grouping is where the speed comes from. Waiting up to 200 ms to fill a file is where much of the delay comes from. Same choice, both results. We publish both.

## Day 6. Pain: two brokers must not pick the same offset

Tweet 1:
An offset is a record's number inside a partition. If many brokers hand out numbers, you get clashes and gaps. In Manifester one metadata service hands out offsets, and only when it saves a finished file of records.

Tweet 2:
That rule is boring and that is the point. Brokers can race, retry, or die in any order. Numbers still come from one place, in one order, with no reuse.

## Day 7. Pain: the old broker that comes back

Tweet 1:
Kafka operators know the zombie problem. An old broker wakes up and acts like it still owns a partition. Kafka fences it off. We removed that whole job, because our brokers never own partitions or offsets in the first place.

Tweet 2:
A woken broker can still save a file of records to S3. It cannot hand out wrong offsets, because it never hands out offsets at all. The metadata service does that, for every file, in order.

## Day 8. Pain: retries can write a record twice

Tweet 1:
Producers retry when an answer gets lost, and retries can write the same record twice. Kafka solves this with idempotent producers. We chose not to support that feature, and we say so in writing before you have to ask.

Tweet 2:
What we do protect is our own save step. Saving the same record file twice never creates duplicate records. Retrying the whole produce can. That is the deal: simpler and faster saves, at-least-once delivery.

## Day 9. Pain: our first design hit a wall

Tweet 1:
Our first metadata service saved each record file with an AWS DynamoDB transaction, one deal at a time. It stopped at about 240,000 messages per second. We measured the wall, published the number, and replaced the design.

Tweet 2:
Why the wall? With 288 partitions, nearly every file holds records for many partitions, so saves could not line up and run side by side. One at a time means the limit is deals per second, and tuning does not move that.

## Day 10. Fix: one ordered list, no overwriting

Tweet 1:
The new metadata service keeps one ordered list of saves. Each save takes the next empty slot in the list, and a slot that is already filled cannot be overwritten. Every copy of the service reads the list from the top.

Tweet 2:
That filled slot is the safety. No lease to renew and no leader vote to argue about. If your save is in the list, every copy sees it in the same place and gives the records the same offsets.

## Day 11. Fix: pick the database by testing both

Tweet 1:
Our metadata service needs a small, fast database on its own disk to track which records sit where. We tested two, Pebble and bbolt, on the same work. Pebble handled 1,100 to 1,330 record files per second. bbolt did 300 to 430.

Tweet 2:
bbolt also used more memory, so we deleted it. The test, the workload, and the numbers are in a short doc anyone can rerun. A choice you cannot retest is just taste.

## Day 12. Fix: shuffle the order 100,000 times

Tweet 1:
Saves, retries, and crashes can land in any order, and the bad order is the one you did not think of. So we ran our metadata service 100,000 times with the order shuffled by a random seed, and compared each run with a tiny, simple version.

Tweet 2:
Zero mismatches. The simple version is short enough to read in one sitting, which is why it counts. And any failing run can be repeated exactly, because the seed fixes every random choice.

## Day 13. Fix: run the whole system on a laptop, with crashes

Tweet 1:
You cannot crash production 2,000 times to see what happens. So we built a fake copy of our whole system that runs on one laptop. It fakes S3 delays, lost answers, slow disks, and dead brokers, all picked by one random seed.

Tweet 2:
Change the seed and you get a new history. Keep the seed and the same failure repeats step by step. A bug stops being a story in a log and becomes a run you can start again.

## Day 14. Fix: plant mistakes and see if tests catch them

Tweet 1:
A test suite that never fails is not proof. So we plant known mistakes in our broker on purpose. Hand out offsets too early. Skip an entry in the save list. Forget a file before its time. Then we check that the tests go red.

Tweet 2:
If a planted mistake stays green, the suite has a blind spot, and we fix the suite. Confidence you have not tried to break is just a feeling.

## Day 15. Proof: 2,000 random runs

Tweet 1:
One big test run tried 2,000 random histories of our metadata service, 50.7 million steps in total. Leaders died, standbys took over, saved state was restored, and old list entries were cleaned up, all under random delays.

Tweet 2:
This costs no cloud bill. Fake time jumps from event to event, so an hour of system time takes seconds. The slow part of testing was never the machines. It was the waiting.

## Day 16. Proof: the AWS run, and its limits

Tweet 1:
Textbook Kafka stores every byte three times and pays to copy it between zones. On AWS, Manifester stored each byte once in S3 and held about 998,000 messages per second for 4 minutes, across 288 partitions. Latency at p99 was 756 ms.

Tweet 2:
Now the limits, in the same thread. It was one run. Each stage ran 1 to 4 minutes with one sample. That shows the write path can reach near 1M per second. It is not a number to size your next year on.

## Day 17. Pain: the leader dies while everyone waits

Tweet 1:
Metadata services fail at the worst time, under load, while producers wait on them. We killed the main writer of ours at 500,000 messages per second, on purpose. Producers paused about 8 seconds, then answers resumed. Zero errors.

Tweet 2:
The standby was ready because it had been reading the same save list all along. Takeover meant saying "you lead now" to a copy that already had the state. Failover is a data problem first, a timer problem second.

## Day 18. Pain: an idle consumer that costs money

Tweet 1:
An idle consumer should cost almost nothing. Ours asked our metadata service "where do I read next?" about 20,600 times per second while reading nothing. Our read answer returns right away, so consumers ask again at once, and every ask costs us a database read.

Tweet 2:
With 64 idle consumers that rate reaches about 48,000 asks per second. Our fix comes first on the list: let a read wait for new records instead of answering empty, and remember the answer for a short time.

## Day 19. Pain: reads from S3 are a counting problem

Tweet 1:
Reading from S3 looks cheap until you count requests. We fetch one piece of a file per batch of records. At 1 GB per second of reading that is about 6,500 requests per second, close to where a new S3 area starts saying slow down.

Tweet 2:
So reads are our next build. Combine nearby batches into one request, keep hot data on the broker for a while, and only then add record deletion, because deleting a file the index still points to would break reads.

## Day 20. Proof: watch the design change its mind

Tweet 1:
We keep a written list of design choices, 53 so far. When a new choice replaces an old one, we mark the old row as replaced and keep its reasons. You can read why transactions went away and what measurement killed them.

Tweet 2:
I'm Arun, and I'm building Manifester, Kafka records on S3, in public. Next we build the read path. Follow if you want the numbers when they improve, and a post on the days they do not.
