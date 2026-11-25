-------------------------- MODULE SnapshotRetention --------------------------
(***************************************************************************)
(* Invariant 8 of AGENTS.md, for one run of the file tier in one shard:   *)
(*                                                                         *)
(*   A snapshot holds only durable state, and log retirement never drops  *)
(*   a record that recovery needs: whenever the process dies, the run's   *)
(*   durable history comes back (or, once it finished, it reads as        *)
(*   finished).                                                            *)
(*                                                                         *)
(* Every action names the Go code it stands for (engine/shard.go unless   *)
(* said otherwise). One shard-loop message is one action: the loop runs   *)
(* one message at a time, so that is the code's atomic step. I/O that    *)
(* runs in another goroutine (snapshot put, delete, WAL sync) is a step  *)
(* of its own, and so is the message that reports it back.               *)
(*                                                                         *)
(* Left out (see SKILL.md, scope): other runs' state (only their records  *)
(* are modelled, as "o"), the feed bound (Config.Feeds unset), failed    *)
(* log writes, load errors, recovery's own follow-up (recover.go pins),  *)
(* and the compaction trigger (CompactEvery, stale): compaction and its   *)
(* checkpoints may run at any point, which only adds behaviours.          *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANT MaxLSN \* bound on the log's length (a TLC state constraint)

VARIABLES
    log,       \* the shard's log, LSN i = log[i]: [k |-> "s"|"e"|"d"|"o"|"c", snap |-> Nat]
               \* s = the run's start record, e = its event, d = the event that
               \* finished it, o = another run's record, c = its checkpoint record
    synced,    \* WAL: records 1..synced are on disk (fsync done)
    durable,   \* shardLog.durable: what the shard has been told by ack()
    retired,   \* shardLog.retired: records below it may be gone (Retire)
    held,      \* shardLog.held: <<[lsn, kind |-> "cp"|"done"|"evict", snap]>>
    r,         \* the *run (fields as in engine/shard.go)
    io,        \* snapshot put in flight: [data, lsn, done] or NoIO
    file,      \* snaps "snap/<id>": the LSN its state covers, 0 = absent
    del        \* finishJob's delete: "none" | "running" | "deleted"

NoIO == [data |-> 0, lsn |-> 0, done |-> FALSE]

vars == <<log, synced, durable, retired, held, r, io, file, del>>

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
Inf == MaxLSN + 100

Init ==
    \* startRun: the start record is LSN 1 (r.lastLSN, r.startLSN = l.lsn).
    /\ log = << [k |-> "s", snap |-> 0] >>
    /\ synced = 0
    /\ durable = 0
    /\ retired = 1
    /\ held = << >>
    /\ r = [inMem |-> TRUE,     \* r.st != nil
            snap |-> 0,         \* LSN of r.snap (evicted state kept in memory), 0 = nil
            onDisk |-> FALSE,
            done |-> FALSE,
            finished |-> FALSE, \* finish(): gone from s.runs
            pinned |-> FALSE,   \* in s.pins
            marker |-> FALSE,   \* finish() waits for the done marker (addMarker)
            finishJob |-> FALSE,
            writing |-> FALSE,
            compactCP |-> FALSE,
            lastLSN |-> 1, startLSN |-> 1,
            cpLSN |-> 0, cpRec |-> 0, postSnap |-> 0,
            pendSnap |-> 0, pendFirst |-> 0]
    /\ io = NoIO
    /\ file = 0
    /\ del = "none"

(* pin(): the lowest LSN r still needs (ADR 0016). *)
Pin(rr) == IF rr.cpRec = 0 THEN rr.startLSN
           ELSE IF rr.postSnap /= 0 /\ rr.postSnap < rr.cpRec THEN rr.postSnap
           ELSE rr.cpRec

(* release(h), for the kinds this run holds. *)
Rel(h, rr) ==
    CASE h.kind = "cp" ->
            IF rr.done \/ h.snap < rr.cpLSN THEN rr
            ELSE LET same == h.snap = rr.pendSnap
                     post == IF same THEN rr.pendFirst
                             ELSE IF h.snap = rr.cpLSN THEN rr.postSnap
                             ELSE h.snap + 1
                 IN [rr EXCEPT !.cpLSN = h.snap, !.cpRec = h.lsn, !.postSnap = post,
                               !.pendSnap = IF same THEN 0 ELSE @,
                               !.pendFirst = IF same THEN 0 ELSE @]
      [] h.kind = "done" ->
            \* finish(): out of s.runs; s.pins keeps its records; the
            \* delete starts once the done marker is durable.
            [rr EXCEPT !.finished = TRUE, !.pinned = TRUE, !.marker = TRUE]
      [] h.kind = "evict" -> rr \* maybeEvict again: the Evict action

RECURSIVE ReleaseAll(_, _)
ReleaseAll(hs, rr) == IF hs = << >> THEN rr ELSE ReleaseAll(Tail(hs), Rel(Head(hs), rr))

Prefix(hs, n) == SelectSeq(hs, LAMBDA h: h.lsn <= n)
Suffix(hs, n) == SelectSeq(hs, LAMBDA h: h.lsn > n)

(* holdAt(): released at once if already durable, else kept in LSN order. *)
HoldAt(h, rr) ==
    IF durable >= h.lsn THEN <<held, Rel(h, rr)>> ELSE <<Append(held, h), rr>>

(* record(): the run's own event; pendFirst and postSnap remember the   *)
(* first record after a pending / checkpointed snapshot.               *)
Recorded(rr, n) ==
    [rr EXCEPT !.lastLSN = n,
               !.pendFirst = IF rr.pendSnap /= 0 /\ rr.pendFirst = 0 THEN n ELSE @,
               !.postSnap = IF rr.cpRec /= 0 /\ rr.postSnap = 0 THEN n ELSE @]

(* Busy: ack() releases everything durable in one message. *)
Quiet == Prefix(held, durable) = << >>

-----------------------------------------------------------------------------
(* An event applied to the run (eventReply -> record), possibly the one    *)
(* that finishes it (commands -> done -> hold hDone).                      *)
Event ==
    /\ Quiet
    /\ r.inMem /\ ~r.done
    /\ \E fin \in BOOLEAN :
         LET n == Len(log) + 1
             rr == Recorded(r, n)
         IN /\ log' = Append(log, [k |-> IF fin THEN "d" ELSE "e", snap |-> 0])
            /\ IF fin
               THEN LET x == HoldAt([lsn |-> n, kind |-> "done", snap |-> 0], [rr EXCEPT !.done = TRUE])
                    IN held' = x[1] /\ r' = x[2]
               ELSE r' = rr /\ UNCHANGED held
    /\ UNCHANGED <<synced, durable, retired, io, file, del>>

(* Another run's record in the same log. *)
Other ==
    /\ Quiet
    /\ log' = Append(log, [k |-> "o", snap |-> 0])
    /\ UNCHANGED <<synced, durable, retired, held, r, io, file, del>>

(* The committer's group commit reaches the disk. *)
Sync ==
    /\ synced < Len(log)
    /\ \E n \in (synced + 1)..Len(log) : synced' = n
    /\ UNCHANGED <<log, durable, retired, held, r, io, file, del>>

(* mAck -> ack(): durable moves up; held entries up to it are released.   *)
Ack ==
    /\ Quiet
    /\ durable < synced
    /\ \E n \in (durable + 1)..synced :
         /\ durable' = n
         /\ r' = ReleaseAll(Prefix(held, n), r)
         /\ held' = Suffix(held, n)
    /\ UNCHANGED <<log, synced, retired, io, file, del>>

(* maybeCompact -> checkpointLive: snapshot a run where it stands. *)
CheckpointLive ==
    /\ Quiet
    /\ ~r.finished /\ ~r.done /\ ~r.writing /\ r.inMem
    /\ durable >= r.lastLSN
    /\ r' = [r EXCEPT !.writing = TRUE, !.compactCP = TRUE,
                      !.pendSnap = r.lastLSN, !.pendFirst = 0]
    /\ io' = [data |-> r.lastLSN, lsn |-> r.lastLSN, done |-> FALSE]
    /\ UNCHANGED <<log, synced, durable, retired, held, file, del>>

(* maybeCompact, an evicted run: log its checkpoint again, further ahead. *)
Relog ==
    /\ Quiet
    /\ ~r.finished /\ ~r.done /\ ~r.writing /\ ~r.inMem
    /\ r.onDisk /\ r.cpRec /= 0
    /\ LET n == Len(log) + 1
           x == HoldAt([lsn |-> n, kind |-> "cp", snap |-> r.cpLSN], r)
       IN /\ log' = Append(log, [k |-> "c", snap |-> r.cpLSN])
          /\ held' = x[1] /\ r' = x[2]
    /\ UNCHANGED <<synced, durable, retired, io, file, del>>

(* maybeCompact: retire what no live run, nor a finished run whose       *)
(* snapshot is still being deleted (s.pins), needs.                       *)
Compact ==
    /\ Quiet
    /\ LET live == IF r.finished THEN Inf ELSE Pin(r)
           pins == IF r.pinned THEN Pin(r) ELSE Inf
           bound == Min(durable + 1, Min(live, pins))
       IN /\ bound > retired
          /\ retired' = bound
    /\ UNCHANGED <<log, synced, durable, held, r, io, file, del>>

(* maybeEvict: hold until durable, then snapshot and drop the state.     *)
Evict ==
    /\ Quiet
    /\ r.inMem /\ ~r.done /\ held = << >>   \* r.holds == 0
    /\ IF durable < r.lastLSN
       THEN /\ held' = Append(held, [lsn |-> r.lastLSN, kind |-> "evict", snap |-> 0])
            /\ UNCHANGED <<r, io>>
       ELSE /\ r' = [r EXCEPT !.snap = r.lastLSN, !.inMem = FALSE,
                              !.pendSnap = r.lastLSN, !.pendFirst = 0,
                              !.writing = TRUE]
            /\ IF r.writing
               THEN UNCHANGED io
               ELSE io' = [data |-> r.lastLSN, lsn |-> r.lastLSN, done |-> FALSE] \* writeSnapshot
            /\ UNCHANGED held
    /\ UNCHANGED <<log, synced, durable, retired, file, del>>

(* load -> loaded: from r.snap at once, or from the snapshot file.       *)
Load ==
    /\ Quiet
    /\ ~r.inMem /\ ~r.done
    /\ r' = [r EXCEPT !.inMem = TRUE, !.snap = 0, !.onDisk = FALSE]
    /\ UNCHANGED <<log, synced, durable, retired, held, io, file, del>>

(* The snapshot put (snaps.Put, an atomic replace) completes.            *)
PutDone ==
    /\ io /= NoIO /\ ~io.done
    /\ file' = io.data
    /\ io' = [io EXCEPT !.done = TRUE]
    /\ UNCHANGED <<log, synced, durable, retired, held, r, del>>

(* mSnapStored. *)
SnapStored ==
    /\ Quiet
    /\ io /= NoIO /\ io.done
    /\ LET r1 == [r EXCEPT !.writing = FALSE]
       IN IF r1.done
          THEN \* Deleting only now orders the delete after the write.
               /\ r' = [r1 EXCEPT !.finishJob = FALSE]
               /\ del' = IF r1.finishJob THEN "running" ELSE del
               /\ io' = NoIO
               /\ UNCHANGED <<log, held>>
          ELSE LET n == Len(log) + 1
                   r2 == [r1 EXCEPT !.compactCP = FALSE]
                   x == HoldAt([lsn |-> n, kind |-> "cp", snap |-> io.lsn], r2)
                   r3 == x[2]
               IN /\ log' = Append(log, [k |-> "c", snap |-> io.lsn])
                  /\ held' = x[1]
                  /\ IF r3.inMem \/ r3.snap = 0
                     THEN r' = r3 /\ io' = NoIO
                     ELSE IF io.lsn = r3.lastLSN
                          THEN r' = [r3 EXCEPT !.snap = 0, !.onDisk = TRUE] /\ io' = NoIO
                          ELSE \* evicted again with newer state: writeSnapshot
                               /\ r' = [r3 EXCEPT !.writing = TRUE]
                               /\ io' = [data |-> r3.snap, lsn |-> r3.lastLSN, done |-> FALSE]
                  /\ UNCHANGED del
    /\ UNCHANGED <<synced, durable, retired, file>>

(* finish -> addMarker -> start(): the done marker is durable.           *)
MarkerDurable ==
    /\ Quiet
    /\ r.marker
    /\ r' = [r EXCEPT !.marker = FALSE, !.finishJob = r.writing]
    /\ del' = IF r.writing THEN del ELSE "running"
    /\ UNCHANGED <<log, synced, durable, retired, held, io, file>>

(* finishJob: snaps.Delete("snap/<id>") completes.                       *)
DeleteDone ==
    /\ del = "running"
    /\ file' = 0
    /\ del' = "deleted"
    /\ UNCHANGED <<log, synced, durable, retired, held, r, io>>

(* mSnapDeleted: out of s.pins. *)
SnapDeleted ==
    /\ Quiet
    /\ del = "deleted"
    /\ del' = "none"
    /\ r' = [r EXCEPT !.pinned = FALSE]
    /\ UNCHANGED <<log, synced, durable, retired, held, io, file>>

Next == \/ Event \/ Other \/ Sync \/ Ack \/ CheckpointLive \/ Relog \/ Compact
        \/ Evict \/ Load \/ PutDone \/ SnapStored \/ MarkerDurable
        \/ DeleteDone \/ SnapDeleted

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* What recovery (engine/recover.go) sees if the process dies now, with  *)
(* the log's first k records surviving (k >= synced: unsynced records    *)
(* may survive too, as a prefix) and the records below c gone. Retire    *)
(* drops whole segments, oldest first, some time after it is asked to:  *)
(* any c up to `retired` can be what is on disk.                         *)

Kept(i, k, c) == i >= c /\ i <= k
Own(i) == log[i].k \in {"s", "e", "d"}
Found(k, c) == \E i \in 1..k : Kept(i, k, c) /\ log[i].k \in {"s", "c"}
HasStart(k, c) == \E i \in 1..k : Kept(i, k, c) /\ log[i].k = "s"
CpLSN(k, c) == LET cs == {log[i].snap : i \in {j \in 1..k : Kept(j, k, c) /\ log[j].k = "c"}}
            IN IF cs = {} THEN 0 ELSE CHOOSE m \in cs : \A x \in cs : x <= m
DoneBy(k) == \E i \in 1..k : log[i].k = "d"

(* recover(): the run comes back with every event up to k (from the     *)
(* snapshot and the records after it), or, if it had finished by k,      *)
(* reads as finished. It is never refused as tampered.                   *)
RecoversAt(k, c) ==
    LET snap == file /= 0
    IN /\ k >= 1 /\ ~Found(k, c) => DoneBy(k)
       /\ Found(k, c) /\ snap => file >= CpLSN(k, c)
       /\ Found(k, c) /\ ~snap /\ ~HasStart(k, c) => DoneBy(k)
       /\ Found(k, c) /\ snap =>
            \A i \in (file + 1)..k : Own(i) => Kept(i, k, c)

Recoverable == \A k \in synced..Len(log), c \in 1..retired : RecoversAt(k, c)

(* A snapshot never covers what a crash could lose. *)
SnapshotDurable == file <= synced /\ (io /= NoIO => io.data <= durable)

(* An evicted run whose state is only on disk finds its latest state.    *)
LoadSeesLatest == (~r.inMem /\ ~r.done /\ r.snap = 0 /\ io = NoIO) => file = r.lastLSN

(* Writes of one run are serialized (the writing flag).                  *)
OneWrite == (io /= NoIO) <=> r.writing

TypeOK == /\ durable <= synced /\ synced <= Len(log)
          /\ retired <= durable + 1

StateBound == Len(log) <= MaxLSN
=============================================================================
