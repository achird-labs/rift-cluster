# Chapter 7 — The Verification Plane

> **Retired by D-71** (RFC-007 §3.2, #552), which records the scope decision; **D-74** is the
> entry that replaces this chapter's mechanism and supersedes D-32, D-37, D-38 and D-39.
> **Nothing this chapter used to describe is running code.** The fleet request-journal merge was
> removed in full: the per-writer `(node_id, seq, clear_gen)` shards, the k-way merge-on-read and
> its anti-entropy pull, the replicated clear generations, the vector `since` cursor and the
> merged SSE tail, the fleet-wide tail across every imposter, and the `numberOfRequests` fleet
> sum. RFC-001 §7.5.1 and §7.5.2, which specified them, carry their own retirement callouts.
>
> **What replaced it:** upstream Rift's own per-imposter `RequestJournal`, unwrapped.
> `GET /imposters/:port/requests` (and its `savedRequests` spelling) answers for **the node you
> reached**, with upstream's own scalar `?since=` cursor and its own `x-rift-next-index` /
> `x-rift-truncated` headers; `numberOfRequests` is that node's own count, not a fleet total. A
> test that needs fleet-wide verification pins a node or reads all of them (RFC-007 §3.3). If Rift
> ever grows a shared journal, it grows it in the engine, once, for every deployment shape.
>
> `Rift-Cluster-Partial` survives this removal but narrows: it is stamped only on the two reads
> that genuinely fan out across the fleet — `/_fleet/members` and `/_fleet/health` — never on a
> requests read, which now reaches exactly one node and knows it. The spaces listing fans out too
> and keeps reporting its own incompleteness in the body (`partial`, beside `unavailable`) — an
> enumeration refused by policy and one shortened by a slow peer are different facts, and a boolean
> header cannot tell them apart.
>
> **Where the still-true part went.** This chapter's second half was never about the journal:
> **proxyOnce owner claims** (D-40, seams U-16/U-17) are exactly-once *recording*, arbitrated on
> the ownership ring, and they stay. That section has been *moved* to [Chapter 6 — Flow
> State](06-flow-state.md), beside the ring and the fencing it depends on — and the section it was
> **Amended by D-66** (#529: a claim the cluster cannot serialize is refused `503`, never
> forwarded) went with it. Read both there.
>
> The file is kept because other documents link to this path. It has no body.
