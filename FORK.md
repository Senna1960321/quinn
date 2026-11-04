# This fork

sail's fork of [quinn-rs/quinn](https://github.com/quinn-rs/quinn). The
`sail` branch tracks upstream's `0.11.x` release line, which is the
version sail uses. sail takes `quinn-proto` and `quinn` from it, at one
revision, through `[patch.crates-io]`; `quinn-udp`, which `quinn` depends
on by path, then comes from here too, unpatched. Besides the patches
below, the crates follow upstream's `0.11.x` line, with selected fixes
ahead of the latest release on crates.io (see
[Ahead of the release](#ahead-of-the-release)).

Upstream is merged into `sail`, never rebased onto it, and the branch is
never force-pushed: sail pins revisions of it. Moving to a new upstream
minor line (0.12) is a merge of that line, done when sail moves its
`quinn` dependency.

## Patches

- **ClientHello scattering** (`quinn-proto`): the client's first Initial
  packets are decryptable by anyone, and a ClientHello at offset 0 in one
  CRYPTO frame gives its SNI to any middlebox. As Chrome's chaos
  protection and quic-go do, the client splits the hello at random points
  (always inside the server name and the ECH extension type), sends the
  pieces in shuffled order with PING and PADDING frames between them, and
  retransmits lost pieces as ordinary CRYPTO data. Servers are
  unaffected. It is on by default;
  `TransportConfig::scramble_client_hello(false)` turns it off.

- **BBR's window is bounded in Startup** (`quinn-proto`), from upstream
  pull request #2798 (by poka-IT, not yet merged; same licence as quinn).
  Startup grew the window while `cwnd_gain < target_window`, a gain
  compared with a byte count, so on a connection that stays app-limited
  the window followed the bytes acked without bound (over 300 MB was seen
  on one TUIC connection), and a sender could flood a bottleneck.
  It now grows while `cwnd < target_window`; the bandwidth filter takes
  app-limited samples only when they raise it, and no zero-rate ones.

- **BBR's minimum RTT expires** (`quinn-proto`). BBR took the
  connection's lifetime minimum RTT (`RttEstimator::min`), which never
  rises, and ProbeRtt could not refresh it. On a long-lived connection,
  which a Hysteria2 client keeps for all its traffic, one low sample
  (an undelayed path, a few packets that a reordering link let through
  early) sized the window for good: when the path's RTT later rose, the
  window stayed at a few packets and throughput collapsed for the rest
  of the connection's life (0.5 Mbit/s on a 50 Mbit/s, 150 ms path, with
  min_rtt still 42 us). As quiche keeps it, the minimum is now the
  lowest of the latest RTT samples (`RttEstimator::latest`, added) over
  a 10 s window: it expires 10 s after it was set, the next ACKs' sample
  replaces it and starts ProbeRtt, and leaving ProbeRtt stamps it anew.
  Upstream `main` still uses `rtt.min()`; the BBRv3 port (upstream
  #2481), which has a windowed min-RTT filter, is to replace this.

- **BBR samples the delivery rate** (`quinn-proto`). BBR's bandwidth was
  the bytes acked between two ACKs over the time between them, which
  reads low when one ACK covers many packets and high when ACKs bunch.
  As quiche's `BandwidthSampler` and Linux's `tcp_rate.c` take it
  (draft-cheng-iccrg-delivery-rate-estimation), each ACK now gives one
  sample from the newest packet it acknowledges: the bytes delivered
  since that packet was sent, over the longer of the time it took to
  send them and to acknowledge them, and none over less than the minimum
  RTT. For that the controller is told of each packet sent and lost, and
  of the number and space of each one acked (`Controller::on_packet_sent`,
  `on_packet_lost`, and `on_ack` with upstream's arguments); `SpaceId`
  is exported. Two allowances come with it, both measured on sail#11:
  - The BDP is taken over at least 2 ms of RTT. At microseconds of RTT
    the window of a right estimate is a few dozen KB, and quiche's
    ack-aggregation allowance, which this BBR has, gave it no more than
    0.9 MB while ACKs came in batches: uploads ran 19-27% below the old
    estimator's.
  - The send window, twice the congestion window, is held open up to
    what the peer allows to be outstanding, but at most 20 ms of
    sending at the congestion window's rate (over at least 2 ms of RTT).
    Without it uploads at microseconds of RTT stayed 4-6% below; with
    the peer's allowance alone, a server's thousand slow connections
    used up the window budget.

  With these, sail's Hysteria2 and TUIC at 150 and 300 ms RTT and at 2%
  loss uploaded to a sing-box server at 1.07-1.23 times what a sing-box
  client did, and between sail's own ends sent 1.19-2.5 times what
  sing-box did between its own. No unshaped, 1% loss, reordering or
  deep-queue cell lost more than 3% against the old estimator, but for
  unshaped TUIC uploads to sing-box under a send window cap below 32
  MiB, at 0.92-0.98 in four runs: the server's credit held both
  estimators there alike, with the same bytes buffered and in flight.
  Upstream `main` still has the old estimator; a BBRv3 port (upstream
  #2481) would replace it.

- **Up to 16 segments per send** (`quinn`). A connection handed the
  socket at most 10 datagrams per segmented send (`MAX_TRANSMIT_SEGMENTS`),
  a number upstream chose to keep `poll_transmit`'s buffer small. More
  per send costs the sender less CPU, but past 16 the upload on a long
  path got slower. A Hysteria2 client uploading to sing-box on Linux,
  medians of 6 rounds on loopback and at 50 ms RTT and of 8 at 150 ms
  (netem, no rate limit), 1 GiB a round; 10 was run twice in every job:

  | segments | loopback Mbit/s | CPU s/GiB | 150 ms Mbit/s | CPU s/GiB | 50 ms Mbit/s | CPU s/GiB |
  |---|---|---|---|---|---|---|
  | 10 | 3545, 3487 | 2.90 | 441, 454 | 1.86 | 1151, 1129 | 1.96 |
  | 16 | 3707 | 2.58 | 443 | 1.60 | 1118 | 1.69 |
  | 20 | 3800 | 2.54 | 427 | 1.60 | 1114 | 1.64 |
  | 32 | 3747 | 2.45 | 431 | 1.43 | 1108 | 1.50 |

  The two runs of 10 differ by 2.8% at 150 ms and 2.0% at 50 ms; against
  their pooled medians (449 and 1135), 16 is 1.3% and 1.5% lower, inside
  that, and 20 and 32 are 4-5% lower at 150 ms. Downloads did not change
  at any count. 64, Linux's own limit, fails: a segmented send is one
  UDP datagram to the kernel, and 64 segments of 1452 bytes do not fit
  one. The count is the least of 16, what the socket can segment, and
  what fits a datagram at the current MTU, (65535 - 8 - 40) / MTU: 7 at
  a 9000-byte MTU.
  The rows for 16, 20 and 32 were measured with the next patch applied.
  The cost is memory, which the next patch takes back: the send buffer
  grows to about 17 segments (25 KB at 1452) where it was 11 (16 KB).
  Upstream `main` still has 10.

- **An idle connection gives its send buffer back** (`quinn`). Upstream
  keeps a connection's send buffer at the size of its largest batch for
  the connection's life. With 16 segments that is about 25 KB against
  16 KB, on a server whose clients are mostly idle or only acknowledge
  what they receive. The buffer now shrinks to one MTU when a connection
  has ended 8 transmit passes in a row with nothing left to send and
  without a segmented send among them. Not at the first such pass: a
  connection sending at full rate ends most passes blocked by congestion
  control or pacing, and would reallocate for every batch. The next
  transmit is unaffected: each one reserves what it writes.

  8 is a judgement. Measured with it at 32 segments, a Hysteria2 client
  that uploaded and then downloaded 1 GiB shrank and regrew its buffer
  7 to 18 times on loopback and 41 to 54 times at 150 ms RTT, against
  some 23,000 batches per GiB. The patch by itself costs nothing that
  shows: at 10 segments an upload ran at 3537 Mbit/s and 2.88 CPU
  seconds per GiB on loopback where the unpatched crate had 3545 and
  2.90, and at 446 Mbit/s at 150 ms RTT against 449.

  A sail Hysteria2 server on glibc with 1,000 clients that had each
  downloaded 4 MiB and gone idle held, per connection, 162-174 KB
  unpatched at 10 segments, 147-152 with this patch at 10, 153-163 at
  16 and 174-191 at 32, where 32 without this patch held 259-271. glibc
  keeps the freed space for reuse and the resident size stays there;
  under mimalloc, at 32 segments, it fell by about 15% within 20 s of
  the last download (one run).

- **Connection credit on receipt** (`quinn-proto`), off unless
  `WindowTuning::credit_on_receipt(true)` is set with a budget. Upstream,
  like quic-go, Chromium and Envoy, gives connection-level credit
  (MAX_DATA) back only as stream data is read. A stream nobody reads
  holds up to its stream window of the connection's window, and two or
  three such streams leave the others no credit at all (sail#18). With
  the option, the credit for stream data is given as it arrives, lent out
  of the window budget, and repaid as the stream is read, stopped, reset
  or freed, or when the connection is dropped. A stalled stream then
  holds its own window and no more. Where the budget refuses, credit
  comes back on read as upstream's does. No reference implementation
  does this; nothing changes on the wire.

## Ahead of the release

The latest full upstream merge is the `0.11.x` line's tip as of
2026-10-22. The crates include the upstream fixes below beyond their
releases on crates.io, with three later fixes selectively backported
on 2026-11-04:

- `quinn` is 0.11.12 and these fixes:
  - `bcd86796`, `handshake_data` is cancellation-safe: retrying it after
    a dropped call no longer panics.
  - `12d7f077`, a connection whose transmit fails is closed and its
    state cleaned up before the error is returned.
  - `16ecc700`, a finished stream's waker is removed from
    `blocked_readers` instead of going stale there.
  - `f97fb5aa`, `wait_idle` wakes when the endpoint driver is dropped
    (backport of upstream #2901).
  - `c6d94ce4`, no handshake starts once the endpoint driver is gone.
  - `6e5b60d9`, accepting releases the endpoint lock while TLS is set up.
  - `8675d095`, ignored receive errors count toward the receive budget.
  - `c2a47f6d` (backport), UDP receive errors other than `WouldBlock`
    reach the endpoint instead of being retried in the Tokio and
    async-io runtime loops.
- `quinn-proto` is 0.11.19 and these fixes:
  - `50f5519c`, an illegal ordered read leaves the receive state as it
    was.
  - `5303ccc1`, switching a stream to unordered drops duplicates of
    what was already read.
  - `e716bf53`, a disabled idle timeout is honoured when accepting.
  - `9ba03eb5`, transport parameters in nonminimal integer encodings
    are accepted.
  - `e367976e`, only a datagram frame larger than the advertised limit
    is a protocol violation; one that fits it but not the receive
    buffer's memory is dropped.
  - `98c9372f`, a close reason is cut to fit the error code's width.
  - `5aebe8f7` and the commits before it, acceptance in three phases,
    which `6e5b60d9` builds on.
  - `4b52785a` (backport), stopping a stream after receiving its reset
    no longer gives connection-level flow-control credit twice.
  - `77a51bdc` (backport), MTU probes wait for path validation, so they
    cannot bypass the anti-amplification limit on an unvalidated path
    during migration.
- `quinn-udp` is 0.5.16 and `b86fc003` (`IPV6_V6ONLY` is queried only on
  IPv6 sockets), and one line of a test.

## When it could go away

When upstream quinn scatters the client's first CRYPTO data itself (no
such change is in upstream `main` or `0.11.x` today). sail would then set
upstream's option and drop the `quinn-proto` entry in `[patch.crates-io]`;
the BBR patches go with the BBRv3 port, and the `quinn` entry when
upstream raises its segment count or makes it configurable.
