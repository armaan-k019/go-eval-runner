The six records stamped 07:08 to 07:10 were taken before the set-based reporting change and with Postgres reached
through `localhost:5432`, which on this host goes through Docker's userland port proxy (`docker-proxy` was using
about 70% of a core during those runs). From 07:14 on, the harness connects to the Postgres container's bridge IP
directly. Compare like with like: the `overhead-batched` / `overhead-unbatched` pairs from 07:14 on are the A/B.
