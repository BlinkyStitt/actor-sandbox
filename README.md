we want an example that has stream tracking and postgres transactions for deduping.
i think thats a pattern that might be useful generally.

it should do things highly parallelized. i don't want to set a bunch of channels up and then have arbitrary buffer sizes. i want to just spawn a million futures and let the scheduler figure it out. i think that will be way simpler right.

postgres is going to need a semaphore so that we don't try to open a million connections at once. we still want a connection pool and pool connect timeouts. i think a semaphore in front of postgres is going to be useful because we want to hold our connections open for as little time as possible and we do compute between multiple queries on the same task.

activemq is going to need some thought. i don't love how the library needs the session to be mut. we also need a pattern for naming our producers and subscribers. and for reconnecting on error. and for connecting to 3 nodes

we also need to collect errors. if we are just spawning things all over the place, its too easy to lose an error. so we need a cancellation token thats passed around. maybe a helper spawn function that logs an error
