# riak dt

## WHAT?

Currently under initial development, riak_dt is a platform for
convergent data types. It's built on
[riak core](https://github.com/basho/riak_core) and deployed with
[riak](https://github.com/basho/riak). All of our current work is
around supporting fast, replicated, eventually consistent counters
(though more data types are in the repo, and on the way.) This work is
based on the paper -
[A Comprehensive study of Convergent and Commutative Replicated Data Types]
(http://hal.inria.fr/docs/00/55/55/88/PDF/techreport.pdf) - which you
may find an interesting read.


## WHY?

Riak's current model for handling concurrent writes is to store
sibling values and present them to the client for resolution on
read. The client must encode the logic to merge these into a single,
meaningful value, and then inform Riak by doing a further
write. Convergent data types remove this burden from the client, as
their structure guarantees they will deterministically converge to a
single value. The simplest of these data types is a counter.

## HOW?

This code is currently unsupported by Basho, and is in active
development so _use at your own risk_. But, with that warning ringing
in your ears, we do encourage you to have a go at building and running
riak\_dt.

Here is how to deploy riak_dt, and count some things.

### Build from source

You must build the [riak_dt branch](https://github.com/basho/riak) of
riak from source.

    > git clone riak
    > git checkout origin/riak-dt
    > make devrel

This should have fetched all the riak dependancies from github,
compiled them, and created four release directories under the `dev`
directory. Now start your nodes and build your cluster.

    > for d in dev/*; do $d/bin/riak start; done
    > for d in dev/*; do $d/bin/riak ping; done
    > for d in dev/dev{2,3,4}; do $d/bin/riak-admin cluster join 'dev1@127.0.0.1'; done
    > dev/dev1/bin/riak-admin cluster plan
    > dev/dev1/bin/riak-admin cluster commit
    > dev/dev1/bin/riak-admin ringready

    TRUE All nodes agree on the ring ['dev1@127.0.0.1','dev2@127.0.0.1',
                                  'dev3@127.0.0.1','dev4@127.0.0.1']


If that worked and you see all four nodes in the ring then proceed to…

### Count things

At the moment we only have a simple HTTP API for counters. There is no
explicit `create` operation for a counter. If you increment a counter
that doesn't exist it will be created for you.

Create / increment a counter named `a`:

    curl -X POST localhost:8091/counters/a -d'1'

Read it from any and all nodes:

    curl localhost:809[1-4]/counters/a

Increment by a specific amount:

    curl -X POST localhost:8094/counters/a -d'10'
    curl localhost:809[1-4]/counters/a

You can decrement a counter, too:

    curl -X POST localhost:8092/counters/a -d'-1'
    curl localhost:809[1-4]/counters/a
    curl -X POST localhost:8092/counters/a -d'-8'
    curl localhost:809[1-4]/counters/a

So, `counters` is the resource prefix for all counters, and the next
part in the path is the key for your counter (in this case `a`).
Finally there is the action, with payload. `POST` to update, `GET` to
read.

## CAVEATS

One of the fundamental problems with state based convergent data types
is the accrual of garbage over time. In the case of counters this
garbage comes from actor id churn. Each time a riak vnode starts up it
gets a new actor id. The counters are based on version vectors, so the
more actors incrementing the counter, the bigger the data gets. We're
working to address this. For this reason alone don't use this in
production.

See the
[issues list](https://github.com/basho/riak_dt/issues?state=open) for
how we plan to proceed. Please checkout the repo, build it and play
with it, and let us know what you think.
