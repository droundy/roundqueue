[![Build Status](https://travis-ci.org/droundy/roundqueue.svg?branch=master)](https://travis-ci.org/droundy/roundqueue)

# Round queue

The `rq` program implements a very simple queuing system.  This system
favors convention over configuration, and hence requires zero
configuration, but does impose a constraint on how you set up your
system.  In particular, all users who use `rq` must have home
directories within a single parent directory (this is typically
`/home`, but could be some other directory).  Also, `rq` requires that
home directories be shared across any computers involved in the
cluster, and mounted at the same point on each computer.

This program is designed to be very easy to configure and use, and
should be reliable.  However, it is *not* designed to be efficient
when there are large numbers of jobs or users.  It is intended for a
small computing cluster, where $O(N^2)$ operation is acceptable.

## Installing rq

To install rq, you must have rust installed (see [https://rustup.rs]()).
Then just type
```
cargo install roundqueue
```

## Using rq

Each user running `rq` should run `rq daemon` on each compute node on
which they wish jobs to be able to run.  On my cluster, I do this on
users' behalf using `systemd` to start them up.  This means that with
$N$ users on $M$ nodes, you will have $NM$ daemons running.

To submit a job, you can run something like

    rq run --job-name my-compute-job ./compute --flag-for-compute --other-flag

in the directory where you want this command to run.  By default, jobs
are assumed to run on a single cpu.  You can see the running (and
waiting to run) jobs by simply executing

    rq

You can cancel a job by executing

    rq cancel my-compute-job

Finally, you can see how busy the nodes are with

    rq nodes

## How does it work

The security design of `rq` involves never communicating directly over
the network.  Instead, communication is entirely done by creating and
reading files in the home directory (which is presumably shared over
the network).  This simplifies the design of `rq`, and presumably
increases its security, provided your file system is secure.

It is not necessary for all users to have the `rq daemon` running on
all nodes, however, any nodes that a given user does not have the
daemon running on will not run that user's jobs.

Each users job information is stored in the `$HOME/.roundqueue/`
directory.  Each job is a separate file in a subdirectory there.

### rq run

Submitting a job consists of creating a single JSON file in
`$HOME/.roundqueue/waiting/`.  That's all.
