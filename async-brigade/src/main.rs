#![recursion_limit="256"]

use futures::try_join;
use std::time::Instant;
use tokio::net::UnixStream;
use tokio::prelude::*;
use utils::{Stats, UsefulDuration};

struct Pipe {
    read: UnixStream,
    write: UnixStream,
}

fn pipe() -> Result<Pipe, std::io::Error> {
    let (read, write) = UnixStream::pair()?;
    Ok(Pipe { read, write })
}

// TODO: Use more complex busywork to stress CPU state restore from ctxt switch
fn busywork(buf_ptr: *const u8, num_iters: usize) {
    for _check in 0..num_iters {
        assert_eq!(unsafe { buf_ptr.read_volatile() }, b'*');
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const NUM_TASKS: usize = 512;
    const NUM_CHAINS: usize = 16;
    const NUM_CHECKS: usize = 65536;
    const NUM_WARMUP_REPS: usize = 100;
    const NUM_REPS: usize = 10000;

    let mut pipes = Vec::with_capacity(NUM_CHAINS);

    for _chain in 0..NUM_CHAINS {
        let Pipe { read: mut upstream_read, write: first_write} = pipe()?;
        for _i in 0..NUM_TASKS/NUM_CHAINS {
            let next_pipe = pipe()?;
            let mut downstream_write = next_pipe.write;
            tokio::spawn(async move {
                let mut buf = [0_u8; 1];

                // Establish 'async' block's return type. Yeah.
                if false {
                    return Ok::<(), std::io::Error>(());
                }

                loop {
                    upstream_read.read_exact(&mut buf).await?;
                    busywork(buf.as_ptr(), NUM_CHECKS);
                    downstream_write.write_all(&buf).await?;
                }
            });
            upstream_read = next_pipe.read;
        }
        pipes.push(Pipe { read: upstream_read, write: first_write });
    }

    let mut buf1 = [0_u8; 1];
    let mut buf2 = [0_u8; 1];
    let mut buf3 = [0_u8; 1];
    let mut buf4 = [0_u8; 1];
    let mut buf5 = [0_u8; 1];
    let mut buf6 = [0_u8; 1];
    let mut buf7 = [0_u8; 1];
    let mut buf8 = [0_u8; 1];
    let mut buf9 = [0_u8; 1];
    let mut buf10 = [0_u8; 1];
    let mut buf11 = [0_u8; 1];
    let mut buf12 = [0_u8; 1];
    let mut buf13 = [0_u8; 1];
    let mut buf14 = [0_u8; 1];
    let mut buf15 = [0_u8; 1];
    let mut buf16 = [0_u8; 1];

    // Warm up.
    for _i in 0..NUM_WARMUP_REPS {
        for Pipe { read: _, write: ref mut first_write } in &mut pipes {
            first_write.write_all(b"*").await?;
        }
        for Pipe { read: ref mut upstream_read, write: _ } in &mut pipes {
            upstream_read.read(&mut buf1).await?;
        }
    }

    let mut pipe_drain = pipes.drain(..);
    let mut pipe1 = pipe_drain.next().unwrap();
    let mut pipe2 = pipe_drain.next().unwrap();
    let mut pipe3 = pipe_drain.next().unwrap();
    let mut pipe4 = pipe_drain.next().unwrap();
    let mut pipe5 = pipe_drain.next().unwrap();
    let mut pipe6 = pipe_drain.next().unwrap();
    let mut pipe7 = pipe_drain.next().unwrap();
    let mut pipe8 = pipe_drain.next().unwrap();
    let mut pipe9 = pipe_drain.next().unwrap();
    let mut pipe10 = pipe_drain.next().unwrap();
    let mut pipe11 = pipe_drain.next().unwrap();
    let mut pipe12 = pipe_drain.next().unwrap();
    let mut pipe13 = pipe_drain.next().unwrap();
    let mut pipe14 = pipe_drain.next().unwrap();
    let mut pipe15 = pipe_drain.next().unwrap();
    let mut pipe16 = pipe_drain.next().unwrap();
    assert!(pipe_drain.next().is_none());

    let mut stats = Stats::new();
    for _i in 0..NUM_REPS {
        let start = Instant::now();
        try_join!(
            pipe1.write.write_all(b"*"),
            pipe2.write.write_all(b"*"),
            pipe3.write.write_all(b"*"),
            pipe4.write.write_all(b"*"),
            pipe5.write.write_all(b"*"),
            pipe6.write.write_all(b"*"),
            pipe7.write.write_all(b"*"),
            pipe8.write.write_all(b"*"),
            pipe9.write.write_all(b"*"),
            pipe10.write.write_all(b"*"),
            pipe11.write.write_all(b"*"),
            pipe12.write.write_all(b"*"),
            pipe13.write.write_all(b"*"),
            pipe14.write.write_all(b"*"),
            pipe15.write.write_all(b"*"),
            pipe16.write.write_all(b"*"),
        )?;
        try_join!(
            pipe1.read.read(&mut buf1),
            pipe2.read.read(&mut buf2),
            pipe3.read.read(&mut buf3),
            pipe4.read.read(&mut buf4),
            pipe5.read.read(&mut buf5),
            pipe6.read.read(&mut buf6),
            pipe7.read.read(&mut buf7),
            pipe8.read.read(&mut buf8),
            pipe9.read.read(&mut buf9),
            pipe10.read.read(&mut buf10),
            pipe11.read.read(&mut buf11),
            pipe12.read.read(&mut buf12),
            pipe13.read.read(&mut buf13),
            pipe14.read.read(&mut buf14),
            pipe15.read.read(&mut buf15),
            pipe16.read.read(&mut buf16),
        )?;
        let end = Instant::now();

        stats.push(UsefulDuration::from(end - start).into());
    }

    println!("{} iterations, {} tasks, mean {} per iteration, stddev {}",
             NUM_REPS, NUM_TASKS,
             UsefulDuration::from(stats.mean()),
             UsefulDuration::from(stats.population_stddev()));

    Ok(())
}
