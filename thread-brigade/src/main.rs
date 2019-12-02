use std::time::Instant;
use std::os::unix::net::UnixStream;
use std::io::prelude::*;
use utils::{Stats, UsefulDuration};

struct Pipe {
    read: UnixStream,
    write: UnixStream,
}

fn pipe() -> Result<Pipe, std::io::Error> {
    let (read, write) = UnixStream::pair()?;
    Ok(Pipe { read, write })
}

fn busywork(buf_ptr: *const u8, num_iters: usize) {
    for _check in 0..num_iters {
        assert_eq!(unsafe { buf_ptr.read_volatile() }, b'*');
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const NUM_TASKS: usize = 512;
    const NUM_CHAINS: usize = 8;
    const NUM_CHECKS: usize = 8192;
    const NUM_WARMUP_REPS: usize = 100;
    const NUM_REPS: usize = 10000;

    let mut pipes = Vec::with_capacity(NUM_CHAINS);

    for _chain in 0..NUM_CHAINS {
        let Pipe { read: mut upstream_read, write: first_write} = pipe()?;
        for _i in 0..NUM_TASKS/NUM_CHAINS {
            let next_pipe = pipe()?;
            let mut downstream_write = next_pipe.write;
            std::thread::spawn(move || -> Result<(), std::io::Error> {
                let mut buf = [0_u8; 1];

                loop {
                    upstream_read.read_exact(&mut buf)?;
                    busywork(buf.as_ptr(), NUM_CHECKS);
                    downstream_write.write_all(&buf)?;
                }
            });
            upstream_read = next_pipe.read;
        }
        pipes.push(Pipe { read: upstream_read, write: first_write });
    }

    let mut buf = [0_u8; 1];

    // Warm up.
    for _i in 0..NUM_WARMUP_REPS {
        for Pipe { read: _, write: ref mut first_write } in &mut pipes {
            first_write.write_all(b"*")?;
        }
        for Pipe { read: ref mut upstream_read, write: _ } in &mut pipes {
            upstream_read.read(&mut buf)?;
        }
    }

    let mut stats = Stats::new();
    for _i in 0..NUM_REPS {
        let start = Instant::now();
        for Pipe { read: _, write: ref mut first_write } in &mut pipes {
            first_write.write_all(b"*")?;
        }
        for Pipe { read: ref mut upstream_read, write: _ } in &mut pipes {
            upstream_read.read(&mut buf)?;
        }
        let end = Instant::now();

        stats.push(UsefulDuration::from(end - start).into());
    }

    println!("{} iterations, {} tasks, mean {} per iteration, stddev {}",
             NUM_REPS, NUM_TASKS,
             UsefulDuration::from(stats.mean()),
             UsefulDuration::from(stats.population_stddev()));

    Ok(())
}
