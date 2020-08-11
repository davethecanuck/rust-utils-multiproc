use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use log::{info, warn, error};

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    NewJob(Job),
    Terminate,
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);
        assert!(size < 64);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            let worker = Worker::new(id, Arc::clone(&receiver));
            workers.push(worker);
        }
        ThreadPool { workers, sender }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        match self.sender.send(Message::NewJob(job)) {
            Err(e) => {
                error!("Threadpool send of new job failed with error: {:?}.", e);
            }
            _ => (),
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        warn!("Sending terminate message to all workers.");

        for _ in &self.workers {
            match self.sender.send(Message::Terminate) {
                Err(e) => error!("Failed to terminate worker: {}", e),
                _ => (),
            }
        }
        warn!("Shutting down all workers.");

        for worker in &mut self.workers {
            warn!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                match thread.join() {
                    Err(e) => {
                        error!("Failed to join worker thread {}: {:?}", 
                                 worker.id, e);
                    },
                    _ => ()
                }
            }
        }
    }
}

type Receiver = Arc<Mutex<mpsc::Receiver<Message>>>;

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Receiver) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv();
            match message {
                Ok(Message::NewJob(job)) => {
                    info!("Worker {} got a job; executing.", id);
                    job();
                }
                Ok(Message::Terminate) => {
                    info!("Worker {} was told to terminate.", id);
                    break;
                }
                Err(e) => {
                    error!("Worker {} failed with error: {:?}.", id, e);
                    break;
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        // Create thread pool
        {
            let pool = super::ThreadPool::new(4);
            for i in 1..5 {
                pool.execute(move || {
                    info!("In-thread: i={}", i);
                });
            }
            // but total is popped off stack here
        } // pool goes out of scope here - threads join
        assert!(true);
    }
}

