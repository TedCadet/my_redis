use bytes::Bytes;
use mini_redis::client;
use tokio::sync::{mpsc, oneshot};

// Provided by the requester and used by the manager task to send
// the command response back to the requester.
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[tokio::main]
async fn main() {
    // Create a new channel with a capacity of at most 32.
    let (tx, mut rx) = mpsc::channel::<Command>(32);
    let tx2 = tx.clone();

    let manager = tokio::spawn(async move {
        // Establish a connection to the server
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        // Start receiving messages
        while let Some(cmd) = rx.recv().await {
            use Command::*;

            match cmd {
                Get { key, resp } => {
                    let response = client.get(&key).await;
                    // Ignore errors
                    let _ = resp.send(response);
                }
                Set { key, val, resp } => {
                    let response = client.set(&key, val).await;
                    // Ignore errors
                    let _ = resp.send(response);
                }
            }
        }
    });

    // Spawn two tasks, one gets a key, the other sets a key
    let t1 = tokio::spawn(async move {
        let (tx_resp, rx_resp) = oneshot::channel();

        let cmd = Command::Get {
            key: "foo".to_string(),
            resp: tx_resp,
        };

        // Send the GET request
        tx.send(cmd).await.unwrap();

        // Await the response
        let res = rx_resp.await;
        println!("GOT = {:?}", res);
    });

    let t2 = tokio::spawn(async move {
        let (tx_resp, rx_resp) = oneshot::channel();

        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
            resp: tx_resp,
        };

        // send the SET request
        tx2.send(cmd).await.unwrap();

        // await the response
        let res = rx_resp.await;
        println!("GOT = {:?}", res);
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
}

#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    },
}
