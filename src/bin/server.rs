use bytes::Bytes;
use mini_redis::Command::{self, Get, Set};
use mini_redis::{Connection, Frame};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening");

    let db = Arc::new(Mutex::new(HashMap::<String, Bytes>::new()));

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();

        // Clone the handle to the handle map
        let db = db.clone();

        // A new task is spawned for each inbound socket. The socket is
        // moved to the new task and processed there.
        println!("Accepted");
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: Db) {
    // The 'Connection' lets us read/write redis **frames** instead of
    // byte streams. The 'Connection' type is defined by mini-redis.
    let mut connection = Connection::new(socket);

    // if let Some(frame) = connection.read_frame().await.unwrap() {
    //     println!("Got: {:?}", frame);

    //     // Respond with an error
    //     let response = Frame::Error("unimplemented".to_string());
    //     connection.write_frame(&response).await.unwrap();
    // }

    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("Reading frame");
        let response: Frame = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone())
                    // `Frame::Bulk` expects data to be of type `Bytes`. This
                    // type will be covered later in the tutorial. For now,
                    // `&Vec<u8>` is converted to `Bytes` using `into()`.
                    // Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
    }

    println!("process done");
}
