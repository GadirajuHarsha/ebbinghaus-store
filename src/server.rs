pub mod proto
{
    tonic::include_proto!("ebbinghaus");
}

use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tonic::transport::Channel;
use proto::ebbinghaus_service_server::EbbinghausService;
use proto::ebbinghaus_service_client::EbbinghausServiceClient;
use proto::{
    GetRequest, GetResponse,
    PutRequest, PutResponse,
    DeleteRequest, DeleteResponse,
    ReplicateRequest, ReplicateResponse,
    ReplicationRecord, RecordOp,
};

#[derive(Clone, Debug, PartialEq)]
pub enum NodeRole
{
    Leader,
    Follower,
}

#[derive(Clone)]
pub struct StorageServer
{
    // wrap the engine in a mutex because every read mutates rehearsal state
    engine: Arc<Mutex<crate::memtable::MemTable>>,
    role: NodeRole,
    // follower channels for async replication from leader
    followers: Arc<Mutex<Vec<EbbinghausServiceClient<Channel>>>>,
}

impl StorageServer
{
    // initialize as a leader node
    pub fn new_leader(engine: Arc<Mutex<crate::memtable::MemTable>>) -> Self
    {
        StorageServer
        {
            engine,
            role: NodeRole::Leader,
            followers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    // initialize as a follower node
    pub fn new_follower(engine: Arc<Mutex<crate::memtable::MemTable>>) -> Self
    {
        StorageServer
        {
            engine,
            role: NodeRole::Follower,
            followers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    // convenience constructor defaulting to leader
    pub fn new(engine: Arc<Mutex<crate::memtable::MemTable>>) -> Self
    {
        Self::new_leader(engine)
    }

    // register a follower node for async replication
    pub async fn add_follower(&self, addr: String) -> Result<(), Box<dyn std::error::Error>>
    {
        let endpoint = tonic::transport::Endpoint::from_shared(addr)?;
        let channel = endpoint.connect_lazy();
        let client = EbbinghausServiceClient::new(channel);

        let mut followers = self.followers.lock().await;
        followers.push(client);
        Ok(())
    }
}

#[tonic::async_trait]
impl EbbinghausService for StorageServer
{
    // retrieve a value by key (triggers rehearsal and promotion)
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status>
    {
        let req = request.into_inner();
        let mut engine = self.engine.lock().await;

        match engine.get(&req.key)
        {
            Some(val) =>
            {
                Ok(Response::new(GetResponse
                {
                    found: true,
                    value: val.clone(),
                }))
            },
            None =>
            {
                Ok(Response::new(GetResponse
                {
                    found: false,
                    value: Vec::new(),
                }))
            },
        }
    }

    // write a key-value pair to the engine
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status>
    {
        if self.role == NodeRole::Follower
        {
            return Err(Status::failed_precondition("cannot write directly to follower node"));
        }

        let req = request.into_inner();
        let mut engine = self.engine.lock().await;

        match engine.put(crate::wal::RecordType::Put, &req.key, &req.value)
        {
            Ok(_) =>
            {
                // asynchronously replicate to followers
                if self.role == NodeRole::Leader
                {
                    let followers = self.followers.clone();
                    let record = ReplicationRecord
                    {
                        op: RecordOp::Put as i32,
                        key: req.key.clone(),
                        value: req.value.clone(),
                        timestamp: 0,
                        access_count: 0,
                    };

                    tokio::spawn(async move
                    {
                        let mut followers_lock = followers.lock().await;
                        for client in followers_lock.iter_mut()
                        {
                            let _ = client.replicate(ReplicateRequest
                            {
                                records: vec![record.clone()],
                            }).await;
                        }
                    });
                }

                Ok(Response::new(PutResponse
                {
                    success: true,
                }))
            },
            Err(e) =>
            {
                Err(Status::internal(e.to_string()))
            },
        }
    }

    // insert a tombstone record to delete a key
    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status>
    {
        if self.role == NodeRole::Follower
        {
            return Err(Status::failed_precondition("cannot write directly to follower node"));
        }

        let req = request.into_inner();
        let mut engine = self.engine.lock().await;

        match engine.delete(&req.key)
        {
            Ok(_) =>
            {
                // asynchronously replicate tombstone to followers
                if self.role == NodeRole::Leader
                {
                    let followers = self.followers.clone();
                    let record = ReplicationRecord
                    {
                        op: RecordOp::Delete as i32,
                        key: req.key.clone(),
                        value: Vec::new(),
                        timestamp: 0,
                        access_count: 0,
                    };

                    tokio::spawn(async move
                    {
                        let mut followers_lock = followers.lock().await;
                        for client in followers_lock.iter_mut()
                        {
                            let _ = client.replicate(ReplicateRequest
                            {
                                records: vec![record.clone()],
                            }).await;
                        }
                    });
                }

                Ok(Response::new(DeleteResponse
                {
                    success: true,
                }))
            },
            Err(e) =>
            {
                Err(Status::internal(e.to_string()))
            },
        }
    }

    // handle batch replication from leader to follower
    async fn replicate(&self, request: Request<ReplicateRequest>) -> Result<Response<ReplicateResponse>, Status>
    {
        let req = request.into_inner();
        let mut engine = self.engine.lock().await;

        for record in req.records
        {
            match record.op()
            {
                RecordOp::Put =>
                {
                    if let Err(e) = engine.put(crate::wal::RecordType::Put, &record.key, &record.value)
                    {
                        return Err(Status::internal(e.to_string()));
                    }
                },
                RecordOp::Delete =>
                {
                    if let Err(e) = engine.delete(&record.key)
                    {
                        return Err(Status::internal(e.to_string()));
                    }
                },
            }
        }

        Ok(Response::new(ReplicateResponse
        {
            acknowledged: true,
        }))
    }
}
