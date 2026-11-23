pub mod proto
{
    tonic::include_proto!("ebbinghaus");
}

use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use proto::ebbinghaus_service_server::EbbinghausService;
use proto::{
    GetRequest, GetResponse,
    PutRequest, PutResponse,
    DeleteRequest, DeleteResponse,
    ReplicateRequest, ReplicateResponse,
};

#[derive(Clone)]
pub struct StorageServer
{
    // wrap the engine in a mutex because every read mutates rehearsal state
    engine: Arc<Mutex<crate::memtable::MemTable>>,
}

impl StorageServer
{
    // initialize a new storage server instance wrapping the engine
    pub fn new(engine: Arc<Mutex<crate::memtable::MemTable>>) -> Self
    {
        StorageServer
        {
            engine,
        }
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
        let req = request.into_inner();
        let mut engine = self.engine.lock().await;

        match engine.put(crate::wal::RecordType::Put, &req.key, &req.value)
        {
            Ok(_) =>
            {
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
        let req = request.into_inner();
        let mut engine = self.engine.lock().await;

        match engine.delete(&req.key)
        {
            Ok(_) =>
            {
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
    async fn replicate(&self, _request: Request<ReplicateRequest>) -> Result<Response<ReplicateResponse>, Status>
    {
        // stub for leader-follower replication in pillar 2
        Ok(Response::new(ReplicateResponse
        {
            acknowledged: true,
        }))
    }
}
