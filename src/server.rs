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
    RehearsalRecord, RehearsalBatch, RehearsalAck,
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
    // prometheus metrics collector for latency and request tracking
    metrics: Arc<crate::metrics::MetricsRegistry>,
    // in-memory buffer on follower to aggregate rehearsal counts before sync to leader
    rehearsal_buffer: Arc<Mutex<Vec<RehearsalRecord>>>,
}

impl StorageServer
{
    // initialize as a leader node with default metrics registry
    pub fn new_leader(engine: Arc<Mutex<crate::memtable::MemTable>>) -> Self
    {
        Self::new_leader_with_metrics(engine, Arc::new(crate::metrics::MetricsRegistry::new()))
    }

    // initialize as a leader node with explicit metrics registry
    pub fn new_leader_with_metrics(
        engine: Arc<Mutex<crate::memtable::MemTable>>,
        metrics: Arc<crate::metrics::MetricsRegistry>,
    ) -> Self
    {
        StorageServer
        {
            engine,
            role: NodeRole::Leader,
            followers: Arc::new(Mutex::new(Vec::new())),
            metrics,
            rehearsal_buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    // initialize as a follower node with default metrics registry
    pub fn new_follower(engine: Arc<Mutex<crate::memtable::MemTable>>) -> Self
    {
        Self::new_follower_with_metrics(engine, Arc::new(crate::metrics::MetricsRegistry::new()))
    }

    // initialize as a follower node with explicit metrics registry
    pub fn new_follower_with_metrics(
        engine: Arc<Mutex<crate::memtable::MemTable>>,
        metrics: Arc<crate::metrics::MetricsRegistry>,
    ) -> Self
    {
        StorageServer
        {
            engine,
            role: NodeRole::Follower,
            followers: Arc::new(Mutex::new(Vec::new())),
            metrics,
            rehearsal_buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    // convenience constructor defaulting to leader
    pub fn new(engine: Arc<Mutex<crate::memtable::MemTable>>) -> Self
    {
        Self::new_leader(engine)
    }

    // accessor for metrics registry
    pub fn get_metrics(&self) -> Arc<crate::metrics::MetricsRegistry>
    {
        self.metrics.clone()
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

    // sync buffered read rehearsals on follower back to leader node
    pub async fn sync_rehearsals_to_leader(&self, leader_addr: &str) -> Result<bool, Box<dyn std::error::Error>>
    {
        let batch: Vec<RehearsalRecord> =
        {
            let mut buf = self.rehearsal_buffer.lock().await;
            std::mem::take(&mut *buf)
        };

        if batch.is_empty()
        {
            return Ok(true);
        }

        let mut client = EbbinghausServiceClient::connect(leader_addr.to_string()).await?;
        let response = client.sync_rehearsal(RehearsalBatch
        {
            rehearsals: batch,
        }).await?;

        Ok(response.into_inner().acknowledged)
    }
}

#[tonic::async_trait]
impl EbbinghausService for StorageServer
{
    // retrieve a value by key (triggers rehearsal and promotion)
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status>
    {
        let timer = self.metrics.request_duration.with_label_values(&["get"]).start_timer();
        let req = request.into_inner();
        let mut engine = self.engine.lock().await;

        let result = engine.get(&req.key).cloned();
        match result
        {
            Some(val) =>
            {
                self.metrics.requests_total.with_label_values(&["get", "hit"]).inc();
                self.metrics.memtable_bytes.set(engine.get_current_size() as i64);
                let score = engine.average_retention_score();
                self.metrics.average_retention_score.set(score);

                // buffer rehearsal on follower to sync back to leader later
                if self.role == NodeRole::Follower
                {
                    let mut buf = self.rehearsal_buffer.lock().await;
                    if let Some(record) = buf.iter_mut().find(|r| r.key == req.key)
                    {
                        record.access_count_delta += 1;
                    }
                    else
                    {
                        buf.push(RehearsalRecord
                        {
                            key: req.key.clone(),
                            access_count_delta: 1,
                        });
                    }
                }

                drop(timer);
                Ok(Response::new(GetResponse
                {
                    found: true,
                    value: val,
                }))
            },
            None =>
            {
                self.metrics.requests_total.with_label_values(&["get", "miss"]).inc();
                drop(timer);
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
        let timer = self.metrics.request_duration.with_label_values(&["put"]).start_timer();
        if self.role == NodeRole::Follower
        {
            self.metrics.requests_total.with_label_values(&["put", "rejected"]).inc();
            drop(timer);
            return Err(Status::failed_precondition("cannot write directly to follower node"));
        }

        let req = request.into_inner();
        let mut engine = self.engine.lock().await;

        match engine.put(crate::wal::RecordType::Put, &req.key, &req.value)
        {
            Ok(_) =>
            {
                self.metrics.requests_total.with_label_values(&["put", "success"]).inc();
                self.metrics.memtable_bytes.set(engine.get_current_size() as i64);
                if engine.evicted_count > 0
                {
                    self.metrics.evictions_total.with_label_values(&["retention_decay"]).inc_by(engine.evicted_count as u64);
                    engine.evicted_count = 0;
                }
                let score = engine.average_retention_score();
                self.metrics.average_retention_score.set(score);

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

                drop(timer);
                Ok(Response::new(PutResponse
                {
                    success: true,
                }))
            },
            Err(e) =>
            {
                self.metrics.requests_total.with_label_values(&["put", "error"]).inc();
                drop(timer);
                Err(Status::internal(e.to_string()))
            },
        }
    }

    // insert a tombstone record to delete a key
    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status>
    {
        let timer = self.metrics.request_duration.with_label_values(&["delete"]).start_timer();
        if self.role == NodeRole::Follower
        {
            self.metrics.requests_total.with_label_values(&["delete", "rejected"]).inc();
            drop(timer);
            return Err(Status::failed_precondition("cannot write directly to follower node"));
        }

        let req = request.into_inner();
        let mut engine = self.engine.lock().await;

        match engine.delete(&req.key)
        {
            Ok(_) =>
            {
                self.metrics.requests_total.with_label_values(&["delete", "success"]).inc();
                self.metrics.memtable_bytes.set(engine.get_current_size() as i64);
                if engine.evicted_count > 0
                {
                    self.metrics.evictions_total.with_label_values(&["retention_decay"]).inc_by(engine.evicted_count as u64);
                    engine.evicted_count = 0;
                }

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

                drop(timer);
                Ok(Response::new(DeleteResponse
                {
                    success: true,
                }))
            },
            Err(e) =>
            {
                self.metrics.requests_total.with_label_values(&["delete", "error"]).inc();
                drop(timer);
                Err(Status::internal(e.to_string()))
            },
        }
    }

    // handle batch replication from leader to follower
    async fn replicate(&self, request: Request<ReplicateRequest>) -> Result<Response<ReplicateResponse>, Status>
    {
        let timer = self.metrics.request_duration.with_label_values(&["replicate"]).start_timer();
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
                        self.metrics.requests_total.with_label_values(&["replicate", "error"]).inc();
                        drop(timer);
                        return Err(Status::internal(e.to_string()));
                    }
                },
                RecordOp::Delete =>
                {
                    if let Err(e) = engine.delete(&record.key)
                    {
                        self.metrics.requests_total.with_label_values(&["replicate", "error"]).inc();
                        drop(timer);
                        return Err(Status::internal(e.to_string()));
                    }
                },
            }
        }

        self.metrics.requests_total.with_label_values(&["replicate", "success"]).inc();
        self.metrics.memtable_bytes.set(engine.get_current_size() as i64);
        drop(timer);

        Ok(Response::new(ReplicateResponse
        {
            acknowledged: true,
        }))
    }

    // sync rehearsal access counts from followers to leader to eliminate decay skew
    async fn sync_rehearsal(&self, request: Request<RehearsalBatch>) -> Result<Response<RehearsalAck>, Status>
    {
        let timer = self.metrics.request_duration.with_label_values(&["sync_rehearsal"]).start_timer();
        let req = request.into_inner();
        let mut engine = self.engine.lock().await;

        for item in req.rehearsals
        {
            engine.record_rehearsal(&item.key, item.access_count_delta);
        }

        self.metrics.requests_total.with_label_values(&["sync_rehearsal", "success"]).inc();
        drop(timer);

        Ok(Response::new(RehearsalAck
        {
            acknowledged: true,
        }))
    }
}
