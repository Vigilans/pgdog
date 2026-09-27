//! Which finished requests `read_your_writes` records as writes.

use crate::frontend::{
    client::{
        Transaction, TransactionType,
        query_engine::{QueryEngineContext, query::committed_write},
    },
    router::parser::{Route, Shard, ShardWithPriority},
};

use super::test_client;

fn committed(transaction: Option<TransactionType>, route: Option<Route>, rollback: bool) -> bool {
    let mut client = test_client();
    client.transaction = transaction.map(Transaction::new);
    client.client_request.route = route;
    let mut context = QueryEngineContext::new(&mut client);
    context.rollback = rollback;
    committed_write(&context)
}

fn write() -> Option<Route> {
    Some(Route::write(ShardWithPriority::new_default_unset(
        Shard::All,
    )))
}

fn read() -> Option<Route> {
    Some(Route::read(ShardWithPriority::new_default_unset(
        Shard::Direct(0),
    )))
}

#[tokio::test]
async fn test_read_only_transaction_commit_is_not_a_write() {
    assert!(!committed(Some(TransactionType::ReadOnly), write(), false));
}

#[tokio::test]
async fn test_rollback_is_not_a_write() {
    assert!(!committed(Some(TransactionType::ReadWrite), write(), true));
    assert!(!committed(
        Some(TransactionType::ErrorReadWrite),
        write(),
        true
    ));
}

#[tokio::test]
async fn test_read_write_transaction_commit_is_a_write() {
    assert!(committed(Some(TransactionType::ReadWrite), write(), false));
}

#[tokio::test]
async fn test_extended_batch_sync_is_a_write() {
    // The Sync request that ends a split extended batch has no route of its own.
    assert!(committed(Some(TransactionType::Implicit), None, false));
}

#[tokio::test]
async fn test_autocommit_statement_follows_its_route() {
    assert!(committed(None, write(), false));
    assert!(!committed(None, read(), false));
}
