use super::*;
use crate::peers::{attention, net};
use ism_core::dto::{AttentionCode, AttentionCompleteness, AttentionOwner};
use serde_json::Value;

const PATH: &str = "/api/peer/v1/attention";

#[tokio::test]
async fn remote_attention_is_signed_scoped_minimal_and_authoritative_per_peer() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&b.state).await.unwrap().remove(0);
    let owner = b.state.peer_attention.begin(&peer.id).unwrap();
    b.state
        .peer_attention
        .change(&peer.id, owner, Some(true))
        .unwrap();
    assert_eq!(
        b.state
            .http
            .get(format!("{}{PATH}", a.url))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        net::request(
            &b.state,
            &peer.url,
            "wrong-secret",
            reqwest::Method::GET,
            PATH,
            None
        )
        .send()
        .await
        .unwrap()
        .status(),
        401
    );
    let receiver_peer = net::peers(&a.state).await.unwrap().remove(0);
    let share: String = sqlx::query_scalar("SELECT id FROM shares WHERE name='Family'")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO immich_servers(id,name,url,compatible,maintenance_json,created_at)
         VALUES ('private-server','Private server','https://private.example',0,'not-json',?)",
    )
    .bind(crate::db::now())
    .execute(&a.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO mappings(root_id,name,container_path,server_id,created_at)
         VALUES ('private-root','Private folder','/mnt/private-example','private-server',?)",
    )
    .bind(crate::db::now())
    .execute(&a.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
         VALUES ('private-share','Private share','private-root','','[]',1)",
    )
    .execute(&a.state.db)
    .await
    .unwrap();
    for (id, share_id, path) in [
        ("shared-conflict", share.as_str(), "family-example.jpg"),
        ("private-conflict", "private-share", "private-example.jpg"),
    ] {
        sqlx::query(
            "INSERT INTO conflicts(id,share_id,rel_path,kind,detail,local_json,remote_json,
             remote_vv_json,peer_id,status,created_at)
             VALUES (?,?,?,'edit','private diagnostic','{}','{}','{}',?,'open',?)",
        )
        .bind(id)
        .bind(share_id)
        .bind(path)
        .bind(&receiver_peer.id)
        .bind(crate::db::now())
        .execute(&a.state.db)
        .await
        .unwrap();
    }
    std::fs::write(a.shared.parent().unwrap().join(".ism-root"), "wrong-root").unwrap();
    let report: Value = net::get_json(&b.state, &peer, PATH).await.unwrap();
    let json = report.to_string();
    for private in [
        "Private server",
        "Private share",
        "private-example.jpg",
        "family-example.jpg",
        "private diagnostic",
        "/mnt/private-example",
        "private-root",
        "photos",
        a._dir.path().to_str().unwrap(),
    ] {
        assert!(
            !json.contains(private),
            "Peer report must not disclose {private}"
        );
    }
    assert!(json.len() < 256 * 1024);
    assert_eq!(report["shares"], serde_json::json!(["Family"]));
    assert_eq!(
        report["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["code"] == "recovery_decision")
            .unwrap()["count"],
        1
    );
    let admitted = a.state.replication_operation_lock.lock().await;
    let registration = a
        .state
        .replication_operation_lock
        .register(&admitted, "synthetic-operation");
    drop(admitted);
    let waiting: Value = net::get_json(&b.state, &peer, PATH).await.unwrap();
    assert!(
        waiting["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["code"] != "transfer_dependency"),
        "an operation alone does not establish blocked transfers"
    );
    drop(registration);
    attention::refresh(&b.state, &peer, owner).await.unwrap();
    let snapshot = attention::snapshot(&b.state).await.unwrap();
    assert!(
        snapshot
            .items
            .iter()
            .any(|item| item.code == AttentionCode::MappingUnavailable
                && item.blocks_work
                && item.owner == AttentionOwner::Receiver
                && item.peer_id.as_deref() == Some(&peer.id))
    );
    for (change, restore) in [
        (
            "UPDATE peers SET status='offline'",
            "UPDATE peers SET status='online'",
        ),
        (
            "UPDATE peers SET secret_enc='rotated-unreadable-ciphertext'",
            "",
        ),
        (
            "UPDATE shares SET name='Renamed' WHERE name='Family'",
            "UPDATE shares SET name='Family' WHERE name='Renamed'",
        ),
        (
            "UPDATE shares SET path='changed-scope' WHERE name='Family'",
            "UPDATE shares SET path='' WHERE name='Family'",
        ),
        (
            "UPDATE shares SET root_id='changed-root' WHERE name='Family'",
            "UPDATE shares SET root_id='photos' WHERE name='Family'",
        ),
        (
            "UPDATE shares SET enabled=0 WHERE name='Family'",
            "UPDATE shares SET enabled=1 WHERE name='Family'",
        ),
    ] {
        let original_secret: String = sqlx::query_scalar("SELECT secret_enc FROM peers WHERE id=?")
            .bind(&peer.id)
            .fetch_one(&b.state.db)
            .await
            .unwrap();
        let original_root: String =
            sqlx::query_scalar("SELECT root_id FROM shares WHERE name='Family'")
                .fetch_one(&b.state.db)
                .await
                .unwrap();
        sqlx::query(change).execute(&b.state.db).await.unwrap();
        let invalidated = attention::snapshot(&b.state).await.unwrap();
        assert!(
            invalidated.items.iter().all(|item| !item.blocks_work),
            "{change}"
        );
        assert!(
            invalidated
                .coverage
                .iter()
                .filter(|source| source.peer_id.as_deref() == Some(&peer.id))
                .all(|source| source.completeness != AttentionCompleteness::Complete),
            "{change}"
        );
        if !restore.is_empty() {
            sqlx::query(restore).execute(&b.state.db).await.unwrap();
        }
        sqlx::query("UPDATE peers SET secret_enc=? WHERE id=?")
            .bind(original_secret)
            .bind(&peer.id)
            .execute(&b.state.db)
            .await
            .unwrap();
        sqlx::query("UPDATE shares SET root_id=? WHERE name='Family'")
            .bind(original_root)
            .execute(&b.state.db)
            .await
            .unwrap();
    }
    let source = snapshot
        .coverage
        .iter()
        .find(|source| {
            source.code == AttentionCode::MappingUnavailable
                && source.peer_id.as_deref() == Some(&peer.id)
        })
        .unwrap();
    assert_eq!(source.completeness, AttentionCompleteness::Complete);
    std::fs::write(a.shared.parent().unwrap().join(".ism-root"), "photos").unwrap();
    sqlx::query("UPDATE conflicts SET status='resolved' WHERE id='shared-conflict'")
        .execute(&a.state.db)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
         SELECT 'unavailable-peer','Unavailable','https://unavailable.example','other-instance',
                secret_enc,'offline',created_at FROM peers WHERE id=?",
    )
    .bind(&peer.id)
    .execute(&b.state.db)
    .await
    .unwrap();
    attention::refresh(&b.state, &peer, owner).await.unwrap();
    let cleared = attention::snapshot(&b.state).await.unwrap();
    assert!(
        !cleared
            .items
            .iter()
            .any(|item| item.code == AttentionCode::MappingUnavailable
                && item.peer_id.as_deref() == Some(&peer.id))
    );
    assert!(
        cleared
            .coverage
            .iter()
            .any(|source| source.code == AttentionCode::MappingUnavailable
                && source.peer_id.as_deref() == Some(&peer.id)
                && source.completeness == AttentionCompleteness::Complete)
    );
    assert!(
        cleared
            .coverage
            .iter()
            .any(|source| source.code == AttentionCode::MappingUnavailable
                && source.peer_id.as_deref() == Some("unavailable-peer")
                && source.completeness == AttentionCompleteness::Unavailable)
    );

    let restarted = AppState::new(
        Config::for_tests(b._dir.path()),
        b.state.db.clone(),
        SecretBox::open_or_create(b._dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let missing = attention::snapshot(&restarted).await.unwrap();
    assert!(
        missing
            .items
            .iter()
            .any(|item| item.code == AttentionCode::PeerAttentionUnavailable && !item.blocks_work)
    );
    assert!(
        missing
            .coverage
            .iter()
            .filter(|source| source.peer_id.as_deref() == Some(&peer.id))
            .all(|source| source.completeness != AttentionCompleteness::Complete)
    );
}

#[tokio::test]
async fn remote_attention_operation_wait_counts_only_authorized_queued_work_for_the_peer() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&b.state).await.unwrap().remove(0);
    let receiver_peer = net::peers(&a.state).await.unwrap().remove(0);
    let share: String = sqlx::query_scalar("SELECT id FROM shares WHERE name='Family'")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    let admitted = a.state.replication_operation_lock.lock().await;
    let registration = a
        .state
        .replication_operation_lock
        .register(&admitted, "synthetic-operation");
    drop(admitted);
    sqlx::query(
        "INSERT INTO operations(id,idempotency_key,principal,source,request_json,status,created_at)
         VALUES('synthetic-operation','synthetic-operation','example','ui','{}','running',?)",
    )
    .bind(crate::db::now())
    .execute(&a.state.db)
    .await
    .unwrap();
    let empty: Value = net::get_json(&b.state, &peer, PATH).await.unwrap();
    assert!(
        empty["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["code"] != "transfer_dependency")
    );
    sqlx::query(
        "INSERT INTO shares(id,name,root_id,path,peer_ids_json,enabled)
         SELECT 'private-share','Private share',root_id,'private-example','[]',1
         FROM shares WHERE id=?",
    )
    .bind(&share)
    .execute(&a.state.db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO peers(id,name,url,instance_id,secret_enc,status,created_at)
         SELECT 'other-peer','Other peer','https://other.example','other-instance',
                secret_enc,'online',created_at FROM peers WHERE id=?",
    )
    .bind(&receiver_peer.id)
    .execute(&a.state.db)
    .await
    .unwrap();
    for (id, peer_id, share_id) in [
        ("other-share", receiver_peer.id.as_str(), "private-share"),
        ("other-peer", "other-peer", share.as_str()),
        ("relevant-one", receiver_peer.id.as_str(), share.as_str()),
        ("relevant-two", receiver_peer.id.as_str(), share.as_str()),
    ] {
        sqlx::query(
            "INSERT INTO transfers(id,peer_id,share_id,rel_path,sha256,size,vv_json,status,queued_at,updated_at)
             VALUES(?,?,?,'example.jpg','synthetic-hash',10,'{}','queued',?,?)",
        )
        .bind(id)
        .bind(peer_id)
        .bind(share_id)
        .bind(crate::db::now())
        .bind(crate::db::now())
        .execute(&a.state.db)
        .await
        .unwrap();
        let report: Value = net::get_json(&b.state, &peer, PATH).await.unwrap();
        let waits: Vec<_> = report["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["code"] == "transfer_dependency")
            .collect();
        if id.starts_with("other-") {
            assert!(
                waits.is_empty(),
                "unrelated work cannot block this peer/share"
            );
        } else {
            assert_eq!(waits.len(), 1);
            assert_eq!(waits[0]["share_name"], "Family");
            assert_eq!(waits[0]["blocks_work"], true);
            assert_eq!(waits[0]["count"], if id == "relevant-one" { 1 } else { 2 });
        }
        assert!(!report.to_string().contains("Private share"));
    }
    drop(registration);
    let unknown: Value = net::get_json(&b.state, &peer, PATH).await.unwrap();
    let item = unknown["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["code"] == "transfer_dependency")
        .unwrap();
    assert_eq!(item["state"], "unavailable");
    assert_eq!(item["blocks_work"], false);
    assert_eq!(item["count"], 0);
}

#[tokio::test]
async fn remote_attention_selects_conflicts_before_grouping_without_changing_local_counts() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&a.state).await.unwrap().remove(0);
    let share: String = sqlx::query_scalar("SELECT id FROM shares WHERE name='Family'")
        .fetch_one(&a.state.db)
        .await
        .unwrap();
    for (peer_id, count) in [(peer.id.as_str(), 7), ("unrelated-peer", 12)] {
        for index in 0..count {
            sqlx::query(
                "INSERT INTO conflicts(id,share_id,rel_path,kind,detail,local_json,remote_json,
                 remote_vv_json,peer_id,status,created_at)
                 VALUES (?,?,?,'edit','example','{}','{}','{}',?,'open',?)",
            )
            .bind(format!("{peer_id}-{index}"))
            .bind(&share)
            .bind(format!("example-{index}.jpg"))
            .bind(peer_id)
            .bind(crate::db::now())
            .execute(&a.state.db)
            .await
            .unwrap();
        }
    }
    let selection = crate::local_evidence::Selection {
        peer_id: peer.id.clone(),
        roots: Default::default(),
        shares: [share].into_iter().collect(),
        servers: Default::default(),
        libraries: Default::default(),
    };
    let (selected, _) = crate::local_evidence::selected_snapshot(
        &a.state,
        "Example",
        &crate::db::now(),
        Some(&selection),
    )
    .await
    .unwrap();
    let decisions: Vec<_> = selected
        .iter()
        .filter(|item| item.code == AttentionCode::RecoveryDecision)
        .collect();
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0].peer_id.as_deref(), Some(peer.id.as_str()));
    assert_eq!(decisions[0].count, 7);
    assert_eq!(decisions[0].examples.len(), 5);
    let (local, _) = crate::local_evidence::snapshot(&a.state, "Example", &crate::db::now())
        .await
        .unwrap();
    let mut counts: Vec<_> = local
        .iter()
        .filter(|item| item.code == AttentionCode::RecoveryDecision)
        .map(|item| item.count)
        .collect();
    counts.sort_unstable();
    assert_eq!(counts, [7, 12]);
}

#[tokio::test]
async fn remote_attention_healthy_peer_has_partial_coverage_without_retry_warning() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    b.state = AppState::new(
        Config::for_tests(b._dir.path()),
        b.state.db.clone(),
        SecretBox::open_or_create(b._dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let peer = net::peers(&b.state).await.unwrap().remove(0);
    let owner = b.state.peer_attention.begin(&peer.id).unwrap();
    b.state
        .peer_attention
        .change(&peer.id, owner, Some(true))
        .unwrap();
    attention::refresh(&b.state, &peer, owner).await.unwrap();
    let healthy = attention::snapshot(&b.state).await.unwrap();
    assert!(healthy.items.is_empty());
    assert!(
        healthy.warnings.is_empty(),
        "scope limits are not retryable failures"
    );
    assert!(crate::peers::activity::refresh_peer(&b.state, &peer).await);
    let combined = crate::attention::snapshot(&b.state).await.unwrap();
    assert_eq!(combined.completeness, AttentionCompleteness::Partial);
    assert!(
        combined
            .items
            .iter()
            .all(|item| item.attention == ism_core::dto::AttentionLevel::None && !item.blocks_work),
        "{:?}",
        combined.items
    );
    assert!(combined.warnings.is_empty());
    for code in [
        AttentionCode::OperationRecovery,
        AttentionCode::WorkerStopped,
        AttentionCode::WorkerStalled,
    ] {
        assert!(
            healthy
                .coverage
                .iter()
                .any(|source| source.peer_id.as_deref() == Some(&peer.id)
                    && source.code == code
                    && source.completeness == AttentionCompleteness::Partial)
        );
    }
    b.state
        .peer_attention
        .change(&peer.id, owner, Some(false))
        .unwrap();
    let stale = attention::snapshot(&b.state).await.unwrap();
    assert!(!stale.warnings.is_empty());
    assert!(
        stale
            .items
            .iter()
            .any(|item| item.code == AttentionCode::PeerAttentionUnavailable)
    );
    assert!(
        stale
            .coverage
            .iter()
            .all(|source| source.completeness != AttentionCompleteness::Complete)
    );
}

#[tokio::test]
async fn remote_attention_rejects_old_invalid_oversized_and_changed_membership_evidence() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&b.state).await.unwrap().remove(0);
    let owner = b.state.peer_attention.begin(&peer.id).unwrap();
    b.state
        .peer_attention
        .change(&peer.id, owner, Some(true))
        .unwrap();
    std::fs::write(a.shared.parent().unwrap().join(".ism-root"), "wrong-root").unwrap();
    let mut report: Value = net::get_json(&b.state, &peer, PATH).await.unwrap();
    let server = MockServer::start().await;
    sqlx::query("UPDATE peers SET url=? WHERE id=?")
        .bind(server.uri())
        .bind(&peer.id)
        .execute(&b.state.db)
        .await
        .unwrap();
    let peer = net::peer(&b.state, &peer.id).await.unwrap();
    for response in [
        ResponseTemplate::new(404),
        ResponseTemplate::new(200).set_body_string("x".repeat(256 * 1024 + 1)),
    ] {
        server.reset().await;
        Mock::given(method("GET"))
            .and(path(PATH))
            .respond_with(response)
            .mount(&server)
            .await;
        attention::refresh(&b.state, &peer, owner).await.unwrap();
        let unknown = attention::snapshot(&b.state).await.unwrap();
        assert!(
            unknown
                .coverage
                .iter()
                .filter(|source| source.peer_id.as_deref() == Some(&peer.id))
                .all(|source| source.completeness != AttentionCompleteness::Complete)
        );
        assert!(unknown.items.iter().all(|item| !item.blocks_work));
    }

    server.reset().await;
    report["instance_id"] = "wrong-instance".into();
    Mock::given(method("GET"))
        .and(path(PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(&report))
        .mount(&server)
        .await;
    attention::refresh(&b.state, &peer, owner).await.unwrap();
    assert!(
        attention::snapshot(&b.state)
            .await
            .unwrap()
            .items
            .iter()
            .all(|item| !item.blocks_work)
    );
    server.reset().await;
    report["instance_id"] = a.state.instance_id.clone().into();
    Mock::given(method("GET"))
        .and(path(PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(&report))
        .mount(&server)
        .await;
    attention::refresh(&b.state, &peer, owner).await.unwrap();
    assert!(
        attention::snapshot(&b.state)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.blocks_work)
    );
    sqlx::query("UPDATE shares SET peer_ids_json='[]' WHERE name='Family'")
        .execute(&b.state.db)
        .await
        .unwrap();
    let revoked = attention::snapshot(&b.state).await.unwrap();
    assert!(revoked.items.iter().all(|item| !item.blocks_work));
    assert!(
        revoked
            .coverage
            .iter()
            .filter(|source| source.peer_id.as_deref() == Some(&peer.id))
            .all(|source| source.completeness != AttentionCompleteness::Complete)
    );
}

#[tokio::test]
async fn remote_attention_membership_aba_fences_held_response() {
    let (mut a, mut b) = pair().await;
    stop_workers(&mut a).await;
    stop_workers(&mut b).await;
    let peer = net::peers(&b.state).await.unwrap().remove(0);
    let owner = b.state.peer_attention.begin(&peer.id).unwrap();
    b.state
        .peer_attention
        .change(&peer.id, owner, Some(true))
        .unwrap();
    std::fs::write(a.shared.parent().unwrap().join(".ism-root"), "wrong-root").unwrap();
    attention::refresh(&b.state, &peer, owner).await.unwrap();
    assert!(
        attention::snapshot(&b.state)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.blocks_work)
    );

    std::fs::write(a.shared.parent().unwrap().join(".ism-root"), "photos").unwrap();
    a.attention_hold
        .armed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let state = b.state.clone();
    let request_peer = peer.clone();
    let request =
        tokio::spawn(async move { attention::refresh(&state, &request_peer, owner).await });
    tokio::time::timeout(Duration::from_secs(9), a.attention_hold.entered.notified())
        .await
        .expect("old response must be held");

    let share = super::super::shares(&b.state).await.unwrap().remove(0);
    let input = |peer_ids| ShareInput {
        name: share.name.clone(),
        location: Location {
            root_id: share.root_id.clone(),
            path: share.path.as_str().into(),
        },
        peer_ids,
        enabled: true,
    };
    admin::update_share(&b.state, &share.id, input(Vec::new()))
        .await
        .unwrap();
    admin::update_share(&b.state, &share.id, input(vec![peer.id.clone()]))
        .await
        .unwrap();
    a.attention_hold.release.notify_one();
    request.await.unwrap().unwrap();
    let after = attention::snapshot(&b.state).await.unwrap();
    assert!(after.items.iter().all(|item| !item.blocks_work));
    assert!(
        after
            .coverage
            .iter()
            .filter(|source| source.peer_id.as_deref() == Some(&peer.id))
            .all(|source| source.completeness == AttentionCompleteness::Unavailable)
    );

    std::fs::write(a.shared.parent().unwrap().join(".ism-root"), "wrong-root").unwrap();
    let owner = b.state.peer_attention.begin(&peer.id).unwrap();
    b.state
        .peer_attention
        .change(&peer.id, owner, Some(true))
        .unwrap();
    attention::refresh(&b.state, &peer, owner).await.unwrap();
    assert!(
        attention::snapshot(&b.state)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.blocks_work)
    );
}
