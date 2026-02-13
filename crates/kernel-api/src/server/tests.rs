use super::*;

#[test]
fn trace_builder_walks_causal_chain() {
    let mut config = RunConfig::default();
    config.npc_count_min = 3;
    config.npc_count_max = 3;
    config.snapshot_every_ticks = 2;

    let mut engine = EngineApi::from_config(config.clone());
    engine.run_to_tick(3);

    let root_event = engine
        .events()
        .iter()
        .rev()
        .find(|event| event.event_type == EventType::PressureEconomyUpdated)
        .expect("expected aggregate pressure event");

    let nodes = build_trace_nodes(root_event, engine.events(), engine.reason_packets(), 5);
    assert!(!nodes.is_empty());
    assert_eq!(nodes[0].event.event_id, root_event.event_id);
}

#[test]
fn pagination_enforces_max_bounds() {
    let (start, end, next_cursor) = paginate(100, Some(10), Some(20)).expect("page should work");
    assert_eq!(start, 10);
    assert_eq!(end, 30);
    assert_eq!(next_cursor, Some(30));

    let out_of_range = paginate(5, Some(10), Some(1));
    assert!(out_of_range.is_err());
}
