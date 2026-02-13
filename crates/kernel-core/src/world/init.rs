use super::*;

impl AgentWorld {
    pub fn new(config: RunConfig) -> Self {
        let status = RunStatus {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: config.run_id.clone(),
            current_tick: 0,
            max_ticks: config.max_ticks(),
            mode: RunMode::Paused,
            queue_depth: 0,
        };

        let (min, max) = config.normalized_npc_bounds();
        let count = usize::from((min + max) / 2);
        let mut agents = BTreeMap::new();
        let mut next_wake_tick_by_agent = BTreeMap::new();
        let mut wake_reason_by_agent = BTreeMap::new();
        let mut economy_ledger = EconomyLedger::default();
        let mut social_graph = SocialGraph::default();
        let rumor_network = RumorNetwork::with_distortion_bps(config.rumor_distortion_bps);
        let mut spatial_model = SpatialModel::new();
        let mut institutions = BTreeMap::new();
        let mut observation_bus = ObservationBus::default();

        spatial_model.locations.insert(
            "settlement:greywall".to_string(),
            Location {
                location_id: "settlement:greywall".to_string(),
            },
        );
        spatial_model.locations.insert(
            "settlement:rivergate".to_string(),
            Location {
                location_id: "settlement:rivergate".to_string(),
            },
        );
        spatial_model.routes.insert(
            "route:greywall-rivergate".to_string(),
            Route {
                route_id: "route:greywall-rivergate".to_string(),
                origin: "settlement:greywall".to_string(),
                destination: "settlement:rivergate".to_string(),
                travel_time: 2,
                risk_level: 20,
                weather_modifier: 1,
            },
        );
        institutions.insert(
            "institution:market".to_string(),
            InstitutionState::new("institution:market".to_string(), 8, 1, 75, 20),
        );
        institutions.insert(
            "institution:court".to_string(),
            InstitutionState::new("institution:court".to_string(), 4, 2, 70, 25),
        );
        economy_ledger.accounts.insert(
            "wallet:market".to_string(),
            AccountBalance {
                money: 500,
                food: 200,
                fuel: 80,
                medicine: 30,
            },
        );
        economy_ledger.accounts.insert(
            "wallet:landlord".to_string(),
            AccountBalance {
                money: 200,
                food: 20,
                fuel: 10,
                medicine: 5,
            },
        );
        economy_ledger.accounts.insert(
            "wallet:employer".to_string(),
            AccountBalance {
                money: 1000,
                food: 0,
                fuel: 0,
                medicine: 0,
            },
        );
        economy_ledger.accounts.insert(
            "wallet:joint_escrow".to_string(),
            AccountBalance {
                money: 0,
                food: 0,
                fuel: 0,
                medicine: 0,
            },
        );

        for idx in 0..count {
            let id = format!("npc_{idx:03}");
            next_wake_tick_by_agent.insert(id.clone(), 1);
            wake_reason_by_agent.insert(id.clone(), contracts::WakeReason::Idle);
            let npc_seed = mix_seed(config.seed, idx as u64 + 1);
            let location_id = if sample_range_i64(npc_seed, 1, 0, 100) < 18 {
                "settlement:rivergate".to_string()
            } else {
                "settlement:greywall".to_string()
            };
            let identity = generated_identity(npc_seed);
            let drives = generated_drives(&config, npc_seed);
            agents.insert(
                id.clone(),
                NpcAgent::from_identity(
                    id.clone(),
                    identity,
                    Aspiration {
                        aspiration_id: "asp:survive".to_string(),
                        label: "survive".to_string(),
                        updated_tick: 0,
                        cause: "init".to_string(),
                    },
                    location_id.clone(),
                    drives,
                ),
            );
            observation_bus.register_agent(&id, &location_id);

            economy_ledger.accounts.insert(
                format!("wallet:{id}"),
                AccountBalance {
                    money: 10,
                    food: 2,
                    fuel: 1,
                    medicine: 0,
                },
            );
        }

        let agent_ids = agents.keys().cloned().collect::<Vec<_>>();
        for from in &agent_ids {
            for to in &agent_ids {
                if from == to {
                    continue;
                }
                let edge_seed = mix_seed(config.seed, stable_pair_hash(from, to));
                social_graph.update_edge(
                    from,
                    to,
                    EdgeUpdate {
                        trust_delta: sample_range_i64(edge_seed, 2, -20, 60),
                        reputation_delta: sample_range_i64(edge_seed, 3, -10, 30),
                        ..EdgeUpdate::default()
                    },
                );
            }
        }

        let planner_worker_threads = usize::from(config.planner_worker_threads.max(1));
        let planner_pool = if planner_worker_threads > 1 {
            rayon::ThreadPoolBuilder::new()
                .num_threads(planner_worker_threads)
                .build()
                .ok()
        } else {
            None
        };
        Self {
            planner_config: PlannerConfig {
                beam_width: usize::from(config.planning_beam_width.max(1)),
                horizon: usize::from(config.planning_horizon.max(1)),
                idle_threshold: config.idle_threshold,
            },
            config,
            status,
            queued_commands: Vec::new(),
            event_log: Vec::new(),
            reason_packet_log: Vec::new(),
            agents,
            reason_envelopes: Vec::new(),
            operator_catalog: OperatorCatalog::default_catalog(),
            next_command_sequence: 0,
            weather: "clear".to_string(),
            state_hash: 0,
            replay_hash: 0,
            economy_ledger,
            social_graph,
            rumor_network,
            spatial_model,
            institutions,
            reaction_depth_by_event: BTreeMap::new(),
            groups: BTreeMap::new(),
            stock_staples: 120,
            stock_fuel: 45,
            stock_medicine: 20,
            production_backlog: 0,
            market_price_index: 100,
            emitted_transfer_count: 0,
            last_processed_tick: None,
            action_count_this_tick: BTreeMap::new(),
            channel_reservations: BTreeMap::new(),
            max_actions_per_tick: 2,
            last_operator_by_agent: BTreeMap::new(),
            operator_history_by_agent: BTreeMap::new(),
            last_family_by_agent: BTreeMap::new(),
            family_streak_by_agent: BTreeMap::new(),
            planner_worker_threads,
            planner_pool,
            observation_bus,
            recent_events_by_location: BTreeMap::new(),
            event_index_by_id: BTreeMap::new(),
            next_wake_tick_by_agent,
            wake_reason_by_agent,
            claimed_resources_this_tick: BTreeMap::new(),
            shared_arcs: BTreeMap::new(),
            joint_contracts: BTreeMap::new(),
            last_step_metrics: StepMetrics::default(),
        }
    }
}
