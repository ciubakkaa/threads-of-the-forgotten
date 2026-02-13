use super::*;
use rayon::prelude::*;

impl AgentWorld {
    pub(super) fn evaluate_ready_agents(
        &mut self,
        ready_agents: Vec<String>,
        tick: u64,
    ) -> Vec<AgentEvaluation> {
        let workloads = ready_agents
            .into_iter()
            .map(|agent_id| (agent_id.clone(), self.observation_bus.drain(&agent_id)))
            .collect::<Vec<_>>();

        let mut evaluations = if self.planner_worker_threads <= 1 || workloads.len() <= 1 {
            workloads
                .into_iter()
                .filter_map(|(agent_id, observations)| {
                    self.evaluate_single_agent(&agent_id, tick, observations)
                })
                .collect::<Vec<_>>()
        } else {
            let world_ref: &AgentWorld = &*self;
            if let Some(pool) = &self.planner_pool {
                pool.install(|| {
                    workloads
                        .into_par_iter()
                        .filter_map(|(agent_id, observations)| {
                            world_ref.evaluate_single_agent(&agent_id, tick, observations)
                        })
                        .collect::<Vec<_>>()
                })
            } else {
                workloads
                    .into_iter()
                    .filter_map(|(agent_id, observations)| {
                        world_ref.evaluate_single_agent(&agent_id, tick, observations)
                    })
                    .collect::<Vec<_>>()
            }
        };

        evaluations.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        evaluations
    }

    pub(super) fn evaluate_single_agent(
        &self,
        agent_id: &str,
        tick: u64,
        observations: Vec<Observation>,
    ) -> Option<AgentEvaluation> {
        let mut updated_agent = self.agents.get(agent_id)?.clone();
        let location_id = updated_agent.location_id.clone();
        let world_view = self.build_world_view_for(agent_id, &location_id, tick);
        let result = updated_agent.tick(
            tick,
            &world_view,
            &observations,
            &self.operator_catalog,
            &self.planner_config,
        );
        Some(AgentEvaluation {
            agent_id: agent_id.to_string(),
            location_id,
            updated_agent,
            result,
        })
    }

    pub(super) fn build_world_view_for(
        &self,
        actor_id: &str,
        location_id: &str,
        tick: u64,
    ) -> PlanningWorldView {
        let food_urgent = self
            .agents
            .get(actor_id)
            .map(|agent| {
                agent.drives.drives.food.current >= agent.drives.drives.food.urgency_threshold
            })
            .unwrap_or(false);
        let morality = self
            .agents
            .get(actor_id)
            .map(|agent| agent.identity.personality.morality)
            .unwrap_or(0);
        let mut npc_ids = self
            .agents
            .iter()
            .filter(|(id, _)| id.as_str() != actor_id)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        npc_ids.sort_by(|a, b| {
            let a_location = self
                .agents
                .get(a)
                .map(|agent| agent.location_id.as_str() == location_id)
                .unwrap_or(false);
            let b_location = self
                .agents
                .get(b)
                .map(|agent| agent.location_id.as_str() == location_id)
                .unwrap_or(false);
            let a_trust = self
                .social_graph
                .get_edge(actor_id, a)
                .map(|edge| edge.trust)
                .unwrap_or(0);
            let b_trust = self
                .social_graph
                .get_edge(actor_id, b)
                .map(|edge| edge.trust)
                .unwrap_or(0);
            let a_jitter = target_jitter(actor_id, a, tick);
            let b_jitter = target_jitter(actor_id, b, tick);
            let a_score = (if a_location { 1 } else { 0 }) * 1_000 + a_trust * 2 + a_jitter;
            let b_score = (if b_location { 1 } else { 0 }) * 1_000 + b_trust * 2 + b_jitter;
            b_score.cmp(&a_score).then(a.cmp(b))
        });
        let location_ids = self
            .agents
            .values()
            .map(|other| other.location_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut facts = BTreeMap::from([
            ("agent.id".to_string(), actor_id.to_string()),
            ("agent.food_urgent".to_string(), food_urgent.to_string()),
            ("agent.morality".to_string(), morality.to_string()),
            ("agent.location".to_string(), location_id.to_string()),
            ("agent.current_tick".to_string(), tick.to_string()),
            ("world.weather".to_string(), self.weather.clone()),
            (
                "world.market_price_index".to_string(),
                self.market_price_index.to_string(),
            ),
            (
                "world.stock_staples".to_string(),
                self.stock_staples.to_string(),
            ),
        ]);
        if let Some(agent) = self.agents.get(actor_id) {
            facts.insert("agent.fatigue".to_string(), agent.fatigue.to_string());
            facts.insert(
                "agent.recreation_need".to_string(),
                agent.recreation_need.to_string(),
            );
            facts.insert(
                "agent.time_budget.work".to_string(),
                agent.time_budget.work.to_string(),
            );
            facts.insert(
                "agent.time_budget.social".to_string(),
                agent.time_budget.social.to_string(),
            );
            facts.insert(
                "agent.time_budget.recovery".to_string(),
                agent.time_budget.recovery.to_string(),
            );
            facts.insert(
                "agent.time_budget.household".to_string(),
                agent.time_budget.household.to_string(),
            );
            facts.insert(
                "agent.time_budget.exploration".to_string(),
                agent.time_budget.exploration.to_string(),
            );
        }
        if let Some(wallet) = self
            .economy_ledger
            .accounts
            .get(&format!("wallet:{actor_id}"))
        {
            facts.insert("agent.money".to_string(), wallet.money.to_string());
            facts.insert("agent.food".to_string(), wallet.food.to_string());
        }
        if let Some(last_operator) = self.last_operator_by_agent.get(actor_id) {
            facts.insert("agent.last_operator".to_string(), last_operator.clone());
        }
        if let Some(last_family) = self.last_family_by_agent.get(actor_id) {
            facts.insert("agent.last_family".to_string(), last_family.clone());
        }
        if let Some(streak) = self.family_streak_by_agent.get(actor_id) {
            facts.insert("agent.family_streak".to_string(), streak.to_string());
        }
        if let Some(history) = self.operator_history_by_agent.get(actor_id) {
            for (idx, operator_id) in history.iter().rev().take(3).enumerate() {
                facts.insert(format!("agent.recent_operator_{}", idx + 1), operator_id.clone());
            }
        }
        if let Some(channels) = self.channel_reservations.get(actor_id) {
            for (channel, reserved_until) in channels {
                if *reserved_until > tick {
                    facts.insert(format!("agent.channel_blocked.{channel}"), "true".to_string());
                }
            }
        }
        for other in &npc_ids {
            let trust = self
                .social_graph
                .get_edge(actor_id, other)
                .map(|edge| edge.trust)
                .unwrap_or(0);
            facts.insert(
                format!("social.trust:{}:{}", actor_id, other),
                trust.to_string(),
            );
            if let Some(other_agent) = self.agents.get(other) {
                facts.insert(
                    format!("npc.location:{other}"),
                    other_agent.location_id.clone(),
                );
            }
        }
        let mut object_ids = Vec::new();
        let location_token = location_id.replace(':', "_");
        for idx in 0..24 {
            object_ids.push(format!("item:coin_purse:{location_token}:{idx:02}"));
            object_ids.push(format!("item:bread:{location_token}:{idx:02}"));
        }

        PlanningWorldView {
            npc_ids,
            location_ids,
            object_ids,
            institution_ids: vec![
                "institution:market".to_string(),
                "institution:court".to_string(),
            ],
            resource_types: vec!["money".to_string(), "food".to_string()],
            facts,
        }
    }

    pub(super) fn commit_agent_evaluation(
        &mut self,
        evaluation: AgentEvaluation,
        tick: u64,
        sequence_in_tick: &mut u64,
    ) {
        let actor_id = evaluation.agent_id.clone();
        let location_id = evaluation.location_id.clone();
        let previous_agent = self.agents.get(&actor_id).cloned();
        self.agents
            .insert(evaluation.agent_id.clone(), evaluation.updated_agent);
        let result = evaluation.result;

        let mut reason_packet_id = None;
        if let Some(envelope) = result.reason.clone() {
            reason_packet_id = Some(self.push_reason_packet(tick, &actor_id, &envelope));
            self.reason_envelopes.push(envelope);
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::PlanCreated,
                location_id.clone(),
                vec![ActorRef {
                    actor_id: actor_id.clone(),
                    actor_kind: "planner".to_string(),
                }],
                reason_packet_id.clone(),
                Vec::new(),
                None,
            );
        }
        if let Some(change) = result.aspiration_change {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::AspirationChanged,
                location_id.clone(),
                vec![ActorRef {
                    actor_id: actor_id.clone(),
                    actor_kind: "aspiring".to_string(),
                }],
                reason_packet_id.clone(),
                Vec::new(),
                Some(json!({
                    "old": change.old_aspiration.map(|asp| asp.label),
                    "new": change.new_aspiration.label,
                    "cause": change.cause,
                })),
            );
        }
        for drive in result.drive_threshold_crossings {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::DriveThresholdCrossed,
                location_id.clone(),
                vec![ActorRef {
                    actor_id: actor_id.clone(),
                    actor_kind: "drive".to_string(),
                }],
                reason_packet_id.clone(),
                Vec::new(),
                Some(json!({ "drive": format!("{drive:?}").to_lowercase() })),
            );
        }
        if let Some(current_agent) = self.agents.get(&actor_id).cloned() {
            self.emit_commitment_lifecycle_events(
                previous_agent.as_ref(),
                &current_agent,
                tick,
                sequence_in_tick,
                &location_id,
                reason_packet_id.clone(),
            );
            self.emit_arc_lifecycle_events(
                previous_agent.as_ref(),
                &current_agent,
                tick,
                sequence_in_tick,
                &location_id,
                reason_packet_id.clone(),
            );
        }

        match result.action {
            AgentAction::Execute(bound) => {
                self.increment_action_count(&actor_id);
                if let Some(target_npc) = bound.parameters.target_npc.as_ref() {
                    let target_location = self
                        .agents
                        .get(target_npc)
                        .map(|npc| npc.location_id.clone())
                        .unwrap_or_else(|| location_id.clone());
                    if !self
                        .spatial_model
                        .can_interact_same_location(&location_id, &target_location)
                    {
                        self.mark_agent_for_replan(&actor_id, tick);
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::PlanInterrupted,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "replan".to_string(),
                            }],
                            reason_packet_id.clone(),
                            Vec::new(),
                            Some(json!({
                                "reason": "location_mismatch",
                                "target_npc": target_npc,
                            })),
                        );
                        self.schedule_follow_up(
                            &actor_id,
                            tick,
                            0,
                            contracts::WakeReason::Reactive,
                        );
                        return;
                    }
                }

                let channels = inferred_operator_channels(
                    self.operator_catalog.operator_by_id(&bound.operator_id),
                    &bound.operator_id,
                    &bound.parameters,
                );
                if !self.channels_available(&actor_id, &channels, tick) {
                    self.mark_agent_for_replan(&actor_id, tick);
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::PlanInterrupted,
                        location_id.clone(),
                        vec![ActorRef {
                            actor_id: actor_id.clone(),
                            actor_kind: "replan".to_string(),
                        }],
                        reason_packet_id.clone(),
                        Vec::new(),
                        Some(json!({ "reason": "channel_conflict", "channels": channels })),
                    );
                    self.schedule_follow_up(&actor_id, tick, 0, contracts::WakeReason::Reactive);
                    return;
                }

                let conflict_key = inferred_conflict_key(&actor_id, &bound);
                let mutation = WorldMutation {
                    agent_id: actor_id.clone(),
                    operator_id: bound.operator_id.clone(),
                    deltas: conflict_key
                        .as_ref()
                        .map(|key| {
                            vec![WorldFactDelta {
                                fact_key: key.clone(),
                                delta: 1,
                                from_value: 0,
                                to_value: 1,
                                location_id: location_id.clone(),
                            }]
                        })
                        .unwrap_or_default(),
                    resource_transfers: Vec::new(),
                };

                if let Some(resource_key) = mutation_resource_key(&mutation) {
                    let capacity = resource_capacity(&resource_key);
                    let mut conflicting_agent_id = None::<String>;
                    let mut claimants_snapshot = Vec::<String>::new();
                    {
                        let claimants = self
                            .claimed_resources_this_tick
                            .entry(resource_key.clone())
                            .or_default();
                        if !claimants.iter().any(|id| id == &actor_id)
                            && claimants.len() >= capacity
                        {
                            conflicting_agent_id = claimants.first().cloned();
                            claimants_snapshot = claimants.clone();
                        } else if !claimants.iter().any(|id| id == &actor_id) {
                            claimants.push(actor_id.clone());
                        }
                    }
                    if let Some(conflicting_agent_id) = conflicting_agent_id {
                        self.mark_agent_for_replan(&actor_id, tick);
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::ConflictDetected,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "contender".to_string(),
                            }],
                            reason_packet_id.clone(),
                            Vec::new(),
                            Some(json!({
                                "conflicting_agent_id": conflicting_agent_id,
                                "resource": resource_key,
                                "resource_capacity": capacity,
                                "current_claimants": claimants_snapshot,
                            })),
                        );
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::PlanInterrupted,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "replan".to_string(),
                            }],
                            None,
                            Vec::new(),
                            Some(json!({ "reason": "conflict_rejected" })),
                        );
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::ConflictResolved,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "contender".to_string(),
                            }],
                            reason_packet_id,
                            Vec::new(),
                            Some(json!({
                                "conflicting_agent_id": conflicting_agent_id,
                                "resource": resource_key,
                                "resource_capacity": capacity,
                            })),
                        );
                        let backoff = conflict_backoff_ticks(&actor_id, &resource_key);
                        self.schedule_follow_up(
                            &actor_id,
                            tick,
                            backoff,
                            contracts::WakeReason::Interrupted,
                        );
                        return;
                    }
                }

                let operator_id = bound.operator_id.clone();
                let parameters = bound.parameters.clone();
                let event_id = self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::NpcActionCommitted,
                    location_id.clone(),
                    vec![ActorRef {
                        actor_id: actor_id.clone(),
                        actor_kind: "actor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    Vec::new(),
                    Some(json!({
                        "operator_id": operator_id,
                        "parameters": parameters,
                        "visibility_scope": "local",
                        "directed_target": bound.parameters.target_npc,
                    })),
                );
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::PlanStepStarted,
                    location_id.clone(),
                    vec![ActorRef {
                        actor_id: actor_id.clone(),
                        actor_kind: "executor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![event_id.clone()],
                    None,
                );

                self.apply_operator_side_effects(
                    &actor_id,
                    &location_id,
                    &bound,
                    tick,
                    sequence_in_tick,
                    &event_id,
                    reason_packet_id.clone(),
                );
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::PlanStepCompleted,
                    location_id.clone(),
                    vec![ActorRef {
                        actor_id: actor_id.clone(),
                        actor_kind: "executor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![event_id.clone()],
                    None,
                );
                self.last_operator_by_agent
                    .insert(actor_id.clone(), operator_id.clone());
                self.record_operator_history(&actor_id, &operator_id);
                self.reserve_channels(&actor_id, &channels, tick, bound.duration_ticks);
                self.schedule_observer_reactions(
                    &location_id,
                    &actor_id,
                    &event_id,
                    bound.parameters.target_npc.as_deref(),
                    tick,
                    sequence_in_tick,
                );

                self.schedule_follow_up(
                    &actor_id,
                    tick,
                    bound.duration_ticks,
                    contracts::WakeReason::PlanStepComplete,
                );
            }
            AgentAction::Idle(reason) => {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::AgentIdle,
                    location_id.clone(),
                    vec![ActorRef {
                        actor_id: actor_id.clone(),
                        actor_kind: "idle".to_string(),
                    }],
                    reason_packet_id,
                    Vec::new(),
                    Some(json!({ "reason": reason })),
                );
                self.schedule_follow_up(&actor_id, tick, 1, contracts::WakeReason::Idle);
            }
            AgentAction::Continue => {
                self.schedule_follow_up(
                    &actor_id,
                    tick,
                    1,
                    contracts::WakeReason::PlanStepComplete,
                );
            }
            AgentAction::Replan => {
                self.mark_agent_for_replan(&actor_id, tick);
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::PlanInterrupted,
                    location_id,
                    vec![ActorRef {
                        actor_id: actor_id.clone(),
                        actor_kind: "replan".to_string(),
                    }],
                    reason_packet_id,
                    Vec::new(),
                    Some(json!({ "reason": "agent_requested_replan" })),
                );
                self.increment_action_count(&actor_id);
                self.schedule_follow_up(&actor_id, tick, 0, contracts::WakeReason::Reactive);
            }
        }
    }

    pub(super) fn mark_agent_for_replan(&mut self, actor_id: &str, tick: u64) {
        self.channel_reservations.remove(actor_id);
        if let Some(agent) = self.agents.get_mut(actor_id) {
            if let Some(active_plan) = agent.active_plan.as_ref() {
                let commitment_id = format!("commitment:{}:{}", actor_id, active_plan.plan_id);
                if let Some(commitment) = agent
                    .commitments
                    .iter_mut()
                    .find(|commitment| commitment.commitment_id == commitment_id)
                {
                    commitment.status = "broken".to_string();
                    commitment.due_tick = tick;
                }
            }
            agent.active_plan = None;
            agent.occupancy = contracts::NpcOccupancyState {
                npc_id: actor_id.to_string(),
                tick,
                occupancy: contracts::NpcOccupancyKind::Idle,
                state_tag: "replan".to_string(),
                until_tick: tick + 1,
                interruptible: true,
                location_id: agent.location_id.clone(),
                active_plan_id: None,
                active_operator_id: None,
            };
        }
    }

    fn emit_commitment_lifecycle_events(
        &mut self,
        previous_agent: Option<&NpcAgent>,
        current_agent: &NpcAgent,
        tick: u64,
        sequence_in_tick: &mut u64,
        location_id: &str,
        reason_packet_id: Option<String>,
    ) {
        let prev = previous_agent
            .map(|agent| {
                agent
                    .commitments
                    .iter()
                    .map(|commitment| (commitment.commitment_id.clone(), commitment.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let current = current_agent
            .commitments
            .iter()
            .map(|commitment| (commitment.commitment_id.clone(), commitment.clone()))
            .collect::<BTreeMap<_, _>>();

        for (commitment_id, commitment) in &current {
            let plan_id = commitment_plan_id(commitment_id);
            let track = plan_id.and_then(|plan_id| {
                current_agent
                    .goal_tracks
                    .iter()
                    .find(|track| track.plan_id == plan_id)
            });
            let track_details = track.map(|track| {
                json!({
                    "track_id": track.track_id,
                    "stage": track.stage,
                    "status": format!("{:?}", track.status).to_lowercase(),
                    "progress_ticks": track.progress_ticks,
                    "blockers": track.blockers,
                    "last_break_reason": track.last_break_reason,
                })
            });
            match prev.get(commitment_id) {
                None => {
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::CommitmentStarted,
                        location_id.to_string(),
                        vec![ActorRef {
                            actor_id: current_agent.id.clone(),
                            actor_kind: "committer".to_string(),
                        }],
                        reason_packet_id.clone(),
                        Vec::new(),
                        Some(json!({
                            "commitment_id": commitment.commitment_id,
                            "action_family": commitment.action_family,
                            "due_tick": commitment.due_tick,
                            "inertia_score": commitment.inertia_score,
                            "status": commitment.status,
                            "track": track_details,
                        })),
                    );
                }
                Some(previous) => {
                    if commitment.status == "completed" && previous.status != "completed" {
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::CommitmentCompleted,
                            location_id.to_string(),
                            vec![ActorRef {
                                actor_id: current_agent.id.clone(),
                                actor_kind: "committer".to_string(),
                            }],
                            reason_packet_id.clone(),
                            Vec::new(),
                            Some(json!({
                                "commitment_id": commitment.commitment_id,
                                "action_family": commitment.action_family,
                                "progress_ticks": commitment.progress_ticks,
                                "track": track_details,
                            })),
                        );
                    } else if commitment.status == "broken" && previous.status != "broken" {
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::CommitmentBroken,
                            location_id.to_string(),
                            vec![ActorRef {
                                actor_id: current_agent.id.clone(),
                                actor_kind: "committer".to_string(),
                            }],
                            reason_packet_id.clone(),
                            Vec::new(),
                            Some(json!({
                                "commitment_id": commitment.commitment_id,
                                "action_family": commitment.action_family,
                                "progress_ticks": commitment.progress_ticks,
                                "track": track_details,
                            })),
                        );
                    } else if commitment.status == "active"
                        && previous.status == "active"
                        && commitment.progress_ticks > previous.progress_ticks
                    {
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::CommitmentContinued,
                            location_id.to_string(),
                            vec![ActorRef {
                                actor_id: current_agent.id.clone(),
                                actor_kind: "committer".to_string(),
                            }],
                            reason_packet_id.clone(),
                            Vec::new(),
                            Some(json!({
                                "commitment_id": commitment.commitment_id,
                                "action_family": commitment.action_family,
                                "progress_ticks": commitment.progress_ticks,
                                "delta_progress": commitment.progress_ticks.saturating_sub(previous.progress_ticks),
                                "track": track_details,
                            })),
                        );
                    }
                }
            }
        }
    }

    fn emit_arc_lifecycle_events(
        &mut self,
        previous_agent: Option<&NpcAgent>,
        current_agent: &NpcAgent,
        tick: u64,
        sequence_in_tick: &mut u64,
        location_id: &str,
        reason_packet_id: Option<String>,
    ) {
        let prev = previous_agent
            .map(|agent| {
                agent
                    .arc_tracks
                    .iter()
                    .map(|track| (track.arc_id.clone(), track.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let current = current_agent
            .arc_tracks
            .iter()
            .map(|track| (track.arc_id.clone(), track.clone()))
            .collect::<BTreeMap<_, _>>();

        for (arc_id, track) in &current {
            let stage = track
                .milestones
                .get(track.milestone_index)
                .cloned()
                .unwrap_or_else(|| "complete".to_string());
            let details = Some(json!({
                "arc_id": arc_id,
                "family": track.family,
                "title": track.title,
                "stage": stage,
                "milestone_index": track.milestone_index,
                "progress": track.progress,
                "status": format!("{:?}", track.status).to_lowercase(),
                "blockers": track.blockers,
                "last_break_reason": track.last_break_reason,
                "reason": track
                    .last_break_reason
                    .clone()
                    .unwrap_or_else(|| "status_change".to_string()),
            }));
            match prev.get(arc_id) {
                None => {
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::NarrativeWhySummary,
                        location_id.to_string(),
                        vec![ActorRef {
                            actor_id: current_agent.id.clone(),
                            actor_kind: "arc_owner".to_string(),
                        }],
                        reason_packet_id.clone(),
                        Vec::new(),
                        details,
                    );
                }
                Some(previous) => {
                    if track.milestone_index > previous.milestone_index
                        || (matches!(track.status, crate::agent::ArcTrackStatus::Completed)
                            && !matches!(previous.status, crate::agent::ArcTrackStatus::Completed))
                    {
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::NarrativeWhySummary,
                            location_id.to_string(),
                            vec![ActorRef {
                                actor_id: current_agent.id.clone(),
                                actor_kind: "arc_owner".to_string(),
                            }],
                            reason_packet_id.clone(),
                            Vec::new(),
                            details.clone(),
                        );
                    } else if track.status != previous.status {
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::OpportunityRejected,
                            location_id.to_string(),
                            vec![ActorRef {
                                actor_id: current_agent.id.clone(),
                                actor_kind: "arc_owner".to_string(),
                            }],
                            reason_packet_id.clone(),
                            Vec::new(),
                            details.clone(),
                        );
                    }
                }
            }
        }
    }

    pub(super) fn schedule_follow_up(
        &mut self,
        actor_id: &str,
        tick: u64,
        duration: u64,
        reason: contracts::WakeReason,
    ) {
        let current = self
            .action_count_this_tick
            .get(actor_id)
            .copied()
            .unwrap_or(0);
        let next_tick = if duration > 0 {
            tick + duration
        } else if current < self.max_actions_per_tick {
            if self.has_active_channel_reservation(actor_id, tick) {
                tick + 1
            } else {
                tick
            }
        } else {
            tick + 1
        };
        self.schedule_unique(actor_id, next_tick, reason);
    }

    pub(super) fn increment_action_count(&mut self, actor_id: &str) {
        let entry = self
            .action_count_this_tick
            .entry(actor_id.to_string())
            .or_insert(0);
        *entry = entry.saturating_add(1);
    }

    pub(super) fn record_operator_history(&mut self, actor_id: &str, operator_id: &str) {
        let family = operator_id.split(':').next().unwrap_or("general").to_string();
        let previous_family = self
            .last_family_by_agent
            .insert(actor_id.to_string(), family.clone());
        let streak = self
            .family_streak_by_agent
            .entry(actor_id.to_string())
            .or_insert(0);
        if previous_family.as_deref() == Some(family.as_str()) {
            *streak = streak.saturating_add(1).min(8);
        } else {
            *streak = 1;
        }

        let history = self
            .operator_history_by_agent
            .entry(actor_id.to_string())
            .or_default();
        history.push_back(operator_id.to_string());
        while history.len() > 6 {
            let _ = history.pop_front();
        }
    }

    pub(super) fn channels_available(
        &self,
        actor_id: &str,
        channels: &[String],
        tick: u64,
    ) -> bool {
        let Some(existing) = self.channel_reservations.get(actor_id) else {
            return true;
        };
        channels.iter().all(|channel| {
            existing
                .get(channel)
                .map(|reserved_until| *reserved_until <= tick)
                .unwrap_or(true)
        })
    }

    pub(super) fn reserve_channels(
        &mut self,
        actor_id: &str,
        channels: &[String],
        tick: u64,
        duration: u64,
    ) {
        if duration == 0 {
            return;
        }
        let reservations = self
            .channel_reservations
            .entry(actor_id.to_string())
            .or_default();
        for channel in channels {
            reservations.insert(channel.clone(), tick + duration);
        }
    }

    pub(super) fn prune_channel_reservations(&mut self, tick: u64) {
        self.channel_reservations.retain(|_, channels| {
            channels.retain(|_, reserved_until| *reserved_until > tick);
            !channels.is_empty()
        });
    }

    fn has_active_channel_reservation(&self, actor_id: &str, tick: u64) -> bool {
        self.channel_reservations
            .get(actor_id)
            .is_some_and(|channels| channels.values().any(|reserved_until| *reserved_until > tick))
    }

    pub(super) fn apply_operator_side_effects(
        &mut self,
        actor_id: &str,
        location_id: &str,
        bound: &contracts::BoundOperator,
        tick: u64,
        sequence_in_tick: &mut u64,
        source_event_id: &str,
        reason_packet_id: Option<String>,
    ) {
        if let Some(operator) = self.operator_catalog.operator_by_id(&bound.operator_id) {
            if let Some(agent) = self.agents.get_mut(actor_id) {
                for (drive, relief) in &operator.drive_effects {
                    if *relief > 0 {
                        agent.drives.apply_effect(*drive, *relief);
                    }
                }
            }
        }

        if bound.operator_id.contains("steal") {
            let mut victim = "wallet:market".to_string();
            if let Some(target_npc) = bound.parameters.target_npc.as_ref() {
                victim = format!("wallet:{target_npc}");
            }
            let thief_wallet = format!("wallet:{actor_id}");
            let transfer = self.economy_ledger.transfer(
                &victim,
                &thief_wallet,
                ResourceKind::Money,
                1,
                source_event_id,
                tick,
            );
            if transfer.is_ok() {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::TheftCommitted,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "actor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    Some(json!({ "victim_wallet": victim })),
                );
                if let Some(target_npc) = bound.parameters.target_npc.as_ref() {
                    self.social_graph.update_edge(
                        actor_id,
                        target_npc,
                        EdgeUpdate {
                            trust_delta: -self.config.relationship_negative_trust_delta,
                            grievance_delta: self.config.relationship_grievance_delta,
                            fear_delta: 10,
                            ..EdgeUpdate::default()
                        },
                    );
                }
            }
        }

        if bound.operator_id.contains("social") {
            if let Some(target_npc) = bound.parameters.target_npc.as_ref() {
                self.social_graph.update_edge(
                    actor_id,
                    target_npc,
                    EdgeUpdate {
                        trust_delta: self.config.relationship_positive_trust_delta,
                        respect_delta: 5,
                        obligation_delta: 3,
                        ..EdgeUpdate::default()
                    },
                );
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::RelationshipShifted,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "actor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    Some(json!({ "target_npc": target_npc })),
                );
                self.update_shared_arc_from_interaction(
                    actor_id,
                    target_npc,
                    &bound.operator_id,
                    tick,
                    sequence_in_tick,
                    location_id,
                    source_event_id,
                    reason_packet_id.clone(),
                );

                if bound.operator_id.contains("gossip") {
                    let rumor = RumorPayload {
                        rumor_id: format!("rumor:{tick}:{}", self.event_log.len() + 1),
                        source_npc_id: actor_id.to_string(),
                        core_claim: "street_whispers".to_string(),
                        details: format!("{actor_id} gossiped about {target_npc}"),
                        hop_count: 0,
                    };
                    self.propagate_rumor(
                        &rumor,
                        actor_id,
                        location_id,
                        tick,
                        sequence_in_tick,
                        source_event_id,
                    );
                }
            }
        }

        if bound.operator_id.contains("work") {
            let worker = format!("wallet:{actor_id}");
            if self
                .economy_ledger
                .process_wage("wallet:employer", &worker, 1, tick)
                .is_ok()
            {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::WagePaid,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "worker".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    None,
                );
            }
        }

        if bound.operator_id.contains("eat") {
            let eater = format!("wallet:{actor_id}");
            let _ = self.economy_ledger.transfer(
                &eater,
                "wallet:market",
                ResourceKind::Food,
                1,
                source_event_id,
                tick,
            );
        }

        if bound.operator_id.starts_with("leisure:") {
            if matches!(
                bound.operator_id.as_str(),
                "leisure:drink" | "leisure:gamble" | "leisure:attend_event"
            ) {
                let actor_wallet = format!("wallet:{actor_id}");
                let _ = self.economy_ledger.transfer(
                    &actor_wallet,
                    "wallet:market",
                    ResourceKind::Money,
                    1,
                    source_event_id,
                    tick,
                );
            }
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::ConversationHeld,
                location_id.to_string(),
                vec![ActorRef {
                    actor_id: actor_id.to_string(),
                    actor_kind: "participant".to_string(),
                }],
                reason_packet_id.clone(),
                vec![source_event_id.to_string()],
                Some(json!({ "operator_id": bound.operator_id })),
            );
        }

        if bound.operator_id.contains("travel") {
            if let Some(target_location) = bound.parameters.target_location.as_ref() {
                let can_reach = self.spatial_model.locations.contains_key(target_location);
                if can_reach {
                    if let Some(agent) = self.agents.get_mut(actor_id) {
                        let previous_location = agent.location_id.clone();
                        agent.location_id = target_location.clone();
                        agent.occupancy.location_id = target_location.clone();
                        self.observation_bus.move_agent(
                            actor_id,
                            &previous_location,
                            target_location,
                        );
                    }
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::TravelWindowShifted,
                        target_location.clone(),
                        vec![ActorRef {
                            actor_id: actor_id.to_string(),
                            actor_kind: "traveler".to_string(),
                        }],
                        reason_packet_id,
                        vec![source_event_id.to_string()],
                        None,
                    );
                }
            }
        }

        if bound.operator_id.contains("petition") || bound.operator_id.contains("enforce_law") {
            if let Some(institution) = self.institutions.get_mut("institution:court") {
                let queued = institution.enqueue(ServiceRequest {
                    request_id: format!("svc:{tick}:{}", institution.queue.len() + 1),
                    npc_id: actor_id.to_string(),
                    submitted_tick: tick,
                    status_score: 40,
                    connected_score: 30,
                });
                if !queued {
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::InstitutionQueueFull,
                        location_id.to_string(),
                        vec![ActorRef {
                            actor_id: actor_id.to_string(),
                            actor_kind: "requester".to_string(),
                        }],
                        None,
                        vec![source_event_id.to_string()],
                        Some(json!({ "institution_id": "institution:court" })),
                    );
                }
            }
        }
    }

    fn update_shared_arc_from_interaction(
        &mut self,
        actor_id: &str,
        target_npc: &str,
        operator_id: &str,
        tick: u64,
        sequence_in_tick: &mut u64,
        location_id: &str,
        source_event_id: &str,
        reason_packet_id: Option<String>,
    ) {
        if actor_id == target_npc {
            return;
        }
        let (left, right) = if actor_id <= target_npc {
            (actor_id.to_string(), target_npc.to_string())
        } else {
            (target_npc.to_string(), actor_id.to_string())
        };

        let arc_kind = if operator_id.contains("court") {
            "courtship"
        } else if operator_id.contains("betray")
            || operator_id.contains("threaten")
            || operator_id.contains("argue")
        {
            "rivalry"
        } else {
            "bond"
        };
        let key = format!("shared_arc:{arc_kind}:{left}:{right}");
        let stages = match arc_kind {
            "courtship" => vec![
                "noticed".to_string(),
                "courting".to_string(),
                "exclusive".to_string(),
                "household_union".to_string(),
            ],
            "rivalry" => vec![
                "friction".to_string(),
                "public_dispute".to_string(),
                "feud".to_string(),
            ],
            _ => vec![
                "acquaintance".to_string(),
                "regular_contact".to_string(),
                "trusted_pair".to_string(),
                "shared_life".to_string(),
            ],
        };
        let mut created = false;
        if !self.shared_arcs.contains_key(&key) {
            self.shared_arcs.insert(
                key.clone(),
                SharedArcState {
                    arc_id: key.clone(),
                    participants: vec![left.clone(), right.clone()],
                    arc_kind: arc_kind.to_string(),
                    stage_index: 0,
                    stage_label: stages[0].clone(),
                    progress: 0,
                    status: "active".to_string(),
                    joint_plan_status: None,
                    joint_plan_id: None,
                    joint_plan_proposer: None,
                    joint_plan_last_actor: None,
                    joint_plan_updated_tick: None,
                    started_tick: tick,
                    last_updated_tick: tick,
                    root_event_id: Some(source_event_id.to_string()),
                },
            );
            created = true;
        }

        let delta = if operator_id.contains("court") {
            18
        } else if operator_id.contains("confide") || operator_id.contains("console") {
            12
        } else if operator_id.contains("betray") {
            -28
        } else if operator_id.contains("threaten") || operator_id.contains("argue") {
            -14
        } else {
            7
        };

        let mut maybe_romance_complete = false;
        let mut maybe_broken = false;
        let mut stage_changed = false;
        let mut joint_plan_opened = false;
        let mut joint_plan_accepted = false;
        let mut joint_plan_executing = false;
        let mut joint_plan_completed = false;
        let mut joint_plan_rejected = false;
        if let Some(shared_arc) = self.shared_arcs.get_mut(&key) {
            shared_arc.progress = (shared_arc.progress + delta).clamp(-100, 400);
            shared_arc.last_updated_tick = tick;
            if shared_arc.progress <= -45 {
                shared_arc.status = "broken".to_string();
                shared_arc.stage_label = "collapsed".to_string();
                if shared_arc.joint_plan_status.is_some() {
                    shared_arc.joint_plan_status = Some("broken".to_string());
                    shared_arc.joint_plan_last_actor = Some(actor_id.to_string());
                    shared_arc.joint_plan_updated_tick = Some(tick);
                    joint_plan_rejected = true;
                }
                maybe_broken = true;
            } else if shared_arc.progress >= 100 {
                shared_arc.progress -= 100;
                let next_stage = shared_arc
                    .stage_index
                    .saturating_add(1)
                    .min(stages.len().saturating_sub(1));
                if next_stage > shared_arc.stage_index {
                    shared_arc.stage_index = next_stage;
                    shared_arc.stage_label = stages[next_stage].clone();
                    stage_changed = true;
                }
                if shared_arc.stage_index + 1 >= stages.len() {
                    shared_arc.status = "completed".to_string();
                    if shared_arc
                        .joint_plan_status
                        .as_ref()
                        .is_some_and(|status| status == "executing")
                    {
                        shared_arc.joint_plan_status = Some("completed".to_string());
                        shared_arc.joint_plan_last_actor = Some(actor_id.to_string());
                        shared_arc.joint_plan_updated_tick = Some(tick);
                        joint_plan_completed = true;
                    }
                    if shared_arc.arc_kind == "courtship" {
                        maybe_romance_complete = true;
                    }
                } else {
                    shared_arc.status = "active".to_string();
                }
            } else {
                shared_arc.status = "active".to_string();
            }

            if shared_arc.status != "broken" {
                let is_positive = delta > 0;
                let status = shared_arc
                    .joint_plan_status
                    .clone()
                    .unwrap_or_else(|| "none".to_string());
                if status == "none" && is_positive && shared_arc.stage_index >= 1 {
                    shared_arc.joint_plan_status = Some("proposed".to_string());
                    shared_arc.joint_plan_id = Some(format!(
                        "joint:{}:{}",
                        shared_arc.arc_id, shared_arc.stage_index
                    ));
                    shared_arc.joint_plan_proposer = Some(actor_id.to_string());
                    shared_arc.joint_plan_last_actor = Some(actor_id.to_string());
                    shared_arc.joint_plan_updated_tick = Some(tick);
                    joint_plan_opened = true;
                } else if status == "proposed"
                    && is_positive
                    && shared_arc
                        .joint_plan_proposer
                        .as_ref()
                        .is_some_and(|proposer| proposer != actor_id)
                {
                    shared_arc.joint_plan_status = Some("accepted".to_string());
                    shared_arc.joint_plan_last_actor = Some(actor_id.to_string());
                    shared_arc.joint_plan_updated_tick = Some(tick);
                    joint_plan_accepted = true;
                } else if status == "accepted" && is_positive {
                    shared_arc.joint_plan_status = Some("executing".to_string());
                    shared_arc.joint_plan_last_actor = Some(actor_id.to_string());
                    shared_arc.joint_plan_updated_tick = Some(tick);
                    joint_plan_executing = true;
                } else if matches!(status.as_str(), "proposed" | "accepted") && delta < 0 {
                    shared_arc.joint_plan_status = Some("rejected".to_string());
                    shared_arc.joint_plan_last_actor = Some(actor_id.to_string());
                    shared_arc.joint_plan_updated_tick = Some(tick);
                    joint_plan_rejected = true;
                }
            }
        }

        if let Some(shared_arc) = self.shared_arcs.get(&key).cloned() {
            let joint_plan_id = shared_arc.joint_plan_id.clone();
            let contract_id = format!("contract:{}", shared_arc.arc_id);
            let details = Some(json!({
                "arc_id": shared_arc.arc_id.clone(),
                "arc_kind": shared_arc.arc_kind.clone(),
                "participants": shared_arc.participants.clone(),
                "stage_index": shared_arc.stage_index,
                "stage": shared_arc.stage_label.clone(),
                "progress": shared_arc.progress,
                "status": shared_arc.status.clone(),
                "joint_plan_status": shared_arc.joint_plan_status.clone(),
                "joint_plan_id": shared_arc.joint_plan_id.clone(),
                "joint_plan_proposer": shared_arc.joint_plan_proposer.clone(),
                "joint_plan_last_actor": shared_arc.joint_plan_last_actor.clone(),
                "joint_plan_updated_tick": shared_arc.joint_plan_updated_tick,
                "contract_id": contract_id,
                "operator_id": operator_id,
            }));
            if created {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::GroupMembershipChanged,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "arc_initiator".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    details.clone(),
                );
            }
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::NarrativeWhySummary,
                location_id.to_string(),
                vec![ActorRef {
                    actor_id: actor_id.to_string(),
                    actor_kind: "arc_actor".to_string(),
                }],
                reason_packet_id.clone(),
                vec![source_event_id.to_string()],
                details.clone(),
            );
            if joint_plan_opened {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::OpportunityOpened,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "plan_proposer".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    details.clone(),
                );
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::ContractSigned,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "contract_proposer".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    details.clone(),
                );
            }
            if joint_plan_accepted {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::OpportunityAccepted,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "plan_acceptor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    details.clone(),
                );
            }
            if joint_plan_executing {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::CommitmentStarted,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "joint_executor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    details.clone(),
                );
            }
            if stage_changed {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::OpportunityAccepted,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "milestone_advancer".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    details.clone(),
                );
            }
            if maybe_broken {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::CommitmentBroken,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "arc_actor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    details.clone(),
                );
            }
            if joint_plan_rejected {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::OpportunityRejected,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "plan_rejector".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    details.clone(),
                );
            }
            if joint_plan_completed {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::CommitmentCompleted,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "joint_executor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    details.clone(),
                );
            }
            if maybe_romance_complete {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::RomanceAdvanced,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "arc_actor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    details,
                );
            }

            if let Some(joint_plan_id) = joint_plan_id {
                let commitment_id = format!("joint_commitment:{joint_plan_id}");
                if joint_plan_opened {
                    self.ensure_joint_contract(
                        &contract_id,
                        &shared_arc.arc_id,
                        &shared_arc.arc_kind,
                        &shared_arc.participants,
                        tick,
                    );
                }
                if joint_plan_accepted || joint_plan_executing {
                    self.upsert_joint_commitment(
                        actor_id,
                        target_npc,
                        &commitment_id,
                        &shared_arc.arc_kind,
                        "active",
                        tick,
                    );
                    self.apply_joint_contract_contribution(
                        &contract_id,
                        actor_id,
                        operator_id,
                        tick,
                    );
                }
                if joint_plan_completed {
                    self.upsert_joint_commitment(
                        actor_id,
                        target_npc,
                        &commitment_id,
                        &shared_arc.arc_kind,
                        "completed",
                        tick,
                    );
                    self.apply_joint_contract_contribution(
                        &contract_id,
                        actor_id,
                        operator_id,
                        tick,
                    );
                    if shared_arc.arc_kind == "courtship" {
                        self.promote_household_union(
                            actor_id,
                            target_npc,
                            &shared_arc.arc_id,
                            tick,
                            sequence_in_tick,
                            location_id,
                            source_event_id,
                            reason_packet_id.clone(),
                        );
                    }
                }
                if joint_plan_rejected || maybe_broken {
                    self.upsert_joint_commitment(
                        actor_id,
                        target_npc,
                        &commitment_id,
                        &shared_arc.arc_kind,
                        "broken",
                        tick,
                    );
                }
            }
        }
    }

    fn upsert_joint_commitment(
        &mut self,
        actor_id: &str,
        target_npc: &str,
        commitment_id: &str,
        action_family: &str,
        status: &str,
        tick: u64,
    ) {
        for npc_id in [actor_id, target_npc] {
            let Some(agent) = self.agents.get_mut(npc_id) else {
                continue;
            };
            if let Some(existing) = agent
                .commitments
                .iter_mut()
                .find(|commitment| commitment.commitment_id == commitment_id)
            {
                existing.status = status.to_string();
                existing.due_tick = tick + 12;
                if status == "active" {
                    existing.progress_ticks = existing.progress_ticks.saturating_add(1);
                }
            } else {
                agent.commitments.push(CommitmentState {
                    commitment_id: commitment_id.to_string(),
                    npc_id: npc_id.to_string(),
                    action_family: format!("joint:{action_family}"),
                    started_tick: tick,
                    due_tick: tick + 12,
                    cadence_ticks: 1,
                    progress_ticks: if status == "active" { 1 } else { 0 },
                    inertia_score: 45,
                    status: status.to_string(),
                });
                if agent.commitments.len() > 12 {
                    agent
                        .commitments
                        .sort_by(|a, b| b.started_tick.cmp(&a.started_tick));
                    agent.commitments.truncate(12);
                }
            }
        }
    }

    fn ensure_joint_contract(
        &mut self,
        contract_id: &str,
        arc_id: &str,
        arc_kind: &str,
        participants: &[String],
        tick: u64,
    ) {
        if self.joint_contracts.contains_key(contract_id) {
            return;
        }
        let mut contributions = BTreeMap::new();
        for participant in participants {
            contributions.insert(
                participant.clone(),
                JointContribution {
                    money_paid: 0,
                    actions_done: 0,
                },
            );
        }
        self.joint_contracts.insert(
            contract_id.to_string(),
            JointContract {
                contract_id: contract_id.to_string(),
                arc_id: arc_id.to_string(),
                participants: participants.to_vec(),
                arc_kind: arc_kind.to_string(),
                status: "active".to_string(),
                proposed_tick: tick,
                due_tick: tick + 24,
                required_money_each: if arc_kind == "courtship" { 4 } else { 2 },
                required_actions_each: if arc_kind == "courtship" { 3 } else { 2 },
                contributions,
                last_updated_tick: tick,
            },
        );
    }

    fn apply_joint_contract_contribution(
        &mut self,
        contract_id: &str,
        actor_id: &str,
        operator_id: &str,
        tick: u64,
    ) {
        let Some(contract) = self.joint_contracts.get_mut(contract_id) else {
            return;
        };
        if !contract.participants.iter().any(|npc| npc == actor_id) {
            return;
        }
        if !matches!(
            contract.status.as_str(),
            "active" | "accepted" | "executing"
        ) {
            return;
        }
        let contribution = contract
            .contributions
            .entry(actor_id.to_string())
            .or_insert(JointContribution {
                money_paid: 0,
                actions_done: 0,
            });
        let family = operator_id.split(':').next().unwrap_or("household");
        if matches!(family, "social" | "household" | "leisure") {
            contribution.actions_done = contribution.actions_done.saturating_add(1);
        }
        if matches!(family, "livelihood" | "trade" | "household") {
            let from = format!("wallet:{actor_id}");
            if self
                .economy_ledger
                .transfer(
                    &from,
                    "wallet:joint_escrow",
                    ResourceKind::Money,
                    1,
                    contract_id,
                    tick,
                )
                .is_ok()
            {
                contribution.money_paid = contribution.money_paid.saturating_add(1);
            }
        }
        contract.status = "executing".to_string();
        contract.last_updated_tick = tick;
    }

    fn promote_household_union(
        &mut self,
        actor_id: &str,
        target_npc: &str,
        arc_id: &str,
        tick: u64,
        sequence_in_tick: &mut u64,
        location_id: &str,
        source_event_id: &str,
        reason_packet_id: Option<String>,
    ) {
        for npc_id in [actor_id, target_npc] {
            if let Some(agent) = self.agents.get_mut(npc_id) {
                agent.life_domain.marital_status = "married".to_string();
                agent.life_domain.spouse_npc_id = Some(if npc_id == actor_id {
                    target_npc.to_string()
                } else {
                    actor_id.to_string()
                });
                agent.life_domain.home_quality = (agent.life_domain.home_quality + 8).clamp(0, 100);
                agent.life_domain.property_value =
                    agent.life_domain.property_value.saturating_add(12);
                agent.life_domain.social_reputation =
                    (agent.life_domain.social_reputation + 6).clamp(-100, 100);
                agent.life_domain.last_domain_event_tick = tick;
            }
        }

        let pair_hash = super::stable_pair_hash(actor_id, target_npc);
        let should_have_child = (pair_hash + tick) % 5 == 0;
        if should_have_child {
            for npc_id in [actor_id, target_npc] {
                if let Some(agent) = self.agents.get_mut(npc_id) {
                    agent.life_domain.children_count =
                        agent.life_domain.children_count.saturating_add(1).min(4);
                    agent.life_domain.last_domain_event_tick = tick;
                }
            }
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::GroupMembershipChanged,
                location_id.to_string(),
                vec![
                    ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "parent".to_string(),
                    },
                    ActorRef {
                        actor_id: target_npc.to_string(),
                        actor_kind: "parent".to_string(),
                    },
                ],
                reason_packet_id.clone(),
                vec![source_event_id.to_string()],
                Some(json!({
                    "arc_id": arc_id,
                    "milestone": "child_born",
                })),
            );
        }

        self.push_event(
            tick,
            sequence_in_tick,
            EventType::NarrativeWhySummary,
            location_id.to_string(),
            vec![ActorRef {
                actor_id: actor_id.to_string(),
                actor_kind: "arc_actor".to_string(),
            }],
            reason_packet_id,
            vec![source_event_id.to_string()],
            Some(json!({
                "arc_id": arc_id,
                "stage": "household_union",
                "status": "completed",
                "social_consequence": "marriage_household_formed",
            })),
        );
    }
}

fn target_jitter(actor_id: &str, target_id: &str, tick: u64) -> i64 {
    let mut hash = tick.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for byte in actor_id.as_bytes().iter().chain(target_id.as_bytes()) {
        hash = hash.rotate_left(7) ^ u64::from(*byte);
        hash = hash.wrapping_mul(0x517C_C1B7_2722_0A95);
    }
    (hash % 17) as i64 - 8
}

fn inferred_conflict_key(_actor_id: &str, bound: &contracts::BoundOperator) -> Option<String> {
    if let Some(target_object) = bound.parameters.target_object.as_ref() {
        return Some(format!("object:{target_object}"));
    }
    if bound.operator_id.contains("steal") {
        if let Some(target_npc) = bound.parameters.target_npc.as_ref() {
            return Some(format!("wallet:{target_npc}"));
        }
        return Some("wallet:market".to_string());
    }
    if bound.operator_id.contains("work") {
        return Some(format!("job_slot:{}", bound.operator_id));
    }
    None
}

fn mutation_resource_key(mutation: &WorldMutation) -> Option<String> {
    if let Some(delta) = mutation.deltas.first() {
        return Some(delta.fact_key.clone());
    }
    if let Some(transfer) = mutation.resource_transfers.first() {
        return Some(format!(
            "{}:{}:{}",
            transfer.from_account, transfer.to_account, transfer.amount
        ));
    }
    None
}

fn resource_capacity(resource_key: &str) -> usize {
    if resource_key.starts_with("object:item:coin_purse") {
        return 4;
    }
    if resource_key == "wallet:market" {
        return 3;
    }
    1
}

fn conflict_backoff_ticks(actor_id: &str, resource_key: &str) -> u64 {
    let mut hash = 0_u64;
    for byte in actor_id.as_bytes().iter().chain(resource_key.as_bytes()) {
        hash = hash.rotate_left(5) ^ u64::from(*byte);
        hash = hash.wrapping_mul(0x517C_C1B7_2722_0A95);
    }
    1 + (hash % 2)
}

fn commitment_plan_id(commitment_id: &str) -> Option<&str> {
    let mut parts = commitment_id.splitn(3, ':');
    let kind = parts.next()?;
    if kind != "commitment" {
        return None;
    }
    let _agent = parts.next()?;
    parts.next()
}
