fn apply_cors_headers(headers: &mut axum::http::HeaderMap) {
    headers.insert(
        HeaderName::from_static("access-control-allow-origin"),
        HeaderValue::from_static("*"),
    );
    headers.insert(
        HeaderName::from_static("access-control-allow-methods"),
        HeaderValue::from_static("GET,POST,OPTIONS,PUT,PATCH,DELETE"),
    );
    headers.insert(
        HeaderName::from_static("access-control-allow-headers"),
        HeaderValue::from_static("*"),
    );
    headers.insert(
        HeaderName::from_static("access-control-max-age"),
        HeaderValue::from_static("3600"),
    );
}

fn default_sqlite_path() -> String {
    std::env::var("THREADS_SQLITE_PATH")
        .ok()
        .filter(|path| !path.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_SQLITE_PATH.to_string())
}

fn fallback_snapshot_window(
    engine: &EngineApi,
    around_tick: Option<u64>,
    from_tick: Option<u64>,
    to_tick: Option<u64>,
) -> Vec<Snapshot> {
    let current_tick = engine.status().current_tick;
    let snapshot = engine.snapshot_for_current_tick();
    if current_tick == 0 {
        if let Some(around) = around_tick {
            return if around >= snapshot.tick {
                vec![snapshot]
            } else {
                Vec::new()
            };
        }
        let from = from_tick.unwrap_or(0);
        let to = to_tick.unwrap_or(snapshot.tick);
        return if snapshot.tick >= from && snapshot.tick <= to {
            vec![snapshot]
        } else {
            Vec::new()
        };
    }

    if let Some(around) = around_tick {
        if around >= snapshot.tick {
            return vec![snapshot];
        }
        return Vec::new();
    }

    let from_tick = from_tick.unwrap_or(1);
    let to_tick = to_tick.unwrap_or(current_tick);

    if snapshot.tick >= from_tick && snapshot.tick <= to_tick {
        vec![snapshot]
    } else {
        Vec::new()
    }
}

fn paginate(
    total: usize,
    cursor: Option<usize>,
    page_size: Option<usize>,
) -> Result<(usize, usize, Option<usize>), HttpApiError> {
    let start = cursor.unwrap_or(0);
    if start > total {
        return Err(HttpApiError::invalid_query(
            "cursor is out of bounds",
            Some(format!("cursor={start} total={total}")),
        ));
    }

    let size = page_size
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .max(1)
        .min(MAX_PAGE_SIZE);
    let end = start.saturating_add(size).min(total);
    let next_cursor = if end < total { Some(end) } else { None };

    Ok((start, end, next_cursor))
}

fn parse_event_type_filter(
    requested_types: &[String],
) -> Result<Option<HashSet<EventType>>, HttpApiError> {
    if requested_types.is_empty() {
        return Ok(None);
    }

    let mut filter = HashSet::new();

    for value in requested_types {
        let normalized = value.trim().to_lowercase();
        let event_type = match normalized.as_str() {
            "system_tick" | "systemtick" => EventType::SystemTick,
            "npc_action_committed" | "npcactioncommitted" => EventType::NpcActionCommitted,
            "command_applied" | "commandapplied" => EventType::CommandApplied,
            "rumor_injected" | "rumorinjected" => EventType::RumorInjected,
            "caravan_spawned" | "caravanspawned" => EventType::CaravanSpawned,
            "npc_removed" | "npcremoved" => EventType::NpcRemoved,
            "bad_harvest_forced" | "badharvestforced" => EventType::BadHarvestForced,
            "winter_severity_set" | "winterseverityset" => EventType::WinterSeveritySet,
            "theft_committed" | "theftcommitted" => EventType::TheftCommitted,
            "item_transferred" | "itemtransferred" => EventType::ItemTransferred,
            "investigation_progressed" | "investigationprogressed" => {
                EventType::InvestigationProgressed
            }
            "arrest_made" | "arrestmade" => EventType::ArrestMade,
            "site_discovered" | "sitediscovered" => EventType::SiteDiscovered,
            "leverage_gained" | "leveragegained" => EventType::LeverageGained,
            "relationship_shifted" | "relationshipshifted" => EventType::RelationshipShifted,
            "pressure_economy_updated" | "pressureeconomyupdated" => {
                EventType::PressureEconomyUpdated
            }
            "household_consumption_applied" | "householdconsumptionapplied" => {
                EventType::HouseholdConsumptionApplied
            }
            "rent_due" | "rentdue" => EventType::RentDue,
            "rent_unpaid" | "rentunpaid" => EventType::RentUnpaid,
            "eviction_risk_changed" | "evictionriskchanged" => EventType::EvictionRiskChanged,
            "household_buffer_exhausted" | "householdbufferexhausted" => {
                EventType::HouseholdBufferExhausted
            }
            "job_sought" | "jobsought" => EventType::JobSought,
            "contract_signed" | "contractsigned" => EventType::ContractSigned,
            "wage_paid" | "wagepaid" => EventType::WagePaid,
            "wage_delayed" | "wagedelayed" => EventType::WageDelayed,
            "contract_breached" | "contractbreached" => EventType::ContractBreached,
            "employment_terminated" | "employmentterminated" => EventType::EmploymentTerminated,
            "production_started" | "productionstarted" => EventType::ProductionStarted,
            "production_completed" | "productioncompleted" => EventType::ProductionCompleted,
            "spoilage_occurred" | "spoilageoccurred" => EventType::SpoilageOccurred,
            "stock_shortage" | "stockshortage" => EventType::StockShortage,
            "stock_recovered" | "stockrecovered" => EventType::StockRecovered,
            "trust_changed" | "trustchanged" => EventType::TrustChanged,
            "obligation_created" | "obligationcreated" => EventType::ObligationCreated,
            "obligation_called" | "obligationcalled" => EventType::ObligationCalled,
            "grievance_recorded" | "grievancerecorded" => EventType::GrievanceRecorded,
            "relationship_status_changed" | "relationshipstatuschanged" => {
                EventType::RelationshipStatusChanged
            }
            "belief_formed" | "beliefformed" => EventType::BeliefFormed,
            "belief_updated" | "beliefupdated" => EventType::BeliefUpdated,
            "belief_disputed" | "beliefdisputed" => EventType::BeliefDisputed,
            "rumor_mutated" | "rumormutated" => EventType::RumorMutated,
            "belief_forgotten" | "beliefforgotten" => EventType::BeliefForgotten,
            "institution_profile_updated" | "institutionprofileupdated" => {
                EventType::InstitutionProfileUpdated
            }
            "institution_case_resolved" | "institutioncaseresolved" => {
                EventType::InstitutionCaseResolved
            }
            "institutional_error_recorded" | "institutionalerrorrecorded" => {
                EventType::InstitutionalErrorRecorded
            }
            "group_formed" | "groupformed" => EventType::GroupFormed,
            "group_membership_changed" | "groupmembershipchanged" => {
                EventType::GroupMembershipChanged
            }
            "group_split" | "groupsplit" => EventType::GroupSplit,
            "group_dissolved" | "groupdissolved" => EventType::GroupDissolved,
            "conversation_held" | "conversationheld" => EventType::ConversationHeld,
            "loan_extended" | "loanextended" => EventType::LoanExtended,
            "romance_advanced" | "romanceadvanced" => EventType::RomanceAdvanced,
            "illness_contracted" | "illnesscontracted" => EventType::IllnessContracted,
            "illness_recovered" | "illnessrecovered" => EventType::IllnessRecovered,
            "observation_logged" | "observationlogged" => EventType::ObservationLogged,
            "insult_exchanged" | "insultexchanged" => EventType::InsultExchanged,
            "punch_thrown" | "punchthrown" => EventType::PunchThrown,
            "brawl_started" | "brawlstarted" => EventType::BrawlStarted,
            "brawl_stopped" | "brawlstopped" => EventType::BrawlStopped,
            "guards_dispatched" | "guardsdispatched" => EventType::GuardsDispatched,
            "apprenticeship_progressed" | "apprenticeshipprogressed" => {
                EventType::ApprenticeshipProgressed
            }
            "succession_transferred" | "successiontransferred" => EventType::SuccessionTransferred,
            "route_risk_updated" | "routeriskupdated" => EventType::RouteRiskUpdated,
            "travel_window_shifted" | "travelwindowshifted" => EventType::TravelWindowShifted,
            "narrative_why_summary" | "narrativewhysummary" => EventType::NarrativeWhySummary,
            "opportunity_opened" | "opportunityopened" => EventType::OpportunityOpened,
            "opportunity_expired" | "opportunityexpired" => EventType::OpportunityExpired,
            "opportunity_accepted" | "opportunityaccepted" => EventType::OpportunityAccepted,
            "opportunity_rejected" | "opportunityrejected" => EventType::OpportunityRejected,
            "commitment_started" | "commitmentstarted" => EventType::CommitmentStarted,
            "commitment_continued" | "commitmentcontinued" => EventType::CommitmentContinued,
            "commitment_completed" | "commitmentcompleted" => EventType::CommitmentCompleted,
            "commitment_broken" | "commitmentbroken" => EventType::CommitmentBroken,
            "market_cleared" | "marketcleared" => EventType::MarketCleared,
            "market_failed" | "marketfailed" => EventType::MarketFailed,
            "accounting_transfer_recorded" | "accountingtransferrecorded" => {
                EventType::AccountingTransferRecorded
            }
            "institution_queue_updated" | "institutionqueueupdated" => {
                EventType::InstitutionQueueUpdated
            }
            _ => {
                return Err(HttpApiError::invalid_query(
                    "invalid event type filter",
                    Some(format!("event_type={value}")),
                ))
            }
        };

        filter.insert(event_type);
    }

    Ok(Some(filter))
}

fn build_trace_nodes(
    root_event: &Event,
    events: &[Event],
    reason_packets: &[ReasonPacket],
    max_depth: usize,
) -> Vec<TraceNode> {
    let events_by_id = events
        .iter()
        .map(|event| (event.event_id.as_str(), event))
        .collect::<HashMap<_, _>>();

    let reason_packets_by_id = reason_packets
        .iter()
        .map(|packet| (packet.reason_packet_id.as_str(), packet))
        .collect::<HashMap<_, _>>();

    let mut nodes = Vec::new();
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();

    queue.push_back((root_event.event_id.clone(), 0_usize));

    while let Some((event_id, depth)) = queue.pop_front() {
        if !visited.insert(event_id.clone()) {
            continue;
        }

        let Some(event) = events_by_id.get(event_id.as_str()).copied() else {
            continue;
        };

        let reason_packet = event.reason_packet_id.as_ref().and_then(|reason_id| {
            reason_packets_by_id
                .get(reason_id.as_str())
                .copied()
                .cloned()
        });

        nodes.push(TraceNode {
            depth,
            event: event.clone(),
            reason_packet,
        });

        if depth >= max_depth {
            continue;
        }

        for parent_id in &event.caused_by {
            queue.push_back((parent_id.clone(), depth + 1));
        }
    }

    nodes
}

fn reconnect_token(tick: u64, sequence_in_tick: Option<u64>, label: &str) -> String {
    match sequence_in_tick {
        Some(sequence) => format!("{label}:{tick}:{sequence}"),
        None => format!("{label}:{tick}"),
    }
}

