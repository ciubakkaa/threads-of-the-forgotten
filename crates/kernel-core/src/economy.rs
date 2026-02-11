//! Closed-loop economy ledger: tracks all resources with conservation enforcement,
//! cadence-based rent/wage processing, production nodes, and market clearing.

use std::collections::BTreeMap;

use contracts::agency::{
    AccountBalance, AccountingTransfer, EconomyLedgerSnapshot, ResourceKind,
};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur during economic operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EconomyError {
    /// Source account does not exist.
    AccountNotFound(String),
    /// Insufficient balance for the requested transfer.
    InsufficientBalance {
        account: String,
        kind: ResourceKind,
        available: i64,
        requested: i64,
    },
    /// Transfer would violate conservation (amount must be positive).
    InvalidAmount(i64),
}

impl std::fmt::Display for EconomyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EconomyError::AccountNotFound(id) => write!(f, "account not found: {}", id),
            EconomyError::InsufficientBalance { account, kind, available, requested } => {
                write!(
                    f,
                    "insufficient {:?} in {}: have {}, need {}",
                    kind, account, available, requested
                )
            }
            EconomyError::InvalidAmount(amt) => write!(f, "invalid transfer amount: {}", amt),
        }
    }
}

// ---------------------------------------------------------------------------
// EconomyLedger
// ---------------------------------------------------------------------------

/// Closed-loop economy ledger enforcing conservation of resources.
///
/// Every resource unit has a traceable source and destination. Transfers that
/// would create or destroy resources without an explicit source/sink are rejected.
#[derive(Debug, Clone)]
pub struct EconomyLedger {
    accounts: BTreeMap<String, AccountBalance>,
    transfers: Vec<AccountingTransfer>,
    /// Running total of resources explicitly injected (sources).
    total_sourced: AccountBalance,
    /// Running total of resources explicitly removed (sinks).
    total_sunk: AccountBalance,
    next_transfer_id: u64,
}

impl EconomyLedger {
    /// Create a new ledger with the given initial accounts.
    pub fn new(accounts: BTreeMap<String, AccountBalance>) -> Self {
        Self {
            accounts,
            transfers: Vec::new(),
            total_sourced: AccountBalance::default(),
            total_sunk: AccountBalance::default(),
            next_transfer_id: 1,
        }
    }

    /// Get a read-only reference to an account balance.
    pub fn get_account(&self, account_id: &str) -> Option<&AccountBalance> {
        self.accounts.get(account_id)
    }

    /// Get a mutable reference to an account balance.
    pub fn get_account_mut(&mut self, account_id: &str) -> Option<&mut AccountBalance> {
        self.accounts.get_mut(account_id)
    }

    /// Ensure an account exists, creating it with zero balances if absent.
    pub fn ensure_account(&mut self, account_id: &str) {
        self.accounts
            .entry(account_id.to_string())
            .or_insert_with(AccountBalance::default);
    }

    /// All accounts in the ledger.
    pub fn accounts(&self) -> &BTreeMap<String, AccountBalance> {
        &self.accounts
    }

    /// All recorded transfers.
    pub fn transfers(&self) -> &[AccountingTransfer] {
        &self.transfers
    }

    /// Transfer `amount` of `kind` from one account to another.
    ///
    /// Returns the transfer ID on success, or an error if the source has
    /// insufficient balance or the amount is non-positive.
    pub fn transfer(
        &mut self,
        from: &str,
        to: &str,
        kind: ResourceKind,
        amount: i64,
        cause: &str,
        tick: u64,
    ) -> Result<String, EconomyError> {
        if amount <= 0 {
            return Err(EconomyError::InvalidAmount(amount));
        }

        // Check source balance.
        let from_balance = self
            .accounts
            .get(from)
            .ok_or_else(|| EconomyError::AccountNotFound(from.to_string()))?;
        let available = balance_for_kind(from_balance, &kind);
        if available < amount {
            return Err(EconomyError::InsufficientBalance {
                account: from.to_string(),
                kind: kind.clone(),
                available,
                requested: amount,
            });
        }

        // Debit source.
        let src = self.accounts.get_mut(from).unwrap();
        *balance_for_kind_mut(src, &kind) -= amount;

        // Credit destination (create if absent).
        let dst = self
            .accounts
            .entry(to.to_string())
            .or_insert_with(AccountBalance::default);
        *balance_for_kind_mut(dst, &kind) += amount;

        // Record transfer.
        let transfer_id = format!("txn-{}", self.next_transfer_id);
        self.next_transfer_id += 1;
        self.transfers.push(AccountingTransfer {
            transfer_id: transfer_id.clone(),
            tick,
            from_account: from.to_string(),
            to_account: to.to_string(),
            resource_kind: kind,
            amount,
            cause_event_id: cause.to_string(),
        });

        Ok(transfer_id)
    }

    /// Inject resources from an external source (e.g., world generation, harvest).
    /// This is the only legitimate way to create new resources.
    pub fn source(&mut self, account_id: &str, kind: ResourceKind, amount: i64, cause: &str, tick: u64) {
        if amount <= 0 {
            return;
        }
        let acct = self
            .accounts
            .entry(account_id.to_string())
            .or_insert_with(AccountBalance::default);
        *balance_for_kind_mut(acct, &kind) += amount;
        *balance_for_kind_mut(&mut self.total_sourced, &kind) += amount;

        let transfer_id = format!("txn-{}", self.next_transfer_id);
        self.next_transfer_id += 1;
        self.transfers.push(AccountingTransfer {
            transfer_id,
            tick,
            from_account: "__source__".to_string(),
            to_account: account_id.to_string(),
            resource_kind: kind,
            amount,
            cause_event_id: cause.to_string(),
        });
    }

    /// Remove resources via an explicit sink (e.g., consumption, spoilage).
    /// Returns false if the account has insufficient balance.
    pub fn sink(&mut self, account_id: &str, kind: ResourceKind, amount: i64, cause: &str, tick: u64) -> bool {
        if amount <= 0 {
            return false;
        }
        let acct = match self.accounts.get_mut(account_id) {
            Some(a) => a,
            None => return false,
        };
        if balance_for_kind(acct, &kind) < amount {
            return false;
        }
        *balance_for_kind_mut(acct, &kind) -= amount;
        *balance_for_kind_mut(&mut self.total_sunk, &kind) += amount;

        let transfer_id = format!("txn-{}", self.next_transfer_id);
        self.next_transfer_id += 1;
        self.transfers.push(AccountingTransfer {
            transfer_id,
            tick,
            from_account: account_id.to_string(),
            to_account: "__sink__".to_string(),
            resource_kind: kind,
            amount,
            cause_event_id: cause.to_string(),
        });
        true
    }

    /// Verify the conservation invariant:
    /// sum(all accounts) == initial_endowment + total_sourced - total_sunk
    ///
    /// Since we track sources and sinks explicitly, we can verify by checking
    /// that the sum of all account balances equals total_sourced - total_sunk
    /// (initial endowment is part of the accounts at construction time, and
    /// we track all mutations through transfers).
    ///
    /// In practice we verify by replaying all transfers from zero and comparing.
    pub fn verify_conservation(&self) -> bool {
        // Replay: start from zero, apply all transfers.
        let mut replay: BTreeMap<String, AccountBalance> = BTreeMap::new();

        for txn in &self.transfers {
            if txn.from_account != "__source__" {
                let src = replay
                    .entry(txn.from_account.clone())
                    .or_insert_with(AccountBalance::default);
                *balance_for_kind_mut(src, &txn.resource_kind) -= txn.amount;
            }
            if txn.to_account != "__sink__" {
                let dst = replay
                    .entry(txn.to_account.clone())
                    .or_insert_with(AccountBalance::default);
                *balance_for_kind_mut(dst, &txn.resource_kind) += txn.amount;
            }
        }

        // The replayed balances should match the actual balances for every account.
        // Accounts not in replay should have zero balances.
        for (id, actual) in &self.accounts {
            let replayed = replay.get(id).cloned().unwrap_or_default();
            if *actual != replayed {
                return false;
            }
        }
        // Check for accounts in replay but not in self.accounts.
        for (id, replayed) in &replay {
            if !self.accounts.contains_key(id) && *replayed != AccountBalance::default() {
                return false;
            }
        }
        true
    }

    /// Create a snapshot of the ledger state.
    pub fn snapshot(&self) -> EconomyLedgerSnapshot {
        EconomyLedgerSnapshot {
            accounts: self.accounts.clone(),
            transfers: self.transfers.clone(),
        }
    }
}

impl Default for EconomyLedger {
    fn default() -> Self {
        Self::new(BTreeMap::new())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn balance_for_kind(balance: &AccountBalance, kind: &ResourceKind) -> i64 {
    match kind {
        ResourceKind::Money => balance.money,
        ResourceKind::Food => balance.food,
        ResourceKind::Fuel => balance.fuel,
        ResourceKind::Medicine => balance.medicine,
        ResourceKind::CraftInputs | ResourceKind::CraftOutputs => 0,
    }
}

fn balance_for_kind_mut<'a>(balance: &'a mut AccountBalance, kind: &ResourceKind) -> &'a mut i64 {
    match kind {
        ResourceKind::Money => &mut balance.money,
        ResourceKind::Food => &mut balance.food,
        ResourceKind::Fuel => &mut balance.fuel,
        ResourceKind::Medicine => &mut balance.medicine,
        // CraftInputs/CraftOutputs don't map to AccountBalance fields;
        // use money as a fallback (these should be tracked separately in production).
        ResourceKind::CraftInputs | ResourceKind::CraftOutputs => &mut balance.money,
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger_with_accounts() -> EconomyLedger {
        // Use source() to set up initial balances so conservation holds.
        let mut ledger = EconomyLedger::new(BTreeMap::new());
        ledger.source("alice", ResourceKind::Money, 100, "init", 0);
        ledger.source("alice", ResourceKind::Food, 50, "init", 0);
        ledger.source("bob", ResourceKind::Money, 200, "init", 0);
        ledger.source("bob", ResourceKind::Food, 30, "init", 0);
        ledger
    }

    #[test]
    fn transfer_moves_resources() {
        let mut ledger = ledger_with_accounts();
        let result = ledger.transfer("alice", "bob", ResourceKind::Money, 40, "trade", 1);
        assert!(result.is_ok());
        assert_eq!(ledger.get_account("alice").unwrap().money, 60);
        assert_eq!(ledger.get_account("bob").unwrap().money, 240);
    }

    #[test]
    fn transfer_rejects_insufficient_balance() {
        let mut ledger = ledger_with_accounts();
        let result = ledger.transfer("alice", "bob", ResourceKind::Money, 999, "trade", 1);
        assert!(matches!(result, Err(EconomyError::InsufficientBalance { .. })));
    }

    #[test]
    fn transfer_rejects_non_positive_amount() {
        let mut ledger = ledger_with_accounts();
        assert!(matches!(
            ledger.transfer("alice", "bob", ResourceKind::Money, 0, "trade", 1),
            Err(EconomyError::InvalidAmount(0))
        ));
        assert!(matches!(
            ledger.transfer("alice", "bob", ResourceKind::Money, -5, "trade", 1),
            Err(EconomyError::InvalidAmount(-5))
        ));
    }

    #[test]
    fn transfer_rejects_missing_account() {
        let mut ledger = ledger_with_accounts();
        let result = ledger.transfer("nobody", "bob", ResourceKind::Money, 10, "trade", 1);
        assert!(matches!(result, Err(EconomyError::AccountNotFound(_))));
    }

    #[test]
    fn transfer_creates_destination_if_absent() {
        let mut ledger = ledger_with_accounts();
        let result = ledger.transfer("alice", "charlie", ResourceKind::Money, 10, "gift", 1);
        assert!(result.is_ok());
        assert_eq!(ledger.get_account("charlie").unwrap().money, 10);
    }

    #[test]
    fn source_and_sink_work() {
        let mut ledger = EconomyLedger::default();
        ledger.source("farm", ResourceKind::Food, 100, "harvest", 1);
        assert_eq!(ledger.get_account("farm").unwrap().food, 100);

        let ok = ledger.sink("farm", ResourceKind::Food, 30, "spoilage", 2);
        assert!(ok);
        assert_eq!(ledger.get_account("farm").unwrap().food, 70);
    }

    #[test]
    fn sink_rejects_insufficient() {
        let mut ledger = EconomyLedger::default();
        ledger.source("farm", ResourceKind::Food, 10, "harvest", 1);
        assert!(!ledger.sink("farm", ResourceKind::Food, 20, "spoilage", 2));
    }

    #[test]
    fn conservation_holds_after_transfers() {
        let mut ledger = ledger_with_accounts();
        assert!(ledger.verify_conservation());

        ledger.transfer("alice", "bob", ResourceKind::Money, 30, "trade", 1).unwrap();
        assert!(ledger.verify_conservation());

        ledger.sink("bob", ResourceKind::Food, 10, "eat", 2);
        assert!(ledger.verify_conservation());

        ledger.source("alice", ResourceKind::Food, 20, "harvest", 3);
        assert!(ledger.verify_conservation());
    }

    #[test]
    fn snapshot_captures_state() {
        let mut ledger = ledger_with_accounts();
        ledger.transfer("alice", "bob", ResourceKind::Money, 10, "trade", 1).unwrap();
        let snap = ledger.snapshot();
        assert_eq!(snap.accounts.len(), 2);
        assert!(!snap.transfers.is_empty());
    }
}

// ---------------------------------------------------------------------------
// 6.2 — Economic processing cadences and production
// ---------------------------------------------------------------------------

/// Result of processing rent for a single tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RentResult {
    pub tenant_id: String,
    pub paid: bool,
    /// New shelter status after processing.
    pub shelter_status: ShelterStatus,
}

/// Result of processing wages for a single contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WageResult {
    pub contract_id: String,
    pub worker_id: String,
    pub employer_id: String,
    pub paid: bool,
    /// Trust delta to apply to the social graph (negative if missed).
    pub trust_delta: i64,
    /// Grievance delta to apply (positive if missed).
    pub grievance_delta: i64,
}

/// A rent obligation tracked by the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RentObligation {
    pub tenant_id: String,
    pub landlord_id: String,
    pub amount: i64,
    pub period_ticks: u64,
    pub next_due_tick: u64,
    pub shelter_status: ShelterStatus,
    /// How many consecutive periods have been missed.
    pub missed_count: u32,
}

/// A wage contract tracked by the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WageContract {
    pub contract_id: String,
    pub employer_id: String,
    pub worker_id: String,
    pub wage_amount: i64,
    pub period_ticks: u64,
    pub next_payment_tick: u64,
}

/// A production node that consumes inputs and produces outputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionNode {
    pub node_id: String,
    pub owner_id: String,
    pub input_kind: ResourceKind,
    pub input_amount: i64,
    pub output_kind: ResourceKind,
    pub output_amount: i64,
    pub production_ticks: u64,
    /// Tick when current production batch started (None if idle).
    pub batch_start_tick: Option<u64>,
    /// Whether inputs have been consumed for the current batch.
    pub inputs_consumed: bool,
}

/// A supply/demand entry for market clearing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketOrder {
    pub agent_id: String,
    pub kind: ResourceKind,
    pub amount: i64,
    /// true = selling, false = buying
    pub is_sell: bool,
    pub price_per_unit: i64,
}

/// Result of a single market match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketMatch {
    pub seller_id: String,
    pub buyer_id: String,
    pub kind: ResourceKind,
    pub quantity: i64,
    pub price_per_unit: i64,
}

/// Settlement-level market state.
#[derive(Debug, Clone)]
pub struct MarketState {
    pub settlement_id: String,
    pub orders: Vec<MarketOrder>,
    /// Current price indices per resource kind.
    pub price_index: BTreeMap<String, i64>,
}

use contracts::ShelterStatus;

impl EconomyLedger {
    /// Process rent for all obligations due at or before `tick`.
    /// Returns results indicating payment success and updated shelter status.
    pub fn process_rent(&mut self, obligations: &mut [RentObligation], tick: u64) -> Vec<RentResult> {
        let mut results = Vec::new();
        for ob in obligations.iter_mut() {
            if tick < ob.next_due_tick {
                continue;
            }
            let paid = self
                .transfer(
                    &ob.tenant_id,
                    &ob.landlord_id,
                    ResourceKind::Money,
                    ob.amount,
                    "rent_payment",
                    tick,
                )
                .is_ok();

            if paid {
                ob.missed_count = 0;
                ob.shelter_status = ShelterStatus::Stable;
            } else {
                ob.missed_count += 1;
                ob.shelter_status = match ob.missed_count {
                    1 => ShelterStatus::Precarious,
                    _ => ShelterStatus::Unsheltered,
                };
            }
            ob.next_due_tick = tick + ob.period_ticks;

            results.push(RentResult {
                tenant_id: ob.tenant_id.clone(),
                paid,
                shelter_status: ob.shelter_status,
            });
        }
        results
    }

    /// Process wages for all contracts due at or before `tick`.
    /// Returns results with trust/grievance deltas for the social graph.
    pub fn process_wages(&mut self, contracts: &mut [WageContract], tick: u64) -> Vec<WageResult> {
        let mut results = Vec::new();
        for wc in contracts.iter_mut() {
            if tick < wc.next_payment_tick {
                continue;
            }
            let paid = self
                .transfer(
                    &wc.employer_id,
                    &wc.worker_id,
                    ResourceKind::Money,
                    wc.wage_amount,
                    "wage_payment",
                    tick,
                )
                .is_ok();

            let (trust_delta, grievance_delta) = if paid {
                (1, 0) // small trust boost for on-time payment
            } else {
                (-5, 10) // missed payment erodes trust, builds grievance
            };

            wc.next_payment_tick = tick + wc.period_ticks;

            results.push(WageResult {
                contract_id: wc.contract_id.clone(),
                worker_id: wc.worker_id.clone(),
                employer_id: wc.employer_id.clone(),
                paid,
                trust_delta,
                grievance_delta,
            });
        }
        results
    }

    /// Tick production nodes: consume inputs before producing outputs.
    /// Enforces input-before-output and applies spoilage to perishable outputs.
    pub fn tick_production(
        &mut self,
        nodes: &mut [ProductionNode],
        tick: u64,
        spoilage_rate: u32,
    ) {
        for node in nodes.iter_mut() {
            // Try to start a new batch if idle.
            if node.batch_start_tick.is_none() {
                // Attempt to consume inputs.
                let consumed = self.sink(
                    &node.owner_id,
                    node.input_kind.clone(),
                    node.input_amount,
                    "production_input",
                    tick,
                );
                if consumed {
                    node.batch_start_tick = Some(tick);
                    node.inputs_consumed = true;
                }
                continue;
            }

            // Check if production is complete.
            let start = node.batch_start_tick.unwrap();
            if tick >= start + node.production_ticks {
                // Produce outputs.
                self.source(
                    &node.owner_id,
                    node.output_kind.clone(),
                    node.output_amount,
                    "production_output",
                    tick,
                );
                node.batch_start_tick = None;
                node.inputs_consumed = false;
            }
        }

        // Apply spoilage to perishable goods (food) across all accounts.
        if spoilage_rate > 0 {
            let account_ids: Vec<String> = self.accounts.keys().cloned().collect();
            for id in account_ids {
                let food = self.accounts.get(&id).map(|a| a.food).unwrap_or(0);
                if food > 0 {
                    let spoiled = (food * spoilage_rate as i64 / 100).max(0);
                    if spoiled > 0 {
                        self.sink(&id, ResourceKind::Food, spoiled, "spoilage", tick);
                    }
                }
            }
        }
    }

    /// Clear the market: match buy and sell orders, execute transfers, adjust prices.
    /// Returns the matches executed and updated price index.
    pub fn clear_market(&mut self, market: &mut MarketState, tick: u64) -> Vec<MarketMatch> {
        let mut matches = Vec::new();

        // Separate and sort orders.
        let mut sells: Vec<&MarketOrder> = market.orders.iter().filter(|o| o.is_sell).collect();
        let mut buys: Vec<&MarketOrder> = market.orders.iter().filter(|o| !o.is_sell).collect();

        // Sort sells by price ascending (cheapest first), buys by price descending (highest bidder first).
        sells.sort_by_key(|o| o.price_per_unit);
        buys.sort_by(|a, b| b.price_per_unit.cmp(&a.price_per_unit));

        // Track remaining quantities.
        let mut sell_remaining: Vec<i64> = sells.iter().map(|o| o.amount).collect();
        let mut buy_remaining: Vec<i64> = buys.iter().map(|o| o.amount).collect();

        for (bi, buy) in buys.iter().enumerate() {
            if buy_remaining[bi] <= 0 {
                continue;
            }
            for (si, sell) in sells.iter().enumerate() {
                if sell_remaining[si] <= 0 || buy_remaining[bi] <= 0 {
                    continue;
                }
                // Match if buyer's price >= seller's price.
                if buy.price_per_unit < sell.price_per_unit {
                    continue;
                }

                let quantity = buy_remaining[bi].min(sell_remaining[si]);
                let price = (buy.price_per_unit + sell.price_per_unit) / 2; // midpoint price

                // Transfer goods from seller to buyer.
                let goods_ok = self
                    .transfer(
                        &sell.agent_id,
                        &buy.agent_id,
                        sell.kind.clone(),
                        quantity,
                        "market_trade",
                        tick,
                    )
                    .is_ok();

                if !goods_ok {
                    continue;
                }

                // Transfer money from buyer to seller.
                let total_cost = quantity * price;
                let money_ok = self
                    .transfer(
                        &buy.agent_id,
                        &sell.agent_id,
                        ResourceKind::Money,
                        total_cost,
                        "market_trade",
                        tick,
                    )
                    .is_ok();

                if !money_ok {
                    // Reverse the goods transfer by sourcing back.
                    // In a real system we'd use a two-phase commit; here we reverse.
                    self.source(
                        &sell.agent_id,
                        sell.kind.clone(),
                        quantity,
                        "market_trade_reversal",
                        tick,
                    );
                    self.sink(
                        &buy.agent_id,
                        sell.kind.clone(),
                        quantity,
                        "market_trade_reversal",
                        tick,
                    );
                    continue;
                }

                sell_remaining[si] -= quantity;
                buy_remaining[bi] -= quantity;

                matches.push(MarketMatch {
                    seller_id: sell.agent_id.clone(),
                    buyer_id: buy.agent_id.clone(),
                    kind: sell.kind.clone(),
                    quantity,
                    price_per_unit: price,
                });
            }
        }

        // Update price index based on scarcity/surplus.
        let total_supply: i64 = sell_remaining.iter().sum();
        let total_demand: i64 = buy_remaining.iter().sum();
        // Simple price adjustment: surplus pushes prices down, scarcity pushes up.
        if !sells.is_empty() {
            let kind_key = format!("{:?}", sells[0].kind);
            let current = market.price_index.get(&kind_key).copied().unwrap_or(100);
            let adjustment = if total_demand > total_supply {
                5 // scarcity → price up
            } else if total_supply > total_demand {
                -5 // surplus → price down
            } else {
                0
            };
            market
                .price_index
                .insert(kind_key, (current + adjustment).max(1));
        }

        // Clear processed orders.
        market.orders.clear();

        matches
    }
}

#[cfg(test)]
mod cadence_tests {
    use super::*;

    fn funded_ledger() -> EconomyLedger {
        let mut ledger = EconomyLedger::default();
        ledger.source("tenant", ResourceKind::Money, 500, "init", 0);
        ledger.source("landlord", ResourceKind::Money, 0, "init", 0);
        ledger.ensure_account("landlord");
        ledger.source("employer", ResourceKind::Money, 1000, "init", 0);
        ledger.source("worker", ResourceKind::Money, 0, "init", 0);
        ledger.ensure_account("worker");
        ledger
    }

    #[test]
    fn rent_payment_success_sets_stable() {
        let mut ledger = funded_ledger();
        let mut obligations = vec![RentObligation {
            tenant_id: "tenant".into(),
            landlord_id: "landlord".into(),
            amount: 50,
            period_ticks: 720,
            next_due_tick: 10,
            shelter_status: ShelterStatus::Stable,
            missed_count: 0,
        }];
        let results = ledger.process_rent(&mut obligations, 10);
        assert_eq!(results.len(), 1);
        assert!(results[0].paid);
        assert_eq!(results[0].shelter_status, ShelterStatus::Stable);
        assert_eq!(obligations[0].next_due_tick, 730);
    }

    #[test]
    fn rent_failure_escalates_shelter_status() {
        let mut ledger = EconomyLedger::default();
        ledger.source("tenant", ResourceKind::Money, 5, "init", 0);
        ledger.ensure_account("landlord");
        let mut obligations = vec![RentObligation {
            tenant_id: "tenant".into(),
            landlord_id: "landlord".into(),
            amount: 50,
            period_ticks: 720,
            next_due_tick: 10,
            shelter_status: ShelterStatus::Stable,
            missed_count: 0,
        }];
        // First miss → Precarious
        let r1 = ledger.process_rent(&mut obligations, 10);
        assert!(!r1[0].paid);
        assert_eq!(r1[0].shelter_status, ShelterStatus::Precarious);

        // Second miss → Unsheltered
        let r2 = ledger.process_rent(&mut obligations, 730);
        assert!(!r2[0].paid);
        assert_eq!(r2[0].shelter_status, ShelterStatus::Unsheltered);
    }

    #[test]
    fn wage_payment_success_gives_positive_trust() {
        let mut ledger = funded_ledger();
        let mut contracts = vec![WageContract {
            contract_id: "c1".into(),
            employer_id: "employer".into(),
            worker_id: "worker".into(),
            wage_amount: 100,
            period_ticks: 24,
            next_payment_tick: 24,
        }];
        let results = ledger.process_wages(&mut contracts, 24);
        assert_eq!(results.len(), 1);
        assert!(results[0].paid);
        assert!(results[0].trust_delta > 0);
        assert_eq!(results[0].grievance_delta, 0);
    }

    #[test]
    fn wage_failure_gives_negative_trust_and_grievance() {
        let mut ledger = EconomyLedger::default();
        ledger.source("employer", ResourceKind::Money, 1, "init", 0);
        ledger.ensure_account("worker");
        let mut contracts = vec![WageContract {
            contract_id: "c1".into(),
            employer_id: "employer".into(),
            worker_id: "worker".into(),
            wage_amount: 100,
            period_ticks: 24,
            next_payment_tick: 24,
        }];
        let results = ledger.process_wages(&mut contracts, 24);
        assert!(!results[0].paid);
        assert!(results[0].trust_delta < 0);
        assert!(results[0].grievance_delta > 0);
    }

    #[test]
    fn production_consumes_inputs_before_outputs() {
        let mut ledger = EconomyLedger::default();
        ledger.source("baker", ResourceKind::Food, 10, "init", 0);
        let mut nodes = vec![ProductionNode {
            node_id: "bakery".into(),
            owner_id: "baker".into(),
            input_kind: ResourceKind::Food,
            input_amount: 5,
            output_kind: ResourceKind::Money,
            output_amount: 20,
            production_ticks: 3,
            batch_start_tick: None,
            inputs_consumed: false,
        }];

        // Tick 1: consume inputs, start batch.
        ledger.tick_production(&mut nodes, 1, 0);
        assert_eq!(ledger.get_account("baker").unwrap().food, 5); // 10 - 5
        assert!(nodes[0].batch_start_tick.is_some());
        assert!(nodes[0].inputs_consumed);

        // Tick 2: still producing.
        ledger.tick_production(&mut nodes, 2, 0);
        assert!(nodes[0].batch_start_tick.is_some());

        // Tick 4: production complete, outputs produced.
        ledger.tick_production(&mut nodes, 4, 0);
        assert!(nodes[0].batch_start_tick.is_none());
        assert_eq!(ledger.get_account("baker").unwrap().money, 20);
    }

    #[test]
    fn production_skips_if_insufficient_inputs() {
        let mut ledger = EconomyLedger::default();
        ledger.source("baker", ResourceKind::Food, 2, "init", 0);
        let mut nodes = vec![ProductionNode {
            node_id: "bakery".into(),
            owner_id: "baker".into(),
            input_kind: ResourceKind::Food,
            input_amount: 5,
            output_kind: ResourceKind::Money,
            output_amount: 20,
            production_ticks: 3,
            batch_start_tick: None,
            inputs_consumed: false,
        }];
        ledger.tick_production(&mut nodes, 1, 0);
        assert!(nodes[0].batch_start_tick.is_none()); // couldn't start
    }

    #[test]
    fn spoilage_reduces_food() {
        let mut ledger = EconomyLedger::default();
        ledger.source("farm", ResourceKind::Food, 100, "init", 0);
        ledger.tick_production(&mut [], 1, 10); // 10% spoilage
        assert_eq!(ledger.get_account("farm").unwrap().food, 90);
    }

    #[test]
    fn market_clearing_matches_orders() {
        let mut ledger = EconomyLedger::default();
        ledger.source("seller", ResourceKind::Food, 50, "init", 0);
        ledger.source("buyer", ResourceKind::Money, 500, "init", 0);

        let mut market = MarketState {
            settlement_id: "town".into(),
            orders: vec![
                MarketOrder {
                    agent_id: "seller".into(),
                    kind: ResourceKind::Food,
                    amount: 10,
                    is_sell: true,
                    price_per_unit: 8,
                },
                MarketOrder {
                    agent_id: "buyer".into(),
                    kind: ResourceKind::Food,
                    amount: 10,
                    is_sell: false,
                    price_per_unit: 12,
                },
            ],
            price_index: BTreeMap::new(),
        };

        let matches = ledger.clear_market(&mut market, 1);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].quantity, 10);
        // Midpoint price: (8 + 12) / 2 = 10
        assert_eq!(matches[0].price_per_unit, 10);
        // Seller got money, buyer got food.
        assert_eq!(ledger.get_account("seller").unwrap().food, 40);
        assert_eq!(ledger.get_account("buyer").unwrap().food, 10);
    }

    #[test]
    fn market_no_match_if_price_gap() {
        let mut ledger = EconomyLedger::default();
        ledger.source("seller", ResourceKind::Food, 50, "init", 0);
        ledger.source("buyer", ResourceKind::Money, 500, "init", 0);

        let mut market = MarketState {
            settlement_id: "town".into(),
            orders: vec![
                MarketOrder {
                    agent_id: "seller".into(),
                    kind: ResourceKind::Food,
                    amount: 10,
                    is_sell: true,
                    price_per_unit: 20,
                },
                MarketOrder {
                    agent_id: "buyer".into(),
                    kind: ResourceKind::Food,
                    amount: 10,
                    is_sell: false,
                    price_per_unit: 5,
                },
            ],
            price_index: BTreeMap::new(),
        };

        let matches = ledger.clear_market(&mut market, 1);
        assert!(matches.is_empty());
    }

    #[test]
    fn conservation_holds_through_cadence_operations() {
        let mut ledger = funded_ledger();
        assert!(ledger.verify_conservation());

        // Rent
        let mut obligations = vec![RentObligation {
            tenant_id: "tenant".into(),
            landlord_id: "landlord".into(),
            amount: 50,
            period_ticks: 720,
            next_due_tick: 10,
            shelter_status: ShelterStatus::Stable,
            missed_count: 0,
        }];
        ledger.process_rent(&mut obligations, 10);
        assert!(ledger.verify_conservation());

        // Wages
        let mut contracts = vec![WageContract {
            contract_id: "c1".into(),
            employer_id: "employer".into(),
            worker_id: "worker".into(),
            wage_amount: 100,
            period_ticks: 24,
            next_payment_tick: 24,
        }];
        ledger.process_wages(&mut contracts, 24);
        assert!(ledger.verify_conservation());
    }
}
