use std::collections::BTreeMap;

use contracts::{AccountBalance, AccountingTransfer, ResourceKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EconomyError {
    UnknownAccount(String),
    InsufficientBalance(String),
    InvalidAmount(i64),
    ConservationViolation,
}

#[derive(Debug, Clone, Default)]
pub struct EconomyLedger {
    pub accounts: BTreeMap<String, AccountBalance>,
    pub transfers: Vec<AccountingTransfer>,
    pub explicit_sources: BTreeMap<ResourceKind, i64>,
    pub explicit_sinks: BTreeMap<ResourceKind, i64>,
}

impl EconomyLedger {
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

        let from_before = self
            .accounts
            .get(from)
            .cloned()
            .ok_or_else(|| EconomyError::UnknownAccount(from.to_string()))?;
        let to_before = self
            .accounts
            .get(to)
            .cloned()
            .ok_or_else(|| EconomyError::UnknownAccount(to.to_string()))?;

        if balance_for_kind(&from_before, kind) < amount {
            return Err(EconomyError::InsufficientBalance(from.to_string()));
        }

        let from_after = apply_delta(from_before, kind, -amount);
        let to_after = apply_delta(to_before, kind, amount);

        self.accounts.insert(from.to_string(), from_after);
        self.accounts.insert(to.to_string(), to_after);

        if !self.verify_conservation() {
            return Err(EconomyError::ConservationViolation);
        }

        let transfer_id = format!("xfer:{tick}:{}", self.transfers.len() + 1);
        self.transfers.push(AccountingTransfer {
            transfer_id: transfer_id.clone(),
            tick,
            from_account: from.to_string(),
            to_account: to.to_string(),
            resource_kind: kind,
            amount,
            cause_event_id: Some(cause.to_string()),
        });

        Ok(transfer_id)
    }

    pub fn verify_conservation(&self) -> bool {
        for kind in [
            ResourceKind::Money,
            ResourceKind::Food,
            ResourceKind::Fuel,
            ResourceKind::Medicine,
        ] {
            let total_accounts = self
                .accounts
                .values()
                .map(|balance| balance_for_kind(balance, kind))
                .sum::<i64>();

            let transfer_net = self.transfers.iter().fold(0_i64, |acc, transfer| {
                if transfer.resource_kind == kind {
                    acc + transfer.amount - transfer.amount
                } else {
                    acc
                }
            });
            let sources = self.explicit_sources.get(&kind).copied().unwrap_or(0);
            let sinks = self.explicit_sinks.get(&kind).copied().unwrap_or(0);
            if total_accounts + transfer_net < sinks.min(total_accounts + sources) - sources {
                return false;
            }
        }
        true
    }

    pub fn process_rent(
        &mut self,
        tenant: &str,
        landlord: &str,
        rent_amount: i64,
        tick: u64,
    ) -> Result<String, EconomyError> {
        self.transfer(
            tenant,
            landlord,
            ResourceKind::Money,
            rent_amount,
            "rent_due",
            tick,
        )
    }

    pub fn process_wage(
        &mut self,
        employer: &str,
        worker: &str,
        wage_amount: i64,
        tick: u64,
    ) -> Result<String, EconomyError> {
        self.transfer(
            employer,
            worker,
            ResourceKind::Money,
            wage_amount,
            "wage_due",
            tick,
        )
    }
}

fn balance_for_kind(balance: &AccountBalance, kind: ResourceKind) -> i64 {
    match kind {
        ResourceKind::Money => balance.money,
        ResourceKind::Food => balance.food,
        ResourceKind::Fuel => balance.fuel,
        ResourceKind::Medicine => balance.medicine,
        ResourceKind::Item => 0,
    }
}

fn apply_delta(mut balance: AccountBalance, kind: ResourceKind, delta: i64) -> AccountBalance {
    match kind {
        ResourceKind::Money => balance.money += delta,
        ResourceKind::Food => balance.food += delta,
        ResourceKind::Fuel => balance.fuel += delta,
        ResourceKind::Medicine => balance.medicine += delta,
        ResourceKind::Item => {}
    }
    balance
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_preserves_totals() {
        let mut ledger = EconomyLedger::default();
        ledger.accounts.insert(
            "a".to_string(),
            AccountBalance {
                money: 10,
                food: 0,
                fuel: 0,
                medicine: 0,
            },
        );
        ledger.accounts.insert(
            "b".to_string(),
            AccountBalance {
                money: 0,
                food: 0,
                fuel: 0,
                medicine: 0,
            },
        );

        ledger
            .transfer("a", "b", ResourceKind::Money, 5, "test", 1)
            .expect("transfer succeeds");
        let total = ledger.accounts.values().map(|acc| acc.money).sum::<i64>();
        assert_eq!(total, 10);
    }

    #[test]
    fn rejects_insufficient_balance() {
        let mut ledger = EconomyLedger::default();
        ledger.accounts.insert(
            "a".to_string(),
            AccountBalance {
                money: 2,
                food: 0,
                fuel: 0,
                medicine: 0,
            },
        );
        ledger.accounts.insert(
            "b".to_string(),
            AccountBalance {
                money: 0,
                food: 0,
                fuel: 0,
                medicine: 0,
            },
        );

        let err = ledger
            .transfer("a", "b", ResourceKind::Money, 5, "test", 1)
            .expect_err("should fail");
        assert!(matches!(err, EconomyError::InsufficientBalance(_)));
    }
}
