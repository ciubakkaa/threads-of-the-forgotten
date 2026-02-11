//! Drive system: tracks NPC needs as continuous pressure values that decay over time.

use contracts::agency::{DriveContext, DriveKind, DriveValue};

/// The drive system tracks 7 fundamental NPC needs as pressure values.
/// Pressure increases over time (via decay) and decreases when actions satisfy drives.
/// Values are clamped to [0, 100].
#[derive(Debug, Clone)]
pub struct DriveSystem {
    pub food: DriveValue,
    pub shelter: DriveValue,
    pub income: DriveValue,
    pub safety: DriveValue,
    pub belonging: DriveValue,
    pub status: DriveValue,
    pub health: DriveValue,
}

impl DriveSystem {
    /// Advance time: increase pressure values at context-sensitive rates.
    pub fn tick_decay(&mut self, context: &DriveContext) {
        self.food.current = (self.food.current + self.food.decay_rate * food_context_multiplier(context)).min(100);
        self.shelter.current = (self.shelter.current + self.shelter.decay_rate * shelter_context_multiplier(context)).min(100);
        self.income.current = (self.income.current + self.income.decay_rate).min(100);
        self.safety.current = (self.safety.current + self.safety.decay_rate * safety_context_multiplier(context)).min(100);
        self.belonging.current = (self.belonging.current + self.belonging.decay_rate * belonging_context_multiplier(context)).min(100);
        self.status.current = (self.status.current + self.status.decay_rate).min(100);
        self.health.current = (self.health.current + self.health.decay_rate * health_context_multiplier(context)).min(100);
    }

    /// Reduce pressure on a specific drive by `delta`, clamped to [0, 100].
    pub fn apply_effect(&mut self, drive: DriveKind, delta: i64) {
        let dv = self.get_mut(drive);
        dv.current = (dv.current - delta).clamp(0, 100);
    }

    /// Return the top N most urgent drives, sorted by descending pressure.
    pub fn top_pressures(&self, n: usize) -> Vec<(DriveKind, i64)> {
        let mut all = self.all_pressures();
        all.sort_by(|a, b| b.1.cmp(&a.1));
        all.truncate(n);
        all
    }

    /// True if any drive pressure exceeds its urgency threshold.
    pub fn any_urgent(&self) -> bool {
        self.food.current > self.food.urgency_threshold
            || self.shelter.current > self.shelter.urgency_threshold
            || self.income.current > self.income.urgency_threshold
            || self.safety.current > self.safety.urgency_threshold
            || self.belonging.current > self.belonging.urgency_threshold
            || self.status.current > self.status.urgency_threshold
            || self.health.current > self.health.urgency_threshold
    }

    /// Get a reference to the DriveValue for a given kind.
    pub fn get(&self, kind: DriveKind) -> &DriveValue {
        match kind {
            DriveKind::Food => &self.food,
            DriveKind::Shelter => &self.shelter,
            DriveKind::Income => &self.income,
            DriveKind::Safety => &self.safety,
            DriveKind::Belonging => &self.belonging,
            DriveKind::Status => &self.status,
            DriveKind::Health => &self.health,
        }
    }

    /// Get a mutable reference to the DriveValue for a given kind.
    pub fn get_mut(&mut self, kind: DriveKind) -> &mut DriveValue {
        match kind {
            DriveKind::Food => &mut self.food,
            DriveKind::Shelter => &mut self.shelter,
            DriveKind::Income => &mut self.income,
            DriveKind::Safety => &mut self.safety,
            DriveKind::Belonging => &mut self.belonging,
            DriveKind::Status => &mut self.status,
            DriveKind::Health => &mut self.health,
        }
    }

    /// All (kind, pressure) pairs.
    fn all_pressures(&self) -> Vec<(DriveKind, i64)> {
        vec![
            (DriveKind::Food, self.food.current),
            (DriveKind::Shelter, self.shelter.current),
            (DriveKind::Income, self.income.current),
            (DriveKind::Safety, self.safety.current),
            (DriveKind::Belonging, self.belonging.current),
            (DriveKind::Status, self.status.current),
            (DriveKind::Health, self.health.current),
        ]
    }
}

/// Context multiplier: food decays faster during physical labor.
fn food_context_multiplier(ctx: &DriveContext) -> i64 {
    if ctx.physical_labor { 2 } else { 1 }
}

/// Context multiplier: shelter pressure rises when exposed.
fn shelter_context_multiplier(ctx: &DriveContext) -> i64 {
    if ctx.exposed { 2 } else { 1 }
}

/// Context multiplier: safety pressure rises in danger.
fn safety_context_multiplier(ctx: &DriveContext) -> i64 {
    if ctx.in_danger { 2 } else { 1 }
}

/// Context multiplier: belonging pressure rises when isolated.
fn belonging_context_multiplier(ctx: &DriveContext) -> i64 {
    if ctx.isolated { 2 } else { 1 }
}

/// Context multiplier: health decays faster during physical labor.
fn health_context_multiplier(ctx: &DriveContext) -> i64 {
    if ctx.physical_labor { 2 } else { 1 }
}

impl Default for DriveSystem {
    fn default() -> Self {
        Self {
            food: DriveValue { current: 0, decay_rate: 2, urgency_threshold: 70 },
            shelter: DriveValue { current: 0, decay_rate: 1, urgency_threshold: 60 },
            income: DriveValue { current: 0, decay_rate: 1, urgency_threshold: 60 },
            safety: DriveValue { current: 0, decay_rate: 1, urgency_threshold: 80 },
            belonging: DriveValue { current: 0, decay_rate: 1, urgency_threshold: 50 },
            status: DriveValue { current: 0, decay_rate: 1, urgency_threshold: 50 },
            health: DriveValue { current: 0, decay_rate: 1, urgency_threshold: 70 },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_context() -> DriveContext {
        DriveContext {
            physical_labor: false,
            exposed: false,
            in_danger: false,
            isolated: false,
        }
    }

    #[test]
    fn tick_decay_increases_pressure() {
        let mut ds = DriveSystem::default();
        let ctx = default_context();
        ds.tick_decay(&ctx);
        assert!(ds.food.current > 0);
        assert!(ds.shelter.current > 0);
    }

    #[test]
    fn physical_labor_doubles_food_decay() {
        let mut a = DriveSystem::default();
        let mut b = DriveSystem::default();
        let normal = default_context();
        let labor = DriveContext { physical_labor: true, ..default_context() };
        a.tick_decay(&normal);
        b.tick_decay(&labor);
        assert!(b.food.current > a.food.current);
    }

    #[test]
    fn apply_effect_reduces_pressure_clamped() {
        let mut ds = DriveSystem::default();
        ds.food.current = 50;
        ds.apply_effect(DriveKind::Food, 30);
        assert_eq!(ds.food.current, 20);

        // Clamp to 0
        ds.apply_effect(DriveKind::Food, 100);
        assert_eq!(ds.food.current, 0);
    }

    #[test]
    fn apply_effect_negative_delta_increases_clamped() {
        let mut ds = DriveSystem::default();
        ds.food.current = 90;
        ds.apply_effect(DriveKind::Food, -20);
        assert_eq!(ds.food.current, 100); // clamped to 100
    }

    #[test]
    fn top_pressures_returns_sorted() {
        let mut ds = DriveSystem::default();
        ds.food.current = 80;
        ds.safety.current = 90;
        ds.income.current = 10;
        let top = ds.top_pressures(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0], (DriveKind::Safety, 90));
        assert_eq!(top[1], (DriveKind::Food, 80));
    }

    #[test]
    fn any_urgent_detects_threshold_crossing() {
        let mut ds = DriveSystem::default();
        assert!(!ds.any_urgent());
        ds.food.current = ds.food.urgency_threshold + 1;
        assert!(ds.any_urgent());
    }

    #[test]
    fn pressure_clamped_at_100() {
        let mut ds = DriveSystem::default();
        ds.food.current = 99;
        ds.food.decay_rate = 5;
        ds.tick_decay(&default_context());
        assert_eq!(ds.food.current, 100);
    }
}
