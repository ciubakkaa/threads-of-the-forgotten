use contracts::{DriveContext, DriveKind, DriveSystem, DriveValue};

#[derive(Debug, Clone)]
pub struct DriveSubsystem {
    pub drives: DriveSystem,
}

impl DriveSubsystem {
    pub fn new(drives: DriveSystem) -> Self {
        Self { drives }
    }

    pub fn tick_decay(&mut self, context: &DriveContext) {
        fn context_decay(base: i64, context: &DriveContext, kind: DriveKind) -> i64 {
            let mut delta = base.max(0);
            if context.physical_labor && matches!(kind, DriveKind::Food | DriveKind::Health) {
                delta += 2;
            }
            if context.exposed_to_weather && matches!(kind, DriveKind::Shelter | DriveKind::Health)
            {
                delta += 1;
            }
            if context.social_stress > 0
                && matches!(
                    kind,
                    DriveKind::Belonging | DriveKind::Status | DriveKind::Safety
                )
            {
                delta += (context.social_stress / 25).max(1);
            }
            if context.illness_level > 0 && matches!(kind, DriveKind::Health | DriveKind::Food) {
                delta += (context.illness_level / 20).max(1);
            }
            delta
        }

        fn decay(value: &mut DriveValue, context: &DriveContext, kind: DriveKind) {
            let delta = context_decay(value.decay_rate, context, kind);
            value.current = (value.current + delta).clamp(0, 100);
        }

        decay(&mut self.drives.food, context, DriveKind::Food);
        decay(&mut self.drives.shelter, context, DriveKind::Shelter);
        decay(&mut self.drives.income, context, DriveKind::Income);
        decay(&mut self.drives.safety, context, DriveKind::Safety);
        decay(&mut self.drives.belonging, context, DriveKind::Belonging);
        decay(&mut self.drives.status, context, DriveKind::Status);
        decay(&mut self.drives.health, context, DriveKind::Health);
    }

    pub fn apply_effect(&mut self, drive: DriveKind, delta: i64) {
        let target = match drive {
            DriveKind::Food => &mut self.drives.food,
            DriveKind::Shelter => &mut self.drives.shelter,
            DriveKind::Income => &mut self.drives.income,
            DriveKind::Safety => &mut self.drives.safety,
            DriveKind::Belonging => &mut self.drives.belonging,
            DriveKind::Status => &mut self.drives.status,
            DriveKind::Health => &mut self.drives.health,
        };
        target.current = (target.current - delta).clamp(0, 100);
    }

    pub fn top_pressures(&self, n: usize) -> Vec<(DriveKind, i64)> {
        let mut values = vec![
            (DriveKind::Food, self.drives.food.current),
            (DriveKind::Shelter, self.drives.shelter.current),
            (DriveKind::Income, self.drives.income.current),
            (DriveKind::Safety, self.drives.safety.current),
            (DriveKind::Belonging, self.drives.belonging.current),
            (DriveKind::Status, self.drives.status.current),
            (DriveKind::Health, self.drives.health.current),
        ];
        values.sort_by(|a, b| b.1.cmp(&a.1));
        values.truncate(n.min(values.len()));
        values
    }

    pub fn any_urgent(&self) -> bool {
        [
            &self.drives.food,
            &self.drives.shelter,
            &self.drives.income,
            &self.drives.safety,
            &self.drives.belonging,
            &self.drives.status,
            &self.drives.health,
        ]
        .iter()
        .any(|value| value.current >= value.urgency_threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_drives() -> DriveSystem {
        DriveSystem {
            food: DriveValue {
                current: 10,
                decay_rate: 3,
                urgency_threshold: 60,
            },
            shelter: DriveValue {
                current: 10,
                decay_rate: 1,
                urgency_threshold: 60,
            },
            income: DriveValue {
                current: 10,
                decay_rate: 2,
                urgency_threshold: 60,
            },
            safety: DriveValue {
                current: 10,
                decay_rate: 1,
                urgency_threshold: 60,
            },
            belonging: DriveValue {
                current: 10,
                decay_rate: 1,
                urgency_threshold: 60,
            },
            status: DriveValue {
                current: 10,
                decay_rate: 1,
                urgency_threshold: 60,
            },
            health: DriveValue {
                current: 10,
                decay_rate: 2,
                urgency_threshold: 60,
            },
        }
    }

    #[test]
    fn drive_decay_is_monotonic() {
        let mut subsystem = DriveSubsystem::new(sample_drives());
        let initial = subsystem.drives.food.current;
        subsystem.tick_decay(&DriveContext::default());
        assert!(subsystem.drives.food.current >= initial);
    }

    #[test]
    fn drive_effect_application_clamps() {
        let mut subsystem = DriveSubsystem::new(sample_drives());
        subsystem.apply_effect(DriveKind::Food, 999);
        assert_eq!(subsystem.drives.food.current, 0);
    }

    #[test]
    fn urgency_respects_threshold() {
        let mut subsystem = DriveSubsystem::new(sample_drives());
        assert!(!subsystem.any_urgent());
        subsystem.drives.health.current = 90;
        assert!(subsystem.any_urgent());
    }
}
