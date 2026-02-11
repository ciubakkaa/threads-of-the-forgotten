//! Spatial model: locations, routes, weather effects, travel duration, and location-gated interactions.

use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// A location in the world (settlement, waypoint, landmark).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub location_id: String,
    pub name: String,
    /// Tags for filtering (e.g. "settlement", "waypoint", "wilderness").
    pub tags: Vec<String>,
}

/// A route connecting two locations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub route_id: String,
    pub origin_id: String,
    pub destination_id: String,
    /// Base travel time in ticks (before weather/risk modifiers).
    pub travel_time: u64,
    /// Risk level 0–100.
    pub risk_level: i64,
    /// Per-route weather modifier applied multiplicatively to travel time (percent, 100 = normal).
    pub weather_modifier: i64,
}

/// Weather conditions that affect routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeatherCondition {
    Clear,
    Rain,
    Storm,
    Snow,
    Fog,
}

/// Current weather state for the world.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeatherState {
    pub condition: WeatherCondition,
    /// Global travel time multiplier percent (100 = normal, 150 = 50% slower).
    pub travel_time_multiplier: i64,
    /// Global risk modifier added to route risk levels.
    pub risk_modifier: i64,
}

impl Default for WeatherState {
    fn default() -> Self {
        Self {
            condition: WeatherCondition::Clear,
            travel_time_multiplier: 100,
            risk_modifier: 0,
        }
    }
}

/// Error type for spatial operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpatialError {
    /// The acting NPC is not at the same location as the target.
    NotColocated {
        actor_id: String,
        actor_location: String,
        target_id: String,
        target_location: String,
    },
    /// No route exists between the two locations.
    NoRoute { from: String, to: String },
}

impl std::fmt::Display for SpatialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpatialError::NotColocated {
                actor_id,
                actor_location,
                target_id,
                target_location,
            } => write!(
                f,
                "{} at {} cannot interact with {} at {}",
                actor_id, actor_location, target_id, target_location
            ),
            SpatialError::NoRoute { from, to } => write!(f, "no route from {} to {}", from, to),
        }
    }
}

// ---------------------------------------------------------------------------
// SpatialModel
// ---------------------------------------------------------------------------

/// The spatial model manages locations, routes, weather, and location-gated interactions.
#[derive(Debug, Clone)]
pub struct SpatialModel {
    locations: BTreeMap<String, Location>,
    routes: BTreeMap<String, Route>,
    weather: WeatherState,
}

impl SpatialModel {
    pub fn new() -> Self {
        Self {
            locations: BTreeMap::new(),
            routes: BTreeMap::new(),
            weather: WeatherState::default(),
        }
    }

    // --- Location management ---

    pub fn add_location(&mut self, location: Location) {
        self.locations
            .insert(location.location_id.clone(), location);
    }

    pub fn get_location(&self, location_id: &str) -> Option<&Location> {
        self.locations.get(location_id)
    }

    pub fn locations(&self) -> &BTreeMap<String, Location> {
        &self.locations
    }

    // --- Route management ---

    pub fn add_route(&mut self, route: Route) {
        self.routes.insert(route.route_id.clone(), route);
    }

    pub fn get_route(&self, route_id: &str) -> Option<&Route> {
        self.routes.get(route_id)
    }

    pub fn routes(&self) -> &BTreeMap<String, Route> {
        &self.routes
    }

    /// Find a route between two locations (checks both directions).
    pub fn find_route(&self, from: &str, to: &str) -> Option<&Route> {
        self.routes.values().find(|r| {
            (r.origin_id == from && r.destination_id == to)
                || (r.origin_id == to && r.destination_id == from)
        })
    }

    // --- Weather ---

    pub fn weather(&self) -> &WeatherState {
        &self.weather
    }

    /// Change weather condition and update global modifiers accordingly.
    pub fn set_weather(&mut self, condition: WeatherCondition) {
        let (travel_mult, risk_mod) = weather_effects(condition);
        self.weather = WeatherState {
            condition,
            travel_time_multiplier: travel_mult,
            risk_modifier: risk_mod,
        };
    }

    // --- Travel duration ---

    /// Calculate effective travel duration for a route given current weather.
    ///
    /// `duration = base_travel_time * route_weather_modifier / 100 * global_weather_multiplier / 100`
    ///
    /// Minimum 1 tick.
    pub fn travel_duration(&self, route: &Route) -> u64 {
        let base = route.travel_time as i64;
        let route_mod = route.weather_modifier.max(1);
        let global_mod = self.weather.travel_time_multiplier.max(1);
        let effective = (base * route_mod / 100) * global_mod / 100;
        effective.max(1) as u64
    }

    /// Calculate effective travel duration between two locations by looking up the route.
    pub fn travel_duration_between(&self, from: &str, to: &str) -> Result<u64, SpatialError> {
        let route = self
            .find_route(from, to)
            .ok_or_else(|| SpatialError::NoRoute {
                from: from.to_string(),
                to: to.to_string(),
            })?;
        Ok(self.travel_duration(route))
    }

    /// Effective risk level for a route given current weather.
    ///
    /// `risk = route.risk_level + weather.risk_modifier`, clamped to [0, 100].
    pub fn effective_risk(&self, route: &Route) -> i64 {
        (route.risk_level + self.weather.risk_modifier).clamp(0, 100)
    }

    // --- Location-gated interaction enforcement ---

    /// Enforce that actor and target are at the same location.
    pub fn enforce_colocation(
        &self,
        actor_id: &str,
        actor_location: &str,
        target_id: &str,
        target_location: &str,
    ) -> Result<(), SpatialError> {
        if actor_location == target_location {
            Ok(())
        } else {
            Err(SpatialError::NotColocated {
                actor_id: actor_id.to_string(),
                actor_location: actor_location.to_string(),
                target_id: target_id.to_string(),
                target_location: target_location.to_string(),
            })
        }
    }

    // --- Perception filtering for traveling agents ---

    /// Filter observations for a traveling agent.
    ///
    /// A traveling agent can only perceive events at their route's origin, destination,
    /// or along the route itself (identified by route_id as location).
    pub fn filter_observations_for_traveler<'a>(
        &self,
        route_id: &str,
        observations: &'a [(String, String)], // (event_id, location_id)
    ) -> Vec<&'a str> {
        let route = match self.routes.get(route_id) {
            Some(r) => r,
            None => return Vec::new(),
        };
        observations
            .iter()
            .filter(|(_, loc)| {
                loc == &route.origin_id || loc == &route.destination_id || loc == route_id
            })
            .map(|(event_id, _)| event_id.as_str())
            .collect()
    }
}

impl Default for SpatialModel {
    fn default() -> Self {
        Self::new()
    }
}

/// Map weather conditions to (travel_time_multiplier, risk_modifier).
fn weather_effects(condition: WeatherCondition) -> (i64, i64) {
    match condition {
        WeatherCondition::Clear => (100, 0),
        WeatherCondition::Rain => (130, 10),
        WeatherCondition::Storm => (180, 30),
        WeatherCondition::Snow => (160, 20),
        WeatherCondition::Fog => (120, 15),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn two_location_model() -> SpatialModel {
        let mut model = SpatialModel::new();
        model.add_location(Location {
            location_id: "town_a".into(),
            name: "Town A".into(),
            tags: vec!["settlement".into()],
        });
        model.add_location(Location {
            location_id: "town_b".into(),
            name: "Town B".into(),
            tags: vec!["settlement".into()],
        });
        model.add_route(Route {
            route_id: "route_ab".into(),
            origin_id: "town_a".into(),
            destination_id: "town_b".into(),
            travel_time: 10,
            risk_level: 20,
            weather_modifier: 100,
        });
        model
    }

    #[test]
    fn travel_duration_clear_weather() {
        let model = two_location_model();
        let route = model.get_route("route_ab").unwrap();
        assert_eq!(model.travel_duration(route), 10);
    }

    #[test]
    fn travel_duration_storm_increases() {
        let mut model = two_location_model();
        model.set_weather(WeatherCondition::Storm);
        let route = model.get_route("route_ab").unwrap();
        let dur = model.travel_duration(route);
        assert!(dur > 10, "storm should increase travel time, got {}", dur);
    }

    #[test]
    fn travel_duration_between_works() {
        let model = two_location_model();
        assert_eq!(model.travel_duration_between("town_a", "town_b").unwrap(), 10);
        assert_eq!(model.travel_duration_between("town_b", "town_a").unwrap(), 10);
    }

    #[test]
    fn travel_duration_between_no_route() {
        let model = two_location_model();
        let err = model.travel_duration_between("town_a", "town_c").unwrap_err();
        assert!(matches!(err, SpatialError::NoRoute { .. }));
    }

    #[test]
    fn effective_risk_with_weather() {
        let mut model = two_location_model();
        let route = model.get_route("route_ab").unwrap();
        assert_eq!(model.effective_risk(route), 20);

        model.set_weather(WeatherCondition::Storm);
        let route = model.get_route("route_ab").unwrap();
        assert_eq!(model.effective_risk(route), 50); // 20 + 30
    }

    #[test]
    fn effective_risk_clamped_at_100() {
        let mut model = SpatialModel::new();
        model.add_route(Route {
            route_id: "r1".into(),
            origin_id: "a".into(),
            destination_id: "b".into(),
            travel_time: 5,
            risk_level: 90,
            weather_modifier: 100,
        });
        model.set_weather(WeatherCondition::Storm); // +30 risk
        let route = model.get_route("r1").unwrap();
        assert_eq!(model.effective_risk(route), 100);
    }

    #[test]
    fn enforce_colocation_same_location() {
        let model = two_location_model();
        assert!(model.enforce_colocation("npc_1", "town_a", "npc_2", "town_a").is_ok());
    }

    #[test]
    fn enforce_colocation_different_location() {
        let model = two_location_model();
        let result = model.enforce_colocation("npc_1", "town_a", "npc_2", "town_b");
        assert!(matches!(result, Err(SpatialError::NotColocated { .. })));
    }

    #[test]
    fn weather_propagation_updates_state() {
        let mut model = two_location_model();
        assert_eq!(model.weather().condition, WeatherCondition::Clear);
        model.set_weather(WeatherCondition::Snow);
        assert_eq!(model.weather().condition, WeatherCondition::Snow);
        assert_eq!(model.weather().travel_time_multiplier, 160);
        assert_eq!(model.weather().risk_modifier, 20);
    }

    #[test]
    fn filter_observations_for_traveler_limits_perception() {
        let model = two_location_model();
        let observations = vec![
            ("evt_1".to_string(), "town_a".to_string()),
            ("evt_2".to_string(), "town_b".to_string()),
            ("evt_3".to_string(), "town_c".to_string()),
            ("evt_4".to_string(), "route_ab".to_string()),
        ];
        let visible = model.filter_observations_for_traveler("route_ab", &observations);
        assert_eq!(visible.len(), 3);
        assert!(visible.contains(&"evt_1"));
        assert!(visible.contains(&"evt_2"));
        assert!(visible.contains(&"evt_4"));
        assert!(!visible.contains(&"evt_3"));
    }

    #[test]
    fn filter_observations_unknown_route() {
        let model = two_location_model();
        let observations = vec![("evt_1".to_string(), "town_a".to_string())];
        let visible = model.filter_observations_for_traveler("nonexistent", &observations);
        assert!(visible.is_empty());
    }

    #[test]
    fn find_route_bidirectional() {
        let model = two_location_model();
        assert!(model.find_route("town_a", "town_b").is_some());
        assert!(model.find_route("town_b", "town_a").is_some());
        assert!(model.find_route("town_a", "town_c").is_none());
    }

    #[test]
    fn travel_duration_minimum_one_tick() {
        let mut model = SpatialModel::new();
        model.add_route(Route {
            route_id: "short".into(),
            origin_id: "a".into(),
            destination_id: "b".into(),
            travel_time: 1,
            risk_level: 0,
            weather_modifier: 50,
        });
        let route = model.get_route("short").unwrap();
        assert_eq!(model.travel_duration(route), 1);
    }

    #[test]
    fn route_weather_modifier_affects_duration() {
        let mut model = SpatialModel::new();
        model.add_route(Route {
            route_id: "mountain".into(),
            origin_id: "a".into(),
            destination_id: "b".into(),
            travel_time: 10,
            risk_level: 40,
            weather_modifier: 150,
        });
        let route = model.get_route("mountain").unwrap();
        // 10 * 150/100 * 100/100 = 15
        assert_eq!(model.travel_duration(route), 15);
    }
}
