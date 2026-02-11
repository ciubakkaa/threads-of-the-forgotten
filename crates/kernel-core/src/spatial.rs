use std::collections::BTreeMap;

use contracts::Observation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub location_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub route_id: String,
    pub origin: String,
    pub destination: String,
    pub travel_time: u64,
    pub risk_level: i64,
    pub weather_modifier: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Weather {
    Clear,
    Rain,
    Storm,
    Snow,
}

#[derive(Debug, Clone)]
pub struct SpatialModel {
    pub locations: BTreeMap<String, Location>,
    pub routes: BTreeMap<String, Route>,
    pub weather: Weather,
}

impl SpatialModel {
    pub fn new() -> Self {
        Self {
            locations: BTreeMap::new(),
            routes: BTreeMap::new(),
            weather: Weather::Clear,
        }
    }

    pub fn travel_duration(&self, route_id: &str) -> Option<u64> {
        let route = self.routes.get(route_id)?;
        let weather_penalty = match self.weather {
            Weather::Clear => 0_i64,
            Weather::Rain => 1,
            Weather::Storm => 2,
            Weather::Snow => 2,
        };
        let risk_penalty = (route.risk_level / 20).max(0) as u64;
        let modifier = (route.weather_modifier.max(0) as u64) * weather_penalty as u64;
        Some(route.travel_time + risk_penalty + modifier)
    }

    pub fn update_weather(&mut self, weather: Weather) {
        self.weather = weather;
    }

    pub fn can_interact_same_location(&self, actor_location: &str, target_location: &str) -> bool {
        actor_location == target_location
    }

    pub fn filter_route_observations<'a>(
        &self,
        route: &Route,
        observations: &'a [Observation],
    ) -> Vec<&'a Observation> {
        observations
            .iter()
            .filter(|obs| {
                obs.location_id == route.origin
                    || obs.location_id == route.destination
                    || obs.location_id == route.route_id
            })
            .collect()
    }
}

impl Default for SpatialModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_gated_interaction() {
        let model = SpatialModel::new();
        assert!(model.can_interact_same_location("loc:a", "loc:a"));
        assert!(!model.can_interact_same_location("loc:a", "loc:b"));
    }

    #[test]
    fn travel_duration_reflects_route_properties() {
        let mut model = SpatialModel::new();
        model.routes.insert(
            "r1".to_string(),
            Route {
                route_id: "r1".to_string(),
                origin: "a".to_string(),
                destination: "b".to_string(),
                travel_time: 3,
                risk_level: 30,
                weather_modifier: 2,
            },
        );

        let clear = model.travel_duration("r1").expect("route exists");
        model.update_weather(Weather::Storm);
        let storm = model.travel_duration("r1").expect("route exists");

        assert!(storm > clear);
    }
}
