use criers_core::Webhook;
use serde_json::{Value, json};

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../examples/critical.json")).unwrap()
}

#[test]
fn equivalent_timezone_offsets_identify_the_same_occurrence() {
    let utc: Webhook = serde_json::from_value(fixture()).unwrap();
    let mut offset = fixture();
    offset["alerts"][0]["startsAt"] = json!("2026-10-10T12:00:00+02:00");
    let offset: Webhook = serde_json::from_value(offset).unwrap();
    offset.validate().unwrap();
    assert_eq!(utc.alerts[0].start_key(), offset.alerts[0].start_key());
}

#[test]
fn canonical_keys_preserve_nanosecond_occurrence_order() {
    let mut earlier = fixture();
    earlier["alerts"][0]["startsAt"] = json!("2026-10-10T10:00:00.000000001Z");
    let mut later = fixture();
    later["alerts"][0]["startsAt"] = json!("2026-10-10T10:00:00.000000002Z");
    let earlier: Webhook = serde_json::from_value(earlier).unwrap();
    let later: Webhook = serde_json::from_value(later).unwrap();
    assert!(earlier.alerts[0].start_key() < later.alerts[0].start_key());
}

#[test]
fn resolution_before_occurrence_start_is_rejected() {
    let mut invalid = fixture();
    invalid["alerts"][0]["status"] = json!("resolved");
    invalid["alerts"][0]["endsAt"] = json!("2026-10-10T09:59:59Z");
    let invalid: Webhook = serde_json::from_value(invalid).unwrap();
    assert_eq!(
        invalid.validate(),
        Err("resolved endsAt cannot precede startsAt")
    );
}
