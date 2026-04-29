#![no_main]

use haqlite::SqlValue;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(data) {
        test_value_roundtrip(&json);
    }

    if let Ok(s) = std::str::from_utf8(data) {
        test_forwarding_request(s);
    }
});

fn test_value_roundtrip(json: &serde_json::Value) {
    if let Ok(sv) = serde_json::from_value::<SqlValue>(json.clone()) {
        if let Ok(back) = serde_json::to_string(&sv) {
            std::hint::black_box(back);
        }
    }
}

fn test_forwarding_request(s: &str) {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed.len() > 1_000_000 {
        return;
    }

    let json_str = format!(
        r#"{{"sql":"{}","params":[]}}"#,
        trimmed.replace('"', "\\\"")
    );
    if let Ok(req) = serde_json::from_str::<serde_json::Value>(&json_str) {
        std::hint::black_box(req);
    }
}
