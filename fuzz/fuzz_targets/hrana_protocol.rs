#![no_main]

use hrana_server::{CursorRequest, HranaStmt, HranaValue, PipelineRequest};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(data) {
        test_hrana_value(&json);
        test_pipeline_request(&json);
        test_cursor_request(&json);
    }
});

fn test_hrana_value(json: &serde_json::Value) {
    if let Ok(v) = serde_json::from_value::<HranaValue>(json.clone()) {
        std::hint::black_box(&v);
        let v2 = v.clone();
        if let Ok(back) = serde_json::to_string(&v2) {
            std::hint::black_box(back);
        }
    }
}

fn test_pipeline_request(json: &serde_json::Value) {
    if let Ok(req) = serde_json::from_value::<PipelineRequest>(json.clone()) {
        std::hint::black_box(&req);
        for stream_req in &req.requests {
            std::hint::black_box(stream_req);
        }
    }
}

fn test_cursor_request(json: &serde_json::Value) {
    if let Ok(req) = serde_json::from_value::<CursorRequest>(json.clone()) {
        std::hint::black_box(&req);
    }
}

fn _test_stmt_fuzz(stmt: &HranaStmt) {
    std::hint::black_box(&stmt.sql);
    for arg in &stmt.args {
        std::hint::black_box(arg);
    }
    for named in &stmt.named_args {
        std::hint::black_box(&named.name);
        std::hint::black_box(&named.value);
    }
}
