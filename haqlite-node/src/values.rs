use haqlite::SqlValue;
use napi::bindgen_prelude::*;

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

pub fn from_js(
    values: Vec<Either6<Null, bool, f64, String, BigInt, Buffer>>,
) -> Result<Vec<SqlValue>> {
    values
        .into_iter()
        .map(|value| {
            Ok(match value {
                Either6::A(_) => SqlValue::Null,
                Either6::B(value) => SqlValue::Integer(i64::from(value)),
                Either6::C(value) => {
                    if !value.is_finite() {
                        return Err(invalid("SQL numbers must be finite"));
                    }
                    if value.fract() == 0.0 {
                        if value.abs() > MAX_SAFE_INTEGER {
                            return Err(invalid(
                                "Unsafe integer parameter; use a signed 64-bit bigint",
                            ));
                        }
                        SqlValue::Integer(value as i64)
                    } else {
                        SqlValue::Real(value)
                    }
                }
                Either6::D(value) => SqlValue::Text(value),
                Either6::E(value) => {
                    // Check magnitude before get_i64: out-of-range negative
                    // BigInts can overflow inside that conversion in debug builds.
                    let limit = if value.sign_bit {
                        i64::MIN.unsigned_abs()
                    } else {
                        i64::MAX as u64
                    };
                    if value.words.len() != 1 || value.words[0] > limit {
                        return Err(invalid(
                            "bigint parameter is outside the signed 64-bit range",
                        ));
                    }
                    SqlValue::Integer(value.get_i64().0)
                }
                Either6::F(value) => SqlValue::Blob(value.to_vec()),
            })
        })
        .collect()
}

pub fn to_js(value: SqlValue) -> Either5<Null, f64, String, BigInt, Buffer> {
    match value {
        SqlValue::Null => Either5::A(Null),
        SqlValue::Integer(value) if value.unsigned_abs() <= MAX_SAFE_INTEGER as u64 => {
            Either5::B(value as f64)
        }
        SqlValue::Integer(value) => Either5::D(value.into()),
        SqlValue::Real(value) => Either5::B(value),
        SqlValue::Text(value) => Either5::C(value),
        SqlValue::Blob(value) => Either5::E(value.into()),
    }
}

fn invalid(message: &str) -> Error {
    Error::new(Status::InvalidArg, message)
}
