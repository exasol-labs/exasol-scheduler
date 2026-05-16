use chrono::{DateTime, Local, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct ParsedSchedule {
    cron: Schedule,
    timezone: ParsedTimeZone,
    normalized: String,
}

impl ParsedSchedule {
    pub fn parse(raw: &str) -> Result<Self, ScheduleParseError> {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.is_empty() || tokens[0] != "CRON" {
            return Err(ScheduleParseError::MissingPrefix);
        }

        let mut timezone = ParsedTimeZone::LocalDefault;
        let mut cron_tokens = &tokens[1..];

        if let Some(last) = cron_tokens.last().copied() {
            if let Some(tz_name) = last.strip_prefix("TZ=") {
                if tz_name.is_empty() {
                    return Err(ScheduleParseError::InvalidTimeZone("".to_string()));
                }
                timezone = parse_timezone(tz_name)?;
                cron_tokens = &cron_tokens[..cron_tokens.len() - 1];
            }
        }

        if cron_tokens.len() != 6 {
            return Err(ScheduleParseError::InvalidFieldCount {
                expected: 6,
                found: cron_tokens.len(),
            });
        }

        let cron_expr = cron_tokens.join(" ");

        // Remap the DOW field (index 5) from standard cron numbering
        // (0/7=Sun, 1=Mon…6=Sat) to cron-rs internal numbering (1=Sun, 2=Mon…7=Sat)
        // so that users can write schedules the same way as standard Unix cron.
        let remapped_cron_expr = {
            let mut parts: Vec<String> = cron_tokens.iter().map(|s| s.to_string()).collect();
            parts[5] = remap_dow_field(&parts[5]);
            parts.join(" ")
        };

        let cron = Schedule::from_str(&remapped_cron_expr)
            .map_err(|err| ScheduleParseError::InvalidCron(err.to_string()))?;

        let normalized = match timezone {
            ParsedTimeZone::LocalDefault => format!("CRON {cron_expr}"),
            ParsedTimeZone::Utc => format!("CRON {cron_expr} TZ=UTC"),
            ParsedTimeZone::Iana(zone) => format!("CRON {cron_expr} TZ={zone}"),
        };

        Ok(Self {
            cron,
            timezone,
            normalized,
        })
    }

    pub fn normalized(&self) -> &str {
        &self.normalized
    }

    pub fn timezone(&self) -> ParsedTimeZone {
        self.timezone
    }

    pub fn next_after(&self, now_utc: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.next_after_with_local(now_utc, LocalTimeZone::System)
    }

    pub fn next_after_with_local(
        &self,
        now_utc: DateTime<Utc>,
        local_tz: LocalTimeZone,
    ) -> Option<DateTime<Utc>> {
        match self.timezone {
            ParsedTimeZone::LocalDefault => match local_tz {
                LocalTimeZone::System => {
                    let local_now = now_utc.with_timezone(&Local);
                    self.cron
                        .after(&local_now)
                        .next()
                        .map(|dt| dt.with_timezone(&Utc))
                }
                LocalTimeZone::Named(zone) => {
                    let local_now = now_utc.with_timezone(&zone);
                    self.cron
                        .after(&local_now)
                        .next()
                        .map(|dt| dt.with_timezone(&Utc))
                }
            },
            ParsedTimeZone::Utc => self
                .cron
                .after(&now_utc)
                .next()
                .map(|dt| dt.with_timezone(&Utc)),
            ParsedTimeZone::Iana(zone) => {
                let zoned_now = now_utc.with_timezone(&zone);
                self.cron
                    .after(&zoned_now)
                    .next()
                    .map(|dt| dt.with_timezone(&Utc))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ParsedTimeZone {
    LocalDefault,
    Utc,
    Iana(Tz),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LocalTimeZone {
    System,
    Named(Tz),
}

#[derive(Debug, Error)]
pub enum ScheduleParseError {
    #[error("schedule must start with CRON")]
    MissingPrefix,
    #[error("cron field count must be {expected}, found {found}")]
    InvalidFieldCount { expected: usize, found: usize },
    #[error("invalid cron expression: {0}")]
    InvalidCron(String),
    #[error("invalid timezone: {0}")]
    InvalidTimeZone(String),
}

fn parse_timezone(value: &str) -> Result<ParsedTimeZone, ScheduleParseError> {
    if value == "UTC" {
        return Ok(ParsedTimeZone::Utc);
    }

    value
        .parse::<Tz>()
        .map(ParsedTimeZone::Iana)
        .map_err(|_| ScheduleParseError::InvalidTimeZone(value.to_string()))
}

// Remap a full DOW field (may be comma-separated) from standard cron numbering
// (0/7=Sun, 1=Mon…6=Sat) to cron-rs internal numbering (1=Sun, 2=Mon…7=Sat).
// Named tokens (MON, FRI, …) are passed through unchanged.
fn remap_dow_field(field: &str) -> String {
    if field == "*" || field == "?" {
        return field.to_string();
    }
    field
        .split(',')
        .map(remap_dow_token)
        .collect::<Vec<_>>()
        .join(",")
}

fn remap_dow_token(token: &str) -> String {
    let (range_part, step_part) = token
        .split_once('/')
        .map_or((token, None), |(r, s)| (r, Some(s)));

    let remapped = if range_part == "*" || range_part == "?" {
        range_part.to_string()
    } else if let Some((start, end)) = range_part.split_once('-') {
        format!("{}-{}", remap_dow_atom(start), remap_dow_atom(end))
    } else {
        remap_dow_atom(range_part)
    };

    match step_part {
        Some(step) => format!("{remapped}/{step}"),
        None => remapped,
    }
}

// If the atom is a number, apply (n % 7) + 1. Named days pass through unchanged.
fn remap_dow_atom(atom: &str) -> String {
    match atom.parse::<u32>() {
        Ok(n) => ((n % 7) + 1).to_string(),
        Err(_) => atom.to_string(),
    }
}
