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
        let cron = Schedule::from_str(&cron_expr)
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
