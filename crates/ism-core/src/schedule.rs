//! Allowed-hours schedules for peer file-content transfers.
//!
//! Windows are wall-clock times in a named time zone, so they follow daylight-saving changes.
//! `start == end` means the whole day; `start > end` crosses midnight and belongs to the day it
//! starts on. An empty window list means "always allowed".

use chrono::{DateTime, Datelike, Duration, NaiveTime, Timelike, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Window {
    /// Days the window starts on, 0 = Sunday ... 6 = Saturday.
    pub days: Vec<u8>,
    /// `HH:MM`, local time.
    pub start: String,
    /// `HH:MM`, local time.
    pub end: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ScheduleError {
    #[error("unknown time zone '{0}'")]
    TimeZone(String),
    #[error("'{0}' is not a HH:MM time")]
    Time(String),
    #[error("day {0} is not between 0 (Sunday) and 6 (Saturday)")]
    Day(u8),
}

#[derive(Debug, Clone)]
pub struct Schedule {
    tz: Tz,
    windows: Vec<(Vec<u8>, NaiveTime, NaiveTime)>,
}

fn parse_time(raw: &str) -> Result<NaiveTime, ScheduleError> {
    NaiveTime::parse_from_str(raw, "%H:%M").map_err(|_| ScheduleError::Time(raw.to_owned()))
}

impl Schedule {
    pub fn new(timezone: &str, windows: &[Window]) -> Result<Self, ScheduleError> {
        let tz: Tz = timezone
            .parse()
            .map_err(|_| ScheduleError::TimeZone(timezone.to_owned()))?;
        let windows = windows
            .iter()
            .map(|w| {
                if let Some(day) = w.days.iter().find(|d| **d > 6) {
                    return Err(ScheduleError::Day(*day));
                }
                Ok((w.days.clone(), parse_time(&w.start)?, parse_time(&w.end)?))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { tz, windows })
    }

    pub fn is_allowed(&self, now: DateTime<Utc>) -> bool {
        if self.windows.is_empty() {
            return true;
        }
        let local = now.with_timezone(&self.tz);
        let today = local.weekday().num_days_from_sunday() as u8;
        let yesterday = (today + 6) % 7;
        let time = NaiveTime::from_hms_opt(local.hour(), local.minute(), local.second())
            .unwrap_or(NaiveTime::MIN);
        self.windows.iter().any(|(days, start, end)| {
            if start == end {
                days.contains(&today)
            } else if start < end {
                days.contains(&today) && time >= *start && time < *end
            } else {
                (days.contains(&today) && time >= *start)
                    || (days.contains(&yesterday) && time < *end)
            }
        })
    }

    /// The next minute at or after `now` when transfers are allowed (within 8 days).
    pub fn next_allowed(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        if self.is_allowed(now) {
            return Some(now);
        }
        self.next_transition(now)
    }

    /// The next minute when allowed-hours eligibility changes (within 8 days).
    pub fn next_transition(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        if self.windows.is_empty() {
            return None;
        }
        let allowed = self.is_allowed(now);
        let start = now
            .with_second(0)
            .and_then(|t| t.with_nanosecond(0))
            .unwrap_or(now)
            + Duration::minutes(1);
        (0..8 * 24 * 60)
            .map(|m| start + Duration::minutes(m))
            .find(|t| self.is_allowed(*t) != allowed)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn every_day(start: &str, end: &str) -> Vec<Window> {
        vec![Window {
            days: (0..7).collect(),
            start: start.into(),
            end: end.into(),
        }]
    }

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).single().unwrap()
    }

    #[test]
    fn empty_schedule_always_allows() {
        let s = Schedule::new("UTC", &[]).unwrap();
        assert!(s.is_allowed(utc(2026, 1, 1, 12, 0)));
    }

    #[test]
    fn overnight_windows_cross_midnight() {
        let s = Schedule::new("UTC", &every_day("22:00", "06:00")).unwrap();
        assert!(s.is_allowed(utc(2026, 1, 1, 23, 0)));
        assert!(s.is_allowed(utc(2026, 1, 2, 5, 59)));
        assert!(!s.is_allowed(utc(2026, 1, 2, 6, 0)));
        assert!(!s.is_allowed(utc(2026, 1, 2, 12, 0)));
        assert_eq!(
            s.next_allowed(utc(2026, 1, 2, 12, 0)),
            Some(utc(2026, 1, 2, 22, 0))
        );
    }

    #[test]
    fn overnight_window_belongs_to_its_start_day() {
        // Friday (5) night only.
        let s = Schedule::new(
            "UTC",
            &[Window {
                days: vec![5],
                start: "22:00".into(),
                end: "02:00".into(),
            }],
        )
        .unwrap();
        // 2026-01-03 is a Saturday.
        assert!(s.is_allowed(utc(2026, 1, 3, 1, 0)));
        assert!(!s.is_allowed(utc(2026, 1, 4, 1, 0)));
    }

    #[test]
    fn follows_daylight_saving_time() {
        // 01:00-05:00 New York time. In winter that is 06:00-10:00 UTC, in summer 05:00-09:00.
        let s = Schedule::new("America/New_York", &every_day("01:00", "05:00")).unwrap();
        assert!(s.is_allowed(utc(2026, 1, 15, 6, 30)));
        assert!(!s.is_allowed(utc(2026, 1, 15, 5, 30)));
        assert!(s.is_allowed(utc(2026, 7, 15, 5, 30)));
        assert!(!s.is_allowed(utc(2026, 7, 15, 9, 30)));
    }

    #[test]
    fn transitions_follow_combined_windows_and_daylight_saving() {
        let mut windows = every_day("08:00", "12:00");
        windows.extend(every_day("11:00", "16:00"));
        let s = Schedule::new("UTC", &windows).unwrap();
        assert_eq!(
            s.next_transition(utc(2026, 1, 1, 10, 0)),
            Some(utc(2026, 1, 1, 16, 0))
        );
        assert_eq!(
            s.next_transition(utc(2026, 1, 1, 17, 0)),
            Some(utc(2026, 1, 2, 8, 0))
        );
        let s = Schedule::new("America/New_York", &every_day("01:00", "02:00")).unwrap();
        // Both occurrences of 01:00 during the autumn clock change remain allowed.
        assert_eq!(
            s.next_transition(utc(2026, 11, 1, 5, 30)),
            Some(utc(2026, 11, 1, 7, 0))
        );
        assert_eq!(
            Schedule::new("UTC", &[])
                .unwrap()
                .next_transition(Utc::now()),
            None
        );
    }

    #[test]
    fn rejects_bad_input() {
        assert!(Schedule::new("Mars/Base", &[]).is_err());
        assert!(Schedule::new("UTC", &every_day("25:00", "01:00")).is_err());
    }
}
