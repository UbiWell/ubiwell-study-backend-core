# Android Dashboard Integration

This document describes how android data is wired into the internal dashboard.
The goal was to plug android-uploaded data into the same daily-summary and
per-user-detail flow that already powers iOS + Garmin, without breaking any
existing iOS-only deployments.

For the raw android collection shapes that this code reads, see
[`ANDROID_SCHEMA_DESIGN.md`](./ANDROID_SCHEMA_DESIGN.md).

## What changed

All changes are in `study_framework_core/core/processing_scripts.py`. No
template, route, or `DashboardBase` changes were required — the existing
dashboard reads from the `daily_summaries` collection and the per-user plot
endpoint, both of which now know about android data.

### 1. User filtering (`generate_daily_summaries`)

Users are picked up for daily-summary generation when any of the device login
fields exist. Android was added to the existing iOS/Garmin filter:

```python
'$or': [
    {'ios_login_time': {'$exists': True}},
    {'android_login_time': {'$exists': True}},
    {'garmin_login_time': {'$exists': True}},
]
```

Without this, an android-only participant would silently be skipped by the cron
job and never appear on the dashboard.

### 2. Daily summary document (`_generate_user_daily_summary`)

The `daily_summaries` doc now carries android-derived fields in addition to the
existing iOS/Garmin ones:

```js
{
  uid, date, date_str,
  location: {
    distance_traveled,        // iOS + android phone GPS combined (meters)
    duration_hours            // iOS + android phone GPS combined
  },
  location_ios:     { distance_traveled, duration_hours },
  location_android: { distance_traveled, duration_hours },
  garmin_wear_duration,
  garmin_on_duration,
  android_activity: {
    steps,                    // sum of android_steps.steps
    battery_samples,          // # of android_battery docs
    screen_events,            // # of android_screen_event docs
    app_usage_uploads,        // # of android_app_usage docs
    wifi_scans,               // # of android_wifi docs
    running_services_pings    // # of android_running_services docs
  },
  ema: {...},
  generated_at
}
```

The core `location.*` keys keep their original meaning for the existing
dashboard column ("Phone Duration", "Distance Traveled"). They now sum across
iOS + android because a single participant on a single day will typically be
uploading from at most one phone OS, so summing is safe and the dashboard works
unchanged for both platforms.

The per-platform breakdown lives under `location_ios` / `location_android` for
anyone who wants to split the view later (e.g., a study-specific dashboard
subclass).

### 3. Location availability (`_get_location_availability`)

Used by the per-user daily plot. Now considers a 30-minute window "covered" if
either `ios_location` (event_id 152) **or** `android_location` has any docs in
the window. iOS is queried first; android is only queried when iOS is empty in
the window, so iOS-only studies pay no extra cost.

### 4. Daily plot (`_generate_daily_plot`)

Two new optional traces are added on the right y-axis (`yaxis2`) when the user
has android data for the day:

- **Android Steps** — bar trace, one bar per `android_steps` reporting interval.
- **Android Battery (%)** — dotted line trace of `android_battery.level` over
  the day.

Both traces are added conditionally (`if android_step_times: ...`) so iOS-only
users see exactly the same plot as before. The y2 axis title was widened to
`"HR (BPM) / Steps / Battery (%)"` to reflect the new mixed-units overlay.

### 5. Weekly trends (`_generate_weekly_trends`)

Two new line traces are added on a second y-axis (`yaxis2`, "Android Counts")
when at least one day in the 7-day window has data:

- **Android Steps** — daily step total from `android_activity.steps`.
- **Android Screen Events** — daily screen-event count.

These come straight from the daily-summary docs, so the trends plot doesn't
re-query raw android collections.

## New / changed helpers in `processing_scripts.py`

| Method | Purpose |
|--------|---------|
| `_get_android_location_info(uid, start, end)` | Distance + duration from `android_location` using `LATITUDE`/`LONGITUDE` (uppercase, per android schema). |
| `_get_android_phone_activity(uid, start, end)` | Returns the `android_activity` rollup dict (steps, battery samples, screen events, etc.). |
| `_get_android_steps_timeline(uid, current_date)` | `(steps_per_interval, times)` for the daily-plot android steps trace. |
| `_get_android_battery_timeline(uid, current_date)` | `(battery_level_pct, times)` for the daily-plot battery trace. |
| `_calculate_distance_from_records(records, lat_key, lon_key)` | Parameterized distance calculator. The pre-existing `_calculate_distance_traveled` is now a thin wrapper that passes `latitude`/`longitude` (iOS). |
| `_get_location_availability` | Now checks android too. |
| `_get_location_info` | Unchanged behavior (iOS only); the merge with android happens in `_generate_user_daily_summary`. |

## Backwards compatibility

- iOS-only studies see no schema change in the dashboard table (same columns,
  same numbers).
- Existing `daily_summaries` docs without `android_activity` / `location_*`
  fields read back as zero / empty in the weekly plot (we use
  `.get(..., 0)` / `or {}`).
- The Garmin column behavior is unchanged.
- The daily plot adds android traces only when data exists, so iOS users still
  see the original 3-trace plot.

## Re-generating after the change

After deploying, re-run the daily summaries for the days you want to see
populated. From the study root, with the study conda env active:

```bash
# No date → cronjob mode: today (midnight → now) for last 7 days + today.
python -m study_framework_core.core.processing_scripts --action generate_summaries

# A specific day (full 24h)
python -m study_framework_core.core.processing_scripts --action generate_summaries --date 2026-05-12
```

If your study uses the per-study wrapper script, the same action name applies:

```bash
/mnt/study/<study_name>/scripts/process_data.sh --action generate_summaries
```

To force-process a single user (e.g., one who hasn't yet got an
`*_login_time`), call the method directly from a one-off Python shell rather
than the CLI, since the CLI doesn't expose `--force-user`:

```python
from study_framework_core.core.processing_scripts import DataProcessor
DataProcessor().generate_daily_summaries(date='2026-05-12', force_user='A1AB12')
```

## Adding android-specific columns to the dashboard table

The default dashboard table (`SimpleDashboard` in `internal_web.py`) does **not**
add an "Android Steps" column out of the box, because that column is meaningless
for iOS-only studies. To add android columns for a specific study, extend
`DashboardBase` (see [`EXTENSION_GUIDE.md`](./EXTENSION_GUIDE.md)) and read
`daily_summary.android_activity.*` in your `generate_custom_row_data`. Example:

```python
from study_framework_core.core.dashboard import DashboardBase, DashboardColumn

class AndroidStudyDashboard(DashboardBase):
    def _get_custom_columns(self):
        return [
            DashboardColumn("android_steps", "Steps", width="10%"),
            DashboardColumn("android_screen_events", "Screen Events", width="12%"),
        ]

    def generate_custom_row_data(self, user_data, date_str):
        from study_framework_core.core.config import get_config
        from study_framework_core.core.handlers import get_db
        from datetime import datetime

        config, db = get_config(), get_db()
        date_ts = int(datetime.strptime(date_str, "%m-%d-%y").timestamp())
        summary = db[config.collections.DAILY_SUMMARY].find_one(
            {'uid': user_data['uid'], 'date': date_ts}
        ) or {}
        activity = summary.get('android_activity', {}) or {}
        return {
            'android_steps': activity.get('steps', 0),
            'android_screen_events': activity.get('screen_events', 0),
        }
```
