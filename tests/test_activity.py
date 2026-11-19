"""Deterministic checks of clock arithmetic and activity scoring, without providers."""

import pytest

from sandbox_bench.activity import ClockAlignment, activity_category, score_activity


def window(alignment, *, late_ack=False, stall=0):
    target = 1000
    start = alignment.guest_at(target)
    events = [
        {
            "index": i,
            "passed": True,
            "external_s3": {"passed": True},
            "started_monotonic": start + 5 * i + (stall if i else 0),
            "guest_monotonic": start + 5 * i + 0.2 + (stall if i else 0),
        }
        for i in range(12)
    ]
    return score_activity(
        {"done": True, "events": events},
        {"start_monotonic": target, "epoch": 2000, "alignment_confirmed": not late_ack},
        alignment,
    )


def test_clock_sample_bounds_include_asymmetric_request_delay():
    clock = ClockAlignment.sample(100, 101, 50)
    assert clock.bounds(60) == (110, 111)
    assert clock.guest_at(110) == 59.5
    assert clock.uncertainty_s == 0.5
    # Changing wall time cannot affect this arithmetic.
    assert window(clock)["passed"]


def test_late_arm_cannot_pass_even_when_guest_ticks_are_locally_on_time():
    result = window(ClockAlignment(50, 50.2), late_ack=True)
    assert result["functional"] and not result["passed"]
    assert activity_category(result) == "unobserved"


def test_real_slow_work_stays_a_cadence_failure():
    result = window(ClockAlignment(50, 50.2), stall=10)
    assert result["functional"] and not result["passed"]
    assert activity_category(result) == "cadence_miss"


def test_clock_interval_straddling_deadline_is_not_a_provider_failure():
    clock = ClockAlignment(50, 51.8)
    start = clock.guest_at(1000)
    events = [
        {
            "index": i,
            "passed": True,
            "external_s3": {"passed": True},
            "started_monotonic": start + 5 * i + 0.2,
            "guest_monotonic": start + 5 * i + 0.4,
        }
        for i in range(12)
    ]
    result = score_activity(
        {"done": True, "events": events},
        {"start_monotonic": 1000, "epoch": 2000, "alignment_confirmed": True},
        clock,
    )
    assert result["functional"] and not result["passed"]
    assert activity_category(result) == "unobserved"


def test_clock_reset_is_detected_and_overlapping_samples_narrow_bounds():
    clock = ClockAlignment(50, 51)
    assert clock.intersect(ClockAlignment(50.2, 50.5)) == ClockAlignment(50.2, 50.5)
    with pytest.raises(ValueError, match="clock changed"):
        clock.intersect(ClockAlignment(500, 501))


def test_missing_observations_are_not_functional_or_provider_failures():
    assert activity_category({"passed": False, "observed": False}) == "unobserved"
    assert activity_category({"passed": False, "armed": False}) == "unarmed"


def test_queued_rows_are_unrun_but_ineligible_resumes_are_not():
    from sandbox_bench.runner import summarize

    state = {
        "run_id": "counts",
        "status": "interrupted",
        "config": {"provider": "beam", "lifecycles": ["fresh", "resume"], "count": 2},
        "trials": [
            {"case": "fresh", "index": 1, "status": "failed", "t0": 1},
            {"case": "fresh", "index": 2, "status": "queued"},
            {"case": "resume", "index": 1, "status": "ineligible"},
        ],
        "owned": {},
        "snapshots": [],
        "operations": [],
    }
    report = summarize(state)
    assert report["cases"]["fresh"]["unrun"] == 1
    assert report["cases"]["resume"]["ineligible"] == 1
    assert report["cases"]["resume"]["unrun"] == 1
