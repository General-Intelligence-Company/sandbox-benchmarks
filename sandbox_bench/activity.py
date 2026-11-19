"""Clock intervals and scoring for the shared activity window."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ClockAlignment:
    # Controller monotonic minus guest monotonic. The sample happened somewhere
    # between request dispatch and response receipt; no symmetric RTT assumption.
    lower: float
    upper: float

    @classmethod
    def sample(cls, sent, received, guest):
        return cls(sent - guest, received - guest)

    def intersect(self, other):
        lower, upper = max(self.lower, other.lower), min(self.upper, other.upper)
        if lower > upper:
            raise ValueError("Guest monotonic clock changed between observations")
        return ClockAlignment(lower, upper)

    @property
    def uncertainty_s(self):
        return (self.upper - self.lower) / 2

    def guest_at(self, controller):
        return controller - (self.lower + self.upper) / 2

    def bounds(self, guest):
        return guest + self.lower, guest + self.upper


def score_activity(value, schedule, alignment):
    """Use conservative clock bounds, so transport delay cannot create a pass."""
    events = value["events"]
    for event in events:
        due = schedule["start_monotonic"] + 5 * event["index"]
        start_lower, start_upper = alignment.bounds(event["started_monotonic"])
        end_lower, end_upper = alignment.bounds(event["guest_monotonic"])
        event.update(
            controller_start_bounds=[start_lower, start_upper],
            controller_end_bounds=[end_lower, end_upper],
            start_lateness_upper_s=max(0, start_upper - due),
        )
    functional = len(events) == 12 and all(
        e.get("passed") and e.get("external_s3", {}).get("passed") for e in events
    )
    aligned = schedule["alignment_confirmed"]
    passed = bool(value["done"] and functional and aligned) and all(
        e["start_lateness_upper_s"] <= 1
        and e["controller_end_bounds"][1] <= schedule["start_monotonic"] + 61
        for e in events
    )
    return {
        "passed": passed,
        "functional": functional,
        "armed": True,
        "observed": bool(value["done"]),
        "alignment_confirmed": aligned,
        "window_start": schedule["epoch"],
        "window_end": schedule["epoch"] + 60,
        "events": events,
        "done": value["done"],
        **({"failure_domain": "harness_control"} if not aligned else {}),
    }


def activity_category(activity):
    if activity.get("passed"):
        return "clean"
    if activity.get("armed") is False:
        return "unarmed"
    if (
        activity.get("observed") is False
        or activity.get("alignment_confirmed") is False
    ):
        return "unobserved"
    events = activity.get("events", [])
    if len(events) == 12 and all(
        e.get("passed") and e.get("external_s3", {}).get("passed") for e in events
    ):
        return "cadence_miss"
    if len(events) == 12:
        return "functional_failure"
    return "unknown"
