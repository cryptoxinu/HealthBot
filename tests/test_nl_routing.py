"""Tests for natural-language pattern routing in message_router.py."""
from __future__ import annotations

from healthbot.bot.message_router import (
    _DELETE_LABS_PATTERN,
    _ONBOARD_PATTERN,
    _OURA_AUTH_PATTERN,
    _PAUSE_OVERDUE_PATTERN,
    _RESET_PATTERN,
    _RESTART_PATTERN,
    _STATUS_CHECK_PATTERN,
    _TROUBLESHOOT_PATTERN,
    _UNPAUSE_OVERDUE_PATTERN,
    _WHOOP_AUTH_PATTERN,
)


class TestStatusCheckPattern:
    """NL health status check pattern."""

    def test_how_am_i_doing(self):
        assert _STATUS_CHECK_PATTERN.match("how am I doing?")

    def test_hows_my_health(self):
        assert _STATUS_CHECK_PATTERN.match("how's my health")

    def test_give_me_a_summary(self):
        assert _STATUS_CHECK_PATTERN.match("give me a summary")

    def test_quick_update(self):
        assert _STATUS_CHECK_PATTERN.match("quick update")

    def test_any_concerns(self):
        assert _STATUS_CHECK_PATTERN.match("any concerns?")

    def test_health_status(self):
        assert _STATUS_CHECK_PATTERN.match("health status")

    def test_what_should_i_know(self):
        assert _STATUS_CHECK_PATTERN.match("what should I know?")

    def test_does_not_match_general_question(self):
        assert not _STATUS_CHECK_PATTERN.match("what is glucose?")

    def test_does_not_match_medication_question(self):
        assert not _STATUS_CHECK_PATTERN.match("should I take vitamin D?")

    def test_does_not_match_partial(self):
        assert not _STATUS_CHECK_PATTERN.match(
            "tell me about how my health is affected by iron"
        )


class TestDeleteLabsPattern:
    def test_delete_my_labs(self):
        assert _DELETE_LABS_PATTERN.search("delete my lab results")

    def test_wipe_blood_work(self):
        assert _DELETE_LABS_PATTERN.search("wipe all blood work")

    def test_does_not_match_health_question(self):
        assert not _DELETE_LABS_PATTERN.search("what do my labs mean?")


class TestResetPattern:
    def test_reset(self):
        assert _RESET_PATTERN.match("reset")

    def test_start_over(self):
        assert _RESET_PATTERN.match("start over")

    def test_does_not_match_partial(self):
        assert not _RESET_PATTERN.match("reset the model")


class TestOnboardPattern:
    def test_onboarding(self):
        assert _ONBOARD_PATTERN.match("onboarding")

    def test_start_onboarding(self):
        assert _ONBOARD_PATTERN.match("start onboarding")

    def test_health_profile(self):
        assert _ONBOARD_PATTERN.match("health profile")

    def test_does_not_match_question(self):
        assert not _ONBOARD_PATTERN.match("what is onboarding?")


class TestWearableAuthPatterns:
    def test_connect_whoop(self):
        assert _WHOOP_AUTH_PATTERN.search("connect my whoop")

    def test_link_oura(self):
        assert _OURA_AUTH_PATTERN.search("link oura")

    def test_does_not_cross_match(self):
        assert not _WHOOP_AUTH_PATTERN.search("connect my oura")
        assert not _OURA_AUTH_PATTERN.search("link whoop")


class TestTroubleshootPattern:
    def test_debug(self):
        assert _TROUBLESHOOT_PATTERN.search("debug")

    def test_whoop_error(self):
        assert _TROUBLESHOOT_PATTERN.search("whoop error")

    def test_cant_sync(self):
        assert _TROUBLESHOOT_PATTERN.search("can't sync to whoop")

    def test_does_not_match_health(self):
        assert not _TROUBLESHOOT_PATTERN.search("why is my iron low")


class TestPauseUnpausePatterns:
    def test_pause_notifications(self):
        assert _PAUSE_OVERDUE_PATTERN.search("pause notifications")

    def test_pause_with_duration(self):
        m = _PAUSE_OVERDUE_PATTERN.search("pause notifications for 2 weeks")
        assert m
        assert m.group(1) == "2 weeks"

    def test_unpause(self):
        assert _UNPAUSE_OVERDUE_PATTERN.search("unpause notifications")

    def test_resume_alerts(self):
        assert _UNPAUSE_OVERDUE_PATTERN.search("resume alerts")


class TestRestartPattern:
    def test_restart(self):
        assert _RESTART_PATTERN.match("restart")

    def test_reboot_bot(self):
        assert _RESTART_PATTERN.match("reboot the bot")

    def test_does_not_match_partial(self):
        assert not _RESTART_PATTERN.match("restart my health profile")
