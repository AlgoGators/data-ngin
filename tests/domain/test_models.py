import unittest
from src.domain.models import RollEvent, BackAdjustment


class TestBackAdjustment(unittest.TestCase):
    def test_cumulative_adjustment_only_counts_future_rolls(self) -> None:
        back_adjustment = BackAdjustment(roll_events=[
            RollEvent(index=2, prior_symbol="ES1", new_symbol="ES2", adjustment=1.5),
            RollEvent(index=5, prior_symbol="ES2", new_symbol="ES3", adjustment=-0.5),
        ])
        # Before both rolls: both apply
        self.assertAlmostEqual(back_adjustment.cumulative_adjustment_at(0), 1.0)
        # Between the two rolls: only the second applies
        self.assertAlmostEqual(back_adjustment.cumulative_adjustment_at(3), -0.5)
        # After both rolls: neither applies
        self.assertAlmostEqual(back_adjustment.cumulative_adjustment_at(6), 0.0)


if __name__ == "__main__":
    unittest.main()
