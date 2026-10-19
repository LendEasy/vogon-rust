"""The audio oracle must tolerate formatting without erasing critical content."""
import unittest
from infra import normalized, wer

class OracleTests(unittest.TestCase):
    def test_currency_format_preserves_amount_and_unit(self):
        self.assertEqual(normalized('fifty dollars'),normalized('$50'))
        self.assertEqual(wer('You said fifty dollars.','You said $50.'),0.)
        self.assertNotEqual(normalized('fifteen dollars'),normalized('$50'))
        self.assertNotEqual(normalized('fifty euros'),normalized('$50'))
    def test_digit_sequences_are_not_added_or_sorted(self):
        self.assertNotEqual(normalized('one two'),normalized('three'))
        self.assertNotEqual(normalized('one two'),normalized('two one'))
        self.assertEqual(normalized('twenty five'),normalized('25'))
    def test_corrections_and_names_remain_distinct(self):
        self.assertGreater(wer('Friday','Monday'),0.)
        self.assertGreater(wer('John','Joan'),0.)
        self.assertGreater(wer('I can pay','I cannot pay'),0.)

if __name__=='__main__':unittest.main()
