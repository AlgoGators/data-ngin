import unittest

from src.api.keys import ROLE_PREFIXES, generate_key, hash_key


class TestKeyGeneration(unittest.TestCase):
    def test_prefix_reflects_role(self):
        plaintext, _, _ = generate_key("db_readwrite")
        self.assertTrue(plaintext.startswith("ag_rw_"), plaintext)

    def test_every_role_has_a_prefix(self):
        for role in ("db_readonly", "db_readwrite", "db_readwrite_all"):
            self.assertIn(role, ROLE_PREFIXES)

    def test_unknown_role_is_rejected(self):
        with self.assertRaises(ValueError):
            generate_key("db_superuser")

    def test_keys_are_unique(self):
        keys = {generate_key("db_readonly")[0] for _ in range(200)}
        self.assertEqual(len(keys), 200)

    def test_hash_matches_the_plaintext(self):
        plaintext, key_hash, _ = generate_key("db_readonly")
        self.assertEqual(hash_key(plaintext), key_hash)

    def test_hash_is_sha256_hex(self):
        self.assertEqual(len(hash_key("anything")), 64)
        int(hash_key("anything"), 16)  # raises if not hex

    def test_prefix_is_stored_form_not_the_key(self):
        """The prefix is for display. It must never be long enough to be
        useful to someone who obtains it."""
        plaintext, _, prefix = generate_key("db_readonly")
        self.assertTrue(plaintext.startswith(prefix))
        self.assertLessEqual(len(prefix), 12)
        self.assertLess(len(prefix), len(plaintext))
