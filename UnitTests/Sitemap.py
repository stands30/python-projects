import unittest
import json

class TestPostalCodeSlugs(unittest.TestCase):
    
    def setUp(self):
        # Load the JSON file content
        file_path = 'Documents/State_Index_2.json'
        with open(file_path, 'r', encoding='utf-8') as file:
            self.data = json.load(file)
    
    def test_slug_and_postal_code_length(self):
        """Test to ensure that all Slug values are at least 4 characters long and PostalCode is exactly 5 characters long."""
        for state in self.data:
            if 'PostalCode' in state:
                for postal in state['PostalCode']:
                    slug = postal.get('Slug', '')
                    postal_code = postal.get('PostalCode', '')
                    
                    # Check that the slug length is at least 4 characters
                    self.assertGreaterEqual(len(slug), 4, f"Slug '{slug}' for PostalCode '{postal_code}' is less than 4 characters")
                    
                    # Check that the postal code length is exactly 5 characters
                    self.assertEqual(len(postal_code), 5, f"PostalCode '{postal_code}' is not 5 characters long")

# To run the test case
if __name__ == '__main__':
    unittest.main()
