"""
Alliance Mapper - Config-Driven Party to Alliance Mapping

DECOUPLED DESIGN:
- Alliance mappings are stored in config/alliances_YYYY.json
- Update the config file to change mappings without touching code
- Supports both historical (2021) and current (2026) election configs

Usage:
    from src.utils.alliance_mapper import AllianceMapper
    
    # For 2021 baseline
    mapper_2021 = AllianceMapper(year=2021)
    alliance = mapper_2021.get_alliance("DMK")  # Returns "DMK_Alliance"
    
    # For 2026 predictions (default)
    mapper_2026 = AllianceMapper()
    alliance = mapper_2026.get_alliance("TVK")  # Returns "TVK_Front"
"""

import json
from pathlib import Path
from typing import Dict, Optional, List

# Path setup
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


class AllianceMapper:
    """
    Config-driven party to alliance mapper.
    
    Attributes:
        year: Election year (2021 for baseline, 2026 for predictions)
        config: Loaded alliance configuration
        party_map: Flat party -> alliance lookup
    """
    
    def __init__(self, year: int = 2026):
        """
        Initialize mapper for a specific election year.
        
        Args:
            year: Election year (default 2026 for current predictions)
        """
        self.year = year
        self.config_path = CONFIG_DIR / f"alliances_{year}.json"
        self.config: Dict = {}
        self.party_map: Dict[str, str] = {}
        self._load_config()
    
    def _load_config(self):
        """Load alliance configuration from JSON file."""
        if not self.config_path.exists():
            print(f"Warning: Alliance config not found: {self.config_path}")
            print("Using empty config - all parties will map to 'Others'")
            self.config = {"alliances": {}}
            return
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        # Build flat party -> alliance map
        for alliance_key, alliance_data in self.config.get('alliances', {}).items():
            for party in alliance_data.get('parties', []):
                self.party_map[party.upper()] = alliance_key
    
    def get_alliance(self, party: str) -> str:
        """
        Map a party name to its alliance.
        
        Args:
            party: Party name (e.g., "DMK", "INC", "BJP")
            
        Returns:
            Alliance key (e.g., "DMK_Alliance", "ADMK_Alliance")
        """
        party_clean = party.strip().upper()
        
        # Direct lookup
        if party_clean in self.party_map:
            return self.party_map[party_clean]
        
        # Fuzzy matching for common variations
        return self._fuzzy_match(party_clean)
    
    def _fuzzy_match(self, party: str) -> str:
        """Fuzzy match for party name variations not in config."""
        # Congress variations
        if 'CONGRESS' in party:
            if 'INC' in self.party_map:
                return self.party_map['INC']
        
        # Communist variations
        if 'COMMUNIST' in party or party.startswith('CPI'):
            for key in ['CPI', 'CPI(M)', 'CPM']:
                if key in self.party_map:
                    return self.party_map[key]
        
        # AIADMK/ADMK variations
        if 'AIADMK' in party or party == 'ADMK':
            for key in ['ADMK', 'AIADMK']:
                if key in self.party_map:
                    return self.party_map[key]
        
        # Independent
        if party == 'IND' or 'INDEPENDENT' in party:
            return "Independent"
        
        return "Others"
    
    def get_alliance_color(self, alliance: str) -> str:
        """Get display color for an alliance."""
        alliance_data = self.config.get('alliances', {}).get(alliance, {})
        return alliance_data.get('color', '#7F8C8D')  # Default gray
    
    def get_alliance_display_name(self, alliance: str) -> str:
        """Get human-readable display name for an alliance."""
        alliance_data = self.config.get('alliances', {}).get(alliance, {})
        return alliance_data.get('display_name', alliance)
    
    def get_all_alliances(self) -> List[str]:
        """Get list of all alliance keys."""
        return list(self.config.get('alliances', {}).keys())
    
    def get_parties_in_alliance(self, alliance: str) -> List[str]:
        """Get list of parties in an alliance."""
        alliance_data = self.config.get('alliances', {}).get(alliance, {})
        return alliance_data.get('parties', [])
    
    def get_alliance_metadata(self) -> Dict:
        """Get all alliance data for frontend rendering."""
        result = {}
        for alliance_key, alliance_data in self.config.get('alliances', {}).items():
            result[alliance_key] = {
                'display_name': alliance_data.get('display_name', alliance_key),
                'color': alliance_data.get('color', '#7F8C8D'),
                'leader_party': alliance_data.get('leader_party'),
                'party_count': len(alliance_data.get('parties', []))
            }
        return result


# Convenience functions for quick access

def get_alliance_2021(party: str) -> str:
    """Quick lookup for 2021 baseline generation."""
    mapper = AllianceMapper(year=2021)
    return mapper.get_alliance(party)


def get_alliance_2026(party: str) -> str:
    """Quick lookup for 2026 predictions."""
    mapper = AllianceMapper(year=2026)
    return mapper.get_alliance(party)


def get_alliance_colors() -> Dict[str, str]:
    """Get all alliance colors for frontend map rendering."""
    mapper = AllianceMapper(year=2026)
    return {k: v['color'] for k, v in mapper.get_alliance_metadata().items()}


if __name__ == "__main__":
    # Test the mapper
    print("Testing AllianceMapper...")
    print("-" * 50)
    
    # Test 2021
    mapper_2021 = AllianceMapper(year=2021)
    print(f"\n2021 Config loaded: {mapper_2021.config_path}")
    print(f"Alliances: {mapper_2021.get_all_alliances()}")
    
    test_parties = ['DMK', 'INC', 'VCK', 'ADMK', 'PMK', 'BJP', 'NTK', 'TVK', 'IND']
    print(f"\nParty Mappings (2021):")
    for party in test_parties:
        print(f"  {party:10s} -> {mapper_2021.get_alliance(party)}")
    
    # Test 2026
    print(f"\n" + "-" * 50)
    mapper_2026 = AllianceMapper(year=2026)
    print(f"\n2026 Config loaded: {mapper_2026.config_path}")
    print(f"Alliances: {mapper_2026.get_all_alliances()}")
    
    print(f"\nParty Mappings (2026):")
    for party in test_parties:
        print(f"  {party:10s} -> {mapper_2026.get_alliance(party)}")
    
    print(f"\nAlliance Colors (2026):")
    for alliance, color in get_alliance_colors().items():
        print(f"  {alliance:20s} -> {color}")
