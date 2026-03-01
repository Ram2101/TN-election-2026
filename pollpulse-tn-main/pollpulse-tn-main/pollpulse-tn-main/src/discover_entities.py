"""
Entity Discovery - Wikidata SPARQL Query for TN Politicians

Fetches Tamil Nadu MLAs, MPs, and key political figures from Wikidata
to create an entity-to-constituency mapping for sentiment routing.

Usage:
    python src/discover_entities.py

Output:
    config/entity_map.json - Politician name -> Constituency mapping
"""

import json
import requests
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

# Path setup
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
OUTPUT_PATH = PROJECT_ROOT / "config" / "entity_map.json"
BASELINE_PATH = PROJECT_ROOT / "data" / "2021_baseline.json"

# Wikidata SPARQL endpoint
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

# Request headers (Wikidata requires User-Agent)
HEADERS = {
    "User-Agent": "PollPulseTN/1.0 (Election Prediction Research Project)",
    "Accept": "application/sparql-results+json"
}


def query_wikidata(sparql: str) -> Optional[Dict]:
    """
    Execute SPARQL query against Wikidata.
    
    Args:
        sparql: SPARQL query string
        
    Returns:
        JSON response or None on error
    """
    try:
        response = requests.get(
            WIKIDATA_ENDPOINT,
            params={"query": sparql, "format": "json"},
            headers=HEADERS,
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Wikidata query error: {e}")
        return None


def fetch_tn_mlas() -> List[Dict]:
    """
    Fetch current Tamil Nadu MLAs from Wikidata.
    
    Returns:
        List of {name, constituency, party, wikidata_id}
    """
    # SPARQL query for Tamil Nadu MLAs (16th Legislative Assembly - 2021-present)
    sparql = """
    SELECT DISTINCT ?person ?personLabel ?constituencyLabel ?partyLabel WHERE {
      # Person is a member of Tamil Nadu Legislative Assembly
      ?person wdt:P39 ?position .
      ?position wdt:P279* wd:Q18227398 .  # Member of Tamil Nadu Legislative Assembly
      
      # Get their constituency (electoral district)
      OPTIONAL { ?person wdt:P768 ?constituency . }
      
      # Get their party
      OPTIONAL { ?person wdt:P102 ?party . }
      
      # Filter for recent/current members
      OPTIONAL { ?person wdt:P39 ?pos . ?pos pq:P580 ?start . }
      
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en,ta". }
    }
    ORDER BY ?personLabel
    LIMIT 500
    """
    
    print("Querying Wikidata for TN MLAs...")
    result = query_wikidata(sparql)
    
    if not result:
        return []
    
    mlas = []
    for binding in result.get('results', {}).get('bindings', []):
        mla = {
            'name': binding.get('personLabel', {}).get('value', ''),
            'constituency': binding.get('constituencyLabel', {}).get('value', ''),
            'party': binding.get('partyLabel', {}).get('value', ''),
            'wikidata_id': binding.get('person', {}).get('value', '').split('/')[-1]
        }
        if mla['name']:
            mlas.append(mla)
    
    print(f"Found {len(mlas)} MLAs from Wikidata")
    return mlas


def fetch_key_politicians() -> List[Dict]:
    """
    Fetch key TN politicians (Chief Ministers, Party Leaders, MPs).
    
    Returns:
        List of {name, role, party, constituency, wikidata_id}
    """
    # SPARQL query for key politicians
    sparql = """
    SELECT DISTINCT ?person ?personLabel ?roleLabel ?partyLabel ?constituencyLabel WHERE {
      {
        # Chief Ministers of Tamil Nadu
        ?person wdt:P39 wd:Q19010626 .
        BIND("Chief Minister" AS ?role)
      } UNION {
        # MPs from Tamil Nadu
        ?person wdt:P39 ?mpPosition .
        ?mpPosition wdt:P279* wd:Q486839 .  # Member of Lok Sabha
        ?person wdt:P768 ?constituency .
        ?constituency wdt:P131* wd:Q1445 .  # Tamil Nadu
        BIND("Member of Parliament" AS ?role)
      } UNION {
        # Major party leaders in TN politics
        VALUES ?partyLeader {
          wd:Q333729    # M. K. Stalin
          wd:Q7121053   # Edappadi K. Palaniswami
          wd:Q1125962   # O. Panneerselvam
          wd:Q3497470   # Vijay (actor/politician)
          wd:Q7457691   # Seeman
          wd:Q333727    # K. Annamalai
          wd:Q6167697   # Thirumavalavan
          wd:Q561574    # Vaiko
          wd:Q333728    # Kanimozhi
          wd:Q7181447   # Udhayanidhi Stalin
        }
        BIND(?partyLeader AS ?person)
        BIND("Party Leader" AS ?role)
      }
      
      OPTIONAL { ?person wdt:P102 ?party . }
      OPTIONAL { ?person wdt:P768 ?constituency . }
      
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en,ta". }
    }
    ORDER BY ?personLabel
    """
    
    print("Querying Wikidata for key politicians...")
    result = query_wikidata(sparql)
    
    if not result:
        return []
    
    politicians = []
    for binding in result.get('results', {}).get('bindings', []):
        politician = {
            'name': binding.get('personLabel', {}).get('value', ''),
            'role': binding.get('roleLabel', {}).get('value', ''),
            'party': binding.get('partyLabel', {}).get('value', ''),
            'constituency': binding.get('constituencyLabel', {}).get('value', ''),
            'wikidata_id': binding.get('person', {}).get('value', '').split('/')[-1]
        }
        if politician['name']:
            politicians.append(politician)
    
    print(f"Found {len(politicians)} key politicians from Wikidata")
    return politicians


def load_baseline_winners() -> Dict[str, Dict]:
    """
    Load 2021 election winners from baseline to supplement entity map.
    
    Returns:
        Dict mapping constituency -> winner data
    """
    if not BASELINE_PATH.exists():
        print("Warning: Baseline data not found. Run generate_2021_baseline.py first.")
        return {}
    
    with open(BASELINE_PATH, 'r', encoding='utf-8') as f:
        baseline = json.load(f)
    
    return baseline.get('constituencies', {})


def create_hardcoded_entities() -> Dict[str, Dict]:
    """
    Hardcoded key politicians for reliability when Wikidata fails.
    
    This is a fallback with the most important political figures.
    """
    return {
        # DMK Leadership
        "M. K. Stalin": {
            "aliases": ["MK Stalin", "Stalin", "Muthuvel Karunanidhi Stalin"],
            "constituency": "KOLATHUR",
            "party": "DMK",
            "role": "Chief Minister"
        },
        "Udhayanidhi Stalin": {
            "aliases": ["Udhayanidhi", "Udhay"],
            "constituency": "CHEPAUK-THIRUVALLIKENI",
            "party": "DMK",
            "role": "Deputy Chief Minister"
        },
        "Kanimozhi": {
            "aliases": ["Kanimozhi Karunanidhi"],
            "constituency": None,  # Rajya Sabha
            "party": "DMK",
            "role": "Party Leader"
        },
        
        # ADMK Leadership
        "Edappadi K. Palaniswami": {
            "aliases": ["EPS", "Edappadi", "Palaniswami"],
            "constituency": "EDAPPADI",
            "party": "ADMK",
            "role": "Opposition Leader"
        },
        "O. Panneerselvam": {
            "aliases": ["OPS", "Panneerselvam"],
            "constituency": "BODINAYAKANUR",
            "party": "ADMK",
            "role": "Party Leader"
        },
        
        # TVK
        "Vijay": {
            "aliases": ["Thalapathy Vijay", "Thalapathy", "Actor Vijay", "Joseph Vijay"],
            "constituency": None,  # Not yet contested
            "party": "TVK",
            "role": "Party Founder"
        },
        
        # NTK
        "Seeman": {
            "aliases": ["Senthamizhan Seeman"],
            "constituency": None,
            "party": "NTK",
            "role": "Party Leader"
        },
        
        # BJP
        "K. Annamalai": {
            "aliases": ["Annamalai", "Annamalai IPS"],
            "constituency": None,
            "party": "BJP",
            "role": "State President"
        },
        
        # VCK
        "Thirumavalavan": {
            "aliases": ["Thol. Thirumavalavan", "Thiruma"],
            "constituency": "CHIDAMBARAM",
            "party": "VCK",
            "role": "Party Leader"
        },
        
        # MDMK
        "Vaiko": {
            "aliases": ["V. Gopalasamy", "Vaiyapuri Gopalsamy"],
            "constituency": None,
            "party": "MDMK",
            "role": "Party Leader"
        },
        
        # PMK
        "Anbumani Ramadoss": {
            "aliases": ["Anbumani", "Dr. Anbumani"],
            "constituency": None,  # Rajya Sabha
            "party": "PMK",
            "role": "Party Leader"
        },
        "S. Ramadoss": {
            "aliases": ["Ramadoss", "Dr. Ramadoss"],
            "constituency": None,
            "party": "PMK",
            "role": "Party Founder"
        }
    }


def build_entity_map(
    mlas: List[Dict],
    politicians: List[Dict],
    baseline: Dict[str, Dict]
) -> Dict:
    """
    Build the final entity map combining all sources.
    
    Structure:
    {
        "politicians": {
            "normalized_name": {
                "canonical_name": "...",
                "aliases": [...],
                "constituency": "...",
                "party": "...",
                "role": "...",
                "source": "wikidata|baseline|hardcoded"
            }
        },
        "alias_index": {
            "lowercase_alias": "normalized_name"
        },
        "constituency_politicians": {
            "CONSTITUENCY_NAME": ["politician1", "politician2"]
        }
    }
    """
    entity_map = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "sources": ["wikidata", "baseline_2021", "hardcoded"],
            "version": "1.0"
        },
        "politicians": {},
        "alias_index": {},
        "constituency_politicians": {}
    }
    
    # Start with hardcoded (most reliable)
    hardcoded = create_hardcoded_entities()
    for name, data in hardcoded.items():
        normalized = name.upper().replace(" ", "_")
        entity_map["politicians"][normalized] = {
            "canonical_name": name,
            "aliases": data.get("aliases", []),
            "constituency": data.get("constituency"),
            "party": data.get("party"),
            "role": data.get("role"),
            "source": "hardcoded"
        }
        
        # Build alias index
        entity_map["alias_index"][name.lower()] = normalized
        for alias in data.get("aliases", []):
            entity_map["alias_index"][alias.lower()] = normalized
    
    # Add Wikidata MLAs
    for mla in mlas:
        name = mla.get('name', '')
        if not name or name.startswith('Q'):  # Skip if just QID
            continue
        
        normalized = name.upper().replace(" ", "_").replace(".", "")
        
        # Don't overwrite hardcoded entries
        if normalized in entity_map["politicians"]:
            continue
        
        constituency = mla.get('constituency', '')
        # Normalize constituency name
        if constituency:
            constituency = constituency.upper().replace(" ASSEMBLY CONSTITUENCY", "").strip()
        
        entity_map["politicians"][normalized] = {
            "canonical_name": name,
            "aliases": [],
            "constituency": constituency if constituency else None,
            "party": mla.get('party', ''),
            "role": "MLA",
            "source": "wikidata"
        }
        
        entity_map["alias_index"][name.lower()] = normalized
    
    # Add Wikidata key politicians
    for pol in politicians:
        name = pol.get('name', '')
        if not name or name.startswith('Q'):
            continue
        
        normalized = name.upper().replace(" ", "_").replace(".", "")
        
        if normalized in entity_map["politicians"]:
            # Update role if more specific
            if pol.get('role') and pol['role'] != 'MLA':
                entity_map["politicians"][normalized]["role"] = pol['role']
            continue
        
        constituency = pol.get('constituency', '')
        if constituency:
            constituency = constituency.upper().replace(" ASSEMBLY CONSTITUENCY", "").strip()
        
        entity_map["politicians"][normalized] = {
            "canonical_name": name,
            "aliases": [],
            "constituency": constituency if constituency else None,
            "party": pol.get('party', ''),
            "role": pol.get('role', 'Politician'),
            "source": "wikidata"
        }
        
        entity_map["alias_index"][name.lower()] = normalized
    
    # Build constituency -> politicians reverse index
    for norm_name, data in entity_map["politicians"].items():
        constituency = data.get("constituency")
        if constituency:
            if constituency not in entity_map["constituency_politicians"]:
                entity_map["constituency_politicians"][constituency] = []
            entity_map["constituency_politicians"][constituency].append(data["canonical_name"])
    
    return entity_map


def generate_entity_map() -> Dict:
    """
    Main function to generate the entity map.
    
    Returns:
        Complete entity map dictionary
    """
    print("=" * 60)
    print("ENTITY DISCOVERY - Fetching TN Politicians")
    print("=" * 60)
    
    # Fetch from Wikidata (with fallback)
    mlas = []
    politicians = []
    
    try:
        mlas = fetch_tn_mlas()
        time.sleep(1)  # Rate limit
        politicians = fetch_key_politicians()
    except Exception as e:
        print(f"Wikidata fetch failed: {e}")
        print("Using hardcoded entities only.")
    
    # Load baseline winners
    baseline = load_baseline_winners()
    print(f"Loaded {len(baseline)} constituencies from baseline")
    
    # Build entity map
    entity_map = build_entity_map(mlas, politicians, baseline)
    
    print(f"\nEntity Map Summary:")
    print(f"  Politicians: {len(entity_map['politicians'])}")
    print(f"  Aliases indexed: {len(entity_map['alias_index'])}")
    print(f"  Constituencies with politicians: {len(entity_map['constituency_politicians'])}")
    
    return entity_map


def main():
    """Main entry point."""
    entity_map = generate_entity_map()
    
    # Save to JSON
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(entity_map, f, indent=2, ensure_ascii=False)
    
    print(f"\nEntity map saved to: {OUTPUT_PATH}")
    
    # Print sample entries
    print("\nSample Entries:")
    print("-" * 40)
    for i, (name, data) in enumerate(list(entity_map['politicians'].items())[:5]):
        print(f"  {data['canonical_name']}")
        print(f"    Constituency: {data.get('constituency', 'N/A')}")
        print(f"    Party: {data.get('party', 'N/A')}")
        print(f"    Aliases: {data.get('aliases', [])}")
        print()
    
    return entity_map


if __name__ == "__main__":
    main()
