import pytest
from pathlib import Path
from kubecuro.healing.pipeline import HealingPipeline

def test_full_chaos_healing():
    """
    TORTURE TEST: Ensures all 23 edge cases are healed 
    enough for a standard YAML parser to load the result.
    """
    # 1. Setup
    chaos_path = Path(__file__).parent / "chaos_manifest.yaml"
    pipeline = HealingPipeline()
    
    # 2. Execute Healing
    result = pipeline.heal_manifest(file_path=chaos_path)
    
    # 3. Assertions
    assert result["success"] is True, f"Healing failed with status: {result['status']}"
    assert result["phase1_complete"] is True
    
    # Verify report accuracy
    report = result["report"]
    assert report["lines_changed"] > 0, "Healer claimed no changes were needed on a broken file!"
    
    # 4. Final Validation: Can it be parsed now?
    from ruamel.yaml import YAML
    yaml = YAML()
    try:
        # If this doesn't raise an exception, Phase 1 is a success
        yaml.load(result["content"])
    except Exception as e:
        pytest.fail(f"Healed YAML is still unparseable: {e}")

if __name__ == "__main__":
    # Allow running directly for quick verification
    pytest.main([__file__])
