# fix_remaining_imports.py

from pathlib import Path

def fix_common_import_issues():
    """Fix common import issues that might remain."""
    
    # Files that commonly have import issues
    files_to_check = [
        "src/scanning/scan_manager.py",
        "src/scanner/simple_scanner.py", 
        "src/scanner/tiered_scanner.py",
        "src/scanner/scanner_main.py",
        "src/scanning/scanner_core.py",
    ]
    
    import_fixes = {
        # Fix relative imports that might be broken
        "from .integration.": "from src.scanning.integration.",
        "from integration.": "from src.scanning.integration.",
        
        # Fix scanner core imports
        "from scanner_core": "from src.scanning.scanner_core",
        "from .scanner_core": "from src.scanning.scanner_core",
        
        # Fix criteria imports
        "from criteria.": "from src.scanning.criteria.",
        "from .criteria.": "from src.scanning.criteria.",
        
        # Fix strategy imports
        "from strategy.": "from src.scanning.strategy.", 
        "from .strategy.": "from src.scanning.strategy.",
    }
    
    for file_path in files_to_check:
        path_obj = Path(file_path)
        if not path_obj.exists():
            continue
            
        try:
            with open(path_obj, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            for old_import, new_import in import_fixes.items():
                if old_import in content:
                    content = content.replace(old_import, new_import)
            
            if content != original_content:
                with open(path_obj, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"✅ Fixed imports in {file_path}")
                
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")

if __name__ == "__main__":
    print("🔄 Fixing remaining import issues...")
    fix_common_import_issues()
    print("🎉 Import fixes completed!")