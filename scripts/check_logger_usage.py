#!/usr/bin/env python3
"""
Scoped converter that only targets src/ folder and main.py files.
Safely converts print statements and logger calls to ContextAwareLogger.
"""
import ast
import re
import tokenize
from pathlib import Path
from collections import defaultdict
from io import StringIO

class ScopedContextLoggerConverter:
    """Safely converts logging using AST - scoped to src/ and main.py only."""
    
    EVENT_TYPE_MAP = {
        'error': 'ERROR',
        'warning': 'WARNING',
        'debug': 'DEBUG',
        'info': 'INFO',
        'price': 'MARKET_DATA_UPDATE',
        'data': 'MARKET_DATA_UPDATE',
        'order': 'ORDER_SUBMISSION',
        'trade': 'ORDER_EXECUTION',
        'execute': 'ORDER_EXECUTION',
        'market': 'MARKET_CONDITION',
        'position': 'POSITION_UPDATE',
        'risk': 'RISK_CHECK',
        'state': 'STATE_TRANSITION',
        'strategy': 'STRATEGY_SIGNAL',
        'signal': 'STRATEGY_SIGNAL',
        'load': 'SYSTEM_STARTUP',
        'start': 'SYSTEM_STARTUP',
        'stop': 'SYSTEM_SHUTDOWN',
    }

    def __init__(self):
        self.conversions = defaultdict(int)
        self.imports_added = False

    def detect_event_type(self, message: str) -> str:
        """Detect appropriate event type from message content."""
        message_lower = message.lower()
        for keyword, event_type in self.EVENT_TYPE_MAP.items():
            if keyword in message_lower:
                return event_type
        return 'SYSTEM_EVENT'

    def extract_symbol(self, message: str) -> str:
        """Extract trading symbol from message if present."""
        symbol_match = re.search(r'\b[A-Z]{2,4}\b', message)
        return f"'{symbol_match.group(0)}'" if symbol_match else 'None'

    def build_context_provider(self, message: str, event_type: str) -> str:
        """Build context provider based on message content."""
        context_fields = ["'timestamp': lambda: datetime.now().isoformat()"]
        
        # Add numeric values if found
        numbers = re.findall(r'\b\d+\.?\d*\b', message)
        if numbers:
            context_fields.append(f"'numeric_values': {numbers}")
            
        # Add string-based context
        if any(word in message.lower() for word in ['price', 'bid', 'ask', 'last']):
            context_fields.append("'data_type': 'market_data'")
        elif any(word in message.lower() for word in ['order', 'trade', 'fill']):
            context_fields.append("'data_type': 'order_execution'")
            
        return "{" + ", ".join(context_fields) + "}"

    def generate_context_log_call(self, message: str, event_type: str, symbol: str) -> str:
        """Generate the ContextAwareLogger call."""
        context_provider = self.build_context_provider(message, event_type)
        
        return f"""get_context_logger().log_event(
    event_type=TradingEventType.{event_type},
    message={message},
    symbol={symbol},
    context_provider={context_provider},
    decision_reason="auto_converted"
)"""

    def add_imports_safely(self, content: str) -> str:
        """Safely add ContextAwareLogger imports without breaking existing structure."""
        lines = content.split('\n')
        
        # Check if imports already exist
        has_context_import = any('context_aware_logger' in line for line in lines)
        has_datetime_import = any('from datetime import datetime' in line for line in lines)
        
        new_imports = []
        if not has_context_import:
            new_imports.append('from src.core.context_aware_logger import get_context_logger, TradingEventType')
        if not has_datetime_import:
            new_imports.append('from datetime import datetime')
        
        if not new_imports:
            return content
            
        # Find the last import line to insert after
        last_import_index = -1
        in_import_section = True
        
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            if not line_stripped:
                continue
                
            if line_stripped.startswith(('import ', 'from ')):
                last_import_index = i
            elif line_stripped.startswith(('def ', 'class ', '@')):
                in_import_section = False
            elif line_stripped and not line_stripped.startswith('#') and in_import_section:
                last_import_index = i
        
        # Insert new imports
        insert_index = last_import_index + 1 if last_import_index >= 0 else 0
        for import_line in reversed(new_imports):
            lines.insert(insert_index, import_line)
        
        return '\n'.join(lines)

    def convert_print_statements(self, content: str) -> str:
        """Convert print statements using regex with better context detection."""
        # Pattern to match print statements with various content types
        print_pattern = r'print\(((?:[^()]|\([^()]*\))*)\)'
        
        def replace_print(match):
            print_content = match.group(1).strip()
            event_type = self.detect_event_type(print_content)
            symbol = self.extract_symbol(print_content)
            
            new_call = self.generate_context_log_call(print_content, event_type, symbol)
            self.conversions['print_to_context'] += 1
            return new_call
        
        new_content = re.sub(print_pattern, replace_print, content)
        return new_content

    def convert_simple_logger_calls(self, content: str) -> str:
        """Convert simple logger calls to context logger."""
        # Pattern for logger.method("message")
        logger_pattern = r'logger\.(info|debug|warning|error|exception)\(((?:[^()]|\([^()]*\))*)\)'
        
        def replace_logger(match):
            method = match.group(1)
            message_content = match.group(2).strip()
            
            event_type = self.detect_event_type(message_content)
            symbol = self.extract_symbol(message_content)
            
            new_call = self.generate_context_log_call(message_content, event_type, symbol)
            self.conversions['logger_to_context'] += 1
            return new_call
        
        new_content = re.sub(logger_pattern, replace_logger, content)
        return new_content

    def remove_simple_logger_imports(self, content: str) -> str:
        """Remove simple logger imports if no longer needed."""
        lines = content.split('\n')
        new_lines = []
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Skip simple logger imports
            if 'get_simple_logger' in line:
                i += 1
                continue
            elif line.strip() == 'logger = get_simple_logger(__name__)':
                i += 1
                continue
            else:
                new_lines.append(line)
                i += 1
        
        return '\n'.join(new_lines)

    def convert_file(self, file_path: Path) -> dict:
        """Safely convert a file to use ContextAwareLogger."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            self.conversions.clear()
            
            # Step 1: Safely add imports
            content = self.add_imports_safely(content)
            
            # Step 2: Convert print statements
            content = self.convert_print_statements(content)
            
            # Step 3: Convert simple logger calls
            content = self.convert_simple_logger_calls(content)
            
            # Step 4: Only remove simple logger imports if we converted all logger calls
            simple_logger_calls_remaining = len(re.findall(r'logger\.(info|debug|warning|error|exception)\(', content))
            if simple_logger_calls_remaining == 0 and 'get_simple_logger' in content:
                content = self.remove_simple_logger_imports(content)
                self.conversions['removed_simple_logger'] += 1
            
            # Only write if changes were made
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                return dict(self.conversions)
            else:
                return {}
                
        except Exception as e:
            print(f"   ❌ Error processing {file_path}: {e}")
            return {}

def get_target_files(project_root: Path):
    """Get only src/ folder files and main.py files."""
    target_files = []
    
    # Add all Python files in src/ folder
    src_files = list(project_root.rglob("src/**/*.py"))
    target_files.extend(src_files)
    
    # Add main.py files at root level
    root_main_files = [
        project_root / "main.py",
        project_root / "app.py", 
        project_root / "run.py",
        project_root / "start.py"
    ]
    
    for main_file in root_main_files:
        if main_file.exists():
            target_files.append(main_file)
    
    # Remove duplicates and excluded files
    excluded_patterns = ['__pycache__', 'venv', 'context_aware_logger.py']
    filtered_files = []
    
    for file_path in target_files:
        if not any(pattern in str(file_path) for pattern in excluded_patterns):
            filtered_files.append(file_path)
    
    return filtered_files

def main():
    """Safely convert only src/ folder and main.py files to use ContextAwareLogger."""
    project_root = Path(__file__).parent.parent
    target_files = get_target_files(project_root)
    
    total_conversions = defaultdict(int)
    converted_files = []
    
    print("🎯 Scoped conversion to src/ folder and main.py files...")
    print("=" * 60)
    print(f"📁 Target scope:")
    print(f"   └─ src/ folder: All Python files")
    print(f"   └─ Root level: main.py, app.py, run.py, start.py")
    print("=" * 60)
    
    converter = ScopedContextLoggerConverter()
    
    for py_file in target_files:
        try:
            relative_path = py_file.relative_to(project_root)
            print(f"📁 Processing: {relative_path}")
            conversions = converter.convert_file(py_file)
            
            if conversions:
                converted_files.append(str(relative_path))
                for conversion_type, count in conversions.items():
                    total_conversions[conversion_type] += count
                
                print(f"   ✅ Conversions: {dict(conversions)}")
            else:
                print(f"   ⏭️  No conversions needed")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Print summary
    print("\n" + "=" * 60)
    print("🎯 SCOPED CONVERSION SUMMARY")
    print("=" * 60)
    print(f"📁 Files in scope: {len(target_files)}")
    print(f"🔄 Files converted: {len(converted_files)}")
    print(f"📊 Total conversions:")
    for conversion_type, count in total_conversions.items():
        print(f"   └─ {conversion_type}: {count}")
    
    if converted_files:
        print(f"\n📋 Successfully converted files:")
        for file_path in converted_files:
            print(f"   └─ {file_path}")
    else:
        print(f"\n❌ No files were converted - check if files exist in scope")

if __name__ == "__main__":
    main()