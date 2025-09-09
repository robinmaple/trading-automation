import sys
sys.path.insert(0, '.')
from src.core.trading_manager import TradingManager
import inspect

def analyze_method_sizes():
    lines = inspect.getsourcelines(TradingManager)[0]
    methods = []
    current_method = None
    current_count = 0
    
    for line in lines:
        if line.strip().startswith('def '):
            if current_method:
                methods.append((current_method, current_count))
            current_method = line.strip().split('def ')[1].split('(')[0]
            current_count = 1
        else:
            current_count += 1
    
    if current_method:
        methods.append((current_method, current_count))
    
    print('Top 15 longest methods in TradingManager:')
    for method, count in sorted(methods, key=lambda x: x[1], reverse=True)[:15]:
        print(f'{method}: {count} lines')
    
    total_lines = len(lines)
    total_methods = len(methods)
    avg_method_size = total_lines / total_methods if total_methods > 0 else 0
    
    print(f'\nTotal lines: {total_lines}')
    print(f'Total methods: {total_methods}')
    print(f'Average method size: {avg_method_size:.1f} lines')

if __name__ == '__main__':
    analyze_method_sizes()