#!/usr/bin/env python3
"""
Training Data Export Utility for Phase B ML development.
Exports labeled order data to CSV for model training and analysis.
"""

import argparse
import sys
from pathlib import Path

# Add the src directory to Python path for imports
src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))

from src.core.database import get_db_session
from src.services.outcome_labeling_service import OutcomeLabelingService


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Export ML training data from trading system')
    
    parser.add_argument('--output', '-o', default='training_data.csv',
                       help='Output CSV file path (default: training_data.csv)')
    
    parser.add_argument('--hours', '-t', type=int, default=168,
                       help='Hours of data to include (default: 168 = 1 week)')
    
    parser.add_argument('--label-types', '-l', nargs='+', 
                       default=['filled_binary', 'time_to_fill', 'slippage', 'profitability'],
                       help='Label types to include (space-separated)')
    
    parser.add_argument('--all-labels', '-a', action='store_true',
                       help='Include all available label types')
    
    parser.add_argument('--list-types', action='store_true',
                       help='List available label types and exit')
    
    return parser.parse_args()


def list_label_types():
    """List all available label types."""
    label_types = [
        'filled_binary     - Binary indicator if order was filled (1.0) or not (0.0)',
        'time_to_fill      - Time in seconds from order creation to execution',
        'slippage          - Price difference between planned and executed price',
        'profitability     - Binary indicator if trade was profitable (1.0) or not (0.0)',
        'probability_accuracy - Binary indicator if high probability prediction was accurate'
    ]
    
    print("Available label types:")
    for label_type in label_types:
        print(f"  {label_type}")


def main():
    """Main function to export training data."""
    args = parse_arguments()
    
    if args.list_types:
        list_label_types()
        return
    
    print("📊 Training Data Export Utility")
    print("=" * 50)
    
    # Initialize database session
    try:
        db_session = get_db_session()
        print(f"✅ Connected to database")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return 1
    
    # Initialize labeling service
    labeling_service = OutcomeLabelingService(db_session)
    
    # Determine label types to export
    if args.all_labels:
        label_types = ['filled_binary', 'time_to_fill', 'slippage', 'profitability', 'probability_accuracy']
    else:
        label_types = args.label_types
    
    print(f"📈 Exporting {args.hours} hours of data")
    print(f"🏷️  Label types: {', '.join(label_types)}")
    print(f"💾 Output file: {args.output}")
    print("-" * 50)
    
    # First, ensure we have recent labels
    print("🔍 Labeling recent orders...")
    labeling_summary = labeling_service.label_completed_orders(hours_back=args.hours)
    
    print(f"   Orders processed: {labeling_summary['total_orders']}")
    print(f"   Orders labeled: {labeling_summary['labeled_orders']}")
    print(f"   Labels created: {labeling_summary['labels_created']}")
    
    if labeling_summary['errors'] > 0:
        print(f"   ⚠️  Errors: {labeling_summary['errors']}")
    
    # Export the training data
    print("📤 Exporting training data...")
    success = labeling_service.export_training_data(args.output, label_types)
    
    if success:
        # Count the number of data points exported
        try:
            import csv
            with open(args.output, 'r') as f:
                reader = csv.reader(f)
                data_points = sum(1 for row in reader) - 1  # Subtract header
            
            print(f"✅ Successfully exported {data_points} data points to {args.output}")
            print("\n📋 File structure:")
            print("   - Each row represents one labeled order")
            print("   - Columns include: label_type, label_value, features, and metadata")
            print("   - Features include: market data, order context, and timing information")
            
        except Exception as e:
            print(f"✅ Data exported to {args.output} (could not count rows: {e})")
    else:
        print("❌ Failed to export training data")
        return 1
    
    # Show sample statistics if we have data
    if labeling_summary['labeled_orders'] > 0:
        print("\n📈 Sample Statistics:")
        
        for label_type in label_types:
            try:
                data = labeling_service.get_labeled_data(label_type, hours_back=args.hours)
                if data:
                    values = [item['label_value'] for item in data]
                    print(f"   {label_type}: {len(values)} samples")
                    
                    if values and isinstance(values[0], (int, float)):
                        avg = sum(values) / len(values)
                        print(f"     Average: {avg:.3f}")
                        
                        if label_type == 'filled_binary':
                            fill_rate = avg * 100
                            print(f"     Fill Rate: {fill_rate:.1f}%")
                        
            except Exception as e:
                print(f"   {label_type}: Error calculating stats - {e}")
    
    db_session.close()
    print("\n🎉 Export completed successfully!")
    return 0


if __name__ == '__main__':
    sys.exit(main())