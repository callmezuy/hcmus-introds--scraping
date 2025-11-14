"""
Script to check and validate scraped data output
"""
import os
import json
import sys

def check_paper_structure(paper_dir, paper_id):
    """
    Check if a paper directory has the required structure
    
    Args:
        paper_dir: Path to paper directory
        paper_id: Paper ID for logging
        
    Returns:
        dict: Validation results
    """
    issues = []
    warnings = []
    
    # Check if directory exists
    if not os.path.exists(paper_dir):
        return {
            'valid': False,
            'issues': [f"Directory does not exist: {paper_dir}"],
            'warnings': []
        }
    
    # Check tex/ directory
    tex_dir = os.path.join(paper_dir, 'tex')
    if not os.path.exists(tex_dir):
        issues.append("Missing 'tex/' directory")
    else:
        # Check for version subdirectories (v1/, v2/, etc.)
        version_dirs = [d for d in os.listdir(tex_dir) 
                       if os.path.isdir(os.path.join(tex_dir, d)) and d.startswith('v')]
        
        if version_dirs:
            # Multiple versions in subdirectories
            versions = sorted([d[1:] for d in version_dirs if d[1:].isdigit()])
            if versions:
                print(f"  ✓ Found {len(versions)} version(s): v{', v'.join(versions)}")
                
            # Check each version has .tex files
            for v_dir in version_dirs:
                v_path = os.path.join(tex_dir, v_dir)
                tex_files = [f for f in os.listdir(v_path) if f.endswith('.tex')]
                if not tex_files:
                    warnings.append(f"No .tex files in '{v_dir}/' subdirectory")
        else:
            # Single version (files directly in tex/)
            tex_files = [f for f in os.listdir(tex_dir) if f.endswith('.tex')]
            if not tex_files:
                issues.append("No .tex files found in 'tex/' directory")
            else:
                print(f"  ✓ Found 1 version (files in tex/)")
    
    # Check metadata.json
    metadata_file = os.path.join(paper_dir, 'metadata.json')
    if not os.path.exists(metadata_file):
        issues.append("Missing 'metadata.json'")
    else:
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
                
            # Check required fields
            required_fields = ['title', 'authors', 'submission_date', 'revised_dates']
            for field in required_fields:
                if field not in metadata:
                    issues.append(f"Missing required field in metadata: {field}")
                elif not metadata[field]:
                    warnings.append(f"Empty field in metadata: {field}")
            
            # Check for versions info
            if 'versions' in metadata:
                if metadata['versions']:
                    print(f"  ✓ Metadata contains {len(metadata['versions'])} version(s)")
                else:
                    warnings.append("Versions field is empty in metadata")
            
            # Check revised_dates
            if 'revised_dates' in metadata and len(metadata['revised_dates']) > 1:
                print(f"  ✓ Has {len(metadata['revised_dates'])} revision(s)")
                
        except json.JSONDecodeError:
            issues.append("Invalid JSON in 'metadata.json'")
        except Exception as e:
            issues.append(f"Error reading metadata.json: {e}")
    
    # Check references.bib
    bib_file = os.path.join(paper_dir, 'references.bib')
    if not os.path.exists(bib_file):
        warnings.append("Missing 'references.bib' (optional but recommended)")
    else:
        size = os.path.getsize(bib_file)
        if size == 0:
            warnings.append("Empty 'references.bib' file")
    
    # Check references.json
    refs_file = os.path.join(paper_dir, 'references.json')
    if not os.path.exists(refs_file):
        warnings.append("Missing 'references.json' (optional)")
    else:
        try:
            with open(refs_file, 'r', encoding='utf-8') as f:
                refs = json.load(f)
            if refs:
                print(f"  ✓ Has {len(refs)} cited papers with arXiv IDs")
        except:
            warnings.append("Invalid JSON in 'references.json'")
    
    return {
        'valid': len(issues) == 0,
        'issues': issues,
        'warnings': warnings
    }

def check_student_data(student_id, data_dir="../data"):
    """
    Check all papers for a student
    
    Args:
        student_id: Student ID
        data_dir: Base data directory
    """
    # Convert to absolute path relative to script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(script_dir, data_dir))
    student_dir = os.path.join(data_dir, str(student_id))
    
    print("=" * 80)
    print(f"Checking scraped data for Student ID: {student_id}")
    print("=" * 80)
    
    if not os.path.exists(student_dir):
        print(f"❌ Student directory not found: {student_dir}")
        return
    
    # Get all paper directories
    paper_dirs = [d for d in os.listdir(student_dir) 
                  if os.path.isdir(os.path.join(student_dir, d)) 
                  and d.startswith('2402-')]
    
    if not paper_dirs:
        print(f"❌ No paper directories found in {student_dir}")
        return
    
    print(f"📊 Found {len(paper_dirs)} paper directories\n")
    
    # Statistics
    valid_papers = 0
    papers_with_issues = 0
    papers_with_warnings = 0
    total_issues = []
    total_warnings = []
    
    # Check each paper
    for i, paper_folder in enumerate(sorted(paper_dirs), 1):
        paper_dir = os.path.join(student_dir, paper_folder)
        paper_id = paper_folder.replace('-', '.')
        
        # Only show first 10, last 10, and every 100th
        show_details = (i <= 10 or i >= len(paper_dirs) - 10 or i % 100 == 0)
        
        if show_details:
            print(f"[{i}/{len(paper_dirs)}] Checking {paper_folder}...")
        
        result = check_paper_structure(paper_dir, paper_id)
        
        if result['valid']:
            valid_papers += 1
            if show_details:
                print(f"  ✅ Valid\n")
        else:
            papers_with_issues += 1
            if show_details:
                print(f"  ❌ Issues found:")
                for issue in result['issues']:
                    print(f"    - {issue}")
                print()
            total_issues.extend(result['issues'])
        
        if result['warnings']:
            papers_with_warnings += 1
            if show_details:
                print(f"  ⚠️  Warnings:")
                for warning in result['warnings']:
                    print(f"    - {warning}")
                print()
            total_warnings.extend(result['warnings'])
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total papers checked:      {len(paper_dirs)}")
    print(f"✅ Valid papers:           {valid_papers} ({valid_papers/len(paper_dirs)*100:.1f}%)")
    print(f"❌ Papers with issues:     {papers_with_issues} ({papers_with_issues/len(paper_dirs)*100:.1f}%)")
    print(f"⚠️  Papers with warnings:   {papers_with_warnings} ({papers_with_warnings/len(paper_dirs)*100:.1f}%)")
    
    if total_issues:
        print(f"\n📋 Most common issues:")
        from collections import Counter
        issue_counts = Counter(total_issues)
        for issue, count in issue_counts.most_common(5):
            print(f"  - {issue}: {count} occurrences")
    
    if total_warnings:
        print(f"\n⚠️  Most common warnings:")
        from collections import Counter
        warning_counts = Counter(total_warnings)
        for warning, count in warning_counts.most_common(5):
            print(f"  - {warning}: {count} occurrences")
    
    # Check performance report
    perf_report = os.path.join(student_dir, 'performance_report.json')
    if os.path.exists(perf_report):
        print("\n" + "=" * 80)
        print("PERFORMANCE STATISTICS")
        print("=" * 80)
        try:
            with open(perf_report, 'r') as f:
                perf = json.load(f)
            
            print(f"Total time:                {perf.get('total_time_minutes', 0):.2f} minutes")
            print(f"Peak memory usage:         {perf.get('peak_memory_mb', 0):.2f} MB")
            
            stats = perf.get('statistics', {})
            print(f"Successful downloads:      {stats.get('successful_downloads', 0)}")
            print(f"Failed downloads:          {stats.get('failed_downloads', 0)}")
            print(f"Total size before:         {stats.get('total_size_bytes', 0) / (1024**3):.2f} GB")
            print(f"Total size after cleanup:  {stats.get('total_size_after_cleanup', 0) / (1024**3):.2f} GB")
            
            if stats.get('total_size_bytes', 0) > 0:
                saved_pct = (1 - stats.get('total_size_after_cleanup', 0) / stats.get('total_size_bytes', 1)) * 100
                print(f"Space saved by cleanup:    {saved_pct:.1f}%")
            
        except Exception as e:
            print(f"Error reading performance report: {e}")
    
    print("\n" + "=" * 80)
    
    if papers_with_issues > 0:
        print("⚠️  Some papers have issues. Review the output above for details.")
        return 1
    else:
        print("✅ All papers passed validation!")
        return 0

def check_specific_paper(student_id, paper_id, data_dir="../data"):
    """
    Check a specific paper in detail
    
    Args:
        student_id: Student ID
        paper_id: Paper ID (e.g., "2402.10011" or "2402-10011")
        data_dir: Base data directory
    """
    # Normalize paper_id to folder format
    paper_folder = paper_id.replace('.', '-')
    paper_dir = os.path.join(data_dir, str(student_id), paper_folder)
    
    print("=" * 80)
    print(f"Detailed check for paper: {paper_id}")
    print("=" * 80)
    
    if not os.path.exists(paper_dir):
        print(f"❌ Paper directory not found: {paper_dir}")
        return
    
    # Directory structure
    print("\n📁 Directory Structure:")
    for root, dirs, files in os.walk(paper_dir):
        level = root.replace(paper_dir, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files:
            file_path = os.path.join(root, file)
            size = os.path.getsize(file_path)
            size_str = f"{size:,} bytes"
            if size > 1024*1024:
                size_str = f"{size/(1024*1024):.2f} MB"
            elif size > 1024:
                size_str = f"{size/1024:.2f} KB"
            print(f"{subindent}{file} ({size_str})")
    
    # Metadata details
    print("\n📄 Metadata Details:")
    metadata_file = os.path.join(paper_dir, 'metadata.json')
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        print(f"  Title: {metadata.get('title', 'N/A')}")
        print(f"  Authors: {', '.join(metadata.get('authors', []))}")
        print(f"  Submission date: {metadata.get('submission_date', 'N/A')}")
        print(f"  Revised dates: {len(metadata.get('revised_dates', []))} revision(s)")
        if metadata.get('revised_dates'):
            for i, date in enumerate(metadata['revised_dates'], 1):
                print(f"    v{i}: {date}")
        print(f"  Categories: {', '.join(metadata.get('categories', []))}")
        print(f"  Versions: {len(metadata.get('versions', []))} version(s)")
        if metadata.get('versions'):
            for v in metadata['versions']:
                print(f"    v{v.get('version', '?')}: {v.get('created', 'N/A')}")
    else:
        print("  ❌ metadata.json not found")
    
    # References details
    print("\n📚 References Details:")
    refs_file = os.path.join(paper_dir, 'references.json')
    if os.path.exists(refs_file):
        with open(refs_file, 'r', encoding='utf-8') as f:
            refs = json.load(f)
        
        print(f"  Total cited papers with arXiv IDs: {len(refs)}")
        if refs:
            print(f"  Sample cited papers:")
            for i, (ref_id, ref_meta) in enumerate(list(refs.items())[:5], 1):
                title = ref_meta.get('title', 'N/A')
                if len(title) > 60:
                    title = title[:57] + "..."
                print(f"    {i}. [{ref_id}] {title}")
            if len(refs) > 5:
                print(f"    ... and {len(refs) - 5} more")
    else:
        print("  ⚠️  references.json not found")
    
    # Validation
    print("\n✅ Validation:")
    result = check_paper_structure(paper_dir, paper_id)
    if result['valid']:
        print("  ✅ Paper structure is VALID")
    else:
        print("  ❌ Paper has ISSUES:")
        for issue in result['issues']:
            print(f"    - {issue}")
    
    if result['warnings']:
        print("  ⚠️  Warnings:")
        for warning in result['warnings']:
            print(f"    - {warning}")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python check_output.py <student_id>")
        print("  python check_output.py <student_id> <paper_id>")
        print("\nExamples:")
        print("  python check_output.py 23127040")
        print("  python check_output.py 23127040 2402.10011")
        sys.exit(1)
    
    student_id = sys.argv[1]
    
    if len(sys.argv) >= 3:
        # Check specific paper
        paper_id = sys.argv[2]
        check_specific_paper(student_id, paper_id)
    else:
        # Check all papers
        exit_code = check_student_data(student_id)
        sys.exit(exit_code)
