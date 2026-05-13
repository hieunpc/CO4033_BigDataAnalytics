#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
clear_data.py - Dọn dẹp dữ liệu và giải phóng dung lượng máy
Xóa cache, temp files, nhưng giữ lại dữ liệu quan trọng
"""

import os
import shutil
from pathlib import Path
from typing import Tuple, List

def get_folder_size(path: Path) -> int:
    """Tính kích thước thư mục (bytes)"""
    total = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = Path(dirpath) / filename
                if filepath.exists():
                    total += filepath.stat().st_size
    except Exception as e:
        print(f"⚠️  Lỗi khi tính kích thước {path}: {e}")
    return total

def format_size(bytes_size: int) -> str:
    """Convert bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.2f} TB"

def remove_directory(path: Path) -> Tuple[bool, int]:
    """Xóa một thư mục và trả về thành công + kích thước"""
    try:
        if path.exists():
            size = get_folder_size(path)
            shutil.rmtree(path)
            return True, size
    except Exception as e:
        print(f"⚠️  Lỗi khi xóa {path}: {e}")
    return False, 0

def remove_files_pattern(directory: Path, pattern: str) -> Tuple[int, int]:
    """Xóa files match pattern, return (count, size)"""
    count = 0
    total_size = 0
    try:
        for filepath in directory.glob(pattern):
            if filepath.is_file():
                try:
                    size = filepath.stat().st_size
                    filepath.unlink()
                    count += 1
                    total_size += size
                except Exception as e:
                    print(f"⚠️  Lỗi khi xóa {filepath}: {e}")
    except Exception as e:
        print(f"⚠️  Lỗi khi tìm files {pattern}: {e}")
    return count, total_size

def clear_cache() -> dict:
    """Xóa tất cả cache files"""
    results = {
        '__pycache__': 0,
        '.pytest_cache': 0,
        '.ipynb_checkpoints': 0,
        'temp_files': 0,
        'total_files': 0,
    }
    
    root = Path('.')
    
    # Xóa __pycache__
    print("\n🗑️  Xóa __pycache__...")
    for pycache in root.rglob('__pycache__'):
        success, size = remove_directory(pycache)
        if success:
            results['__pycache__'] += size
            results['total_files'] += 1
            print(f"   ✅ {pycache.relative_to(root)} ({format_size(size)})")
    
    # Xóa .pytest_cache
    print("\n🗑️  Xóa .pytest_cache...")
    for pytest_cache in root.rglob('.pytest_cache'):
        success, size = remove_directory(pytest_cache)
        if success:
            results['.pytest_cache'] += size
            results['total_files'] += 1
            print(f"   ✅ {pytest_cache.relative_to(root)} ({format_size(size)})")
    
    # Xóa .ipynb_checkpoints
    print("\n🗑️  Xóa .ipynb_checkpoints...")
    for checkpoints in root.rglob('.ipynb_checkpoints'):
        success, size = remove_directory(checkpoints)
        if success:
            results['.ipynb_checkpoints'] += size
            results['total_files'] += 1
            print(f"   ✅ {checkpoints.relative_to(root)} ({format_size(size)})")
    
    # Xóa *.pyc files
    print("\n🗑️  Xóa *.pyc files...")
    count, size = 0, 0
    for pyc in root.rglob('*.pyc'):
        try:
            pyc.unlink()
            count += 1
            size += pyc.stat().st_size
        except Exception as e:
            print(f"   ⚠️  Lỗi: {e}")
    if count > 0:
        results['total_files'] += count
        results['temp_files'] += size
        print(f"   ✅ Xóa {count} files ({format_size(size)})")
    
    # Xóa *.pyo files
    print("\n🗑️  Xóa *.pyo files...")
    count, size = 0, 0
    for pyo in root.rglob('*.pyo'):
        try:
            pyo.unlink()
            count += 1
            size += pyo.stat().st_size
        except Exception as e:
            print(f"   ⚠️  Lỗi: {e}")
    if count > 0:
        results['total_files'] += count
        results['temp_files'] += size
        print(f"   ✅ Xóa {count} files ({format_size(size)})")
    
    # Xóa outputs/temp directory (nếu tồn tại)
    print("\n🗑️  Xóa outputs/temp...")
    temp_dir = Path('outputs/temp')
    if temp_dir.exists():
        success, size = remove_directory(temp_dir)
        if success:
            results['temp_files'] += size
            results['total_files'] += 1
            print(f"   ✅ {temp_dir} ({format_size(size)})")
    else:
        print("   ℹ️  Thư mục không tồn tại")
    
    # Optional: Xóa outputs/logs (comment out nếu muốn giữ)
    print("\n🗑️  Xóa outputs/logs/*.log...")
    log_dir = Path('outputs/logs')
    if log_dir.exists():
        count, size = 0, 0
        for log_file in log_dir.glob('*.log'):
            try:
                log_file.unlink()
                count += 1
                size += log_file.stat().st_size
            except Exception as e:
                print(f"   ⚠️  Lỗi: {e}")
        if count > 0:
            results['total_files'] += count
            results['temp_files'] += size
            print(f"   ✅ Xóa {count} log files ({format_size(size)})")
    
    return results

def show_important_files() -> None:
    """Hiển thị các file quan trọng được giữ lại"""
    print("\n\n📦 Các file/thư mục quan trọng được GIỮ LẠI:")
    print("=" * 60)
    
    important = [
        ('data/raw/BRFSS.csv', 'Dữ liệu gốc'),
        ('data/processed/selected_columns.csv', 'Dữ liệu đã xử lý'),
        ('outputs/metrics/metrics.json', 'Metrics'),
        ('outputs/metrics/spark_metrics.json', 'Spark metrics'),
        ('outputs/metrics/plots', 'Biểu đồ pipeline (PNG)'),
        ('bi_dashboard_clean.py', 'Dashboard app (Streamlit)'),
        ('bi_analysis_clean.py', 'BI console script'),
        ('bi_analysis.ipynb', 'BI notebook'),
        ('run_dashboard_clean.bat', 'Launch dashboard 1-click'),
        ('REPORT.md', 'Báo cáo đồ án'),
        ('pipeline_config.json', 'Cấu hình pipeline'),
    ]
    
    for file_path, description in important:
        path = Path(file_path)
        if path.exists():
            if path.is_file():
                size = path.stat().st_size
                size_str = format_size(size)
                print(f"✅ {file_path:<45} ({size_str:>10}) - {description}")
            else:
                size = get_folder_size(path)
                size_str = format_size(size)
                print(f"✅ {file_path:<45} ({size_str:>10}) - {description}")
        else:
            print(f"⚠️  {file_path:<45} (KHÔNG TÌM THẤY)")

def show_summary(results: dict) -> None:
    """Hiển thị tóm tắt dọn dẹp"""
    print("\n\n" + "=" * 60)
    print("📊 TÓM TẮT DỌNG DẸP")
    print("=" * 60)
    
    total_freed = (
        results['__pycache__'] + 
        results['.pytest_cache'] + 
        results['.ipynb_checkpoints'] + 
        results['temp_files']
    )
    
    print(f"\n🗑️  __pycache__:        {format_size(results['__pycache__']):>15}")
    print(f"🗑️  .pytest_cache:     {format_size(results['.pytest_cache']):>15}")
    print(f"🗑️  .ipynb_checkpoints: {format_size(results['.ipynb_checkpoints']):>15}")
    print(f"🗑️  Temp files:        {format_size(results['temp_files']):>15}")
    print("-" * 60)
    print(f"💾 TỔNG CỘNG GỌI PHÓNG: {format_size(total_freed):>15}")
    print(f"📝 Số items xóa:       {results['total_files']:>15}")
    print("=" * 60)
    
    if total_freed > 0:
        print(f"\n✅ Dọn dẹp thành công! Máy nhẹ hơn {format_size(total_freed)} 🎉")
    else:
        print("\nℹ️  Không có gì để dọn dẹp (máy đã sạch sẽ)")

def main():
    print("\n" + "=" * 60)
    print("🧹 CLEAR DATA - Dọn Dẹp Dữ Liệu & Giải Phóng Dung Lượng")
    print("=" * 60)
    
    print("\nℹ️  Các file sẽ được XÓA:")
    print("   • __pycache__/ folders")
    print("   • .pytest_cache/ folders")
    print("   • .ipynb_checkpoints/ folders")
    print("   • *.pyc, *.pyo files")
    print("   • outputs/temp/ folder")
    print("   • outputs/logs/*.log files")
    
    print("\nℹ️  Các file sẽ được GIỮ LẠI:")
    print("   • data/raw/BRFSS.csv")
    print("   • data/processed/selected_columns.csv")
    print("   • outputs/metrics/")
    print("   • Tất cả Python source files")
    
    # Chạy dọn dẹp
    results = clear_cache()
    
    # Hiển thị file quan trọng
    show_important_files()
    
    # Hiển thị tóm tắt
    show_summary(results)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
