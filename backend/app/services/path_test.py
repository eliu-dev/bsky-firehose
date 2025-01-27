# path_test.py
import sys
print("\nPython's module search paths:")
for path in sys.path:
    print(f"  {path}")

print("\nCurrent working directory:")
import os
print(f"  {os.getcwd()}")